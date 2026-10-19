import os
import sqlite3
import tempfile
import time
import unittest
from fake_stripe import FakeStripeTestMode, SimulatedProcessCrash
from operation_journal import CrewAIPaymentIntentTool, EffectState, OperationJournal, PaymentIntentBoundary

class RecoveryTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.NamedTemporaryFile(delete=False); self.tmp.close()
        self.journal = OperationJournal(self.tmp.name)
        self.gateway = FakeStripeTestMode()
        self.boundary = PaymentIntentBoundary(self.journal, self.gateway)
    def tearDown(self):
        self.journal.db.close(); os.unlink(self.tmp.name)
    def test_lost_create_response_is_unknown_not_not_committed(self):
        self.gateway.lose_next_create_response = True
        op = self.boundary.create(amount=500, operation_id="lost")
        self.assertEqual(op.state, EffectState.UNKNOWN)
        self.assertEqual(len(self.gateway.intents), 1)
    def test_retry_after_lost_response_reuses_same_idempotency_key(self):
        self.gateway.lose_next_create_response = True
        self.boundary.create(amount=500, operation_id="retry")
        op = self.boundary.recover("retry")
        self.assertEqual(op.state, EffectState.INTENT_CREATED)
        self.assertEqual(len(self.gateway.intents), 1)
    def test_provider_reference_lookup_recovers_after_restart(self):
        op = self.boundary.create(amount=500, operation_id="lookup")
        self.journal.observe("lookup", state=EffectState.UNKNOWN, lookup_method="crash_after_reference", authority="application", provider_reference=op.provider_reference, raw={})
        self.assertEqual(self.boundary.recover("lookup").state, EffectState.INTENT_CREATED)
    def test_ambiguous_retrieval_remains_unknown(self):
        op = self.boundary.create(amount=500, operation_id="ambiguous")
        self.journal.observe("ambiguous", state=EffectState.UNKNOWN, lookup_method="lost", authority="application", provider_reference=op.provider_reference, raw={})
        self.gateway.fail_next_retrieve = True
        self.assertEqual(self.boundary.recover("ambiguous").state, EffectState.UNKNOWN)
    def test_mismatched_provider_evidence_cannot_commit(self):
        self.gateway.lose_next_create_response = True
        self.boundary.create(amount=500, operation_id="mismatch")
        ref = next(iter(self.gateway.intents)); self.gateway.intents[ref]["metadata"] = {"operation_id": "other"}
        op = self.boundary.recover("mismatch")
        self.assertEqual(op.state, EffectState.UNKNOWN)
        self.assertIsNone(op.provider_reference)

    def test_mismatched_provider_id_is_evidence_only_after_restart(self):
        self.gateway.lose_next_create_response = True
        self.boundary.create(amount=500, operation_id="mismatch-restart")
        ref = next(iter(self.gateway.intents))
        self.gateway.intents[ref]["metadata"] = {"operation_id": "another-operation"}

        op = self.boundary.recover("mismatch-restart")
        self.assertEqual(op.state, EffectState.UNKNOWN)
        self.assertIsNone(op.provider_reference)
        evidence_ref = self.journal.db.execute(
            "SELECT provider_reference FROM evidence WHERE operation_id=? ORDER BY evidence_id DESC LIMIT 1",
            ("mismatch-restart",),
        ).fetchone()[0]
        self.assertEqual(evidence_ref, ref)  # Candidate retained, but not trusted.

        self.journal.db.close()
        self.journal = OperationJournal(self.tmp.name)
        self.boundary = PaymentIntentBoundary(self.journal, self.gateway)
        self.assertIsNone(self.journal.get("mismatch-restart").provider_reference)
        self.assertEqual(self.boundary.recover("mismatch-restart").state, EffectState.UNKNOWN)
        self.assertNotIn(ref, self.gateway.retrieved_references)
    def test_expired_idempotency_window_refuses_create_retry(self):
        short = PaymentIntentBoundary(self.journal, self.gateway, idempotency_window_seconds=0)
        self.gateway.lose_next_create_response = True
        short.create(amount=500, operation_id="expired")
        time.sleep(0.01)
        self.assertEqual(short.recover("expired").state, EffectState.UNKNOWN)
        self.assertEqual(len(self.gateway.intents), 1)

    def test_post_dispatch_crash_after_window_is_not_replayed(self):
        self.gateway.crash_after_next_create = True
        with self.assertRaises(SimulatedProcessCrash):
            self.boundary.create(amount=500, operation_id="crash-after-dispatch")

        op = self.journal.get("crash-after-dispatch")
        self.assertEqual(op.state, EffectState.DISPATCHING)
        self.assertIsNotNone(op.dispatch_started_at)
        self.assertEqual(self.gateway.create_calls, 1)

        # Simulate process downtime beyond the configured provider retention window.
        self.journal.db.execute(
            "UPDATE operations SET dispatch_started_at=? WHERE operation_id=?",
            (time.time() - 86401, "crash-after-dispatch"),
        )
        self.journal.db.commit()
        self.journal.db.close()
        self.journal = OperationJournal(self.tmp.name)
        self.boundary = PaymentIntentBoundary(
            self.journal, self.gateway, idempotency_window_seconds=86400,
        )

        recovered = self.boundary.create(amount=500, operation_id="crash-after-dispatch")
        self.assertEqual(recovered.state, EffectState.UNKNOWN)
        self.assertEqual(self.gateway.create_calls, 1)
        self.assertEqual(len(self.gateway.intents), 1)
        last_evidence = self.journal.db.execute(
            "SELECT lookup_method FROM evidence WHERE operation_id=? ORDER BY evidence_id DESC LIMIT 1",
            ("crash-after-dispatch",),
        ).fetchone()[0]
        self.assertEqual(last_evidence, "retry_refused_window_expired")

    def test_upgrade_fences_legacy_pending_rows_as_ambiguous(self):
        legacy_file = tempfile.NamedTemporaryFile(delete=False)
        legacy_file.close()
        try:
            db = sqlite3.connect(legacy_file.name)
            db.executescript("""
            CREATE TABLE operations (
              operation_id TEXT PRIMARY KEY, payload_digest TEXT NOT NULL,
              amount INTEGER NOT NULL, currency TEXT NOT NULL, state TEXT NOT NULL,
              idempotency_key TEXT NOT NULL UNIQUE, provider_reference TEXT,
              created_at REAL NOT NULL
            );
            CREATE TABLE evidence (
              evidence_id INTEGER PRIMARY KEY AUTOINCREMENT, operation_id TEXT NOT NULL,
              observed_at REAL NOT NULL, lookup_method TEXT NOT NULL,
              authority TEXT NOT NULL, provider_reference TEXT, state TEXT NOT NULL,
              raw_json TEXT NOT NULL
            );
            INSERT INTO operations VALUES
              ('legacy-pending', 'digest', 500, 'usd', 'PENDING', 'legacy-key', NULL, 1.0);
            """)
            db.commit()
            db.close()

            legacy_journal = OperationJournal(legacy_file.name)
            legacy_op = legacy_journal.get("legacy-pending")
            self.assertEqual(legacy_op.state, EffectState.UNKNOWN)
            self.assertEqual(legacy_op.dispatch_started_at, 1.0)
            legacy_gateway = FakeStripeTestMode()
            legacy_boundary = PaymentIntentBoundary(legacy_journal, legacy_gateway)
            self.assertEqual(legacy_boundary.recover("legacy-pending").state, EffectState.UNKNOWN)
            self.assertEqual(legacy_gateway.create_calls, 0)
            legacy_journal.db.close()
        finally:
            os.unlink(legacy_file.name)

    def test_custom_tool_exposes_claim_safe_result(self):
        tool = CrewAIPaymentIntentTool(self.boundary)
        self.gateway.lose_next_create_response = True
        out = tool._run(500, operation_id="tool")
        self.assertEqual(out["intent_state"], "UNKNOWN")
        self.assertFalse(out["payment_confirmation_attempted"])

if __name__ == "__main__": unittest.main()
