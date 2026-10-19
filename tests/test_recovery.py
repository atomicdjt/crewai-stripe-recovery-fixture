import os
import tempfile
import time
import unittest
from fake_stripe import FakeStripeTestMode
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
    def test_custom_tool_exposes_claim_safe_result(self):
        tool = CrewAIPaymentIntentTool(self.boundary)
        self.gateway.lose_next_create_response = True
        out = tool._run(500, operation_id="tool")
        self.assertEqual(out["intent_state"], "UNKNOWN")
        self.assertFalse(out["payment_confirmation_attempted"])

if __name__ == "__main__": unittest.main()
