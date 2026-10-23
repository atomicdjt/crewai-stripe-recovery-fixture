"""Deterministic two-connection recovery, fencing, and projection regressions.

All providers are behavioral doubles. No real Stripe API is exercised.
"""
import sqlite3
import tempfile
import threading
import time
import unittest
from pathlib import Path

from fake_stripe import FakeStripeTestMode, SimulatedProcessCrash
from operation_journal import (
    EffectState, LostResponse, OperationJournal, PaymentIntentBoundary,
)


class BlockingRetryGateway:
    def __init__(self):
        self.lock = threading.Lock()
        self.calls = []
        self.retry_entered = threading.Event()
        self.release_retry = threading.Event()

    def create_payment_intent(self, *, amount, currency, idempotency_key, metadata):
        with self.lock:
            self.calls.append(idempotency_key)
            number = len(self.calls)
        if number == 1:
            raise LostResponse("initial ambiguous response")
        self.retry_entered.set()
        if not self.release_retry.wait(timeout=5):
            raise AssertionError("timed out waiting to release retry")
        return {"id": "pi_mock_one", "amount": amount, "currency": currency,
                "metadata": dict(metadata), "status": "requires_payment_method"}

    def retrieve_payment_intent(self, provider_reference):
        return None


class ConcurrentRecoveryFencingTests(unittest.TestCase):
    def test_second_worker_does_not_dispatch_during_active_retry(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "journal.sqlite3"
            gateway = BlockingRetryGateway()
            initial_journal = OperationJournal(path)
            original = PaymentIntentBoundary(initial_journal, gateway).create(
                amount=500, currency="usd", operation_id="two-workers"
            )
            self.assertEqual(original.state, EffectState.UNKNOWN)
            initial_journal.db.close()

            results, errors = [], []

            def worker():
                journal = OperationJournal(path)
                try:
                    results.append(PaymentIntentBoundary(
                        journal, gateway, dispatch_lease_seconds=60
                    ).recover(original.operation_id))
                except BaseException as exc:
                    errors.append(exc)
                finally:
                    journal.db.close()

            first = threading.Thread(target=worker)
            second = threading.Thread(target=worker)
            first.start()
            try:
                self.assertTrue(gateway.retry_entered.wait(timeout=5))
                second.start()
                second.join(timeout=5)
                self.assertFalse(second.is_alive(), "other recovery worker hung")
                with gateway.lock:
                    # Initial request + exactly one in-flight retry.
                    self.assertEqual(len(gateway.calls), 2)
                    self.assertEqual(len(set(gateway.calls)), 1)
            finally:
                gateway.release_retry.set()
                first.join(timeout=5)
                if second.is_alive():
                    second.join(timeout=5)
            self.assertFalse(first.is_alive())
            self.assertFalse(errors, repr(errors))
            journal = OperationJournal(path)
            try:
                self.assertEqual(journal.get(original.operation_id).state,
                                 EffectState.INTENT_CREATED)
            finally:
                journal.db.close()

    def test_stale_unknown_is_evidence_only_after_trusted_positive(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "journal.sqlite3"
            first = OperationJournal(path)
            second = OperationJournal(path)
            try:
                op = first.begin(amount=500, currency="usd",
                                 operation_id="stale-positive")
                token = first.begin_dispatch(op.operation_id)
                self.assertTrue(token)
                confirmed = first.observe(
                    op.operation_id, state=EffectState.INTENT_CREATED,
                    lookup_method="create", authority="provider",
                    provider_reference="pi_trusted",
                    raw={"id": "pi_trusted", "amount": 500, "currency": "usd",
                         "metadata": {"operation_id": op.operation_id}},
                    promote_provider_reference=True, owner_token=token)
                self.assertEqual(confirmed.state, EffectState.INTENT_CREATED)

                stale = second.observe(
                    op.operation_id, state=EffectState.UNKNOWN,
                    lookup_method="retry_response_lost", authority="application",
                    provider_reference=None, raw={"error": "late response"},
                    owner_token=token)
                self.assertEqual(stale.state, EffectState.INTENT_CREATED)
                self.assertEqual(stale.provider_reference, "pi_trusted")
                evidence = list(second.db.execute(
                    "SELECT state FROM evidence WHERE operation_id=? ORDER BY evidence_id",
                    (op.operation_id,)))
                self.assertEqual([row["state"] for row in evidence][-2:],
                                 ["INTENT_CREATED", "UNKNOWN"])
            finally:
                first.db.close()
                second.db.close()

    def test_expired_lease_reclaims_with_new_fencing_token(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "journal.sqlite3"
            first = OperationJournal(path)
            second = OperationJournal(path)
            try:
                op = first.begin(amount=500, currency="usd",
                                 operation_id="fence-generation")
                old_token = first.begin_dispatch(op.operation_id, lease_seconds=0)
                self.assertTrue(old_token)
                time.sleep(0.005)
                new_token = second.claim_retry_dispatch(
                    op.operation_id, idempotency_window_seconds=86400,
                    lease_seconds=60)
                self.assertTrue(new_token)
                self.assertNotEqual(old_token, new_token)
                stale = first.observe(
                    op.operation_id, state=EffectState.UNKNOWN,
                    lookup_method="old_lost_response", authority="application",
                    provider_reference=None, raw={"error": "late"},
                    owner_token=old_token)
                self.assertEqual(stale.state, EffectState.DISPATCHING)
                row = second.db.execute(
                    "SELECT dispatch_owner_token, dispatch_generation "
                    "FROM operations WHERE operation_id=?",
                    (op.operation_id,)).fetchone()
                self.assertEqual(row["dispatch_owner_token"], new_token)
                self.assertEqual(row["dispatch_generation"], 2)
                second.observe(
                    op.operation_id, state=EffectState.UNKNOWN,
                    lookup_method="new_lost_response", authority="application",
                    provider_reference=None, raw={"error": "new ambiguous"},
                    owner_token=new_token)
                self.assertEqual(second.get(op.operation_id).state, EffectState.UNKNOWN)
            finally:
                first.db.close()
                second.db.close()

    def test_crash_recovery_after_lease_expiry_preserves_key(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "journal.sqlite3"
            gateway = FakeStripeTestMode()
            first = OperationJournal(path)
            gateway.crash_after_next_create = True
            with self.assertRaises(SimulatedProcessCrash):
                PaymentIntentBoundary(first, gateway,
                    dispatch_lease_seconds=0).create(
                    amount=500, operation_id="crash-reclaim")
            first.db.close()
            time.sleep(0.005)
            second = OperationJournal(path)
            try:
                recovered = PaymentIntentBoundary(second, gateway).recover(
                    "crash-reclaim")
                self.assertEqual(recovered.state, EffectState.INTENT_CREATED)
                self.assertEqual(gateway.create_calls, 2)
                self.assertEqual(len(gateway.intents), 1)
            finally:
                second.db.close()

    def test_migrated_ownerless_inflight_is_quarantined(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "journal.sqlite3"
            db = sqlite3.connect(path)
            db.executescript("""
            CREATE TABLE operations (
              operation_id TEXT PRIMARY KEY, payload_digest TEXT NOT NULL,
              amount INTEGER NOT NULL, currency TEXT NOT NULL, state TEXT NOT NULL,
              idempotency_key TEXT NOT NULL UNIQUE, provider_reference TEXT,
              created_at REAL NOT NULL, dispatch_started_at REAL
            );
            INSERT INTO operations VALUES (
              'ownerless', 'old-digest', 500, 'usd', 'DISPATCHING',
              'old-key', NULL, 0, 1
            );
            """)
            now = time.time()
            db.execute(
                "UPDATE operations SET created_at=?, dispatch_started_at=? "
                "WHERE operation_id='ownerless'", (now, now))
            db.execute(
                "INSERT INTO operations VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                ("legacy-unknown", "old-digest", 500, "usd", "UNKNOWN",
                 "unknown-key", None, now, now))
            db.commit()
            db.close()
            journal = OperationJournal(path)
            gateway = FakeStripeTestMode()
            try:
                self.assertEqual(journal.get("ownerless").state, EffectState.UNKNOWN)
                row = journal.db.execute(
                    "SELECT dispatch_owner_token, dispatch_lease_expires_at "
                    "FROM operations WHERE operation_id='ownerless'").fetchone()
                self.assertEqual(row["dispatch_owner_token"], "legacy-unfenced")
                self.assertEqual(row["dispatch_lease_expires_at"], 1e30)
                self.assertEqual(PaymentIntentBoundary(journal, gateway).recover(
                    "ownerless").state, EffectState.UNKNOWN)
                self.assertEqual(gateway.create_calls, 0)
                # The migration must not quarantine unrelated old UNKNOWN rows.
                self.assertIsNone(journal.db.execute(
                    "SELECT dispatch_lease_expires_at FROM operations "
                    "WHERE operation_id='legacy-unknown'").fetchone()[0])
                recovered = PaymentIntentBoundary(journal, gateway).recover(
                    "legacy-unknown")
                self.assertEqual(recovered.state, EffectState.INTENT_CREATED)
                self.assertEqual(gateway.create_calls, 1)
            finally:
                journal.db.close()


if __name__ == "__main__":
    unittest.main()
