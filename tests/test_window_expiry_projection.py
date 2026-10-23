"""Regression: expired provider key window must not hide an active dispatch owner.

Two independent SQLite connections, no external provider or payment requests.
"""
import tempfile
import time
import unittest
from pathlib import Path

from operation_journal import EffectState, OperationJournal, PaymentIntentBoundary


class NeverCallProvider:
    def create_payment_intent(self, **kwargs):
        raise AssertionError("expired provider key must not trigger a create")

    def retrieve_payment_intent(self, provider_reference):
        raise AssertionError("no correlated provider reference exists")


class WindowExpiryProjectionTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = Path(self.tmp.name) / "journal.sqlite3"
        self.a = OperationJournal(self.path)
        self.b = OperationJournal(self.path)
        self.a.begin(amount=500, currency="usd", operation_id="window-owner")

    def tearDown(self):
        self.a.db.close()
        self.b.db.close()
        self.tmp.cleanup()

    def recover_with_expired_key(self):
        return PaymentIntentBoundary(
            self.b, NeverCallProvider(),
            idempotency_window_seconds=0
        ).recover("window-owner")

    def test_live_owner_remains_dispatching_when_other_worker_refuses_expired_key(self):
        token = self.a.begin_dispatch("window-owner", lease_seconds=300)
        self.assertIsNotNone(token)
        before = self.a.db.execute(
            "SELECT dispatch_lease_expires_at FROM operations "
            "WHERE operation_id='window-owner'"
        ).fetchone()[0]
        self.assertGreater(before, time.time())

        result = self.recover_with_expired_key()
        self.assertEqual(result.state, EffectState.DISPATCHING)
        row = self.a.db.execute(
            "SELECT state, dispatch_owner_token, dispatch_lease_expires_at "
            "FROM operations WHERE operation_id='window-owner'"
        ).fetchone()
        self.assertEqual(row["state"], EffectState.DISPATCHING.value)
        self.assertEqual(row["dispatch_owner_token"], token)
        self.assertEqual(row["dispatch_lease_expires_at"], before)
        self.assertEqual(self.a.db.execute(
            "SELECT lookup_method FROM evidence WHERE operation_id=? "
            "ORDER BY evidence_id DESC LIMIT 1", ("window-owner",)
        ).fetchone()[0], "retry_refused_window_expired")

        # Only the real owner can resolve the current dispatch.
        resolved = self.a.observe(
            "window-owner", state=EffectState.UNKNOWN,
            lookup_method="owner_response_lost", authority="application",
            provider_reference=None, owner_token=token, raw={"error": "lost"}
        )
        self.assertEqual(resolved.state, EffectState.UNKNOWN)
        row = self.a.db.execute(
            "SELECT dispatch_owner_token FROM operations WHERE operation_id=?",
            ("window-owner",)
        ).fetchone()
        self.assertIsNone(row[0])

    def test_expired_owner_can_be_projected_unknown_without_any_provider_retry(self):
        token = self.a.begin_dispatch("window-owner", lease_seconds=0)
        self.assertIsNotNone(token)
        time.sleep(0.005)
        result = self.recover_with_expired_key()
        self.assertEqual(result.state, EffectState.UNKNOWN)
        row = self.a.db.execute(
            "SELECT dispatch_owner_token, dispatch_lease_expires_at "
            "FROM operations WHERE operation_id='window-owner'"
        ).fetchone()
        self.assertIsNone(row["dispatch_owner_token"])
        self.assertIsNone(row["dispatch_lease_expires_at"])
        self.assertIsNone(self.b.claim_retry_dispatch(
            "window-owner", idempotency_window_seconds=0
        ))
        # Late trusted positive remains admissible after the lease expires.
        positive = self.a.observe(
            "window-owner", state=EffectState.INTENT_CREATED,
            lookup_method="late_correlated_provider_response",
            authority="provider", provider_reference="pi_after_expiry",
            promote_provider_reference=True,
            raw={"id": "pi_after_expiry", "amount": 500, "currency": "usd",
                 "metadata": {"operation_id": "window-owner"}},
            owner_token=token
        )
        self.assertEqual(positive.state, EffectState.INTENT_CREATED)


if __name__ == "__main__":
    unittest.main()
