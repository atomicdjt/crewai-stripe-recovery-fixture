"""Evidence-contract regressions discovered during final independent review."""
from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from operation_journal import EffectState, OperationJournal, PaymentIntentBoundary


class StaticGateway:
    def __init__(self, response):
        self.response = response
        self.create_calls = 0

    def create_payment_intent(self, **kwargs):
        self.create_calls += 1
        return self.response

    def retrieve_payment_intent(self, provider_reference):
        return self.response


class EvidenceContractTests(unittest.TestCase):
    def test_generic_observer_cannot_assert_not_committed(self):
        with tempfile.TemporaryDirectory() as directory:
            journal = OperationJournal(Path(directory) / "journal.sqlite3")
            try:
                op = journal.begin(amount=500, currency="usd", operation_id="no-false-negative")
                with self.assertRaisesRegex(ValueError, "provider-authoritative"):
                    journal.observe(
                        op.operation_id,
                        state=EffectState.NOT_COMMITTED,
                        lookup_method="operator_guess",
                        authority="application",
                        provider_reference=None,
                        raw={"reason": "insufficient evidence"},
                    )
                self.assertEqual(journal.get(op.operation_id).state, EffectState.PENDING)
                evidence_count = journal.db.execute(
                    "SELECT COUNT(*) FROM evidence WHERE operation_id=?",
                    (op.operation_id,),
                ).fetchone()[0]
                self.assertEqual(evidence_count, 0)
            finally:
                journal.db.close()

    def test_malformed_metadata_is_persisted_as_unknown(self):
        with tempfile.TemporaryDirectory() as directory:
            journal = OperationJournal(Path(directory) / "journal.sqlite3")
            try:
                gateway = StaticGateway(
                    {"id": "pi_malformed", "amount": 500, "currency": "usd", "metadata": None}
                )
                result = PaymentIntentBoundary(journal, gateway).create(
                    amount=500, currency="usd", operation_id="malformed-metadata"
                )
                self.assertEqual(result.state, EffectState.UNKNOWN)
                self.assertIsNone(result.provider_reference)
                self.assertEqual(gateway.create_calls, 1)
                row = journal.db.execute(
                    "SELECT dispatch_owner_token, dispatch_lease_expires_at "
                    "FROM operations WHERE operation_id=?",
                    (result.operation_id,),
                ).fetchone()
                self.assertIsNone(row["dispatch_owner_token"])
                self.assertIsNone(row["dispatch_lease_expires_at"])
            finally:
                journal.db.close()

    def test_non_string_provider_reference_is_not_promoted(self):
        with tempfile.TemporaryDirectory() as directory:
            journal = OperationJournal(Path(directory) / "journal.sqlite3")
            try:
                gateway = StaticGateway(
                    {
                        "id": {"unexpected": "shape"},
                        "amount": 500,
                        "currency": "usd",
                        "metadata": {"operation_id": "bad-provider-id"},
                    }
                )
                result = PaymentIntentBoundary(journal, gateway).create(
                    amount=500, currency="usd", operation_id="bad-provider-id"
                )
                self.assertEqual(result.state, EffectState.UNKNOWN)
                self.assertIsNone(result.provider_reference)
                self.assertEqual(gateway.create_calls, 1)
            finally:
                journal.db.close()


if __name__ == "__main__":
    unittest.main()
