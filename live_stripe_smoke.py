"""Controlled Stripe test-mode integration.

Creates an unconfirmed PaymentIntent only. No payment method is attached and
no confirmation/capture is attempted.

The fault model is a client crash after Stripe has returned a successful create
response but before that response is committed to the local journal. On restart,
recovery reuses the original idempotency key and must resolve to the same
PaymentIntent.
"""
from __future__ import annotations

import json
import os
import tempfile
import time
import uuid
from importlib.metadata import version
from pathlib import Path

from operation_journal import EffectState, OperationJournal, PaymentIntentBoundary
from stripe_gateway import StripeGateway


def main() -> None:
    secret = os.environ.get("STRIPE_TEST_SECRET_KEY", "")
    if not secret.startswith("sk_test_"):
        raise SystemExit("STRIPE_TEST_SECRET_KEY must be an sk_test_ key")

    operation_id = "live-stripe-" + uuid.uuid4().hex
    amount = 50
    currency = "usd"
    gateway = StripeGateway(secret)

    with tempfile.TemporaryDirectory() as directory:
        db_path = Path(directory) / "journal.sqlite3"

        first = OperationJournal(db_path)
        op = first.begin(amount=amount, currency=currency, operation_id=operation_id)
        owner = first.begin_dispatch(op.operation_id, lease_seconds=0)
        if not owner:
            raise AssertionError("initial dispatch claim failed")
        op = first.get(op.operation_id)

        first_response = gateway.create_payment_intent(
            amount=op.amount,
            currency=op.currency,
            idempotency_key=op.idempotency_key,
            metadata={"operation_id": op.operation_id},
        )
        first_id = first_response.get("id")
        if not isinstance(first_id, str) or not first_id.startswith("pi_"):
            raise AssertionError("Stripe did not return a PaymentIntent ID")

        # Simulate process loss before the successful provider response is
        # journaled. The durable row remains DISPATCHING; lease_seconds=0 makes
        # the owner immediately reclaimable on restart.
        first.db.close()
        time.sleep(0.01)

        second = OperationJournal(db_path)
        try:
            recovered = PaymentIntentBoundary(
                second,
                gateway,
                idempotency_window_seconds=86400,
                dispatch_lease_seconds=60,
            ).recover(operation_id)

            if recovered.state is not EffectState.INTENT_CREATED:
                raise AssertionError(f"unexpected recovery state: {recovered.state}")
            if recovered.provider_reference != first_id:
                raise AssertionError(
                    "idempotent recovery returned a different PaymentIntent"
                )

            retrieved = gateway.retrieve_payment_intent(first_id)
            if retrieved is None:
                raise AssertionError("created PaymentIntent could not be retrieved")
            metadata = retrieved.get("metadata")
            if not isinstance(metadata, dict):
                # StripeObject metadata is Mapping-like but dict() normally
                # yields a plain dict. Normalize defensively for evidence.
                metadata = dict(metadata or {})

            checks = {
                "sdk_version": version("stripe"),
                "operation_id": operation_id,
                "payment_intent_id": first_id,
                "same_id_after_recovery": recovered.provider_reference == first_id,
                "journal_state": recovered.state.value,
                "amount_matches": retrieved.get("amount") == amount,
                "currency_matches": str(retrieved.get("currency", "")).lower() == currency,
                "operation_metadata_matches": metadata.get("operation_id") == operation_id,
                "livemode": bool(retrieved.get("livemode")),
                "payment_confirmation_attempted": False,
            }

            if checks["livemode"]:
                raise AssertionError("test-mode key unexpectedly returned livemode=true")
            for key in (
                "same_id_after_recovery",
                "amount_matches",
                "currency_matches",
                "operation_metadata_matches",
            ):
                if not checks[key]:
                    raise AssertionError(f"failed provider check: {key}")

            print(json.dumps(checks, sort_keys=True))

            # Best-effort cleanup of the unconfirmed test object. Failure here
            # does not change the evidence above and never confirms a payment.
            try:
                gateway.stripe.v1.payment_intents.cancel(first_id)
            except Exception as exc:
                print(json.dumps({"cleanup_warning": type(exc).__name__}))
        finally:
            second.db.close()


if __name__ == "__main__":
    main()
