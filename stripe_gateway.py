"""Optional real Stripe test-mode adapter. Requires `pip install stripe` and STRIPE_TEST_SECRET_KEY."""
from __future__ import annotations
from typing import Any, Mapping
from operation_journal import LostResponse, ProviderUnavailable

class StripeGateway:
    def __init__(self, secret_key: str) -> None:
        if not secret_key.startswith("sk_test_"):
            raise ValueError("Only an sk_test_ key is accepted by this reference adapter")
        import stripe  # optional dependency
        self.stripe = stripe.StripeClient(secret_key)
    def create_payment_intent(self, *, amount: int, currency: str, idempotency_key: str, metadata: Mapping[str, str]) -> Mapping[str, Any]:
        try:
            item = self.stripe.v1.payment_intents.create({"amount": amount, "currency": currency, "metadata": dict(metadata)}, {"idempotency_key": idempotency_key})
            return dict(item)
        except Exception as exc:
            # Network uncertainty is deliberately represented as unknown, never non-commit.
            raise LostResponse(str(exc)) from exc
    def retrieve_payment_intent(self, provider_reference: str) -> Mapping[str, Any] | None:
        try:
            return dict(self.stripe.v1.payment_intents.retrieve(provider_reference))
        except Exception as exc:
            raise ProviderUnavailable(str(exc)) from exc
