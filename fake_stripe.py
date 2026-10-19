"""Fault-injectable Stripe-test-mode behavioural double for deterministic tests."""
from __future__ import annotations
from typing import Any, Mapping
from operation_journal import LostResponse, ProviderUnavailable

class SimulatedProcessCrash(BaseException):
    """Crash after the provider side effect but before the caller records it."""

class FakeStripeTestMode:
    def __init__(self) -> None:
        self.intents: dict[str, dict[str, Any]] = {}
        self.by_key: dict[str, str] = {}
        self.retrieved_references: list[str] = []
        self.create_calls = 0
        self.lose_next_create_response = False
        self.crash_after_next_create = False
        self.fail_next_retrieve = False
        self.hide_retrieval = False
    def create_payment_intent(self, *, amount: int, currency: str, idempotency_key: str, metadata: Mapping[str, str]) -> Mapping[str, Any]:
        self.create_calls += 1
        if idempotency_key not in self.by_key:
            ref = f"pi_test_{len(self.intents) + 1:06d}"
            self.by_key[idempotency_key] = ref
            self.intents[ref] = {"id": ref, "amount": amount, "currency": currency, "metadata": dict(metadata), "livemode": False}
        response = self.intents[self.by_key[idempotency_key]]
        if self.crash_after_next_create:
            self.crash_after_next_create = False
            raise SimulatedProcessCrash("simulated process crash after provider accepted request")
        if self.lose_next_create_response:
            self.lose_next_create_response = False
            raise LostResponse("simulated response loss after provider commit")
        return dict(response)
    def retrieve_payment_intent(self, provider_reference: str) -> Mapping[str, Any] | None:
        self.retrieved_references.append(provider_reference)
        if self.fail_next_retrieve:
            self.fail_next_retrieve = False
            raise ProviderUnavailable("simulated provider 5xx")
        if self.hide_retrieval:
            return None
        item = self.intents.get(provider_reference)
        return dict(item) if item else None
