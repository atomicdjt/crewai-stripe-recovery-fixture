"""Deterministic contract tests for the optional Stripe adapter."""
from __future__ import annotations

import importlib
import sys
import types
import unittest

from operation_journal import LostResponse, ProviderUnavailable


class _FakePaymentIntents:
    def __init__(self):
        self.created = []
        self.retrieved = []

    def create(self, params, options=None):
        self.created.append((params, options))
        return {
            "id": "pi_fake",
            "amount": params["amount"],
            "currency": params["currency"],
            "metadata": params["metadata"],
            "livemode": False,
        }

    def retrieve(self, provider_reference):
        self.retrieved.append(provider_reference)
        return {
            "id": provider_reference,
            "amount": 500,
            "currency": "usd",
            "metadata": {"operation_id": "op"},
            "livemode": False,
        }


class _FakeStripeClient:
    instances = []

    def __init__(self, secret_key, **kwargs):
        self.secret_key = secret_key
        self.kwargs = kwargs
        self.v1 = types.SimpleNamespace(payment_intents=_FakePaymentIntents())
        self.__class__.instances.append(self)


class StripeGatewayContractTests(unittest.TestCase):
    def setUp(self):
        _FakeStripeClient.instances.clear()
        self.fake_stripe = types.SimpleNamespace(StripeClient=_FakeStripeClient)
        self.old = sys.modules.get("stripe")
        sys.modules["stripe"] = self.fake_stripe
        sys.modules.pop("stripe_gateway", None)

    def tearDown(self):
        sys.modules.pop("stripe_gateway", None)
        if self.old is None:
            sys.modules.pop("stripe", None)
        else:
            sys.modules["stripe"] = self.old

    def load_gateway(self):
        return importlib.import_module("stripe_gateway").StripeGateway

    def test_rejects_non_test_secret(self):
        Gateway = self.load_gateway()
        with self.assertRaisesRegex(ValueError, "sk_test_"):
            Gateway("sk_live_forbidden")

    def test_create_passes_exact_idempotency_key_and_disables_sdk_retries(self):
        Gateway = self.load_gateway()
        gateway = Gateway("sk_test_example")
        client = _FakeStripeClient.instances[-1]
        self.assertEqual(client.kwargs.get("max_network_retries"), 0)

        result = gateway.create_payment_intent(
            amount=500,
            currency="usd",
            idempotency_key="idem-123",
            metadata={"operation_id": "op"},
        )
        self.assertEqual(result["id"], "pi_fake")
        params, options = client.v1.payment_intents.created[-1]
        self.assertEqual(params["amount"], 500)
        self.assertEqual(params["currency"], "usd")
        self.assertEqual(params["metadata"], {"operation_id": "op"})
        self.assertEqual(options, {"idempotency_key": "idem-123"})

    def test_retrieve_uses_exact_provider_reference(self):
        Gateway = self.load_gateway()
        gateway = Gateway("sk_test_example")
        result = gateway.retrieve_payment_intent("pi_exact")
        client = _FakeStripeClient.instances[-1]
        self.assertEqual(client.v1.payment_intents.retrieved, ["pi_exact"])
        self.assertEqual(result["id"], "pi_exact")

    def test_create_exception_is_ambiguous_not_noncommit(self):
        class Broken:
            def create(self, *args, **kwargs):
                raise RuntimeError("network ambiguity")
            def retrieve(self, *args, **kwargs):
                raise RuntimeError("lookup unavailable")

        Gateway = self.load_gateway()
        gateway = Gateway("sk_test_example")
        gateway.stripe.v1.payment_intents = Broken()

        with self.assertRaises(LostResponse):
            gateway.create_payment_intent(
                amount=500,
                currency="usd",
                idempotency_key="idem",
                metadata={"operation_id": "op"},
            )
        with self.assertRaises(ProviderUnavailable):
            gateway.retrieve_payment_intent("pi_unknown")


if __name__ == "__main__":
    unittest.main()
