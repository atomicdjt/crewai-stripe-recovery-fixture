"""Durable operation journal and a claim-bounded Stripe/CrewAI recovery boundary.

The important property is epistemic, not merely mechanical: ambiguous provider
outcomes remain UNKNOWN until backed by provider evidence.  This module is
dependency-free; production callers can use StripeGateway below or supply an
adapter with the same two methods.
"""
from __future__ import annotations

import hashlib
import json
import sqlite3
import time
import uuid
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Any, Mapping, Protocol


class EffectState(str, Enum):
    PENDING = "PENDING"
    UNKNOWN = "UNKNOWN"
    COMMITTED = "COMMITTED"
    NOT_COMMITTED = "NOT_COMMITTED"


class LostResponse(RuntimeError):
    """The request may have reached the provider but its result is unavailable."""


class ProviderUnavailable(RuntimeError):
    """A provider lookup was unavailable; this is not evidence of non-commit."""


@dataclass(frozen=True)
class Operation:
    operation_id: str
    payload_digest: str
    amount: int
    currency: str
    state: EffectState
    idempotency_key: str
    provider_reference: str | None
    created_at: float


class PaymentGateway(Protocol):
    def create_payment_intent(self, *, amount: int, currency: str, idempotency_key: str,
                              metadata: Mapping[str, str]) -> Mapping[str, Any]: ...
    def retrieve_payment_intent(self, provider_reference: str) -> Mapping[str, Any] | None: ...


def _canonical_digest(amount: int, currency: str) -> str:
    raw = json.dumps({"amount": amount, "currency": currency.lower()}, sort_keys=True,
                     separators=(",", ":"))
    return hashlib.sha256(raw.encode()).hexdigest()


def _idempotency_key(operation_id: str) -> str:
    return "crewai-op-" + hashlib.sha256(operation_id.encode()).hexdigest()[:48]


class OperationJournal:
    """SQLite journal with append-only provider evidence.

    The current operation row is a projection; evidence rows are never updated
    or deleted by this implementation.
    """
    def __init__(self, filename: str | Path) -> None:
        self.db = sqlite3.connect(str(filename))
        self.db.row_factory = sqlite3.Row
        self.db.executescript("""
        PRAGMA journal_mode=WAL;
        CREATE TABLE IF NOT EXISTS operations (
          operation_id TEXT PRIMARY KEY, payload_digest TEXT NOT NULL,
          amount INTEGER NOT NULL, currency TEXT NOT NULL, state TEXT NOT NULL,
          idempotency_key TEXT NOT NULL UNIQUE, provider_reference TEXT,
          created_at REAL NOT NULL
        );
        CREATE TABLE IF NOT EXISTS evidence (
          evidence_id INTEGER PRIMARY KEY AUTOINCREMENT, operation_id TEXT NOT NULL,
          observed_at REAL NOT NULL, lookup_method TEXT NOT NULL,
          authority TEXT NOT NULL, provider_reference TEXT, state TEXT NOT NULL,
          raw_json TEXT NOT NULL, FOREIGN KEY(operation_id) REFERENCES operations(operation_id)
        );
        """)

    def begin(self, *, amount: int, currency: str, operation_id: str | None = None) -> Operation:
        if amount <= 0:
            raise ValueError("amount must be positive")
        operation_id = operation_id or str(uuid.uuid4())
        digest = _canonical_digest(amount, currency)
        old = self.db.execute("SELECT * FROM operations WHERE operation_id=?", (operation_id,)).fetchone()
        if old:
            if old["payload_digest"] != digest:
                raise ValueError("operation_id cannot be reused with a different payload")
            return self._operation(old)
        now = time.time()
        self.db.execute("INSERT INTO operations VALUES (?, ?, ?, ?, ?, ?, NULL, ?)",
                        (operation_id, digest, amount, currency.lower(), EffectState.PENDING.value,
                         _idempotency_key(operation_id), now))
        self.db.commit()
        return self.get(operation_id)

    def get(self, operation_id: str) -> Operation:
        row = self.db.execute("SELECT * FROM operations WHERE operation_id=?", (operation_id,)).fetchone()
        if not row:
            raise KeyError(operation_id)
        return self._operation(row)

    def observe(self, operation_id: str, *, state: EffectState, lookup_method: str,
                authority: str, provider_reference: str | None, raw: Mapping[str, Any]) -> Operation:
        if authority not in {"provider", "application"}:
            raise ValueError("authority must be provider or application")
        self.db.execute("INSERT INTO evidence(operation_id, observed_at, lookup_method, authority, provider_reference, state, raw_json) VALUES (?, ?, ?, ?, ?, ?, ?)",
                        (operation_id, time.time(), lookup_method, authority, provider_reference,
                         state.value, json.dumps(raw, sort_keys=True)))
        self.db.execute("UPDATE operations SET state=?, provider_reference=COALESCE(?, provider_reference) WHERE operation_id=?",
                        (state.value, provider_reference, operation_id))
        self.db.commit()
        return self.get(operation_id)

    @staticmethod
    def _operation(row: sqlite3.Row) -> Operation:
        return Operation(row["operation_id"], row["payload_digest"], row["amount"], row["currency"],
                         EffectState(row["state"]), row["idempotency_key"], row["provider_reference"], row["created_at"])


class PaymentIntentBoundary:
    """Provider boundary usable directly or through a CrewAI custom tool wrapper."""
    def __init__(self, journal: OperationJournal, gateway: PaymentGateway, *, idempotency_window_seconds: int = 86400) -> None:
        self.journal, self.gateway = journal, gateway
        self.idempotency_window_seconds = idempotency_window_seconds

    def create(self, *, amount: int, currency: str = "usd", operation_id: str | None = None) -> Operation:
        op = self.journal.begin(amount=amount, currency=currency, operation_id=operation_id)
        if op.state is EffectState.COMMITTED:
            return op
        if op.state is EffectState.UNKNOWN:
            return self.recover(op.operation_id)
        try:
            response = self.gateway.create_payment_intent(amount=op.amount, currency=op.currency,
                idempotency_key=op.idempotency_key, metadata={"operation_id": op.operation_id})
        except LostResponse as exc:
            return self.journal.observe(op.operation_id, state=EffectState.UNKNOWN,
                lookup_method="create_response_lost", authority="application", provider_reference=None,
                raw={"error": str(exc)})
        return self._record_provider_response(op, response, "create")

    def recover(self, operation_id: str) -> Operation:
        op = self.journal.get(operation_id)
        if op.state is EffectState.COMMITTED:
            return op
        if op.provider_reference:
            try:
                response = self.gateway.retrieve_payment_intent(op.provider_reference)
            except (LostResponse, ProviderUnavailable) as exc:
                return self.journal.observe(op.operation_id, state=EffectState.UNKNOWN, lookup_method="retrieve_unavailable",
                    authority="application", provider_reference=op.provider_reference, raw={"error": str(exc)})
            if response is None:
                return self.journal.observe(op.operation_id, state=EffectState.UNKNOWN, lookup_method="retrieve_not_found",
                    authority="provider", provider_reference=op.provider_reference, raw={})
            return self._record_provider_response(op, response, "retrieve")
        if time.time() - op.created_at > self.idempotency_window_seconds:
            return self.journal.observe(op.operation_id, state=EffectState.UNKNOWN, lookup_method="retry_refused_window_expired",
                authority="application", provider_reference=None, raw={"window_seconds": self.idempotency_window_seconds})
        try:
            response = self.gateway.create_payment_intent(amount=op.amount, currency=op.currency,
                idempotency_key=op.idempotency_key, metadata={"operation_id": op.operation_id})
        except LostResponse as exc:
            return self.journal.observe(op.operation_id, state=EffectState.UNKNOWN, lookup_method="retry_response_lost",
                authority="application", provider_reference=None, raw={"error": str(exc)})
        return self._record_provider_response(op, response, "idempotent_retry")

    def _record_provider_response(self, op: Operation, response: Mapping[str, Any], lookup_method: str) -> Operation:
        reference = response.get("id")
        metadata = response.get("metadata", {})
        same_operation = metadata.get("operation_id") == op.operation_id
        same_amount = response.get("amount") == op.amount and response.get("currency", "").lower() == op.currency
        if not (reference and same_operation and same_amount):
            return self.journal.observe(op.operation_id, state=EffectState.UNKNOWN, lookup_method=lookup_method,
                authority="provider", provider_reference=str(reference) if reference else None,
                raw=dict(response))
        return self.journal.observe(op.operation_id, state=EffectState.COMMITTED, lookup_method=lookup_method,
            authority="provider", provider_reference=str(reference), raw=dict(response))


class CrewAIPaymentIntentTool:
    """Minimal CrewAI custom-tool shape. Subclass BaseTool in a CrewAI app if desired."""
    name = "create_or_recover_payment_intent"
    description = "Creates or reconciles a Stripe test-mode PaymentIntent with durable operation evidence."
    def __init__(self, boundary: PaymentIntentBoundary) -> None:
        self.boundary = boundary
    def _run(self, amount: int, currency: str = "usd", operation_id: str | None = None) -> dict[str, Any]:
        op = self.boundary.create(amount=amount, currency=currency, operation_id=operation_id)
        return {"operation_id": op.operation_id, "effect_state": op.state.value,
                "provider_reference": op.provider_reference,
                "safe_to_assume_committed": op.state is EffectState.COMMITTED}
