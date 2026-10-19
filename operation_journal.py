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
    DISPATCHING = "DISPATCHING"
    UNKNOWN = "UNKNOWN"
    INTENT_CREATED = "INTENT_CREATED"
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
    dispatch_started_at: float | None


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
          created_at REAL NOT NULL, dispatch_started_at REAL
        );
        CREATE TABLE IF NOT EXISTS evidence (
          evidence_id INTEGER PRIMARY KEY AUTOINCREMENT, operation_id TEXT NOT NULL,
          observed_at REAL NOT NULL, lookup_method TEXT NOT NULL,
          authority TEXT NOT NULL, provider_reference TEXT, state TEXT NOT NULL,
          raw_json TEXT NOT NULL, FOREIGN KEY(operation_id) REFERENCES operations(operation_id)
        );
        """)
        columns = {row["name"] for row in self.db.execute("PRAGMA table_info(operations)")}
        if "dispatch_started_at" not in columns:
            # A pre-fence PENDING row may already have reached the provider
            # before a crash. Fence it as ambiguous instead of replaying it as
            # a new first dispatch after an upgrade.
            self.db.execute("ALTER TABLE operations ADD COLUMN dispatch_started_at REAL")
            self.db.execute(
                "UPDATE operations SET state=?, dispatch_started_at=created_at WHERE state=?",
                (EffectState.UNKNOWN.value, EffectState.PENDING.value),
            )
        # Preserve compatibility with journals written before the resource-
        # creation state was renamed to avoid implying payment completion.
        self.db.execute("UPDATE operations SET state=? WHERE state='COMMITTED'",
                        (EffectState.INTENT_CREATED.value,))
        self.db.commit()

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
        self.db.execute("INSERT INTO operations (operation_id, payload_digest, amount, currency, state, idempotency_key, provider_reference, created_at, dispatch_started_at) VALUES (?, ?, ?, ?, ?, ?, NULL, ?, NULL)",
                        (operation_id, digest, amount, currency.lower(), EffectState.PENDING.value,
                         _idempotency_key(operation_id), now))
        self.db.commit()
        return self.get(operation_id)

    def get(self, operation_id: str) -> Operation:
        row = self.db.execute("SELECT * FROM operations WHERE operation_id=?", (operation_id,)).fetchone()
        if not row:
            raise KeyError(operation_id)
        return self._operation(row)

    def begin_dispatch(self, operation_id: str) -> bool:
        """Durably fence a first provider call before crossing the boundary."""
        now = time.time()
        self.db.execute("BEGIN IMMEDIATE")
        try:
            cursor = self.db.execute(
                "UPDATE operations SET state=?, dispatch_started_at=? "
                "WHERE operation_id=? AND state=?",
                (EffectState.DISPATCHING.value, now, operation_id, EffectState.PENDING.value),
            )
            if cursor.rowcount != 1:
                self.db.rollback()
                return False
            op = self.get(operation_id)
            self._append_evidence(operation_id, observed_at=now,
                lookup_method="dispatch_started", authority="application",
                provider_reference=None, state=EffectState.DISPATCHING,
                raw={"idempotency_key": op.idempotency_key, "attempt": "initial"})
            self.db.commit()
            return True
        except BaseException:
            self.db.rollback()
            raise

    def record_retry_dispatch(self, operation_id: str) -> None:
        """Persist an in-window retry attempt without resetting the first-use clock."""
        now = time.time()
        self.db.execute("BEGIN IMMEDIATE")
        try:
            row = self.db.execute(
                "SELECT idempotency_key FROM operations WHERE operation_id=?",
                (operation_id,),
            ).fetchone()
            if row is None:
                raise KeyError(operation_id)
            self.db.execute("UPDATE operations SET state=? WHERE operation_id=?",
                            (EffectState.DISPATCHING.value, operation_id))
            self._append_evidence(operation_id, observed_at=now,
                lookup_method="retry_dispatch_started", authority="application",
                provider_reference=None, state=EffectState.DISPATCHING,
                raw={"idempotency_key": row["idempotency_key"], "attempt": "retry"})
            self.db.commit()
        except BaseException:
            self.db.rollback()
            raise

    def _append_evidence(self, operation_id: str, *, observed_at: float,
                         lookup_method: str, authority: str,
                         provider_reference: str | None, state: EffectState,
                         raw: Mapping[str, Any]) -> None:
        self.db.execute(
            "INSERT INTO evidence(operation_id, observed_at, lookup_method, authority, provider_reference, state, raw_json) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (operation_id, observed_at, lookup_method, authority, provider_reference,
             state.value, json.dumps(raw, sort_keys=True)),
        )

    def observe(self, operation_id: str, *, state: EffectState, lookup_method: str,
                authority: str, provider_reference: str | None, raw: Mapping[str, Any],
                promote_provider_reference: bool = False) -> Operation:
        if authority not in {"provider", "application"}:
            raise ValueError("authority must be provider or application")
        self._append_evidence(operation_id, observed_at=time.time(),
            lookup_method=lookup_method, authority=authority,
            provider_reference=provider_reference, state=state, raw=raw)
        # Evidence may contain a candidate provider ID even when it fails
        # correlation. Only explicitly correlated provider evidence can make
        # an ID a trusted future retrieval target.
        trusted_reference = provider_reference if promote_provider_reference else None
        self.db.execute("UPDATE operations SET state=?, provider_reference=COALESCE(?, provider_reference) WHERE operation_id=?",
                        (state.value, trusted_reference, operation_id))
        self.db.commit()
        return self.get(operation_id)

    @staticmethod
    def _operation(row: sqlite3.Row) -> Operation:
        return Operation(row["operation_id"], row["payload_digest"], row["amount"], row["currency"],
                         EffectState(row["state"]), row["idempotency_key"], row["provider_reference"],
                         row["created_at"], row["dispatch_started_at"])


class PaymentIntentBoundary:
    """Provider boundary usable directly or through a CrewAI custom tool wrapper."""
    def __init__(self, journal: OperationJournal, gateway: PaymentGateway, *, idempotency_window_seconds: int = 86400) -> None:
        self.journal, self.gateway = journal, gateway
        self.idempotency_window_seconds = idempotency_window_seconds

    def create(self, *, amount: int, currency: str = "usd", operation_id: str | None = None) -> Operation:
        op = self.journal.begin(amount=amount, currency=currency, operation_id=operation_id)
        if op.state is EffectState.INTENT_CREATED:
            return op
        if op.state in {EffectState.UNKNOWN, EffectState.DISPATCHING}:
            return self.recover(op.operation_id)
        if not self.journal.begin_dispatch(op.operation_id):
            # Another caller already owns the first dispatch or has advanced
            # the operation. Return its durable projection without sending a
            # second request from this invocation.
            return self.journal.get(op.operation_id)
        op = self.journal.get(op.operation_id)
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
        if op.state is EffectState.INTENT_CREATED:
            return op
        if op.state is EffectState.PENDING:
            return self.create(amount=op.amount, currency=op.currency,
                               operation_id=op.operation_id)
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
        dispatch_started_at = op.dispatch_started_at
        age = time.time() - dispatch_started_at if dispatch_started_at is not None else None
        if age is None or age < 0 or age >= self.idempotency_window_seconds:
            return self.journal.observe(op.operation_id, state=EffectState.UNKNOWN, lookup_method="retry_refused_window_expired",
                authority="application", provider_reference=None,
                raw={"window_seconds": self.idempotency_window_seconds,
                     "dispatch_age_seconds": age,
                     "reason": "missing_or_expired_dispatch_timestamp"})
        self.journal.record_retry_dispatch(op.operation_id)
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
        return self.journal.observe(op.operation_id, state=EffectState.INTENT_CREATED, lookup_method=lookup_method,
            authority="provider", provider_reference=str(reference), raw=dict(response),
            promote_provider_reference=True)


class CrewAIPaymentIntentTool:
    """Minimal CrewAI custom-tool shape. Subclass BaseTool in a CrewAI app if desired."""
    name = "create_or_recover_payment_intent"
    description = "Creates or reconciles an unconfirmed Stripe test-mode PaymentIntent; does not confirm or complete a payment."
    def __init__(self, boundary: PaymentIntentBoundary) -> None:
        self.boundary = boundary
    def _run(self, amount: int, currency: str = "usd", operation_id: str | None = None) -> dict[str, Any]:
        op = self.boundary.create(amount=amount, currency=currency, operation_id=operation_id)
        return {"operation_id": op.operation_id, "intent_state": op.state.value,
                "provider_reference": op.provider_reference,
                "payment_confirmation_attempted": False}
