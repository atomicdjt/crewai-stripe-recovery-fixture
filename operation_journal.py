"""Durable operation journal and a claim-bounded Stripe/CrewAI recovery boundary.

The important property is epistemic, not merely mechanical: ambiguous provider
outcomes remain UNKNOWN until backed by provider evidence.  This module is
dependency-free; production callers can import StripeGateway from
`stripe_gateway.py` or supply an adapter with the same two methods.
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
          created_at REAL NOT NULL, dispatch_started_at REAL,
          dispatch_owner_token TEXT, dispatch_generation INTEGER NOT NULL DEFAULT 0,
          dispatch_lease_expires_at REAL
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
        # Existing in-flight rows have no owner/fencing provenance. Quarantine
        # them rather than treating an unknown predecessor as a free retry slot.
        if "dispatch_owner_token" not in columns:
            self.db.execute("ALTER TABLE operations ADD COLUMN dispatch_owner_token TEXT")
            self.db.execute(
                "UPDATE operations SET state=?, dispatch_owner_token=? WHERE state=?",
                (EffectState.UNKNOWN.value, "legacy-unfenced",
                 EffectState.DISPATCHING.value),
            )
            legacy_unfenced = True
        else:
            legacy_unfenced = False
        if "dispatch_generation" not in columns:
            self.db.execute(
                "ALTER TABLE operations ADD COLUMN dispatch_generation INTEGER NOT NULL DEFAULT 0"
            )
        if "dispatch_lease_expires_at" not in columns:
            self.db.execute("ALTER TABLE operations ADD COLUMN dispatch_lease_expires_at REAL")
        if legacy_unfenced:
            # No automatic retry for pre-owner in-flight operations. A positive
            # authoritative lookup or explicit operator reconciliation is needed.
            self.db.execute(
                "UPDATE operations SET dispatch_lease_expires_at=? "
                "WHERE dispatch_owner_token=?",
                (1e30, "legacy-unfenced"),
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

    def begin_dispatch(self, operation_id: str, *, lease_seconds: float = 60.0) -> str | None:
        """Atomically claim first dispatch; return a unique fencing token."""
        token = uuid.uuid4().hex
        self.db.execute("BEGIN IMMEDIATE")
        try:
            now = time.time()
            cursor = self.db.execute(
                "UPDATE operations SET state=?, dispatch_started_at=?, "
                "dispatch_owner_token=?, dispatch_lease_expires_at=?, "
                "dispatch_generation=dispatch_generation+1 "
                "WHERE operation_id=? AND state=?",
                (EffectState.DISPATCHING.value, now, token, now + lease_seconds,
                 operation_id, EffectState.PENDING.value),
            )
            if cursor.rowcount != 1:
                self.db.rollback()
                return None
            op = self.get(operation_id)
            self._append_evidence(operation_id, observed_at=now,
                lookup_method="dispatch_started", authority="application",
                provider_reference=None, state=EffectState.DISPATCHING,
                raw={"idempotency_key": op.idempotency_key, "attempt": "initial",
                     "generation": 1, "owner_token": token})
            self.db.commit()
            return token
        except BaseException:
            self.db.rollback()
            raise

    def claim_retry_dispatch(self, operation_id: str, *,
                             idempotency_window_seconds: float,
                             lease_seconds: float = 60.0) -> str | None:
        """One journal owner per retry generation, with age checked atomically.

        Lease expiration permits crash recovery but does NOT prove that the
        previous HTTP call has stopped. The provider idempotency key is still
        essential; no exactly-once network-call claim is made.
        """
        token = uuid.uuid4().hex
        self.db.execute("BEGIN IMMEDIATE")
        try:
            now = time.time()
            cursor = self.db.execute(
                "UPDATE operations SET state=?, dispatch_owner_token=?, "
                "dispatch_lease_expires_at=?, dispatch_generation=dispatch_generation+1 "
                "WHERE operation_id=? AND state IN (?, ?) "
                "AND dispatch_started_at IS NOT NULL "
                "AND dispatch_started_at<=? AND dispatch_started_at>? "
                "AND (dispatch_lease_expires_at IS NULL OR dispatch_lease_expires_at<=?)",
                (EffectState.DISPATCHING.value, token, now + lease_seconds,
                 operation_id, EffectState.UNKNOWN.value, EffectState.DISPATCHING.value,
                 now, now - idempotency_window_seconds, now),
            )
            if cursor.rowcount != 1:
                self.db.rollback()
                return None
            op = self.get(operation_id)
            generation = self.db.execute(
                "SELECT dispatch_generation FROM operations WHERE operation_id=?",
                (operation_id,),
            ).fetchone()[0]
            self._append_evidence(operation_id, observed_at=now,
                lookup_method="retry_dispatch_started", authority="application",
                provider_reference=None, state=EffectState.DISPATCHING,
                raw={"idempotency_key": op.idempotency_key, "attempt": "retry",
                     "generation": generation, "owner_token": token})
            self.db.commit()
            return token
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
                promote_provider_reference: bool = False,
                owner_token: str | None = None) -> Operation:
        """Append evidence and apply only a monotonic, owner-safe projection.

        Stale UNKNOWN results remain in the evidence log but may not demote
        INTENT_CREATED or overwrite a newer dispatch owner's projection.
        """
        if authority not in {"provider", "application"}:
            raise ValueError("authority must be provider or application")
        if state is EffectState.INTENT_CREATED and (
            authority != "provider" or not promote_provider_reference or not provider_reference
        ):
            raise ValueError("INTENT_CREATED requires correlated provider evidence")
        if state is EffectState.NOT_COMMITTED:
            raise ValueError(
                "NOT_COMMITTED requires an explicit provider-authoritative proof path"
            )
        self.db.execute("BEGIN IMMEDIATE")
        try:
            self._append_evidence(operation_id, observed_at=time.time(),
                lookup_method=lookup_method, authority=authority,
                provider_reference=provider_reference, state=state, raw=raw)
            if state is EffectState.INTENT_CREATED:
                # A verified positive can arrive after an old lease expires;
                # preserve it even if the worker is no longer the active owner.
                # Conflicting positive references require manual review.
                self.db.execute(
                    "UPDATE operations SET state=?, provider_reference=?, "
                    "dispatch_owner_token=NULL, dispatch_lease_expires_at=NULL "
                    "WHERE operation_id=? AND state!=? "
                    "AND (provider_reference IS NULL OR provider_reference=?)",
                    (state.value, provider_reference, operation_id,
                     EffectState.NOT_COMMITTED.value, provider_reference),
                )
            elif state is EffectState.UNKNOWN:
                if lookup_method == "retry_refused_window_expired" and owner_token is None:
                    # A key-window refusal is an unowned observation. Keep the
                    # active dispatch projection while a different worker still
                    # owns a *live* lease; its outcome may arrive later.
                    # After the lease expires, persist UNKNOWN and clear the
                    # obsolete owner atomically. This is NOT evidence of
                    # NOT_COMMITTED and never authorizes a post-window retry.
                    now = time.time()
                    self.db.execute(
                        "UPDATE operations SET state=?, dispatch_owner_token=NULL, "
                        "dispatch_lease_expires_at=NULL WHERE operation_id=? "
                        "AND state NOT IN (?, ?) "
                        "AND (dispatch_owner_token IS NULL OR "
                        "(dispatch_lease_expires_at IS NOT NULL "
                        "AND dispatch_lease_expires_at<=?))",
                        (state.value, operation_id, EffectState.INTENT_CREATED.value,
                         EffectState.NOT_COMMITTED.value, now),
                    )
                else:
                    # An attempt may release only its own lease. An unowned
                    # lookup may alter only an unowned operation.
                    predicate = ("dispatch_owner_token IS NULL" if owner_token is None
                                 else "dispatch_owner_token=?")
                    params = [state.value, operation_id,
                              EffectState.INTENT_CREATED.value, EffectState.NOT_COMMITTED.value]
                    if owner_token is not None:
                        params.append(owner_token)
                    self.db.execute(
                        "UPDATE operations SET state=?, dispatch_owner_token=NULL, "
                        "dispatch_lease_expires_at=NULL WHERE operation_id=? "
                        "AND state NOT IN (?, ?) AND " + predicate, params,
                    )
            else:
                # Nonterminal observations never demote a verified result.
                self.db.execute(
                    "UPDATE operations SET state=? WHERE operation_id=? "
                    "AND state NOT IN (?, ?)",
                    (state.value, operation_id, EffectState.INTENT_CREATED.value,
                     EffectState.NOT_COMMITTED.value),
                )
            self.db.commit()
            return self.get(operation_id)
        except BaseException:
            self.db.rollback()
            raise

    @staticmethod
    def _operation(row: sqlite3.Row) -> Operation:
        return Operation(row["operation_id"], row["payload_digest"], row["amount"], row["currency"],
                         EffectState(row["state"]), row["idempotency_key"], row["provider_reference"],
                         row["created_at"], row["dispatch_started_at"])


class PaymentIntentBoundary:
    """Provider boundary usable directly or through a CrewAI custom tool wrapper."""
    def __init__(self, journal: OperationJournal, gateway: PaymentGateway, *,
                 idempotency_window_seconds: int = 86400,
                 dispatch_lease_seconds: float = 60.0) -> None:
        self.journal, self.gateway = journal, gateway
        self.idempotency_window_seconds = idempotency_window_seconds
        self.dispatch_lease_seconds = dispatch_lease_seconds

    def create(self, *, amount: int, currency: str = "usd", operation_id: str | None = None) -> Operation:
        op = self.journal.begin(amount=amount, currency=currency, operation_id=operation_id)
        if op.state is EffectState.INTENT_CREATED:
            return op
        if op.state in {EffectState.UNKNOWN, EffectState.DISPATCHING}:
            return self.recover(op.operation_id)
        token = self.journal.begin_dispatch(
            op.operation_id, lease_seconds=self.dispatch_lease_seconds)
        if token is None:
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
                raw={"error": str(exc)}, owner_token=token)
        return self._record_provider_response(op, response, "create", owner_token=token)

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
        token = self.journal.claim_retry_dispatch(
            op.operation_id, idempotency_window_seconds=self.idempotency_window_seconds,
            lease_seconds=self.dispatch_lease_seconds)
        if token is None:
            # A different worker owns the attempt, positive evidence has
            # arrived, or the original provider idempotency window has closed.
            return self.journal.get(op.operation_id)
        try:
            response = self.gateway.create_payment_intent(amount=op.amount, currency=op.currency,
                idempotency_key=op.idempotency_key, metadata={"operation_id": op.operation_id})
        except LostResponse as exc:
            return self.journal.observe(op.operation_id, state=EffectState.UNKNOWN, lookup_method="retry_response_lost",
                authority="application", provider_reference=None, raw={"error": str(exc)},
                owner_token=token)
        return self._record_provider_response(op, response, "idempotent_retry", owner_token=token)

    def _record_provider_response(self, op: Operation, response: Mapping[str, Any],
                                  lookup_method: str, owner_token: str | None = None) -> Operation:
        reference = response.get("id")
        metadata = response.get("metadata")
        currency = response.get("currency")
        valid_reference = isinstance(reference, str) and bool(reference.strip())
        same_operation = (
            isinstance(metadata, Mapping)
            and metadata.get("operation_id") == op.operation_id
        )
        same_amount = (
            response.get("amount") == op.amount
            and isinstance(currency, str)
            and currency.lower() == op.currency
        )
        if not (valid_reference and same_operation and same_amount):
            return self.journal.observe(op.operation_id, state=EffectState.UNKNOWN, lookup_method=lookup_method,
                authority="provider", provider_reference=reference if valid_reference else None,
                raw=dict(response), owner_token=owner_token)
        return self.journal.observe(op.operation_id, state=EffectState.INTENT_CREATED, lookup_method=lookup_method,
            authority="provider", provider_reference=reference, raw=dict(response),
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
