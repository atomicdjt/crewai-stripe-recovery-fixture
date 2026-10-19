# CrewAI Stripe Recovery Fixture

This is a small, dependency-free reference fixture for an application-owned `operation_id` journal, a CrewAI-compatible custom-tool boundary, and conservative recovery after ambiguous payment-provider responses.

It accompanies the discussion in [CrewAI issue #6414](https://github.com/crewAIInc/crewAI/issues/6414). It is designed for review of the evidence and correlation assumptions, rather than as a production payment component.

## Guarantees demonstrated

- One operation ID binds one canonical amount/currency payload.
- Provider observations are append-only evidence records containing `observed_at`, `lookup_method`, `authority`, raw response, and provider reference.
- A lost response becomes `UNKNOWN`, never `NOT_COMMITTED`.
- Retry uses the same idempotency key only within the configured retention window.
- A provider response commits only when provider reference, amount/currency, and `metadata.operation_id` all correlate.
- The custom tool exposes `safe_to_assume_committed: false` for every non-committed outcome.

## What this does not claim

It is not an upstream CrewAI patch, a distributed exactly-once proof, or proof of Stripe's guarantees. The included tests use a fault-injectable behavioural double; they do **not** call Stripe. No live Stripe test-mode result is included in this repository.

## Run deterministic fault injection

```bash
python -m unittest discover -s tests -v
```

## Optional live Stripe test-mode smoke test

Install `stripe`, export an `sk_test_...` key as `STRIPE_TEST_SECRET_KEY`, then instantiate `StripeGateway`. Never place the key in this repository. A live test should create only an unconfirmed test-mode PaymentIntent and must explicitly record the result as provider evidence. This reference intentionally does not automatically issue live financial-provider calls.

Before interpreting a retry as safe, verify Stripe's current documented idempotency semantics and retention period. Beyond that retention period, this fixture refuses a blind re-create and preserves `UNKNOWN`.

## Integration note

For a CrewAI application, wrap `CrewAIPaymentIntentTool` in the project’s installed `BaseTool` class and preserve `_run`. Keep the journal outside agent memory and commit its SQLite file (or a production transactional equivalent) before provider invocation.
