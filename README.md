# CrewAI Stripe Recovery Fixture

This is a small, dependency-free reference fixture for an application-owned `operation_id` journal, a CrewAI-compatible custom-tool boundary, and conservative recovery after ambiguous payment-provider responses.

It accompanies the discussion in [CrewAI issue #6414](https://github.com/crewAIInc/crewAI/issues/6414). It is designed for review of the evidence and correlation assumptions, rather than as a production payment component.

## Guarantees demonstrated

- One operation ID binds one canonical amount/currency payload.
- Provider observations are append-only evidence records containing `observed_at`, `lookup_method`, `authority`, raw response, and provider reference.
- A lost response becomes `UNKNOWN`, never `NOT_COMMITTED`.
- The journal durably records `DISPATCHING` and the first dispatch timestamp before calling the provider. A crash after provider acceptance cannot leave a fresh `PENDING` row that looks safe to dispatch again.
- Recovery reuses the same idempotency key only within the configured retention window measured from first dispatch. Once that window expires, the outcome remains `UNKNOWN` and no create retry is sent.
- On upgrade from an earlier journal schema, existing `PENDING` rows are conservatively converted to `UNKNOWN` and fenced using their creation time, since the older schema cannot prove whether dispatch happened.
- A response is classified as `INTENT_CREATED` only when provider ID, amount/currency, and `metadata.operation_id` correlate. This describes creation of the unconfirmed PaymentIntent resource, not payment completion.
- Uncorrelated candidate provider IDs are preserved in append-only evidence but never promoted to the operation's trusted retrieval reference.
- The custom tool reports `payment_confirmation_attempted: false`; it does not claim that a payment completed.

## What this does not claim

It is not an upstream CrewAI patch, a distributed exactly-once proof, or proof of Stripe's guarantees. The included tests use a fault-injectable behavioural double, including a simulated process crash after provider acceptance; they do **not** call Stripe. No live Stripe test-mode result is included in this repository. The probe creates only an unconfirmed PaymentIntent; it does not confirm or complete a payment.

## Run deterministic fault injection

```bash
python -m unittest discover -s tests -v
```

## Optional live Stripe test-mode smoke test

Install `stripe`, export an `sk_test_...` key as `STRIPE_TEST_SECRET_KEY`, then instantiate `StripeGateway`. Never place the key in this repository. A live test should create only an unconfirmed test-mode PaymentIntent and must explicitly record the result as provider evidence. This reference intentionally does not automatically issue live financial-provider calls.

Before interpreting a retry as safe, verify Stripe's current documented idempotency semantics and retention period. Beyond that retention period, this fixture refuses a blind re-create and preserves `UNKNOWN`.

## Integration note

For a CrewAI application, wrap `CrewAIPaymentIntentTool` in the project’s installed `BaseTool` class and preserve `_run`. Keep the journal outside agent memory and commit its SQLite file (or a production transactional equivalent) before provider invocation.
