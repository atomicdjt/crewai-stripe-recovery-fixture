# Independent reviewer quickstart — CrewAI recovery boundary

This is a **local, deterministic mock-provider fixture**, not a production payment workflow, live Stripe integration result, or CrewAI core patch.

## Run the reproducible suite

Requires Python 3.11+ and standard library only:

```bash
git clone https://github.com/atomicdjt/crewai-stripe-recovery-fixture.git
cd crewai-stripe-recovery-fixture
python -m unittest discover -s tests -v
```

[Main-branch tests](../tests/test_recovery.py) · [SQLite journal](../operation_journal.py) · [Fake provider](../fake_stripe.py) · [CI workflow](../.github/workflows/tests.yml).

The suite uses temporary SQLite storage and a fake PaymentIntent API. No live Stripe request is performed.

## Adversarial evidence matrix

| Injected condition | Observable expectation | What remains unproved |
| --- | --- | --- |
| Response lost after fake provider creates intent | Journal retains `UNKNOWN` | Does **not** prove create failed |
| Recovery within modeled retention window | Same idempotency key, correlated resource | Does **not** prove all providers promise this retention |
| Provider metadata or identity mismatch | No trusted reference promoted; raw evidence retained | Candidate provider ID is not proof of this operation |
| Crash immediately after dispatch | Durable pre-dispatch marker survives restart | Does **not** prove physical HTTP calls cannot overlap |
| Retry after simulated expiry | Refuse unsafe blind create | `UNKNOWN` may persist indefinitely without authority |

Inspect **which test** exercises each condition; do not generalize isolated unit results to distributed, multi-process scheduling.

### Active improvement is not shipped work

[Open PR #1](https://github.com/atomicdjt/crewai-stripe-recovery-fixture/pull/1) proposes atomic retry-owner claims, lease fencing, and stale-evidence protection following [independent review in CrewAI #6414](https://github.com/crewAIInc/crewAI/issues/6414). Check its exact head and CI separately: **that change is not on main at the October 20 checkpoint**. A lease protects one journal owner; after expiry it does not necessarily stop a slow earlier network request.

### Safe reporting language

"Fault-injected local tests of an application-owned operation journal and a fake provider demonstrate conservative classification of ambiguous results; further adapter and provider-level validation is required."

Do **not** claim a confirmed Stripe charge, live test-mode run, CrewAI maintainer acceptance, exactly-once external effects, or fully independent production validation.

See [project README](../README.md) for integration boundaries.
