# Order workflow on Temporal

A durable e-commerce order in Python on Temporal: **authorize → reserve → capture → ship → notify**.

If a step fails, or the customer cancels before shipping, every step that **started** is undone
(newest first). If an undo cannot succeed, the order waits for a person (`NEEDS_ATTENTION`).

**Why Temporal:** retries, timeouts, cancellation, compensation, and an ops handoff live in one
workflow function instead of a hand-built state machine of queues, cron jobs, and status tables.

**Prove it in three terminals** (after [Installation](#installation)):

```bash
temporal server start-dev          # terminal 1 — UI at http://localhost:8233
python worker.py                   # terminal 2
python client.py 1                 # terminal 3 → COMPLETED, tracking track-1
```

Then try a failure (`python client.py 3 card_declined`) or a cancel (scenario 8 below).

## Installation

Python **3.12+**. You need the Temporal CLI, a venv, and the pinned packages.

### 1. Install the Temporal CLI

| OS | Command |
| --- | --- |
| macOS | `brew install temporal` |
| Linux (or macOS without Homebrew) | `curl -sSf https://temporal.download/cli.sh \| sh` |
| Windows | `scoop install temporal-cli` |

```bash
temporal --version
```

### 2. Start the local Temporal server

```bash
temporal server start-dev
```

Server: `localhost:7233`. Web UI: http://localhost:8233.  
Data is in-memory unless you pass `--db-filename temporal.db`.

### 3. Python environment

```bash
python3.12 -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

Activate the venv in every terminal you use for the worker, client, or tests.

## Run it

| Terminal | Command | Role |
| --- | --- | --- |
| 1 | `temporal server start-dev` | Server + Web UI |
| 2 | `python worker.py` | Workflow + activity worker |
| 3 | `python client.py 1` | Place order 1 |

```bash
pytest -v   # no server needed; 29 tests
```

### Client cheat sheet

```bash
python client.py ORDER_ID [SIMULATE]           # place order
python client.py cancel ORDER_ID               # customer cancel (Update)
python client.py status ORDER_ID               # query current step
python client.py resolve ORDER_ID retry|done_manually   # ops handoff
```

`SIMULATE`: `card_declined` · `gateway_flaky` · `out_of_stock` · `slow_inventory` · `bad_address` · `refund_rejected`

## Demo scenarios

Use a **new order ID** for each run (reuse is rejected on purpose).

| # | Scenario | Command | Expect |
| --- | --- | --- | --- |
| 1 | Happy path | `python client.py 1` | `COMPLETED, tracking track-1` |
| 2 | Flaky gateway | `python client.py 2 gateway_flaky` | Authorize retries twice (stack traces), then COMPLETED |
| 3 | Card declined | `python client.py 3 card_declined` | Void finds nothing; `FAILED: Card declined` |
| 4 | Out of stock | `python client.py 4 out_of_stock` | Release no-op, then void auth; FAILED |
| 5 | Bad address | `python client.py 5 bad_address` | Refund, then release; FAILED |
| 6 | Worker crash | `python client.py 6 slow_inventory`, then `pkill -9 -f worker.py`, restart worker | Reserve retries after ~5 s; authorize **not** re-run; COMPLETED ~15 s after kill |
| 7 | Duplicate order | `python client.py 1` again | `Not started: order-1 already exists` |
| 8 | Customer cancel | `python client.py 8 slow_inventory`, then `python client.py cancel 8` | Reserve **stopped**; void auth; `CANCELLED` |
| 9 | Operator cancel | Same as 8, but `temporal workflow cancel --workflow-id order-9` | Same cleanup; Web UI status **Canceled** |
| 10 | Cancel too late | `python client.py cancel 1` | `Rejected: workflow execution already completed`. Mid-ship: `Too late to cancel` |
| 11 | Refund refused | `python client.py 11 refund_rejected`, then `status 11`, then `resolve 11 done_manually` | Ship fails first (**Bad address**), then the refund undo is refused → `NEEDS_ATTENTION`. After `resolve`, release runs; client still ends `FAILED: Order failed: Bad address` (the original ship error, not the refund) |

While order 6 runs:

```bash
python client.py status 6
# or: temporal workflow query --workflow-id order-6 --name get_status
```

Worker log lines look like `[order-4 void_authorization attempt 1]` (`activity.logger`).

### Expected worker highlights

- **3:** `no hold to void for order 3`
- **4:** `no reservation to release for order 4`, then `voided auth-4`
- **8/9:** warehouse “slow” line, **no** `reserved` line, then void
- **10 (shipping):** `Cancel ignored: the order has already shipped`
- **11:** carrier rejected (Bad address) → `Stripe rejected the refund` → `ERROR` … `refund_payment failed` → after resolve, `released res-11`

## How the saga works

![Business process: each step and its undo](diagrams/1-business-process.svg)

*Happy path and undo order. Cancel before shipping and the ops handoff for stuck undos are in the rules below.*

| Step that fails | Undo (newest first) | Ends as |
| --- | --- | --- |
| Authorize | Void | FAILED |
| Reserve | Release → void | FAILED |
| Capture | Refund → release → void | FAILED |
| Ship | Refund → release | FAILED |
| Notify | — (best-effort) | COMPLETED |
| Cancel before shipping | Same undos as failure at the current step | CANCELLED |

### Rules (rule → why → where)

| Rule | Why | Where |
| --- | --- | --- |
| **Undo before each step** | Side effect can land even if the ACK never returns | `compensations.append` before `step()` |
| **Idempotent activities** | Temporal runs activities at least once | Keys `{order_id}-{step}` · `fake_services.py` |
| **Heartbeats on every call** | Dead worker in ~5 s; cancel delivery | `call_service()` · `HEARTBEAT_TIMEOUT` |
| **Customer cancel races the step** | Don’t wait out a slow warehouse after “cancel” | `start_activity` + `wait_condition` + `handle.cancel()` |
| **Wait for activity stop, then undo** | Avoid undo racing a still-running side effect | `WAIT_CANCELLATION_COMPLETED` |
| **Check cancel between steps** | Late success must not continue the saga | `check_cancel()` · `cancellation_reason()` |
| **Too late once shipping** | Parcel is with the carrier | Validator rejects customer cancel; operator cancel logged ignored |
| **Cancel never stops an undo** | Cleanup must finish | `asyncio.shield` in `finish()` / `run_undo()` |
| **Stuck undo → person** | Don’t retry a refused refund forever | `NEEDS_ATTENTION` · `resolve_undo` |
| **Bounded forward retries** | Outages must not hang forever | `STEP_DEADLINE` (2 min `schedule_to_close`) |
| **Business vs transient errors** | Declines aren’t timeouts | `non_retryable` + `BENIGN` vs plain exceptions |

**Customer cancel** is an Update (`OrderWorkflow.cancel`) with an immediate accept/reject.  
**Operator cancel** is `temporal workflow cancel` (workflow ends **Canceled**).  
Both stop the running step before shipping; undos then run.

## Before and after Temporal

### External services (happy path)

![External services on the happy path](diagrams/2-external-services.svg)

Four systems: Stripe, inventory, carrier, email — each can time out, error, or rate-limit.

### Resiliency by hand

![Resiliency logic built by hand around the order service](diagrams/3-hand-built-resiliency.svg)

Without Temporal, those five business steps need roughly eight extra components (retries, queues,
DLQ, compensation, status table, idempotency store, stuck-order cron, …). Most of the code is
plumbing.

### Durable execution

![Durable execution with Temporal](diagrams/4-durable-execution.svg)

Six of those eight move into the platform:

| Hand-built | With Temporal | In the code |
| --- | --- | --- |
| Status table | Event history + query | `get_status` |
| Retry wrappers | Server-side retry policy | `STEP_RETRY`, `UNDO_RETRY` |
| Queues | Task queue | `TASK_QUEUE = "orders"` |
| DLQ | Non-retryable → undo; stuck undo → ops | `NEEDS_ATTENTION`, `resolve_undo` |
| Compensation handlers | One list, newest first | `compensations` |
| Stuck-order cron | Heartbeats, timeouts, step deadline | `HEARTBEAT_TIMEOUT`, `STEP_TIMEOUT`, `STEP_DEADLINE` |

Still yours:

- **Idempotency keys** at the gateway (`activities.py` / `fake_services.py`)
- **Reconciliation** as a Schedule (safety net — see Known limitations)

Giving up after `STEP_DEADLINE` is safe here because the undo was registered first and is
idempotent (e.g. refund only if a charge exists).

## Who benefits

| Persona | Pain | What this shows |
| --- | --- | --- |
| CTO / VP Eng | Checkout resiliency costs more than features | Diagram 3 vs 4 |
| Developers | Retry/undo logic in every service | `workflow.py`, time-skipping tests |
| SRE / on-call | Stuck orders, risky restarts | Scenarios **6** and **11** |
| Payments / finance | Double charges, orphaned holds | Failure table, idempotency keys, scenario **11** |
| Support | “Where is my order?” / “cancel it” | `status`, `cancel`, scenarios **8–10** |
| Product | New step = multi-team project | One new activity or signal in the workflow |

## How I approached this

- **Use case:** after Stripe accepts a payment — holds, inventory, shipping, and refunds when
  something breaks.
- **Demos first:** `client.py` scenarios made each failure path easy to build and show.
- **Left out on purpose:** see Known limitations (enough for the core idea, not a full platform).

## Known limitations and next steps

Deliberate gaps, with the Temporal feature that would close each:

| Gap | Why it matters | Next step |
| --- | --- | --- |
| `NEEDS_ATTENTION` only logs `ERROR` | Nobody is paged | Alert activity (Slack/PagerDuty) before waiting for ops |
| Status is a query, not a search attribute | Can’t list all `NEEDS_ATTENTION` in the UI | Upsert `OrderStatus` (time-skipping test server doesn’t support custom SAs) |
| Fake services are in-worker memory | Worker kill forgets holds/charges | Real Stripe test mode / sandbox APIs |
| Email/amount in plaintext history | Visible in the Web UI | Payload codec + codec server |
| No workflow versioning | Changing `workflow.py` under open runs breaks replay | `workflow.patched()` or Worker Versioning |

Also next: delivery/dispute signals with durable timers, a reconciliation Schedule, Temporal Cloud.

**Idempotent start:** `REJECT_DUPLICATE` on workflow ID `order-{id}` is intentional — a failed or
completed order ID is not reused; the customer places a new order.

## Troubleshooting

| Symptom | Fix |
| --- | --- |
| `Connection refused` to `localhost:7233` | Start `temporal server start-dev` and wait until it’s healthy |
| Worker starts then does nothing | Activate `.venv`, run from repo root, confirm task queue `orders` |
| Non-determinism / odd replay after a pull | Restart the dev server (in-memory histories won’t match new code) |
| `order-N already exists` | Use a new order ID (`REJECT_DUPLICATE`) |
| Cancel rejected while shipping | Expected — too late once `SHIPPING`; operator cancel is logged and ignored |
| `NEEDS_ATTENTION` forever | Run `python client.py resolve ORDER_ID done_manually` (or `retry`) |
| Setup fails on Windows paths / Python version | Use 3.12+, activate the venv in **every** terminal |
