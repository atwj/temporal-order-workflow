# Order workflow on Temporal

## About

This repo models a simple e-commerce order as one durable process:

**authorize payment → reserve stock → capture payment → ship → email the customer.**

If something goes wrong—or the customer cancels before shipping—completed work is undone in
reverse order (for example: refund, then release stock). If an undo itself gets stuck (Stripe
refuses a refund), the order waits for a person to decide what to do next.

The point of the demo is to show how Temporal keeps that whole story in one place: retries,
timeouts, cancellation, cleanup, and a handoff to ops—without building a maze of queues, cron
jobs, and status tables yourself.

To try the happy path after [Setup](#setup):

```bash
temporal server start-dev          # terminal 1 — UI at http://localhost:8233
python worker.py                   # terminal 2
python client.py 1                 # terminal 3 → COMPLETED, tracking track-1
```

## Setup

You need Python **3.12+**, the Temporal CLI, and the packages in `requirements.txt`.

### Install the Temporal CLI

| OS | Command |
| --- | --- |
| macOS | `brew install temporal` |
| Linux (or macOS without Homebrew) | `curl -sSf https://temporal.download/cli.sh \| sh` |
| Windows | `scoop install temporal-cli` |

```bash
temporal --version
```

### Start Temporal and the app

```bash
# terminal 1 — local server (Web UI: http://localhost:8233)
temporal server start-dev

# from the repo root — Python env (once)
python3.12 -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install -r requirements.txt

# terminal 2
python worker.py

# terminal 3
python client.py 1
```

Activate the venv in every terminal you use. To keep workflow history across server restarts:

```bash
temporal server start-dev --db-filename temporal.db
```

Tests (no server needed):

```bash
pytest -v
```

### Client commands

```bash
python client.py ORDER_ID [SIMULATE]                    # place an order
python client.py cancel ORDER_ID                        # customer cancel
python client.py status ORDER_ID                        # where is this order?
python client.py resolve ORDER_ID retry|done_manually   # ops: unstick a failed undo
```

Use a **new order ID** each time—the same ID cannot run twice on purpose.

`SIMULATE` options: `card_declined`, `gateway_flaky`, `out_of_stock`, `slow_inventory`,
`bad_address`, `refund_rejected`.

## How the saga is designed

![Business process: each step and its undo](diagrams/1-business-process.svg)

The order is a **saga**: each step has a matching undo. If a later step fails, earlier steps are
rolled back, newest first.

| If this fails… | We undo… | Order ends as |
| --- | --- | --- |
| Authorize | Void the card hold | FAILED |
| Reserve stock | Release stock, void hold | FAILED |
| Capture payment | Refund, release stock, void hold | FAILED |
| Ship | Refund, release stock | FAILED |
| Email | Nothing (email is best-effort) | COMPLETED |
| Cancel before shipping | Same undos as a failure at the current step | CANCELLED |

Design choices, in plain language:

- **Remember the undo before doing the step.** A payment or reservation can succeed in the real
  world even if our app never hears the reply. The undo has to be ready either way, and safe to
  run when there was nothing to undo.
- **Assume steps may run more than once.** Timeouts and retries are normal. Every call to Stripe
  (and the stand-in services here) uses an idempotency key so a repeat does not double-charge.
- **Keep long work “alive” with heartbeats.** That is how the system notices a dead worker quickly
  and how a cancel reaches work that is still running.
- **Cancel stops the current step (before shipping), then undoes.** The customer gets an immediate
  yes/no. An operator can cancel from the Temporal CLI. Once shipping has started, it is too late;
  the parcel is with the carrier.
- **Cleanup is not cancelled.** If someone cancels while we are refunding or releasing stock, we
  still finish putting things back.
- **Stuck undos wait for a person.** A refused refund does not retry forever. The order moves to
  `NEEDS_ATTENTION` until ops says `retry` or `done_manually`.
- **Retries are bounded.** A flaky gateway is retried with backoff, but each step has an overall
  time limit so an outage cannot hang an order forever. Clear business failures (card declined,
  out of stock) stop immediately and trigger undo.

## Before and after Temporal

### The systems we call

![External services on the happy path](diagrams/2-external-services.svg)

On the happy path the order talks to four outside systems: payments (Stripe), inventory, the
carrier, and email. Any of them can time out, error, or rate-limit.

### Building resiliency by hand

![Resiliency logic built by hand around the order service](diagrams/3-hand-built-resiliency.svg)

Without something like Temporal, those few business steps usually grow a pile of extra machinery:
retry wrappers, queues, dead-letter queues, compensation handlers, a status table, an idempotency
store, stuck-order cron jobs, and more. Most of the code becomes plumbing, spread across services.

### With durable execution

![Durable execution with Temporal](diagrams/4-durable-execution.svg)

With Temporal, the order is one workflow. Most of that plumbing moves into the platform:

| You used to build… | Temporal covers… |
| --- | --- |
| Status table | History of every step, plus a simple status query |
| Retry / backoff code | Retry policies on each step |
| Queues to other systems | A task queue that workers poll |
| Dead-letter / stuck-order handling | Failed business steps trigger undo; a stuck undo waits for ops |
| Compensation scattered in services | One undo list in the workflow |
| “Is this worker dead?” cron | Heartbeats and timeouts, then retry on another worker |

Two things stay yours:

- **Idempotency keys** at the payment (and other) APIs—Temporal runs steps at least once; it does
  not replace Stripe’s idempotency.
- **Reconciliation** as a safety net (see Future enhancements)—not as the main way failed orders
  are recovered.

## Scenarios

Run these with the worker and Temporal server up. Prefer a fresh order ID for each row.

| # | What you are showing | How to run it | What you should see |
| --- | --- | --- | --- |
| 1 | Happy path | `python client.py 1` | `COMPLETED, tracking track-1` |
| 2 | Transient payment glitch | `python client.py 2 gateway_flaky` | Authorize fails twice, then succeeds; order completes |
| 3 | Card declined | `python client.py 3 card_declined` | Fails immediately; void finds nothing to undo |
| 4 | Out of stock | `python client.py 4 out_of_stock` | Hold is voided; order FAILED |
| 5 | Bad shipping address | `python client.py 5 bad_address` | Payment refunded, stock released; FAILED |
| 6 | Worker dies mid-order | `python client.py 6 slow_inventory`, kill the worker during the wait (`pkill -9 -f worker.py`), restart it | Reserve is retried; authorize is **not** done again; order still completes |
| 7 | Same order twice | `python client.py 1` again | Refused: that order ID already exists |
| 8 | Customer cancels in time | `python client.py 8 slow_inventory`, then quickly `python client.py cancel 8` | Cancel accepted; stock not left reserved; `CANCELLED` |
| 9 | Operator cancels | Same timing as 8, with `temporal workflow cancel --workflow-id order-9` | Same cleanup; UI shows **Canceled** |
| 10 | Cancel too late | `python client.py cancel 1` (after it finished), or cancel while shipping | Rejected, or ignored once shipping has started |
| 11 | Refund needs a human | `python client.py 11 refund_rejected`, then `status 11`, then `resolve 11 done_manually` | Ship fails (**Bad address**), then the refund is refused and the order waits. After resolve, stock is released; the client still ends with `FAILED: … Bad address` (the original ship problem) |

**Timing tip for 6, 8, and 9:** cancel or kill the worker while the log still says the warehouse is
slow—before you see `reserved`.

Check progress anytime with `python client.py status ORDER_ID`.

## Who benefits

| Persona | Pain today | What this demo shows |
| --- | --- | --- |
| **Engineering leaders** | Checkout resiliency costs more to build and run than the product features | Most of the failure machinery moves into the platform (diagrams 3 vs 4) |
| **Developers** | Retries, status, and undo logic copied through every service | The order is one readable flow; failures are demos and tests, not folklore |
| **Support & ops** | “Where is my order?” and stuck refunds mean paging engineering | Status and cancel are simple commands; a refused refund waits for a person (scenarios 8–11) |

## Approach

- **Start from the business:** after a payment is accepted—holds, inventory, shipping, and what
  happens when something breaks.
- **Make failures easy to show:** `client.py` scenarios (decline, flaky gateway, cancel, stuck
  refund) drove the design one path at a time.
- **Keep the core idea small:** enough to show durable execution and a real saga; deliberate gaps
  are listed below.

## Known limitations and future enhancements

| Limitation today | Why it matters | Future enhancement |
| --- | --- | --- |
| A stuck undo only logs an error | Nobody is paged automatically | Alert (Slack / PagerDuty) before waiting for ops |
| You can query one order’s status, not list all stuck ones in the UI | Ops has to know the order ID | Search attributes for status (e.g. all `NEEDS_ATTENTION`) |
| Stand-in “Stripe” state lives in the worker process | Restarting the worker forgets demo holds/charges | Real Stripe test mode (or another sandbox) |
| Customer email sits in workflow history in plain text | Visible to anyone with UI access | Encrypt sensitive payloads |
| Workflow code is not versioned for live changes | Changing the flow under open orders can break replay | Workflow versioning / patched changes when promoting to production |

Also worth adding later: delivery and dispute waits with durable timers, a reconciliation job as a
safety net, and Temporal Cloud for a shared environment.

**Note:** blocking reuse of an order ID after failure or success is intentional—it avoids
accidentally running the same charge twice. The customer places a new order instead.
