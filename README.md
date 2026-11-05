# Order workflow on Temporal

This repo implements an e-commerce order as one durable Temporal workflow in Python:
authorize payment, reserve inventory, capture payment, ship, notify. If a step fails,
the completed steps are undone.

The sections below cover:

- **Installation and Run it:** set up Temporal and Python, then run the workflow locally.
- **Demo scenarios:** 7 scenarios, from the happy path to a worker crash, triggered with `client.py`
  to show durable execution at work.
- **Design, and before and after Temporal:** the order's architecture with and without Temporal.
- **Who benefits:** what Temporal offers each persona involved.
- **How I approached this** and **Known limitations and next steps:** my reasoning, and what I left out.

## Installation

Tested on Python 3.12.5. You need 3 things: the Temporal CLI (which runs a local Temporal
server), a Python virtual environment, and the Python packages.

### 1. Install the Temporal CLI

| OS | Command |
| --- | --- |
| macOS | `brew install temporal` |
| Linux (or macOS without Homebrew) | `curl -sSf https://temporal.download/cli.sh \| sh` |
| Windows | `scoop install temporal-cli` |

Check it installed:

```bash
temporal --version
```

### 2. Start the local Temporal server

In its own terminal (terminal 1), and leave it running:

```bash
temporal server start-dev
```

This starts the Temporal server on `localhost:7233` and the Web UI at http://localhost:8233.
It keeps data in memory, so workflows are gone when you stop it. To keep them between restarts:

```bash
temporal server start-dev --db-filename temporal.db
```

### 3. Create a Python virtual environment

From the repo root:

```bash
python3.12 -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate
python --version                 # should print Python 3.12.x
```

Activate the environment in every terminal you use for the worker, the client or the tests.

### 4. Install the Python packages

```bash
pip install --upgrade pip
pip install -r requirements.txt
```

`requirements.txt` pins exact versions. The top-level packages are `temporalio` (the Temporal
Python SDK), `pytest` and `pytest-asyncio` (for the tests).

## Run it

| Terminal | Command | What it does |
| --- | --- | --- |
| 1 | `temporal server start-dev` | Local Temporal server and Web UI (step 2) |
| 2 | `python worker.py` | Runs the workflow and activity code |
| 3 | `python client.py 1` | Places order 1 and waits for the result |

Then open http://localhost:8233 to see order 1's event history.

Run the tests (no server needed, pytest starts its own):

```bash
pytest -v
```

## Demo scenarios

| # | Scenario | Command | What you should see |
| --- | --- | --- | --- |
| 1 | Happy path | `python client.py 1` | `COMPLETED, tracking track-1`, with all 4 steps and an email in the worker log |
| 2 | Flaky gateway | `python client.py 2 gateway_flaky` | Authorize attempts 1 and 2 fail, attempt 3 succeeds, then COMPLETED |
| 3 | Card declined | `python client.py 3 card_declined` | 1 attempt only, `voided auth-3` (does nothing if no hold exists), `FAILED: Order failed: Card declined` |
| 4 | Out of stock | `python client.py 4 out_of_stock` | `released res-4`, `voided auth-4`, then FAILED |
| 5 | Bad address | `python client.py 5 bad_address` | `refunded charge-5`, then `released res-5`, then FAILED |
| 6 | Worker crash | `python client.py 6 slow_inventory`, kill the worker during the 10 s wait (`pkill -9 -f worker.py`), restart it | Reserve stops heartbeating, is retried after 5 s, and the order completes about 15 s after the kill. Authorize is not re-run. |
| 7 | Duplicate order | `python client.py 1` again | `Not started: order-1 already exists`: the same order cannot run twice |
| 8 | Customer cancels | `python client.py 8 slow_inventory`, then in another terminal during the wait: `python client.py cancel 8` | `Cancel accepted while RESERVING`. Reserve finishes, then `released res-8`, `voided auth-8`, `CANCELLED` |
| 9 | Operator cancels | `python client.py 9 slow_inventory`, then during the wait: `temporal workflow cancel --workflow-id order-9` | Reserve stops at once (no `reserved` line), then `released res-9`, `voided auth-9`, `CANCELLED`. Status in the Web UI: Canceled |
| 10 | Cancel too late | `python client.py cancel 1` | `Cancel rejected`: the order already finished. While an order is shipping, the reply is `Too late to cancel` |

While order 6 runs, check its status:

```bash
temporal workflow query --workflow-id order-6 --name get_status
```

## Design

### 1. Business process

![Business process: each step and its undo](diagrams/1-business-process.svg)

If a step fails, the undo for every step that started is run, newest first. That includes
the step that failed: it may have taken effect before failing.

| Step that fails | Undo run, in this order | Order ends as |
| --- | --- | --- |
| Authorize | Void authorization | FAILED |
| Reserve inventory | Release inventory, Void authorization | FAILED |
| Capture payment | Refund payment, Release inventory, Void authorization | FAILED |
| Ship order | Refund payment, Release inventory | FAILED |
| Notify customer | nothing, failure is logged | COMPLETED |
| Customer or operator cancels (before shipping) | Same as a failure at the current step | CANCELLED |

**Why each undo is added before its step runs:** a step can take effect and still never report
back. A capture can charge the card and then lose its response, or a cancel can stop the
reservation activity after the warehouse already reserved the stock. If the undo were only added
after a step reported success, that charge or reservation would never be undone. So every undo
is added first, and every undo is idempotent and keyed by order ID: if there is nothing to undo,
it does nothing.

**Two ways to cancel:**

- **Customer cancel** is an Update (`OrderWorkflow.cancel`). Its validator accepts it only before
  shipping, so the customer gets an immediate yes or no. It takes effect when the current step
  finishes, then the undo runs. The order ends as FAILED with type `OrderCancelled`.
- **Operator cancel** (`temporal workflow cancel`) stops the running step. Reserve heartbeats, so
  it hears the cancel within seconds. Each step waits for the activity to actually stop before
  the undo runs (`WAIT_CANCELLATION_COMPLETED`); otherwise the activity could finish its side
  effect after the undo. If a step finishes anyway, the workflow checks for the cancel before
  the next step (`workflow.cancellation_reason()`), so the cancel is never lost. The workflow
  ends as Canceled.

Transient failures (a gateway timeout) are retried with backoff. Business failures
(card declined, out of stock, bad address) are not retried: the order compensates.

## Before and after Temporal

### 2. External services: the happy path

![External services on the happy path](diagrams/2-external-services.svg)

On the happy path, the order calls 4 external systems: Stripe for payments, an inventory
service, a carrier API and an email provider. Every one of these calls can time out,
return an error or rate-limit.

### 3. Making it resilient by hand

![Resiliency logic built by hand around the order service](diagrams/3-hand-built-resiliency.svg)

Without Temporal, the 5 business steps need about 8 extra components to survive failures:
retries, queues, a DLQ, compensation handlers, a status table, an idempotency store and
two cron jobs. That resiliency logic is spread across services and is most of the code.

### 4. Durable execution with Temporal

![Durable execution with Temporal](diagrams/4-durable-execution.svg)

With Temporal, the order is one workflow function. 6 of the 8 red components move into the platform:

| Hand-built (diagram 3) | With Temporal | In the code |
| --- | --- | --- |
| Order status table | Event history, plus a query for the current step | `get_status` |
| Retry and backoff wrappers | Retry policy per activity, run by the server | `STEP_RETRY`, `UNDO_RETRY` |
| Queues to carrier and email | Task queue that workers poll | `TASK_QUEUE = "orders"` |
| Dead-letter queue | Non-retryable errors trigger the undo; stuck work shows in the Web UI | `non_retryable=True` |
| Compensation handlers | One list in one place, run newest first | `compensations` |
| Stuck-order cron | Per-attempt timeout, then a retry on any worker | `STEP_TIMEOUT` |

The other 2 stay outside the platform:

- **Idempotency keys stay yours.** Temporal does not replace idempotency at Stripe. Each payment
  call should send an idempotency key per step, such as `{order_id}-auth` or `{order_id}-capture`,
  so a retried call never creates a duplicate payment.
- **Reconciliation becomes a Schedule** (see Known limitations and next steps). It stays as a safety check,
  not as the way failed orders are recovered.

**Why forward steps retry with no attempt cap:** if a step gave up after N timeouts, its side
effect might still have happened, for example a capture that went through but whose response was
lost. The undo would then run against the wrong state (voiding an authorization that was already
captured). So transient errors retry until the outcome is known, and only business failures stop
a step and trigger the undo. The undo-before-step rule above covers the cases where a step's
outcome is still unknown when the order stops, such as a cancel.

## Who benefits, and how

| Persona | Pain today | What's in it for them | Where this repo shows it |
| --- | --- | --- | --- |
| CTO / VP Engineering | Checkout is the revenue path, and its resiliency code costs more to build and run than the features | Teams ship business logic instead of plumbing; one platform for orders, refunds and payouts | Diagram 3 vs diagram 4: 6 of 8 hand-built components move into the platform |
| Developers | Retry loops, status tables and undo handlers in every service; failure paths are hard to test | The order is one readable function; retries and timeouts are config; time-skipping tests run in seconds | `workflow.py`, `tests/test_workflow.py` |
| SRE / on-call | Paged for stuck orders, DLQs to drain, risky restarts | Kill or redeploy workers at any time; every order's history is in the Web UI | Demo scenario 6 (worker crash) |
| Payments and finance ops | Double charges, card holds left behind, reconciliation finds problems the next day | Every completed step is undone on failure; an order ID cannot run twice | Failure table under diagram 1, `REJECT_DUPLICATE` |
| Customer support | "Where is my order?" means asking engineering | The current step is one query away | `get_status` |
| Product manager | Adding a step (fraud check, cancel window, delivery wait) is a multi-team project | A new step is a new activity call or signal in one function | Known limitations and next steps |

## How I approached this
- **Why this use case:** I work with many e-commerce businesses that integrate Stripe.
  I asked what happens in their business after the API call that accepts a payment:
  card holds, inventory, shipping, and refunds when something breaks.
- **Made failures easy to show:** a small command-line client (`client.py`) triggers each scenario
  (failed payment, flaky gateway, no inventory, bad address, worker crash). It also let me
  build and test the workflow one step at a time.
- **Considered but kept out:** see Known limitations and next steps. Worth knowing, but not needed
  to show the core idea.

## Known limitations and next steps

The goal was to show the core idea. So these are some of deliberate gaps,
each with the Temporal feature that should close it.

| Gap today | Why it matters | Next step | Temporal docs |
| --- | --- | --- | --- |
| Forward steps retry until the outcome is known | A long Stripe outage leaves an order waiting, not failed | `schedule_to_close_timeout` plus an alert | [Activity timeouts](https://docs.temporal.io/develop/python/activities/timeouts) |
| An undo that can never succeed retries forever | A rejected refund leaves the order in COMPENSATING with no alert | Alert after N attempts, then hand over to a person with a signal | [Signals, Queries and Updates](https://docs.temporal.io/develop/python/workflows/message-passing) |
| Mocks send no idempotency keys | A retried capture or refund could run twice at a real gateway | Send a key per step and per undo, such as `{order_id}-capture` and `{order_id}-refund` | [Activity idempotency](https://docs.temporal.io/activity-definition#idempotency) |
| Only reserve heartbeats; other steps rely on the 15 s timeout | A slow but healthy call is retried while still running; a dead worker takes 15 s to notice; an operator cancel waits for the step to finish | Per-service timeouts and heartbeats on every long call | [Detecting activity failures](https://docs.temporal.io/encyclopedia/detecting-activity-failures) |
| A customer cancel waits for the current step to finish | A cancel during a slow step takes effect only after that step | Race the step against the cancel flag and cancel the activity | [Cancellation](https://docs.temporal.io/develop/python/workflows/cancellation) |
| Customer email stored in plain text in the event history | Visible to anyone with Web UI access | Encrypt payloads with a payload codec and codec server | [Payload encryption](https://docs.temporal.io/develop/python/data-handling/data-encryption) |

Also next: delivery and dispute signals with a durable timer,
a reconciliation Schedule, workflow versioning, and a Temporal Cloud connection.

A failed order's ID cannot be reused (`REJECT_DUPLICATE`). That is deliberate: it prevents a
double charge, and the customer places a new order instead.