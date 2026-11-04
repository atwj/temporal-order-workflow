# Order workflow on Temporal

An e-commerce order as one durable Temporal workflow (Python):
authorize -> reserve inventory -> capture -> ship -> notify,
with a saga that undoes completed steps if a later one fails.

## Run it

```bash
pip install -r requirements.txt
temporal server start-dev     # terminal 1, Web UI http://localhost:8233
python worker.py              # terminal 2
python client.py 1           # terminal 3
```

## Demo scenarios

| # | Scenario | Command | What you should see |
| --- | --- | --- | --- |
| 1 | Happy path | `python client.py 1` | `COMPLETED, tracking track-1`, with all 4 steps and an email in the worker log |
| 2 | Flaky gateway | `python client.py 2 gateway_flaky` | Authorize attempts 1 and 2 fail, attempt 3 succeeds, then COMPLETED |
| 3 | Card declined | `python client.py 3 card_declined` | 1 attempt only, no undo, `FAILED: Card declined` |
| 4 | Out of stock | `python client.py 4 out_of_stock` | `voided auth-4`, then FAILED |
| 5 | Bad address | `python client.py 5 bad_address` | `refunded charge-5`, then `released res-5`, then FAILED |
| 6 | Worker crash | `python client.py 6 slow_inventory`, kill the worker during the 10 s wait (`pkill -9 -f worker.py`), restart it | Reserve retries within about 15 s and the order completes. Authorize is not re-run. |
| 7 | Duplicate order | `python client.py 1` again | `WorkflowAlreadyStartedError`: the same order cannot run twice |

While order 6 runs, check its status:
`temporal workflow query --workflow-id order-6 --name get_status`

## Design

### 1. Business process

<!-- DRAW: 5 boxes left to right: Authorize payment -> Reserve inventory -> Capture payment -> Ship order -> Notify customer.
     Under Authorize, Reserve, Capture: a red box with its undo (Void authorization, Release inventory, Refund payment), dashed "undo" arrow.
     Note on Notify: "best effort, no undo". -->
![Order business process with each step's undo](diagrams/1-business-process.svg)

If a step fails, every completed step is undone, newest first:

| Step that fails | Undo run, in this order | Order ends as |
| --- | --- | --- |
| Authorize | nothing | FAILED |
| Reserve inventory | Void authorization | FAILED |
| Capture payment | Release inventory, Void authorization | FAILED |
| Ship order | Refund payment, Release inventory | FAILED |
| Notify customer | nothing, failure is logged | COMPLETED |

Transient failures (a gateway timeout) are retried with backoff. Business failures
(card declined, out of stock, bad address) are not retried: the order compensates.

## Before and after Temporal

### 2. External services: the happy path

<!-- DRAW: "Order service" box with the 5 steps inside. Arrows out to green hexagons:
     Stripe (Authorize, Capture), Inventory service (Reserve), Carrier API (Ship), Email (Notify).
     Orders database cylinder below. No retries, queues or crons: happy path only. -->
![Order service calling Stripe, inventory, carrier and email](diagrams/2-external-services.svg)

On the happy path, the order calls 4 external systems: Stripe for payments, an inventory
service, a carrier API and an email provider. Every one of these calls can time out,
return an error or rate-limit.

### 3. Making it resilient by hand

<!-- DRAW: same picture as 2, plus the resiliency logic in RED:
     retries on the Stripe and inventory arrows, queues to carrier and email,
     a dead-letter queue -> compensation handlers, an order status table,
     an idempotency key store, a cron "stuck-order sweeper", a cron "nightly reconciliation".
     Keep the business logic GREEN. Legend: green = business logic, red = resiliency logic. -->
![Hand-built resiliency logic around the order service](diagrams/3-hand-built-resiliency.svg)

Without Temporal, the 5 business steps need about 8 extra components to survive failures:
retries, queues, a DLQ, compensation handlers, a status table, an idempotency store and
two cron jobs. That resiliency logic is spread across services and is most of the code.

### 4. Durable execution with Temporal

<!-- DRAW: left: "Your environment" box with the Worker (OrderWorkflow: 5 steps + compensations, Temporal SDK).
     Right: "Temporal platform" box with 6 tiles: state management, retries, task queues (GREEN = used here),
     durable timers, cron/schedules, human in the loop (GREY dashed = possible enhancements).
     Arrow worker -> platform: "polls tasks, reports results". Worker arrows out to Stripe, inventory, carrier, email. -->
![The order as one Temporal workflow](diagrams/4-durable-execution.svg)

With Temporal, the order is one workflow function. The red components move into the platform:

| Hand-built (diagram 3) | With Temporal | In the code |
| --- | --- | --- |
| Order status table | Event history, plus a query for the current step | `get_status` |
| Retry and backoff wrappers | Retry policy per activity, run by the server | `STEP_RETRY`, `UNDO_RETRY` |
| Queues to carrier and email | Task queue that workers poll | `TASK_QUEUE = "orders"` |
| Dead-letter queue | Non-retryable errors trigger the undo; stuck work shows in the Web UI | `non_retryable=True` |
| Compensation handlers | One list in one place, run newest first | `compensations` |
| Stuck-order cron | Per-attempt timeout, then a retry on any worker | `STEP_TIMEOUT` |

Still my job: idempotency at Stripe (send a key such as `{order_id}-capture`) and
reconciliation as a safety check.

## How I approached this
- Why this use case: [link to your Stripe payments experience]
- Built in this order: happy path, then retries, then the saga
- Kept out on purpose: [see below]
- Time spent: [about N hours]

## Possible enhancements

| Enhancement | Why | Temporal docs |
| --- | --- | --- |
| Heartbeats on slow activities | Notice a dead worker in seconds, not after the 15 s timeout | [Activity heartbeats and timeouts](https://docs.temporal.io/develop/python/activities/timeouts), [Detecting activity failures](https://docs.temporal.io/encyclopedia/detecting-activity-failures) |
| Signals for delivery confirmation and a dispute window | Human in the loop: wait for the carrier or the buyer, with a durable timer | [Signals, Queries and Updates](https://docs.temporal.io/develop/python/workflows/message-passing) |
| An Update with a validator to cancel before shipping | The customer gets an immediate accept or reject | [Update handlers and validators](https://docs.temporal.io/develop/python/workflows/message-passing) |
| Idempotency keys sent to the payment gateway | A retried capture must never charge twice | [Activity idempotency](https://docs.temporal.io/activity-definition#idempotency) |
| A Schedule for daily reconciliation | Replaces a cron job; durable, pausable, visible in the UI | [Schedules](https://docs.temporal.io/develop/python/workflows/schedules) |
| Workflow versioning | Deploy code changes safely while orders are still running | [Versioning and patching](https://docs.temporal.io/develop/python/workflows/versioning) |