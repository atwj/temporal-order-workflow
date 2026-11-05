"""OrderWorkflow: the whole order flow as one function.

Temporal records the result of every step in the workflow's event history.
If the worker crashes, a new worker replays that history and carries on
from the step it was on. So this code must be deterministic: no network,
no random, no clock. All side effects go through activities.
"""

import asyncio
from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ActivityError, ApplicationError, CancelledError
from temporalio.workflow import ActivityCancellationType

with workflow.unsafe.imports_passed_through():
    from activities import (
        authorize_payment,
        capture_payment,
        refund_payment,
        release_inventory,
        reserve_inventory,
        send_notification,
        ship_order,
        void_authorization,
    )
    from shared import Notification, Order

# Forward steps: retry transient errors (timeouts, 5xx) with backoff until the
# outcome is known. No attempt cap: if a step gave up after N timeouts, the
# side effect might still have happened (a charge that went through, stock that
# was reserved), and the undo would run against the wrong state.
# Business failures (non_retryable=True) still stop at once and trigger the undo.
STEP_RETRY = RetryPolicy(
    initial_interval=timedelta(seconds=1),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(seconds=30),
)
# Undo steps: no attempt cap. An undo must happen eventually.
UNDO_RETRY = RetryPolicy(maximum_interval=timedelta(seconds=30))

# A slow step heartbeats. If the heartbeats stop for this long, the worker is
# presumed dead and the step is retried on another worker.
HEARTBEAT_TIMEOUT = timedelta(seconds=5)

# A customer can cancel until the order starts shipping.
CANCELLABLE = {"RECEIVED", "AUTHORIZING", "RESERVING", "CAPTURING"}

# Max time for ONE attempt. If a worker dies mid-activity, this is how
# Temporal notices and retries the activity on another worker.
STEP_TIMEOUT = timedelta(seconds=15)


class OrderCancelled(Exception):
    """Raised inside the workflow when a cancel is seen between steps."""


@workflow.defn
class OrderWorkflow:
    def __init__(self) -> None:
        self.status = "RECEIVED"
        self.cancel_requested = False

    @workflow.query
    def get_status(self) -> str:
        return self.status

    @workflow.update
    async def cancel(self) -> str:
        """Customer cancel. Takes effect when the current step finishes."""
        self.cancel_requested = True
        return f"Cancel accepted while {self.status}"

    @cancel.validator
    def validate_cancel(self) -> None:
        # Runs before the update is accepted: the customer gets an immediate no.
        if self.status not in CANCELLABLE:
            raise ApplicationError(f"Too late to cancel: order is {self.status}")

    @workflow.run
    async def run(self, order: Order) -> str:
        # Saga: the undo for each step, added BEFORE the step runs. A step can
        # take effect and still never report back (a timeout, a cancel, a lost
        # response), so its undo must already be on the list. Every undo is
        # idempotent and keyed by order ID: if there is nothing to undo, it does
        # nothing.
        compensations = []

        try:
            self.status = "AUTHORIZING"
            compensations.append(void_authorization)
            await self.step(authorize_payment, order)
            self.check_cancel()

            self.status = "RESERVING"
            compensations.append(release_inventory)
            await self.step(reserve_inventory, order, heartbeat_timeout=HEARTBEAT_TIMEOUT)
            self.check_cancel()

            self.status = "CAPTURING"
            compensations.append(refund_payment)
            await self.step(capture_payment, order)
            # Money has moved: the hold is gone, so there is nothing to void.
            compensations.remove(void_authorization)
            self.check_cancel()

            self.status = "SHIPPING"
            tracking_id = await self.step(ship_order, order)

        except (ActivityError, OrderCancelled) as err:
            cancelled = isinstance(err, OrderCancelled) or isinstance(err.cause, CancelledError)
            self.status = "COMPENSATING"
            for undo in reversed(compensations):
                await workflow.execute_activity(
                    undo, order, start_to_close_timeout=STEP_TIMEOUT, retry_policy=UNDO_RETRY
                )
            if cancelled:
                self.status = "CANCELLED"
                await self.notify(order, "Your order was cancelled.")
                if workflow.cancellation_reason() is not None:
                    # Operator cancel: the workflow ends as Canceled.
                    raise asyncio.CancelledError()
                # Customer cancel: a business outcome, with a clear reason.
                raise ApplicationError(
                    "Order cancelled by customer", type="OrderCancelled", non_retryable=True
                )
            self.status = "FAILED"
            await self.notify(order, "Sorry, we could not complete your order.")
            reason = err.cause.message if isinstance(err.cause, ApplicationError) else str(err.cause)
            raise ApplicationError(f"Order failed: {reason}", non_retryable=True)

        self.status = "COMPLETED"
        await self.notify(order, f"Your order has shipped. Tracking: {tracking_id}")
        return tracking_id

    def check_cancel(self) -> None:
        # Two ways to cancel: the customer's cancel update, or an operator's
        # `temporal workflow cancel`. A step that finishes after an operator
        # cancel still returns its result, so check for the cancel explicitly.
        if self.cancel_requested or workflow.cancellation_reason() is not None:
            raise OrderCancelled()

    async def step(self, activity_fn, order: Order, heartbeat_timeout=None) -> str:
        return await workflow.execute_activity(
            activity_fn,
            order,
            start_to_close_timeout=STEP_TIMEOUT,
            heartbeat_timeout=heartbeat_timeout,
            retry_policy=STEP_RETRY,
            # On an operator cancel, wait until the activity has actually stopped
            # before running the undo. The default (TRY_CANCEL) moves on at once,
            # so the activity could finish its side effect after the undo ran.
            cancellation_type=ActivityCancellationType.WAIT_CANCELLATION_COMPLETED,
        )

    async def notify(self, order: Order, message: str) -> None:
        # Best effort: a failed email must not fail or undo the order.
        try:
            await workflow.execute_activity(
                send_notification,
                Notification(order.email, message),
                start_to_close_timeout=timedelta(seconds=10),
                retry_policy=RetryPolicy(maximum_attempts=3),
            )
        except ActivityError:
            workflow.logger.warning("notification failed, order not affected")
