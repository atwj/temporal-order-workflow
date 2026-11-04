"""OrderWorkflow: the whole order flow as one function.

Temporal records the result of every step in the workflow's event history.
If the worker crashes, a new worker replays that history and carries on
from the step it was on. So this code must be deterministic: no network,
no random, no clock. All side effects go through activities.
"""

from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ActivityError, ApplicationError

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

# Max time for ONE attempt. If a worker dies mid-activity, this is how
# Temporal notices and retries the activity on another worker.
STEP_TIMEOUT = timedelta(seconds=15)


@workflow.defn
class OrderWorkflow:
    def __init__(self) -> None:
        self.status = "RECEIVED"

    @workflow.query
    def get_status(self) -> str:
        return self.status

    @workflow.run
    async def run(self, order: Order) -> str:
        # Saga: the undo for each completed step, in the order they happened.
        compensations = []

        try:
            self.status = "AUTHORIZING"
            auth_id = await self.step(authorize_payment, order)
            compensations.append((void_authorization, auth_id))

            self.status = "RESERVING"
            reservation_id = await self.step(reserve_inventory, order)
            compensations.append((release_inventory, reservation_id))

            self.status = "CAPTURING"
            charge_id = await self.step(capture_payment, order)
            # Money has moved: the undo is now a refund, not a void.
            compensations.remove((void_authorization, auth_id))
            compensations.append((refund_payment, charge_id))

            self.status = "SHIPPING"
            tracking_id = await self.step(ship_order, order)

        except ActivityError as err:
            # A step failed for good (non-retryable, or out of retries).
            self.status = "COMPENSATING"
            for undo, arg in reversed(compensations):
                await workflow.execute_activity(
                    undo, arg, start_to_close_timeout=STEP_TIMEOUT, retry_policy=UNDO_RETRY
                )
            self.status = "FAILED"
            await self.notify(order, "Sorry, we could not complete your order.")
            reason = err.cause.message if isinstance(err.cause, ApplicationError) else str(err.cause)
            raise ApplicationError(f"Order failed: {reason}", non_retryable=True)

        self.status = "COMPLETED"
        await self.notify(order, f"Your order has shipped. Tracking: {tracking_id}")
        return tracking_id

    async def step(self, activity_fn, order: Order) -> str:
        return await workflow.execute_activity(
            activity_fn, order, start_to_close_timeout=STEP_TIMEOUT, retry_policy=STEP_RETRY
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