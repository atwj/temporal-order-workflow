"""OrderWorkflow: the whole order flow as one function.

Temporal records the result of every step in the workflow's event history.
If the worker crashes, a new worker replays that history and carries on
from the step it was on. So this code must be deterministic: no network,
no random, no clock. All side effects go through activities.
"""

import asyncio
from datetime import timedelta
from typing import Callable, Optional

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ActivityError, ApplicationError, CancelledError
from temporalio.workflow import ActivityCancellationType, ActivityHandle

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

# Forward steps: retry transient errors (timeouts, 5xx) with backoff, for up to
# STEP_DEADLINE in total. Then the step fails and the order is undone. That is
# safe even if the step took effect on its last attempt: its undo was added
# before it ran, and every undo is idempotent.
# Business failures (non_retryable=True) stop at once and trigger the undo.
STEP_RETRY = RetryPolicy(
    initial_interval=timedelta(seconds=1),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(seconds=30),
)
STEP_DEADLINE = timedelta(minutes=2)  # schedule_to_close: all attempts together

# Undo steps: retry transient errors up to UNDO_ATTEMPTS times. If an undo
# still fails, or is rejected (a refused refund), a person decides what to do.
UNDO_ATTEMPTS = 10
UNDO_RETRY = RetryPolicy(
    initial_interval=timedelta(seconds=1),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(seconds=30),
    maximum_attempts=UNDO_ATTEMPTS,
)

# Max time for ONE attempt.
STEP_TIMEOUT = timedelta(seconds=15)

# Every activity heartbeats while it waits on a service. If heartbeats stop
# for this long, the worker is presumed dead and the attempt is retried on
# another worker. A cancel reaches a running activity with its next heartbeat.
HEARTBEAT_TIMEOUT = timedelta(seconds=5)

# A customer can cancel until the order starts shipping.
CANCELLABLE = {"RECEIVED", "AUTHORIZING", "RESERVING", "CAPTURING"}

# What ops can decide when an undo cannot succeed.
OPS_DECISIONS = {"retry", "done_manually"}


class OrderCancelled(Exception):
    """Raised inside the workflow when a cancel is seen between steps."""


@workflow.defn
class OrderWorkflow:
    def __init__(self) -> None:
        self.status = "RECEIVED"
        self.cancel_requested = False
        self.stuck_undo: Optional[str] = None
        self.ops_decision: Optional[str] = None

    # --------------------------------------------------- queries and updates

    @workflow.query
    def get_status(self) -> str:
        if self.status == "NEEDS_ATTENTION":
            return f"NEEDS_ATTENTION: {self.stuck_undo} failed"
        return self.status

    @workflow.update
    async def cancel(self) -> str:
        """Customer cancel. Stops the running step, then undoes the order."""
        self.cancel_requested = True
        return f"Cancel accepted while {self.status}"

    @cancel.validator
    def validate_cancel(self) -> None:
        # Runs before the update is accepted: the customer gets an immediate no.
        if self.status not in CANCELLABLE:
            raise ApplicationError(f"Too late to cancel: order is {self.status}")

    @workflow.update
    async def resolve_undo(self, decision: str) -> str:
        """Ops: after an undo failed, either retry it or confirm it was done by hand."""
        stuck = self.stuck_undo
        self.ops_decision = decision
        return f"{decision} recorded for {stuck}"

    @resolve_undo.validator
    def validate_resolve_undo(self, decision: str) -> None:
        if self.status != "NEEDS_ATTENTION":
            raise ApplicationError(f"Nothing to resolve: order is {self.status}")
        if decision not in OPS_DECISIONS:
            raise ApplicationError(f"Decision must be one of {sorted(OPS_DECISIONS)}")

    # ------------------------------------------------------------- the order

    @workflow.run
    async def run(self, order: Order) -> str:
        # Saga: the undo for each step, added BEFORE the step runs. A step can
        # take effect and still never report back (a timeout, a cancel, a lost
        # response), so its undo must already be on the list. Every undo is
        # idempotent: if there is nothing to undo, it does nothing.
        compensations = []

        try:
            self.status = "AUTHORIZING"
            compensations.append(void_authorization)
            await self.step(authorize_payment, order)
            self.check_cancel()

            self.status = "RESERVING"
            compensations.append(release_inventory)
            await self.step(reserve_inventory, order)
            self.check_cancel()

            self.status = "CAPTURING"
            compensations.append(refund_payment)
            await self.step(capture_payment, order)
            # Money has moved: the hold is gone, so there is nothing to void.
            compensations.remove(void_authorization)
            self.check_cancel()

            # Once the parcel is with the carrier, the order can no longer be
            # cancelled: the step is not interrupted, and a cancel is ignored.
            self.status = "SHIPPING"
            tracking_id = await self.step(ship_order, order, cancellable=False)
            if workflow.cancellation_reason() is not None:
                workflow.logger.warning("Cancel ignored: the order has already shipped")

        except (ActivityError, OrderCancelled) as err:
            cancelled = isinstance(err, OrderCancelled) or isinstance(err.cause, CancelledError)
            self.status = "COMPENSATING"
            for undo in reversed(compensations):
                await self.run_undo(undo, order)
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

    # --------------------------------------------------------------- helpers

    async def step(self, activity_fn, order: Order, cancellable: bool = True) -> str:
        """Run one forward step. A cancel stops it, unless cancellable=False."""
        handle = workflow.start_activity(
            activity_fn,
            order,
            start_to_close_timeout=STEP_TIMEOUT,
            schedule_to_close_timeout=STEP_DEADLINE,
            heartbeat_timeout=HEARTBEAT_TIMEOUT,
            retry_policy=STEP_RETRY,
            # After a cancel, wait until the activity has actually stopped
            # before the undo runs. The default (TRY_CANCEL) moves on at once,
            # so the activity could still finish its side effect after the undo.
            cancellation_type=ActivityCancellationType.WAIT_CANCELLATION_COMPLETED,
        )
        if cancellable:
            try:
                # Whichever comes first: the step finishes, or the customer cancels.
                await workflow.wait_condition(lambda: handle.done() or self.cancel_requested)
            except asyncio.CancelledError:
                pass  # an operator cancel: stop the activity below
            if not handle.done():
                handle.cancel()  # reaches the activity with its next heartbeat
        return await self.finish(handle)

    async def run_undo(self, undo, order: Order) -> None:
        """Run one undo to completion. A cancel never stops an undo."""
        while True:
            handle = workflow.start_activity(
                undo,
                order,
                start_to_close_timeout=STEP_TIMEOUT,
                heartbeat_timeout=HEARTBEAT_TIMEOUT,
                retry_policy=UNDO_RETRY,
            )
            try:
                await self.finish(handle)
                return
            except ActivityError as err:
                # Out of retries, or rejected (a refused refund). Hand over to a
                # person instead of retrying forever, and wait for their decision.
                self.stuck_undo = undo.__name__
                self.status = "NEEDS_ATTENTION"
                workflow.logger.error(
                    f"{undo.__name__} failed ({err.cause}). Waiting for ops: "
                    f"resolve_undo with {sorted(OPS_DECISIONS)}"
                )
                await self.wait_ignoring_cancel(lambda: self.ops_decision is not None)
                decision, self.ops_decision, self.stuck_undo = self.ops_decision, None, None
                self.status = "COMPENSATING"
                if decision == "done_manually":
                    return
                # "retry": run the undo again

    @staticmethod
    async def finish(handle: ActivityHandle):
        """Await an activity that a workflow cancel must not interrupt.

        A workflow cancel interrupts whatever the workflow is awaiting. The
        shield keeps the activity running; the loop keeps waiting for it.
        """
        while True:
            try:
                return await asyncio.shield(handle)
            except asyncio.CancelledError:
                if handle.done():
                    return handle.result()
                continue

    @staticmethod
    async def wait_ignoring_cancel(condition: Callable[[], bool]) -> None:
        while True:
            try:
                await workflow.wait_condition(condition)
                return
            except asyncio.CancelledError:
                continue

    def check_cancel(self) -> None:
        # Two ways to cancel: the customer's cancel update, or an operator's
        # `temporal workflow cancel`. A step that finishes after a cancel still
        # returns its result, so check for the cancel explicitly.
        if self.cancel_requested or workflow.cancellation_reason() is not None:
            raise OrderCancelled()

    async def notify(self, order: Order, message: str) -> None:
        # Best effort: a failed email must not fail or undo the order.
        handle = workflow.start_activity(
            send_notification,
            Notification(order.email, message),
            start_to_close_timeout=timedelta(seconds=10),
            heartbeat_timeout=HEARTBEAT_TIMEOUT,
            retry_policy=RetryPolicy(maximum_attempts=3),
        )
        try:
            await self.finish(handle)
        except ActivityError:
            workflow.logger.warning("notification failed, order not affected")
