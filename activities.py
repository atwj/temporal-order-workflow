"""Activities: the only code that talks to the outside world.

Each one calls a stand-in service (fake_services.py) for Stripe, the
warehouse, the carrier or email.

Three rules every activity follows:

1. Idempotency key on every call. Temporal runs activities at least once:
   a retry can repeat a call that already took effect. The key is
   "{order_id}-{step}", so the repeat returns the first response.
2. Heartbeat while waiting on a service. That is how Temporal spots a dead
   worker within HEARTBEAT_TIMEOUT, and the only way a cancel reaches a
   running activity.
3. Business failures are non-retryable and BENIGN. Retrying a declined card
   never helps, and BENIGN stops the SDK logging a stack trace for an
   expected outcome. Unexpected errors are retried and logged in full.
"""

import asyncio

from temporalio import activity
from temporalio.exceptions import ApplicationError, ApplicationErrorCategory

from fake_services import carrier, stripe, warehouse
from shared import Notification, Order

LATENCY = 0.2  # seconds a normal service call takes
SLOW_LATENCY = 10  # seconds the warehouse takes with simulate="slow_inventory"
HEARTBEAT_EVERY = 1  # seconds; must be well under the workflow's HEARTBEAT_TIMEOUT


def business_failure(message: str, error_type: str) -> ApplicationError:
    return ApplicationError(
        message,
        type=error_type,
        non_retryable=True,
        category=ApplicationErrorCategory.BENIGN,
    )


async def call_service(seconds: float, request):
    """Wait for a service response, heartbeating in the background.

    A real call is one request that blocks, so the heartbeat runs alongside it.
    If the activity is cancelled, the wait raises asyncio.CancelledError and the
    request is never sent: the service is only called after the wait.
    """

    async def heartbeat_loop():
        while True:
            activity.heartbeat()
            await asyncio.sleep(HEARTBEAT_EVERY)

    heartbeats = asyncio.create_task(heartbeat_loop())
    try:
        await asyncio.sleep(seconds)  # network and service time
        return request()
    finally:
        heartbeats.cancel()


def log_response(response, message: str = "") -> None:
    if response.replayed:
        activity.logger.info(
            f"idempotency key already used: returned {response.result}, nothing changed"
        )
    else:
        activity.logger.info(message or response.result)


# ------------------------------------------------------------ payment

@activity.defn
async def authorize_payment(order: Order) -> str:
    if order.simulate == "card_declined":
        activity.logger.info("card declined")
        raise business_failure("Card declined", "CardDeclined")
    if order.simulate == "gateway_flaky" and activity.info().attempt < 3:
        activity.logger.info("gateway timeout")
        # A plain exception is retryable: Temporal retries it per the retry policy.
        raise RuntimeError("Payment gateway timeout")
    response = await call_service(
        LATENCY, lambda: stripe.authorize(order.order_id, key=f"{order.order_id}-auth")
    )
    log_response(response, f"authorized {order.amount_cents} cents")
    return response.result


@activity.defn
async def void_authorization(order: Order) -> None:
    response = await call_service(
        LATENCY, lambda: stripe.void(order.order_id, key=f"{order.order_id}-void")
    )
    log_response(response)


@activity.defn
async def capture_payment(order: Order) -> str:
    response = await call_service(
        LATENCY, lambda: stripe.capture(order.order_id, key=f"{order.order_id}-capture")
    )
    log_response(response, f"captured {order.amount_cents} cents")
    return response.result


@activity.defn
async def refund_payment(order: Order) -> None:
    if order.simulate == "refund_rejected":
        activity.logger.info("Stripe rejected the refund")
        raise business_failure("Refund rejected", "RefundRejected")
    response = await call_service(
        LATENCY, lambda: stripe.refund(order.order_id, key=f"{order.order_id}-refund")
    )
    log_response(response)


# ---------------------------------------------------------- inventory

@activity.defn
async def reserve_inventory(order: Order) -> str:
    if order.simulate == "out_of_stock":
        activity.logger.info(f"{order.item} out of stock")
        raise business_failure("Out of stock", "OutOfStock")
    seconds = LATENCY
    if order.simulate == "slow_inventory":
        activity.logger.info("warehouse is slow: kill the worker or cancel the order now")
        seconds = SLOW_LATENCY
    response = await call_service(
        seconds, lambda: warehouse.reserve(order.order_id, key=f"{order.order_id}-reserve")
    )
    log_response(response, f"reserved {order.quantity} x {order.item}")
    return response.result


@activity.defn
async def release_inventory(order: Order) -> None:
    response = await call_service(
        LATENCY, lambda: warehouse.release(order.order_id, key=f"{order.order_id}-release")
    )
    log_response(response)


# ----------------------------------------------------------- shipping

@activity.defn
async def ship_order(order: Order) -> str:
    if order.simulate in ("bad_address", "refund_rejected"):
        activity.logger.info("carrier rejected the address")
        raise business_failure("Bad address", "BadAddress")
    response = await call_service(
        LATENCY, lambda: carrier.book(order.order_id, key=f"{order.order_id}-ship")
    )
    log_response(response, "shipment booked")
    return response.result


# ------------------------------------------------------- notification

@activity.defn
async def send_notification(note: Notification) -> None:
    # Email providers accept an idempotency key too; a duplicate email is
    # harmless here, so the demo skips it.
    await call_service(LATENCY, lambda: None)
    activity.logger.info(f"email to {note.email}: {note.message}")
