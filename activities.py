"""Activities: the only code that talks to the outside world.

Each one stands in for a call to another system (payment gateway, warehouse,
carrier, email). They are mocked: they print and return fake IDs.

Temporal may run an activity more than once (a retry after a timeout), so a
real version must be idempotent. Here the IDs are derived from the order ID,
so a retry returns the same ID: that is the idempotency key idea.
"""

import asyncio

from temporalio import activity
from temporalio.exceptions import ApplicationError

from shared import Notification, Order


def log(message: str) -> None:
    info = activity.info()
    print(f"[{info.activity_type} attempt {info.attempt}] {message}")


# ------------------------------------------------------------ payment

@activity.defn
async def authorize_payment(order: Order) -> str:
    if order.simulate == "card_declined":
        log("card declined")
        # A business failure: retrying will never help, so don't.
        raise ApplicationError("Card declined", type="CardDeclined", non_retryable=True)
    if order.simulate == "gateway_flaky" and activity.info().attempt < 3:
        log("gateway timeout")
        # A plain exception is retryable: Temporal retries it per the retry policy.
        raise RuntimeError("Payment gateway timeout")
    log(f"authorized {order.amount_cents} cents")
    return f"auth-{order.order_id}"


@activity.defn
async def void_authorization(auth_id: str) -> None:
    log(f"voided {auth_id}")


@activity.defn
async def capture_payment(order: Order) -> str:
    # Real gateway call would send idempotency key f"{order.order_id}-capture".
    log(f"captured {order.amount_cents} cents")
    return f"charge-{order.order_id}"


@activity.defn
async def refund_payment(charge_id: str) -> None:
    log(f"refunded {charge_id}")


# ---------------------------------------------------------- inventory

@activity.defn
async def reserve_inventory(order: Order) -> str:
    if order.simulate == "out_of_stock":
        log(f"{order.item} out of stock")
        raise ApplicationError("Out of stock", type="OutOfStock", non_retryable=True)
    if order.simulate == "slow_inventory":
        log("warehouse is slow: kill the worker now to test recovery")
        await asyncio.sleep(10)
    log(f"reserved {order.quantity} x {order.item}")
    return f"res-{order.order_id}"


@activity.defn
async def release_inventory(reservation_id: str) -> None:
    log(f"released {reservation_id}")


# ----------------------------------------------------------- shipping

@activity.defn
async def ship_order(order: Order) -> str:
    if order.simulate == "bad_address":
        log("carrier rejected the address")
        raise ApplicationError("Bad address", type="BadAddress", non_retryable=True)
    log("shipment booked")
    return f"track-{order.order_id}"


# ------------------------------------------------------- notification

@activity.defn
async def send_notification(note: Notification) -> None:
    log(f"email to {note.email}: {note.message}")