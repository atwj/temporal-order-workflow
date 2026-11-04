"""Data shared by the workflow, activities, worker and starter."""

from dataclasses import dataclass

TASK_QUEUE = "orders"


@dataclass
class Order:
    order_id: str
    item: str
    quantity: int
    amount_cents: int
    email: str
    # Failure injection for demos: "", "card_declined", "gateway_flaky",
    # "out_of_stock", "slow_inventory", "bad_address"
    simulate: str = ""


@dataclass
class Notification:
    email: str
    message: str