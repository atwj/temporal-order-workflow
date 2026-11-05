"""Client: places an order (starts a workflow) and waits for the result,
or cancels an order on the customer's behalf.

Run: python client.py ORDER_ID [SIMULATE]
     SIMULATE = card_declined | gateway_flaky | out_of_stock | slow_inventory | bad_address
     python client.py cancel ORDER_ID
"""

import asyncio
import sys

from temporalio.client import Client, WorkflowFailureError, WorkflowUpdateFailedError
from temporalio.common import WorkflowIDReusePolicy
from temporalio.exceptions import ApplicationError, CancelledError, WorkflowAlreadyStartedError
from temporalio.service import RPCError

from shared import TASK_QUEUE, Order
from workflow import OrderWorkflow


async def cancel(client: Client, order_id: str) -> None:
    handle = client.get_workflow_handle(f"order-{order_id}")
    try:
        # An Update: the workflow's validator accepts or rejects it at once.
        print(await handle.execute_update(OrderWorkflow.cancel))
    except WorkflowUpdateFailedError as err:
        print(f"Cancel rejected: {err.cause.message}")
    except RPCError as err:
        print(f"Cancel rejected: {err.message}")  # order not found, or already finished


async def place(client: Client, order_id: str, simulate: str) -> None:
    order = Order(order_id, "headphones", 1, 12000, "buyer@example.com", simulate)
    try:
        handle = await client.start_workflow(
            OrderWorkflow.run,
            order,
            id=f"order-{order_id}",  # workflow ID = order ID
            task_queue=TASK_QUEUE,
            # The same order ID can never run twice, even after it finished.
            id_reuse_policy=WorkflowIDReusePolicy.REJECT_DUPLICATE,
        )
    except WorkflowAlreadyStartedError:
        print(f"Not started: order-{order_id} already exists. Use a new order ID.")
        return
    print(f"Started order-{order_id}. Web UI: http://localhost:8233")

    try:
        tracking_id = await handle.result()
        print(f"COMPLETED, tracking {tracking_id}")
    except WorkflowFailureError as err:
        cause = err.cause
        if isinstance(cause, CancelledError) or (
            isinstance(cause, ApplicationError) and cause.type == "OrderCancelled"
        ):
            print("CANCELLED: completed steps were undone")
        else:
            print(f"FAILED: {cause.message if isinstance(cause, ApplicationError) else cause}")


async def main() -> None:
    client = await Client.connect("localhost:7233")
    if sys.argv[1] == "cancel":
        await cancel(client, sys.argv[2])
    else:
        await place(client, sys.argv[1], sys.argv[2] if len(sys.argv) > 2 else "")


if __name__ == "__main__":
    asyncio.run(main())
