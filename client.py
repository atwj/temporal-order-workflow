"""Client: places an order (starts a workflow) and waits for the result.

Run: python client.py ORDER_ID [SIMULATE]
     SIMULATE = card_declined | gateway_flaky | out_of_stock | slow_inventory | bad_address
"""

import asyncio
import sys

from temporalio.client import Client, WorkflowFailureError
from temporalio.common import WorkflowIDReusePolicy
from temporalio.exceptions import ApplicationError, WorkflowAlreadyStartedError

from shared import TASK_QUEUE, Order
from workflow import OrderWorkflow


async def main() -> None:
    order_id = sys.argv[1]
    simulate = sys.argv[2] if len(sys.argv) > 2 else ""
    order = Order(order_id, "headphones", 1, 12000, "buyer@example.com", simulate)

    client = await Client.connect("localhost:7233")
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
        print(f"FAILED: {cause.message if isinstance(cause, ApplicationError) else cause}")


if __name__ == "__main__":
    asyncio.run(main())