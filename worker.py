"""Worker: polls the task queue and runs workflow and activity code.

It keeps no state. Kill it any time; restart it and orders carry on.
Run: python worker.py
"""

import asyncio

from temporalio.client import Client
from temporalio.worker import Worker

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
from shared import TASK_QUEUE
from workflow import OrderWorkflow


async def main() -> None:
    client = await Client.connect("localhost:7233")
    worker = Worker(
        client,
        task_queue=TASK_QUEUE,
        workflows=[OrderWorkflow],
        activities=[
            authorize_payment,
            void_authorization,
            reserve_inventory,
            release_inventory,
            capture_payment,
            refund_payment,
            ship_order,
            send_notification,
        ],
    )
    print(f"Worker listening on task queue '{TASK_QUEUE}'. Ctrl+C to stop.")
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())