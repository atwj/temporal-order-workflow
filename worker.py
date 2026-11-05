"""Worker: polls the task queue and runs workflow and activity code.

It keeps no state. Kill it any time; restart it and orders carry on.
Run: python worker.py
"""

import asyncio
import logging

from temporalio import activity, workflow
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


class OrderLogFormatter(logging.Formatter):
    """Prefixes each line with the order and step the SDK attaches to it.

    activity.logger and workflow.logger add the workflow ID, activity type and
    attempt to every record, so each line can be traced to one order.
    """

    def format(self, record: logging.LogRecord) -> str:
        act = getattr(record, "temporal_activity", None)
        wf = getattr(record, "temporal_workflow", None)
        if act:
            prefix = f"[{act['workflow_id']} {act['activity_type']} attempt {act['attempt']}]"
        elif wf:
            prefix = f"[{wf['workflow_id']} workflow]"
        else:
            prefix = f"[{record.name}]"
        line = f"{record.levelname:<7} {prefix} {record.getMessage()}"
        if record.exc_info:
            line += "\n" + self.formatException(record.exc_info)
        return line


def configure_logging() -> None:
    handler = logging.StreamHandler()
    handler.setFormatter(OrderLogFormatter())
    logging.basicConfig(level=logging.WARNING, handlers=[handler])
    # Activity progress is logged at INFO; everything else at WARNING and above.
    logging.getLogger("temporalio.activity").setLevel(logging.INFO)
    logging.getLogger("temporalio.workflow").setLevel(logging.INFO)
    # The order and step go in the prefix, not appended to every message.
    activity.logger.activity_info_on_message = False
    workflow.logger.workflow_info_on_message = False


async def main() -> None:
    configure_logging()
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