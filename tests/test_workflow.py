"""Tests for OrderWorkflow.

They run against Temporal's local test server, which pytest starts for you
(first run downloads it). Time-skipping makes retry backoff instant.

Most tests swap the real activities for mocks that record what ran. That
lets each test check the exact saga order: which undo steps ran, and in
which order.
"""

import asyncio
import uuid

import pytest
from temporalio import activity
from temporalio.client import WorkflowFailureError, WorkflowUpdateFailedError
from temporalio.common import WorkflowIDReusePolicy
from temporalio.exceptions import ApplicationError, CancelledError, WorkflowAlreadyStartedError
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Replayer, Worker

import activities as real
from shared import Order
from workflow import OrderWorkflow

calls: list[str] = []


@pytest.fixture
async def env():
    calls.clear()
    async with await WorkflowEnvironment.start_time_skipping() as env:
        yield env


def mock(name, returns=None, fail=False, flaky=False, fail_times=2):
    """A fake activity with the real one's name, recording that it ran."""

    @activity.defn(name=name)
    async def fake(arg):
        calls.append(name)
        if fail:
            raise ApplicationError(f"{name} failed", non_retryable=True)
        if flaky and activity.info().attempt <= fail_times:
            raise RuntimeError("timeout")
        return returns

    return fake


def mocks(**overrides):
    """All 8 activities as mocks. Override one to make it fail."""
    acts = {
        "authorize_payment": mock("authorize_payment", "auth-1"),
        "void_authorization": mock("void_authorization"),
        "reserve_inventory": mock("reserve_inventory", "res-1"),
        "release_inventory": mock("release_inventory"),
        "capture_payment": mock("capture_payment", "charge-1"),
        "refund_payment": mock("refund_payment"),
        "ship_order": mock("ship_order", "track-1"),
        "send_notification": mock("send_notification"),
    }
    acts.update(overrides)
    return list(acts.values())


def new_order(simulate=""):
    return Order(uuid.uuid4().hex[:8], "headphones", 1, 12000, "a@b.com", simulate)


async def run(env, acts, order=None):
    """Run one order to the end. Returns (handle, result or the failure)."""
    order = order or new_order()
    queue = f"test-{uuid.uuid4().hex[:6]}"
    async with Worker(env.client, task_queue=queue, workflows=[OrderWorkflow], activities=acts):
        handle = await env.client.start_workflow(
            OrderWorkflow.run, order, id=f"order-{order.order_id}", task_queue=queue
        )
        try:
            return handle, await handle.result()
        except WorkflowFailureError as err:
            return handle, err


# ------------------------------------------------------------ happy path

async def test_happy_path_runs_every_step_in_order(env):
    _, result = await run(env, mocks())
    assert result == "track-1"
    assert calls == [
        "authorize_payment", "reserve_inventory", "capture_payment",
        "ship_order", "send_notification",
    ]


# --------------------------------------------------------------- retries

async def test_flaky_gateway_is_retried_until_it_works(env):
    _, result = await run(env, mocks(authorize_payment=mock("authorize_payment", "auth-1", flaky=True)))
    assert result == "track-1"
    assert calls.count("authorize_payment") == 3


async def test_transient_failures_retry_until_the_outcome_is_known(env):
    # 7 timeouts in a row: there is no attempt cap, so the order still completes
    # instead of undoing a step whose outcome is unknown.
    flaky = mock("capture_payment", "charge-1", flaky=True, fail_times=7)
    _, result = await run(env, mocks(capture_payment=flaky))
    assert result == "track-1"
    assert calls.count("capture_payment") == 8
    assert "void_authorization" not in calls


async def test_business_failure_is_not_retried(env):
    _, result = await run(env, mocks(authorize_payment=mock("authorize_payment", fail=True)))
    assert isinstance(result, WorkflowFailureError)
    assert calls.count("authorize_payment") == 1


# ---------------------------------------------------------------- saga

async def test_card_declined_voids_defensively(env):
    # The void does nothing if no hold exists. It is there in case an earlier
    # attempt placed a hold and then timed out.
    await run(env, mocks(authorize_payment=mock("authorize_payment", fail=True)))
    assert calls == ["authorize_payment", "void_authorization", "send_notification"]


async def test_out_of_stock_voids_the_authorization(env):
    await run(env, mocks(reserve_inventory=mock("reserve_inventory", fail=True)))
    assert calls == [
        "authorize_payment", "reserve_inventory",
        "release_inventory", "void_authorization", "send_notification",
    ]


async def test_capture_failure_undoes_newest_first(env):
    await run(env, mocks(capture_payment=mock("capture_payment", fail=True)))
    assert calls == [
        "authorize_payment", "reserve_inventory", "capture_payment",
        # Refund too: the capture may have charged the card before it failed.
        "refund_payment", "release_inventory", "void_authorization",
        "send_notification",
    ]


async def test_ship_failure_refunds_instead_of_voiding(env):
    _, result = await run(env, mocks(ship_order=mock("ship_order", fail=True)))
    assert isinstance(result, WorkflowFailureError)
    assert calls == [
        "authorize_payment", "reserve_inventory", "capture_payment", "ship_order",
        "refund_payment", "release_inventory", "send_notification",
    ]
    assert "void_authorization" not in calls


async def test_failed_email_does_not_fail_the_order(env):
    _, result = await run(env, mocks(send_notification=mock("send_notification", fail=True)))
    assert result == "track-1"
    assert "refund_payment" not in calls


# ------------------------------------------------- query, IDs, replay

async def test_status_query_reports_completed(env):
    order = new_order()
    async with Worker(env.client, task_queue="query-q", workflows=[OrderWorkflow], activities=mocks()):
        handle = await env.client.start_workflow(
            OrderWorkflow.run, order, id=f"order-{order.order_id}", task_queue="query-q"
        )
        await handle.result()
        # A query reads the workflow's state; the worker answers it.
        assert await handle.query(OrderWorkflow.get_status) == "COMPLETED"


async def test_same_order_id_cannot_run_twice(env):
    order = new_order()
    queue = "dup-q"
    async with Worker(env.client, task_queue=queue, workflows=[OrderWorkflow], activities=mocks()):
        start = dict(id=f"order-{order.order_id}", task_queue=queue,
                     id_reuse_policy=WorkflowIDReusePolicy.REJECT_DUPLICATE)
        handle = await env.client.start_workflow(OrderWorkflow.run, order, **start)
        await handle.result()
        with pytest.raises(WorkflowAlreadyStartedError):
            await env.client.start_workflow(OrderWorkflow.run, order, **start)


async def test_replay_of_a_real_history_is_deterministic(env):
    handle, _ = await run(env, mocks())
    history = await handle.fetch_history()
    # Fails with a non-determinism error if workflow.py changed incompatibly.
    await Replayer(workflows=[OrderWorkflow]).replay_workflow(history)


# ------------------------------------------ real activities, end to end

REAL = [
    real.authorize_payment, real.void_authorization, real.reserve_inventory,
    real.release_inventory, real.capture_payment, real.refund_payment,
    real.ship_order, real.send_notification,
]


async def test_real_activities_bad_address_fails_the_order(env):
    _, result = await run(env, REAL, new_order("bad_address"))
    assert isinstance(result, WorkflowFailureError)
    assert "Bad address" in str(result.cause)

# ---------------------------------------------------------- cancellation

world: dict = {}


@activity.defn(name="reserve_inventory")
async def reserve_then_wait(arg):
    """The warehouse reserves at once, then the reply is slow. Heartbeats."""
    calls.append("reserve_inventory")
    world["reserved"] = True
    for _ in range(30):
        await asyncio.sleep(0.1)
        activity.heartbeat()
    return "res-1"


@activity.defn(name="capture_payment")
async def capture_slow_reply(arg):
    """Stripe charges at once, then the reply is slow. No heartbeat."""
    calls.append("capture_payment")
    world["charged"] = True
    await asyncio.sleep(1)
    return "charge-1"


async def start(env, acts, queue):
    order = new_order()
    worker = Worker(env.client, task_queue=queue, workflows=[OrderWorkflow], activities=acts)
    handle = await env.client.start_workflow(
        OrderWorkflow.run, order, id=f"order-{order.order_id}", task_queue=queue
    )
    return worker, handle


async def wait_for_call(name):
    while name not in calls:
        await asyncio.sleep(0.05)


async def test_customer_cancel_undoes_after_the_current_step(env):
    world.clear()
    worker, handle = await start(env, mocks(reserve_inventory=reserve_then_wait), "c1")
    async with worker:
        await wait_for_call("reserve_inventory")
        assert "accepted" in await handle.execute_update(OrderWorkflow.cancel)
        with pytest.raises(WorkflowFailureError) as err:
            await handle.result()
    assert err.value.cause.type == "OrderCancelled"
    # The reservation went through, so it is released.
    assert calls[-3:] == ["release_inventory", "void_authorization", "send_notification"]
    assert "capture_payment" not in calls


async def test_customer_cancel_is_rejected_once_shipping(env):
    @activity.defn(name="ship_order")
    async def slow_ship(arg):
        calls.append("ship_order")
        await asyncio.sleep(1)
        return "track-1"

    worker, handle = await start(env, mocks(ship_order=slow_ship), "c2")
    async with worker:
        await wait_for_call("ship_order")
        with pytest.raises(WorkflowUpdateFailedError) as err:
            await handle.execute_update(OrderWorkflow.cancel)
        assert "Too late to cancel" in str(err.value.cause)
        assert await handle.result() == "track-1"


async def test_operator_cancel_after_side_effect_is_still_undone(env):
    # The heartbeat delivers the cancel, but the stock was already reserved.
    world.clear()
    worker, handle = await start(env, mocks(reserve_inventory=reserve_then_wait), "c3")
    async with worker:
        await wait_for_call("reserve_inventory")
        await handle.cancel()
        with pytest.raises(WorkflowFailureError) as err:
            await handle.result()
    assert isinstance(err.value.cause, CancelledError)
    assert world["reserved"] and "release_inventory" in calls


async def test_operator_cancel_during_capture_is_not_lost(env):
    # Capture finishes after the cancel. The order must not ship, and the
    # charge must be refunded.
    world.clear()
    worker, handle = await start(env, mocks(capture_payment=capture_slow_reply), "c4")
    async with worker:
        await wait_for_call("capture_payment")
        await handle.cancel()
        with pytest.raises(WorkflowFailureError) as err:
            await handle.result()
    assert isinstance(err.value.cause, CancelledError)
    assert world["charged"] and "refund_payment" in calls
    assert "ship_order" not in calls
