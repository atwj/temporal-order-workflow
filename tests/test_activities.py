"""Activities: idempotency keys and heartbeats, without a Temporal server."""

import pytest
from temporalio.testing import ActivityEnvironment

import activities
import fake_services
from shared import Order


@pytest.fixture(autouse=True)
def fresh_services(monkeypatch):
    # New, empty stand-in services for every test.
    for name, cls in [("stripe", fake_services.FakeStripe),
                      ("warehouse", fake_services.FakeWarehouse),
                      ("carrier", fake_services.FakeCarrier)]:
        monkeypatch.setattr(activities, name, cls())
    monkeypatch.setattr(activities, "LATENCY", 0)


def order(simulate=""):
    return Order("42", "headphones", 1, 12000, "buyer@example.com", simulate)


async def test_a_retried_capture_charges_once():
    env = ActivityEnvironment()
    first = await env.run(activities.capture_payment, order())
    second = await env.run(activities.capture_payment, order())  # a retry
    assert first == second == "charge-42"
    assert activities.stripe.responses == {"42-capture": "charge-42"}


async def test_undo_with_nothing_to_undo_changes_nothing():
    env = ActivityEnvironment()
    await env.run(activities.refund_payment, order())
    await env.run(activities.release_inventory, order())
    assert activities.stripe.responses["42-refund"] == "no charge to refund for order 42"
    assert activities.warehouse.responses["42-release"] == "no reservation to release for order 42"


async def test_refund_after_capture_refunds_the_charge():
    env = ActivityEnvironment()
    await env.run(activities.authorize_payment, order())
    await env.run(activities.capture_payment, order())
    await env.run(activities.void_authorization, order())  # hold already captured
    await env.run(activities.refund_payment, order())
    assert activities.stripe.responses["42-void"] == "no hold to void for order 42"
    assert activities.stripe.responses["42-refund"] == "refunded charge-42"
    assert activities.stripe.charges == {}


async def test_activities_heartbeat_while_waiting_on_a_service(monkeypatch):
    monkeypatch.setattr(activities, "LATENCY", 0.3)
    monkeypatch.setattr(activities, "HEARTBEAT_EVERY", 0.1)
    beats = []
    env = ActivityEnvironment()
    env.on_heartbeat = lambda *details: beats.append(1)
    await env.run(activities.reserve_inventory, order())
    assert len(beats) >= 2


async def test_a_cancelled_activity_never_calls_the_service(monkeypatch):
    monkeypatch.setattr(activities, "LATENCY", 5)
    env = ActivityEnvironment()

    import asyncio

    async def cancel_soon():
        await asyncio.sleep(0.2)
        env.cancel()

    canceller = asyncio.create_task(cancel_soon())
    with pytest.raises(asyncio.CancelledError):
        await env.run(activities.reserve_inventory, order())
    await canceller
    assert activities.warehouse.responses == {}
