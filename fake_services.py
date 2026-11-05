"""In-memory stand-ins for Stripe, the warehouse and the carrier.

They behave like the real services in the two ways that matter here:

- Idempotency keys: a request with a key that was already used returns the
  first response and changes nothing. Stripe does this for 24 hours.
- State per order: an undo with nothing to undo (no hold, no charge, no
  reservation) succeeds and changes nothing.

Temporal runs activities at least once, so a retry can repeat a call that
already took effect. The keys make that repeat harmless.

State lives in the worker's memory, so it is lost if the worker is killed.
The real services keep it.
"""

from dataclasses import dataclass, field


@dataclass
class Response:
    result: str
    replayed: bool = False  # True when an earlier request used the same key


@dataclass
class FakeService:
    responses: dict = field(default_factory=dict)  # idempotency key -> result

    def call(self, key: str, action) -> Response:
        if key in self.responses:
            return Response(self.responses[key], replayed=True)
        result = action()
        self.responses[key] = result
        return Response(result)


@dataclass
class FakeStripe(FakeService):
    holds: dict = field(default_factory=dict)    # order ID -> authorization ID
    charges: dict = field(default_factory=dict)  # order ID -> charge ID

    def authorize(self, order_id: str, key: str) -> Response:
        def act():
            self.holds[order_id] = f"auth-{order_id}"
            return self.holds[order_id]
        return self.call(key, act)

    def void(self, order_id: str, key: str) -> Response:
        def act():
            auth_id = self.holds.pop(order_id, None)
            return f"voided {auth_id}" if auth_id else f"no hold to void for order {order_id}"
        return self.call(key, act)

    def capture(self, order_id: str, key: str) -> Response:
        def act():
            self.holds.pop(order_id, None)  # the hold becomes the charge
            self.charges[order_id] = f"charge-{order_id}"
            return self.charges[order_id]
        return self.call(key, act)

    def refund(self, order_id: str, key: str) -> Response:
        def act():
            charge_id = self.charges.pop(order_id, None)
            return f"refunded {charge_id}" if charge_id else f"no charge to refund for order {order_id}"
        return self.call(key, act)


@dataclass
class FakeWarehouse(FakeService):
    reservations: dict = field(default_factory=dict)  # order ID -> reservation ID

    def reserve(self, order_id: str, key: str) -> Response:
        def act():
            self.reservations[order_id] = f"res-{order_id}"
            return self.reservations[order_id]
        return self.call(key, act)

    def release(self, order_id: str, key: str) -> Response:
        def act():
            res_id = self.reservations.pop(order_id, None)
            return f"released {res_id}" if res_id else f"no reservation to release for order {order_id}"
        return self.call(key, act)


@dataclass
class FakeCarrier(FakeService):
    def book(self, order_id: str, key: str) -> Response:
        return self.call(key, lambda: f"track-{order_id}")


stripe = FakeStripe()
warehouse = FakeWarehouse()
carrier = FakeCarrier()
