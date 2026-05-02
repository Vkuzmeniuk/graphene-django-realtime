"""Tests for broadcast fan-out helpers."""

import pytest
from unittest.mock import patch

from graphene_django_realtime.broadcast import (
    broadcast_grouped,
    broadcast_instance,
    broadcast_instance_grouped,
    broadcast_static,
)


# ---------------------------------------------------------------------------
# In-process stubs — no Redis, no channel layer process
# ---------------------------------------------------------------------------


class _Registry:
    """Minimal sync registry stub."""

    def __init__(self, subscribers=None, filters=None):
        self._subscribers = subscribers or {}
        self._filters = filters or {}

    def get_subscribers(self, channel_title, channel_id):
        return self._subscribers

    def get_filters_bulk(self, channel_title, channel_id, sock_keys):
        return self._filters


class _Layer:
    """Records every (sock_key, event) pair sent through it."""

    def __init__(self):
        self.sent: list = []

    async def send(self, sock_key, event):
        self.sent.append((sock_key, event))


# ---------------------------------------------------------------------------
# broadcast_static
# ---------------------------------------------------------------------------


def test_broadcast_static_returns_zero_with_no_subscribers():
    reg = _Registry()
    count = broadcast_static("ch", "1", {"type": "x"}, registry=reg, channel_layer=_Layer())
    assert count == 0


def test_broadcast_static_sends_to_all_subscribers():
    reg = _Registry(subscribers={"s1": "op1", "s2": "op2", "s3": "op3"})
    layer = _Layer()
    count = broadcast_static(
        "ch", "1", {"type": "graphql_event", "graphql_field": "foo", "payload": {}},
        registry=reg, channel_layer=layer,
    )
    assert count == 3
    assert {ev[0] for ev in layer.sent} == {"s1", "s2", "s3"}


def test_broadcast_static_merges_op_id_into_event():
    reg = _Registry(subscribers={"s1": "op_abc"})
    layer = _Layer()
    broadcast_static(
        "ch", "1", {"type": "graphql_event", "graphql_field": "foo", "payload": {"x": 1}},
        registry=reg, channel_layer=layer,
    )
    _, event = layer.sent[0]
    assert event["op_id"] == "op_abc"
    assert event["payload"] == {"x": 1}


def test_broadcast_static_does_not_mutate_original_event():
    original = {"type": "graphql_event", "graphql_field": "foo", "payload": {}}
    reg = _Registry(subscribers={"s1": "op1"})
    broadcast_static("ch", "1", original, registry=reg, channel_layer=_Layer())
    assert "op_id" not in original


# ---------------------------------------------------------------------------
# broadcast_grouped
# ---------------------------------------------------------------------------


def test_broadcast_grouped_calls_builder_once_per_unique_filter_group():
    subs = {"s1": "op1", "s2": "op2", "s3": "op3"}
    filters = {
        "s1": {"start": "2026-01"},
        "s2": {"start": "2026-01"},  # same group as s1
        "s3": {"start": "2026-06"},
    }
    reg = _Registry(subscribers=subs, filters=filters)
    layer = _Layer()
    calls = []

    def build(f):
        calls.append(dict(f))
        return {"type": "graphql_event", "graphql_field": "x", "payload": {"start": f["start"]}}

    count = broadcast_grouped("ch", "1", build, registry=reg, channel_layer=layer)
    assert count == 3
    assert len(calls) == 2  # only 2 distinct filter combos


def test_broadcast_grouped_skips_none_events():
    subs = {"s1": "op1", "s2": "op2"}
    filters = {"s1": {"group": "a"}, "s2": {"group": "b"}}
    reg = _Registry(subscribers=subs, filters=filters)
    layer = _Layer()

    def build(f):
        if f.get("group") == "b":
            return None
        return {"type": "graphql_event", "graphql_field": "x", "payload": {}}

    count = broadcast_grouped("ch", "1", build, registry=reg, channel_layer=layer)
    assert count == 1
    assert layer.sent[0][0] == "s1"


def test_broadcast_grouped_continues_after_builder_exception(caplog):
    subs = {"s1": "op1", "s2": "op2"}
    filters = {"s1": {"fail": "yes"}, "s2": {"fail": "no"}}
    reg = _Registry(subscribers=subs, filters=filters)
    layer = _Layer()

    def build(f):
        if f.get("fail") == "yes":
            raise ValueError("boom")
        return {"type": "graphql_event", "graphql_field": "x", "payload": {}}

    count = broadcast_grouped("ch", "1", build, registry=reg, channel_layer=layer)
    assert count == 1  # only s2 succeeded


# ---------------------------------------------------------------------------
# broadcast_instance / broadcast_instance_grouped (require Django)
# ---------------------------------------------------------------------------


@pytest.mark.django_db
def test_broadcast_instance_serializes_and_broadcasts():
    from tests.testapp.models import Product
    from tests.testapp.schema import ProductNode

    product = Product.objects.create(name="Widget", price="9.99")
    reg = _Registry(subscribers={"s1": "op1"})
    layer = _Layer()

    count = broadcast_instance(
        "products", "all", "productUpdated", product, ProductNode,
        registry=reg, channel_layer=layer,
    )
    assert count == 1
    _, event = layer.sent[0]
    assert event["graphql_field"] == "productUpdated"
    assert event["payload"]["name"] == "Widget"


@pytest.mark.django_db
def test_broadcast_instance_grouped_passes_filters_as_context():
    """Regression: filters must reach serialize_for_broadcast as context."""
    from tests.testapp.models import Product
    from tests.testapp.schema import ProductNode

    product = Product.objects.create(name="Gadget", price="19.99")
    reg = _Registry(
        subscribers={"s1": "op1"},
        filters={"s1": {"start_date": "2026-01-01", "end_date": "2026-12-31"}},
    )
    layer = _Layer()
    captured_contexts = []

    def factory(filters):
        return product

    with patch("graphene_django_realtime.serializers.serialize_for_broadcast") as mock_ser:
        mock_ser.return_value = {"id": "X", "name": "Gadget"}
        broadcast_instance_grouped(
            "products", "all", "productUpdated", ProductNode,
            instance_factory=factory,
            registry=reg,
            channel_layer=layer,
        )
        _, kwargs = mock_ser.call_args
        captured_contexts.append(kwargs.get("context"))

    assert captured_contexts[0] == {"start_date": "2026-01-01", "end_date": "2026-12-31"}


@pytest.mark.django_db
def test_broadcast_instance_grouped_factory_returns_none_skips_group():
    from tests.testapp.schema import ProductNode

    reg = _Registry(subscribers={"s1": "op1"})
    layer = _Layer()

    count = broadcast_instance_grouped(
        "products", "all", "productUpdated", ProductNode,
        instance_factory=lambda filters: None,
        registry=reg,
        channel_layer=layer,
    )
    assert count == 0
    assert layer.sent == []
