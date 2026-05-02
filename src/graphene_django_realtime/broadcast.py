"""
Fan-out helpers for sending Channel-Layer events to every subscriber of a
logical (channel_title, channel_id) pair.

Low-level primitives:

- :func:`broadcast_static` — same raw event dict to every subscriber.
- :func:`broadcast_grouped` — group subscribers by filters, call a builder
  once per group.

High-level helpers (preferred in signal handlers):

- :func:`broadcast_instance` — serialize a model instance and fan out to all
  subscribers of a channel.
- :func:`broadcast_instance_grouped` — same, but re-fetch/serialize once per
  filter group so subscribers with different date ranges get filtered payloads.

Both helpers run from synchronous Django code (signal handlers) and internally
schedule a single ``async_to_sync`` round-trip with all sends in parallel.
"""

from __future__ import annotations

import asyncio
import logging
from collections import defaultdict
from typing import Any, Callable, Dict, Iterable, Optional, Tuple, Type

from asgiref.sync import async_to_sync
from channels.exceptions import ChannelFull
from channels.layers import get_channel_layer

from .registry import RedisSubscriptionRegistry

logger = logging.getLogger(__name__)

#: ``filters_dict -> event_dict`` (or None to skip the group)
EventBuilder = Callable[[Dict[str, str]], Optional[Dict[str, Any]]]


def _resolve_registry(registry: Optional[RedisSubscriptionRegistry]) -> RedisSubscriptionRegistry:
    return registry if registry is not None else RedisSubscriptionRegistry()


async def _send_all(
    sends: Iterable[Tuple[str, Dict[str, Any]]],
    channel_layer: Any,
) -> None:
    async def safe_send(sock_key: str, event: Dict[str, Any]) -> None:
        try:
            await channel_layer.send(sock_key, event)
        except ChannelFull:
            logger.warning(f"Channel full for {sock_key}, dropping event {event.get('type')!r}")
        except Exception as exc:
            logger.error(f"channel_layer.send to {sock_key} failed: {exc}", exc_info=True)

    await asyncio.gather(*(safe_send(sk, ev) for sk, ev in sends), return_exceptions=True)


def broadcast_static(
    channel_title: str,
    channel_id: Any,
    event: Dict[str, Any],
    *,
    op_id_field: str = "op_id",
    registry: Optional[RedisSubscriptionRegistry] = None,
    channel_layer: Any = None,
) -> int:
    """
    Send the same ``event`` (extended with each subscriber's ``op_id``) to
    every subscriber of ``(channel_title, channel_id)``.

    Returns the number of dispatched messages.
    """
    reg = _resolve_registry(registry)
    subscribers = reg.get_subscribers(channel_title, channel_id)
    if not subscribers:
        return 0

    layer = channel_layer or get_channel_layer()
    sends = [
        (sock_key, {**event, op_id_field: op_id})
        for sock_key, op_id in subscribers.items()
    ]
    async_to_sync(_send_all)(sends, layer)
    return len(sends)


def broadcast_grouped(
    channel_title: str,
    channel_id: Any,
    build_event: EventBuilder,
    *,
    op_id_field: str = "op_id",
    registry: Optional[RedisSubscriptionRegistry] = None,
    channel_layer: Any = None,
) -> int:
    """
    For each group of subscribers sharing identical filter values, invoke
    ``build_event(filters)`` once and dispatch the resulting event to every
    subscriber in that group (with their own ``op_id`` merged in).

    Returns the number of dispatched messages.
    """
    reg = _resolve_registry(registry)
    subscribers = reg.get_subscribers(channel_title, channel_id)
    if not subscribers:
        return 0

    sock_keys = list(subscribers.keys())
    filters_by_sock = reg.get_filters_bulk(channel_title, channel_id, sock_keys)

    groups: Dict[Tuple[Tuple[str, str], ...], list] = defaultdict(list)
    for sock_key in sock_keys:
        filters = filters_by_sock.get(sock_key, {})
        group_key = tuple(sorted(filters.items()))
        groups[group_key].append((sock_key, subscribers[sock_key]))

    sends: list[Tuple[str, Dict[str, Any]]] = []
    for group_key, members in groups.items():
        filters = dict(group_key)
        try:
            event = build_event(filters)
        except Exception as exc:
            logger.error(
                f"build_event raised for group {filters!r}: {exc}", exc_info=True
            )
            continue
        if event is None:
            continue
        for sock_key, op_id in members:
            sends.append((sock_key, {**event, op_id_field: op_id}))

    if not sends:
        return 0

    layer = channel_layer or get_channel_layer()
    async_to_sync(_send_all)(sends, layer)
    return len(sends)


# ---------------------------------------------------------------------------
# High-level helpers — serialize + broadcast in one call
# ---------------------------------------------------------------------------

def broadcast_instance(
    channel_title: str,
    channel_id: Any,
    graphql_field: str,
    instance: Any,
    graphql_type: Type,
    *,
    context: Optional[Dict[str, Any]] = None,
    registry: Optional[RedisSubscriptionRegistry] = None,
    channel_layer: Any = None,
) -> int:
    """
    Serialize ``instance`` with ``graphql_type`` and broadcast to every
    subscriber of ``(channel_title, channel_id)``.

    Equivalent to::

        payload = serialize_for_broadcast(instance, graphql_type, context=context)
        broadcast_static(channel_title, channel_id, {
            "type": "graphql_event",
            "graphql_field": graphql_field,
            "payload": payload,
        })
    """
    from .serializers import serialize_for_broadcast

    payload = serialize_for_broadcast(instance, graphql_type, context=context or {})
    return broadcast_static(
        channel_title,
        channel_id,
        {"type": "graphql_event", "graphql_field": graphql_field, "payload": payload},
        registry=registry,
        channel_layer=channel_layer,
    )


def broadcast_instance_grouped(
    channel_title: str,
    channel_id: Any,
    graphql_field: str,
    graphql_type: Type,
    instance_factory: Callable[[Dict[str, str]], Optional[Any]],
    *,
    registry: Optional[RedisSubscriptionRegistry] = None,
    channel_layer: Any = None,
) -> int:
    """
    For each group of subscribers sharing identical filter values, call
    ``instance_factory(filters)`` to obtain a (possibly prefetched) model
    instance, serialize it with ``graphql_type``, and send to the group.

    ``instance_factory`` receives the filter dict and should return the model
    instance ready for serialization, or ``None`` to skip the group.

    Example::

        def make_order(filters):
            since = filters.get("since")
            events_qs = OrderEvent.objects.filter(created__gte=since) if since else OrderEvent.objects.all()
            return (
                Order.objects
                .filter(pk=order.pk)
                .prefetch_related(Prefetch("events", queryset=events_qs))
                .first()
            )

        broadcast_instance_grouped(
            "orders", order.pk, "orderUpdated", OrderNode,
            instance_factory=make_order,
        )
    """
    from .serializers import serialize_for_broadcast

    def build_event(filters: Dict[str, str]) -> Optional[Dict[str, Any]]:
        instance = instance_factory(filters)
        if instance is None:
            return None
        payload = serialize_for_broadcast(instance, graphql_type, context=dict(filters))
        return {"type": "graphql_event", "graphql_field": graphql_field, "payload": payload}

    return broadcast_grouped(
        channel_title,
        channel_id,
        build_event,
        registry=registry,
        channel_layer=channel_layer,
    )
