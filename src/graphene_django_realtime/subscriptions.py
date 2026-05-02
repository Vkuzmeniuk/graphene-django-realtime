"""Helpers for mapping GraphQL subscription fields to channel metadata."""

from __future__ import annotations

from typing import Any, Callable, Dict, Tuple, Union

from graphene.utils.str_converters import to_camel_case

#: Resolver may return a 2-tuple ``(channel_title, channel_id)`` or a
#: 3-tuple ``(channel_title, channel_id, filters_dict)`` where ``filters_dict``
#: is persisted by the registry alongside the subscription so signal-time
#: broadcast logic can read it back per subscriber.
ChannelMetadata = Union[Tuple[str, Any], Tuple[str, Any, Dict[str, Any]]]
_REGISTRY: Dict[str, Callable[[Any, Dict[str, Any]], ChannelMetadata]] = {}

_CHANNEL_PREFIX = "channel_"


class SubscriptionChannelRegistryMixin:
    """
    Mixin that auto-registers subscription channel resolvers defined as
    ``channel_<field_name>`` static methods on the subclass.

    Example::

        class Subscription(SubscriptionChannelRegistryMixin, graphene.ObjectType):
            room_messages = graphene.Field(...)

            @staticmethod
            def channel_room_messages(info, variables):
                return "room", variables["roomId"], {
                    "since": variables.get("since"),
                }

    The method name is stripped of the ``channel_`` prefix and converted to
    camelCase to produce the field name (``room_messages`` →
    ``roomMessages``).

    Explicit registration via :func:`register_subscription_channel` still works
    and takes precedence over auto-discovered methods (last registration wins).
    """

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        for attr_name in vars(cls):
            if not attr_name.startswith(_CHANNEL_PREFIX):
                continue
            resolver = getattr(cls, attr_name)
            if not callable(resolver):
                continue
            field_name = to_camel_case(attr_name[len(_CHANNEL_PREFIX):])
            register_subscription_channel(field_name, resolver)

    @staticmethod
    def register_subscription_channel(
        field_name: str,
        resolver: Callable[[Any, Dict[str, Any]], ChannelMetadata],
    ) -> None:
        """Explicitly register a resolver for subscription channel metadata."""
        register_subscription_channel(field_name, resolver)


def register_subscription_channel(
    field_name: str,
    resolver: Callable[[Any, Dict[str, Any]], ChannelMetadata],
) -> None:
    """Register a resolver callable for the given subscription field."""
    _REGISTRY[field_name] = resolver


def get_subscription_channel(
    field_name: str, info: Any, variables: Dict[str, Any]
) -> ChannelMetadata:
    """Return channel metadata for a subscription field."""
    try:
        resolver = _REGISTRY[field_name]
    except KeyError as exc:  # pragma: no cover - defensive guard
        raise KeyError(f"No channel metadata registered for '{field_name}'") from exc
    return resolver(info, variables)


def clear_subscription_channels() -> None:
    """Drop all registered subscription channel resolvers. Intended for tests."""
    _REGISTRY.clear()
