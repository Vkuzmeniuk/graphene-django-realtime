"""graphene-django-realtime: reuse your graphene-django schema for WebSocket payloads."""

from .broadcast import (
    broadcast_grouped,
    broadcast_instance,
    broadcast_instance_grouped,
    broadcast_static,
)
from .consumer import GraphQLWebsocketConsumer
from .registry import (
    AsyncRedisSubscriptionRegistry,
    DEFAULT_TTL,
    RedisSubscriptionRegistry,
    resolve_default_redis_url,
)
from .serializers import (
    FilterableSerializer,
    GraphQLSerializer,
    MockInfo,
    SerializerFactory,
    auto_discover_types,
    get_graphql_type_for_model,
    register_model_type,
    serialize_for_broadcast,
)
from .subscriptions import (
    SubscriptionChannelRegistryMixin,
    clear_subscription_channels,
    get_subscription_channel,
    register_subscription_channel,
)

__version__ = "0.2.0a0"

__all__ = [
    # serializers
    "GraphQLSerializer",
    "FilterableSerializer",
    "SerializerFactory",
    "MockInfo",
    "serialize_for_broadcast",
    "register_model_type",
    "get_graphql_type_for_model",
    "auto_discover_types",
    # subscription channel registry
    "SubscriptionChannelRegistryMixin",
    "register_subscription_channel",
    "get_subscription_channel",
    "clear_subscription_channels",
    # subscription persistence registry
    "AsyncRedisSubscriptionRegistry",
    "RedisSubscriptionRegistry",
    "resolve_default_redis_url",
    "DEFAULT_TTL",
    # broadcast helpers
    "broadcast_static",
    "broadcast_grouped",
    "broadcast_instance",
    "broadcast_instance_grouped",
    # consumer
    "GraphQLWebsocketConsumer",
]
