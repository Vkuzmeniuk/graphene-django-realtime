# graphene-django-realtime

Reuse your `graphene-django` schema as the single source of truth for your WebSocket layer.

If you're broadcasting model changes to subscribers via Django Channels, this library gives you three things:

1. **A serializer** that walks your `DjangoObjectType` fields and calls the same resolvers the executor would — no parallel `to_dict()` helpers, no drift from your GraphQL output.
2. **A `graphql-transport-ws` consumer base class** that handles `connection_init` / `subscribe` / `complete` / `ping`, persists subscription metadata into Redis, and delivers events via a generic `graphql_event` handler you never need to override.
3. **Broadcast helpers** that fan out events from Django signal handlers to every subscriber of a logical channel, with optional per-subscriber filter grouping so you serialize once per group instead of once per connection.

## Install

```bash
pip install graphene-django-realtime[redis]
```

Requires Django ≥ 4.2, graphene-django ≥ 3.2, channels ≥ 4.0.  
The `[redis]` extra pulls in `redis>=5.0` for the subscription registry. Omit it only if you supply a custom registry class.

## Consumer

Drop `GraphQLWebsocketConsumer` directly into your routing — no subclassing needed for the common case:

```python
# routing.py
from graphene_django_realtime import GraphQLWebsocketConsumer

websocket_urlpatterns = [
    path("graphql/ws/", GraphQLWebsocketConsumer.as_asgi()),
]
```

The base class handles the full `graphql-transport-ws` protocol. Subscription events produced by the broadcast helpers are forwarded automatically via the built-in `graphql_event` handler.

### Authentication

By default `require_authenticated_user = True` checks `scope["user"].is_authenticated`. Override `authenticate()` for custom logic:

```python
class MyConsumer(GraphQLWebsocketConsumer):
    async def authenticate(self) -> bool:
        token = self.scope.get("query_string", b"").decode()
        return await verify_jwt(token)
```

### Per-event authorization

Override `authorize_event()` to re-check permissions on every incoming event (e.g. after a role change mid-session). Returning `False` silently drops the event without disconnecting the client:

```python
class MyConsumer(GraphQLWebsocketConsumer):
    async def authorize_event(self, event) -> bool:
        return await self.scope["user"].has_perm_async("myapp.view_order")
```

### Testing

Swap the Redis registry for an in-memory stub by overriding `registry_class`:

```python
from graphene_django_realtime.registry import AsyncRedisSubscriptionRegistry

class InMemoryRegistry(AsyncRedisSubscriptionRegistry):
    ...  # your stub

class TestConsumer(GraphQLWebsocketConsumer):
    registry_class = InMemoryRegistry
```

## Subscription channel registry

Add a `channel_<field_name>` static method to your `Subscription` class. The mixin discovers and registers these automatically at import time.

```python
from graphene_django_realtime import SubscriptionChannelRegistryMixin

class Subscription(SubscriptionChannelRegistryMixin, graphene.ObjectType):
    order_updated = graphene.Field(OrderNode)
    notification = graphene.Field(NotificationNode)

    @staticmethod
    def channel_order_updated(info, variables):
        # 2-tuple: (channel_title, channel_id)
        return "orders", variables["orderId"]

    @staticmethod
    def channel_notification(info, variables):
        # 3-tuple: (channel_title, channel_id, filters_dict)
        # filters_dict is persisted per subscriber and passed back at broadcast time
        user = info.context["request"]["user"]
        return "notifications", str(user.username), {
            "since": variables.get("since"),
        }
```

The resolver returns a **2-tuple** `(channel_title, channel_id)` or a **3-tuple** `(channel_title, channel_id, filters_dict)`. The `filters_dict` is persisted in Redis so broadcast helpers can re-fetch or re-serialize with per-subscriber context.

## Broadcast helpers

From your Django signal handlers:

```python
from graphene_django_realtime import broadcast_instance, broadcast_instance_grouped

# Same payload to every subscriber of a channel.
broadcast_instance(
    channel_title="notifications",
    channel_id=str(notification.user.username),
    graphql_field="notification",
    instance=notification,
    graphql_type=NotificationNode,
)

# Per-filter-group fan-out — fetches and serializes once per unique filter combo.
def fetch_order(filters):
    since = filters.get("since")
    events_qs = OrderEvent.objects.filter(created__gte=since) if since else OrderEvent.objects.all()
    return (
        Order.objects
        .filter(pk=order.pk)
        .prefetch_related(Prefetch("events", queryset=events_qs))
        .first()
    )

broadcast_instance_grouped(
    channel_title="orders",
    channel_id=order.pk,
    graphql_field="orderUpdated",
    graphql_type=OrderNode,
    instance_factory=fetch_order,
)
```

`broadcast_instance_grouped` groups subscribers by their persisted filter values, calls `instance_factory(filters)` once per group, and serializes with those same filters as `info.context`. With 100 subscribers across 3 filter groups you call the DB 3 times, not 100.

For lower-level control:

```python
from graphene_django_realtime import broadcast_static

broadcast_static("items", item_id, {
    "type": "graphql_event",
    "graphql_field": "itemDeleted",
    "payload": {"id": global_id},
})
```

## Serializer

Call directly when you need the payload without broadcasting:

```python
from graphene_django_realtime import serialize_for_broadcast

payload = serialize_for_broadcast(
    order_instance,
    OrderNode,
    context={"since": "2026-01-01T00:00:00Z"},
)
```

The `context` dict is passed as `info.context` to your resolvers — the same filtering logic as HTTP.

GraphQL types are discovered automatically by importing `schema.py` from each installed Django app. If your types live in a different module (e.g. `types.py`, `nodes.py`), discovery won't find them — register explicitly:

```python
from graphene_django_realtime import register_model_type
from myapp.models import Order
from myapp.schema import OrderNode

register_model_type(Order, OrderNode)
```

### Troubleshooting

**Field is `null` in WebSocket payload** — the serializer only walks fields explicitly defined on the `DjangoObjectType`. Ensure the field is declared on the type and its resolver is accessible.

**N+1 queries** — pre-fetch before passing to `serialize_for_broadcast` or `broadcast_instance_grouped`:

```python
instance = Order.objects.prefetch_related("events", "items").get(pk=pk)
broadcast_instance("orders", order.pk, "orderUpdated", instance, OrderNode)
```

**Related object serializes as `{"id": "..."}` only** — no `DjangoObjectType` is registered for that model. Use `register_model_type` or add an explicit resolver on the parent type.

## Status

**Alpha.** API may shift before 1.0. Pin a version.

## License

MIT
