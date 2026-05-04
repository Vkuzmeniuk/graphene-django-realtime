"""
Channels consumer that speaks ``graphql-transport-ws`` and persists
subscription metadata into a :class:`AsyncRedisSubscriptionRegistry` so that
signal-time broadcasters can find subscribers.

Recommended path — emit channel-layer events with ``type="graphql_event"``
(the ``broadcast_*`` helpers in :mod:`graphene_django_realtime.broadcast`
already do this).  The built-in :meth:`GraphQLWebsocketConsumer.graphql_event`
handler unwraps them into ``next`` messages on the WebSocket; no subclass
code is required.

Custom event types — a subclass may also define handlers matching the
channel-layer ``event["type"]`` it sends itself::

    class MyConsumer(GraphQLWebsocketConsumer):
        async def my_update(self, event):  # raised by send(..., {"type": "my_update", ...})
            await self.send_json({
                "type": "next",
                "id": event["op_id"],
                "payload": {"data": {"myField": event["payload"]}},
            })

Channel-layer events whose ``type`` matches no handler are logged and
dropped — the WebSocket and other subscriptions on it stay alive.
"""

from __future__ import annotations

import logging
from types import SimpleNamespace
from typing import Any, ClassVar, Dict, List, Optional, Type

from channels.generic.websocket import AsyncJsonWebsocketConsumer
from graphene_django.settings import graphene_settings
from graphql import GraphQLError
from graphql.execution import ExecutionResult, subscribe
from graphql.language import OperationType, parse
from graphql.language.ast import FieldNode, OperationDefinitionNode

from .registry import AsyncRedisSubscriptionRegistry
from .subscriptions import get_subscription_channel

logger = logging.getLogger(__name__)


class GraphQLWebsocketConsumer(AsyncJsonWebsocketConsumer):
    """
    Base ``graphql-transport-ws`` consumer.

    Subclasses typically only need to add channel-layer event handlers; all
    protocol mechanics (``connection_init`` / ``subscribe`` / ``complete`` /
    ``ping``) live here.
    """

    #: Subprotocols accepted on connect; first entry is also negotiated on accept.
    subprotocols: ClassVar[List[str]] = ["graphql-transport-ws"]

    #: Whether to require an authenticated ``scope["user"]``. Override
    #: :meth:`authenticate` for non-default checks (e.g. JWT, anonymous mode).
    require_authenticated_user: ClassVar[bool] = True

    #: Registry class used for subscription persistence. Subclasses can swap
    #: this for an in-memory registry in tests.
    registry_class: ClassVar[Type[AsyncRedisSubscriptionRegistry]] = AsyncRedisSubscriptionRegistry

    #: Optional Redis URL override; ``None`` = derive from ``CHANNEL_LAYERS``.
    redis_url: ClassVar[Optional[str]] = None

    #: TTL applied to all subscription registry keys (seconds).
    subscription_ttl: ClassVar[int] = 4200

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._registry: Optional[AsyncRedisSubscriptionRegistry] = None
        # Maps op_id -> graphql field name to validate incoming channel-layer events.
        self._subscribed_fields: Dict[str, str] = {}

    @property
    def registry(self) -> AsyncRedisSubscriptionRegistry:
        if self._registry is None:
            self._registry = self.registry_class(
                redis_url=self.redis_url, ttl=self.subscription_ttl
            )
        return self._registry

    # ---- dispatch ------------------------------------------------------------

    async def dispatch(self, message: Dict[str, Any]) -> None:
        """Soft-fail on channel-layer events with no matching handler.

        Channels' default :meth:`dispatch` raises ``ValueError`` when no
        method matches ``message["type"]``, which propagates up and closes
        the WebSocket with code 1011 — killing every other subscription on
        the same socket.  Backend code that calls ``channel_layer.send`` /
        ``group_send`` with a custom ``type`` (e.g. ``"categories_update"``)
        without a corresponding handler is the most common cause.

        We catch that one specific ValueError and log a hint pointing at the
        ``broadcast_*`` helpers (which emit ``type="graphql_event"``).  All
        other errors — including malformed messages or missing ``websocket.*``
        protocol handlers — are re-raised.
        """
        try:
            await super().dispatch(message)
        except ValueError as exc:
            if not str(exc).startswith("No handler for message type "):
                raise
            msg_type = message.get("type", "")
            if not isinstance(msg_type, str) or msg_type.startswith("websocket."):
                raise
            handler_name = msg_type.replace(".", "_")
            logger.warning(
                "GraphQLWebsocketConsumer: dropping channel-layer event with "
                "unknown type %r. Use broadcast_instance / broadcast_instance_grouped "
                "(which emit type='graphql_event'), or define `async def %s(self, event)` "
                "on your consumer subclass. Connection kept alive.",
                msg_type, handler_name,
            )

    # ---- generic channel-layer event forward --------------------------------

    async def graphql_event(self, event: Dict[str, Any]) -> None:
        """
        Generic forward for any channel-layer event produced by
        :func:`broadcast_static` / :func:`broadcast_grouped`.

        Signal handlers set::

            event = {
                "type": "graphql_event",
                "graphql_field": "<subscription field name>",
                "payload": <serialized object>,
            }

        and this method wraps it into a ``next`` message the client expects.
        Subclasses only need to override this if they need non-standard wrapping.

        Before forwarding, two checks are applied:

        1. ``authorize_event`` — subclasses can override to re-check user
           permissions on every event (e.g. group membership, revoked roles).
        2. Field validation — the incoming ``graphql_field`` must match the
           field this client originally subscribed to for that ``op_id``.  This
           prevents a compromised or misconfigured backend from pushing data for
           fields the client never subscribed to.
        """
        op_id = event.get("op_id")
        graphql_field = event.get("graphql_field")

        if not await self.authorize_event(event):
            logger.debug("graphql_event: authorize_event rejected op=%r field=%r", op_id, graphql_field)
            return

        expected = self._subscribed_fields.get(op_id) if op_id else None
        if expected is not None and graphql_field != expected:
            logger.warning(
                "graphql_event: field %r does not match subscription %r for op %r, dropping",
                graphql_field, expected, op_id,
            )
            return

        await self.send_json({
            "type": "next",
            "id": op_id,
            "payload": {
                "data": {graphql_field: event["payload"]},
            },
        })

    async def authorize_event(self, event: Dict[str, Any]) -> bool:
        """
        Called before forwarding each channel-layer event to the client.

        Override to add per-event authorization, for example::

            async def authorize_event(self, event):
                user = self.scope["user"]
                return await user.has_perm_async("myapp.view_order")

        Returning ``False`` silently drops the event without disconnecting
        the client.  The default implementation always returns ``True``.
        """
        return True

    # ---- connection lifecycle ------------------------------------------------

    async def connect(self) -> None:
        if not await self.authenticate():
            await self.close(4401)
            return

        requested = self.scope.get("subprotocols", [])
        negotiated = next((p for p in self.subprotocols if p in requested), None)
        if negotiated is None:
            logger.warning(f"Unsupported subprotocols requested: {requested}")
            await self.close(4406)
            return

        await self.accept(subprotocol=negotiated)

    async def authenticate(self) -> bool:
        """Return True if the connection should be accepted."""
        if not self.require_authenticated_user:
            return True
        user = self.scope.get("user")
        return bool(user and getattr(user, "is_authenticated", False))

    async def disconnect(self, close_code) -> None:
        if isinstance(close_code, dict):
            msg_type = close_code.get("type")
            op_id = close_code.get("id")
        else:
            msg_type = None
            op_id = None

        if op_id:
            self._subscribed_fields.pop(op_id, None)
            try:
                await self.registry.unregister_op(self.channel_name, op_id)
            except Exception as exc:
                logger.debug(f"Registry cleanup failed on disconnect: {exc}")

        if msg_type == "connection_terminate":
            await self.close(1000)
        elif msg_type == "complete":
            await self.send_json({"type": "complete", "id": op_id})

    # ---- protocol routing ----------------------------------------------------

    async def receive_json(self, content, **kwargs) -> None:
        match content.get("type"):
            case "connection_init":
                await self.send_json({"type": "connection_ack", "payload": {}})
            case "subscribe":
                await self.handle_subscribe(content)
            case "complete":
                await self.disconnect(content)
            case "connection_terminate":
                await self.disconnect(content)
            case "ping":
                try:
                    await self.registry.touch(self.channel_name)
                except Exception as exc:
                    logger.debug(f"Registry touch failed on ping: {exc}")
                await self.send_json({"type": "pong"})
            case _:
                await self.send_json(
                    {"type": "error", "payload": {"message": "Unknown message type"}}
                )

    # ---- subscription handling ----------------------------------------------

    async def handle_subscribe(self, content) -> None:
        payload = content.get("payload") or {}
        op_id = content.get("id")
        query = payload.get("query")
        variables = payload.get("variables") or {}
        operation_name = payload.get("operationName")

        document = parse(query)
        schema = graphene_settings.SCHEMA.graphql_schema
        context = {"request": self.scope}

        field_name, channel_title, channel_id, filters = self._extract_channel_metadata(
            document, operation_name, variables, context
        )

        if field_name and op_id:
            self._subscribed_fields[op_id] = field_name

        if channel_id is not None:
            try:
                await self.registry.register(
                    channel_title=channel_title,
                    channel_id=channel_id,
                    sock_key=self.channel_name,
                    op_id=op_id,
                    filters=filters,
                )
            except Exception as exc:
                logger.warning(f"Failed to register subscription in registry: {exc}")

        try:
            result = await subscribe(
                schema,
                document,
                root_value=None,
                context_value=context,
                variable_values=variables,
                operation_name=operation_name,
            )

            if isinstance(result, ExecutionResult) and result.errors:
                await self.send_json({
                    "type": "error",
                    "id": op_id,
                    "payload": {
                        "data": None,
                        "errors": [str(err) for err in result.errors],
                    },
                })
                return

            async for item in result:
                await self.send_json(
                    {
                        "type": "next",
                        "id": op_id,
                        "payload": {"data": item.data},
                    }
                )

        except Exception as exc:
            logger.exception(f"handle_subscribe error for op {op_id!r}")
            await self.send_json(
                {"type": "error", "id": op_id, "payload": {"message": str(exc)}}
            )

    def _extract_channel_metadata(
        self,
        document: Any,
        operation_name: Optional[str],
        variables: Dict[str, Any],
        context: Dict[str, Any],
    ) -> tuple[Optional[str], Optional[str], Optional[Any], Dict[str, Any]]:
        """Resolve ``(field_name, channel_title, channel_id, filters)`` from the subscription document."""
        operation_def = None
        for definition in document.definitions:
            if not isinstance(definition, OperationDefinitionNode):
                continue
            if definition.operation != OperationType.SUBSCRIPTION:
                continue
            if operation_name and definition.name and definition.name.value != operation_name:
                continue
            operation_def = definition
            break

        if operation_def is None or not operation_def.selection_set.selections:
            return None, None, None, {}

        first_selection = operation_def.selection_set.selections[0]
        if not isinstance(first_selection, FieldNode):
            return None, None, None, {}

        field_name = first_selection.name.value
        info_proxy = SimpleNamespace(context=context)
        try:
            metadata = get_subscription_channel(field_name, info_proxy, variables)
        except KeyError as exc:
            logger.warning(str(exc))
            return field_name, None, None, {}

        if len(metadata) == 3:
            return field_name, metadata[0], metadata[1], dict(metadata[2] or {})
        return field_name, metadata[0], metadata[1], {}
