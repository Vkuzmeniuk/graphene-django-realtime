"""Tests for GraphQLWebsocketConsumer (graphql-transport-ws protocol)."""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from graphene_django_realtime import GraphQLWebsocketConsumer


# ---------------------------------------------------------------------------
# Null registry — prevents real Redis connections during tests
# ---------------------------------------------------------------------------


class _NullRegistry:
    def __init__(self, redis_url=None, ttl=None):
        pass

    async def register(self, **kwargs):
        pass

    async def unregister_op(self, *args):
        pass

    async def touch(self, *args):
        pass


class _TestConsumer(GraphQLWebsocketConsumer):
    """Consumer wired for testing: no auth, no Redis."""

    require_authenticated_user = False
    registry_class = _NullRegistry


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_communicator(consumer_cls=None):
    from channels.testing import WebsocketCommunicator

    cls = consumer_cls or _TestConsumer
    return WebsocketCommunicator(cls.as_asgi(), "/", subprotocols=["graphql-transport-ws"])


async def _connect_and_init(comm):
    connected, subprotocol = await comm.connect()
    assert connected
    await comm.send_json_to({"type": "connection_init", "payload": {}})
    ack = await comm.receive_json_from()
    assert ack["type"] == "connection_ack"
    return subprotocol


# ---------------------------------------------------------------------------
# Connection lifecycle
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_connect_negotiates_subprotocol():
    comm = _make_communicator()
    connected, subprotocol = await comm.connect()
    assert connected
    assert subprotocol == "graphql-transport-ws"
    await comm.disconnect()


@pytest.mark.asyncio
async def test_connect_rejects_missing_subprotocol():
    from channels.testing import WebsocketCommunicator

    comm = WebsocketCommunicator(_TestConsumer.as_asgi(), "/", subprotocols=[])
    connected, code = await comm.connect()
    assert not connected


@pytest.mark.asyncio
async def test_connect_rejects_unauthenticated_user():
    from channels.testing import WebsocketCommunicator
    from django.contrib.auth.models import AnonymousUser

    class AuthConsumer(GraphQLWebsocketConsumer):
        require_authenticated_user = True
        registry_class = _NullRegistry

    comm = WebsocketCommunicator(AuthConsumer.as_asgi(), "/", subprotocols=["graphql-transport-ws"])
    connected, code = await comm.connect()
    assert not connected


@pytest.mark.asyncio
async def test_connection_init_sends_ack():
    comm = _make_communicator()
    await comm.connect()
    await comm.send_json_to({"type": "connection_init", "payload": {}})
    response = await comm.receive_json_from()
    assert response["type"] == "connection_ack"
    await comm.disconnect()


# ---------------------------------------------------------------------------
# Ping / pong
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ping_returns_pong():
    comm = _make_communicator()
    await _connect_and_init(comm)
    await comm.send_json_to({"type": "ping"})
    response = await comm.receive_json_from()
    assert response["type"] == "pong"
    await comm.disconnect()


# ---------------------------------------------------------------------------
# Unknown message type
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_unknown_message_type_returns_error():
    comm = _make_communicator()
    await _connect_and_init(comm)
    await comm.send_json_to({"type": "wat"})
    response = await comm.receive_json_from()
    assert response["type"] == "error"
    await comm.disconnect()


# ---------------------------------------------------------------------------
# graphql_event generic handler
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_graphql_event_handler_formats_next_message():
    sent = []
    consumer = GraphQLWebsocketConsumer()
    consumer.send_json = AsyncMock(side_effect=lambda m: sent.append(m))

    await consumer.graphql_event({
        "type": "graphql_event",
        "op_id": "op_1",
        "graphql_field": "productUpdated",
        "payload": {"id": "Rm9v", "name": "Foo"},
    })

    assert len(sent) == 1
    msg = sent[0]
    assert msg["type"] == "next"
    assert msg["id"] == "op_1"
    assert msg["payload"]["data"]["productUpdated"] == {"id": "Rm9v", "name": "Foo"}


# ---------------------------------------------------------------------------
# handle_subscribe error path
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_subscribe_execution_errors_send_error_frame():
    from graphql import GraphQLError
    from graphql.execution import ExecutionResult

    comm = _make_communicator()
    await _connect_and_init(comm)

    mock_result = ExecutionResult(data=None, errors=[GraphQLError("bad subscription")])

    with patch("graphene_django_realtime.consumer.subscribe", return_value=mock_result):
        await comm.send_json_to({
            "type": "subscribe",
            "id": "sub_1",
            "payload": {
                "query": "subscription { productUpdated { id } }",
                "variables": {"id": "all"},
            },
        })
        # First frame is the echo-back of the subscribe message
        first = await comm.receive_json_from()
        assert first["type"] == "subscribe"

        # Second frame is the error
        error_frame = await comm.receive_json_from()
        assert error_frame["type"] == "error"
        assert error_frame["id"] == "sub_1"
        assert "bad subscription" in error_frame["payload"]["errors"][0]

    await comm.disconnect()


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_subscribe_no_duplicate_error_on_execution_result_errors():
    """
    Regression: the old code raised GraphQLError inside the try block, which
    was caught by the outer except and sent a second error frame. Verify only
    one error frame is produced.
    """
    from graphql import GraphQLError
    from graphql.execution import ExecutionResult

    comm = _make_communicator()
    await _connect_and_init(comm)

    mock_result = ExecutionResult(data=None, errors=[GraphQLError("only once")])

    with patch("graphene_django_realtime.consumer.subscribe", return_value=mock_result):
        await comm.send_json_to({
            "type": "subscribe",
            "id": "sub_2",
            "payload": {"query": "subscription { productUpdated { id } }"},
        })

        frames = []
        while True:
            try:
                frames.append(await comm.receive_json_from(timeout=0.2))
            except Exception:
                break

    error_frames = [f for f in frames if f.get("type") == "error"]
    assert len(error_frames) == 1, f"Expected exactly 1 error frame, got: {error_frames}"

    await comm.disconnect()


# ---------------------------------------------------------------------------
# authenticate() override
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_authenticate_override_accepts_custom_logic():
    class TokenConsumer(GraphQLWebsocketConsumer):
        registry_class = _NullRegistry

        async def authenticate(self):
            return self.scope.get("query_string") == b"token=valid"

    from channels.testing import WebsocketCommunicator

    good = WebsocketCommunicator(
        TokenConsumer.as_asgi(), "/?token=valid",
        subprotocols=["graphql-transport-ws"],
    )
    # channels puts query_string in scope from the URL
    good.scope["query_string"] = b"token=valid"
    connected, _ = await good.connect()
    assert connected
    await good.disconnect()

    bad = WebsocketCommunicator(
        TokenConsumer.as_asgi(), "/?token=wrong",
        subprotocols=["graphql-transport-ws"],
    )
    bad.scope["query_string"] = b"token=wrong"
    connected, _ = await bad.connect()
    assert not connected
