"""
Redis-backed subscription registry.

Tracks which WebSocket connections are subscribed to which logical channels
so signal handlers can fan out updates without a second source of truth.

Key layout (inherited from the previous in-app implementation):

    {channel_title}:{channel_id}                       hash sock_key -> op_id
    connection:{sock_key}                              hash op_id    -> "{channel_title}:{channel_id}"
    {channel_title}:{channel_id}:{sock_key}            hash filter_key -> filter_value

The async registry is used by the WebSocket consumer at subscribe / disconnect /
ping time. The sync registry is used by Django signal handlers to enumerate
subscribers and per-subscriber filters.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Awaitable, Dict, Iterable, Optional, Tuple

try:
    import redis as _redis_sync
    import redis.asyncio as _redis_async
except ImportError as _exc:
    raise ImportError(
        "graphene-django-realtime's Redis registry requires the 'redis' package. "
        "Install it with: pip install graphene-django-realtime[redis]"
    ) from _exc
from ._relay import from_global_id

logger = logging.getLogger(__name__)

DEFAULT_TTL = 4200


def resolve_default_redis_url() -> str:
    """
    Resolve a Redis URL from Django ``CHANNEL_LAYERS`` settings.

    Supports the canonical ``channels-redis`` shapes::

        CHANNEL_LAYERS = {"default": {"CONFIG": {"hosts": [("redis", 6379)]}}}
        CHANNEL_LAYERS = {"default": {"CONFIG": {"hosts": ["redis://redis:6379"]}}}
    """
    from django.conf import settings  # local import: Django may not be imported yet

    layer = settings.CHANNEL_LAYERS["default"]
    hosts = layer["CONFIG"]["hosts"]
    if not hosts:
        raise RuntimeError("CHANNEL_LAYERS['default']['CONFIG']['hosts'] is empty")
    host_cfg = hosts[0]
    if isinstance(host_cfg, str):
        return host_cfg
    return f"redis://{host_cfg[0]}:{host_cfg[1]}"


def _decode(b: Any) -> Any:
    if isinstance(b, (bytes, bytearray)):
        return b.decode("utf-8")
    return b


def _try_decode_global_id(channel_id: Any) -> str:
    """Best-effort decode of a Relay global id to its database id; passthrough on failure."""
    try:
        return from_global_id(channel_id).id  # type: ignore[attr-defined]
    except Exception:
        return str(channel_id)


def _key_main(channel_title: str, channel_id: str) -> str:
    return f"{channel_title}:{channel_id}"


def _key_connection(sock_key: str) -> str:
    return f"connection:{sock_key}"


def _key_filters(channel_title: str, channel_id: str, sock_key: str) -> str:
    return f"{channel_title}:{channel_id}:{sock_key}"


# ---------------------------------------------------------------------------
# Sync registry — used by Django signal handlers
# ---------------------------------------------------------------------------


class RedisSubscriptionRegistry:
    """Synchronous read-side registry. Use from Django signal handlers."""

    def __init__(self, redis_url: Optional[str] = None, ttl: int = DEFAULT_TTL) -> None:
        self._url = redis_url
        self._ttl = ttl
        self._client: Optional[_redis_sync.Redis] = None

    @property
    def client(self) -> _redis_sync.Redis:
        if self._client is None:
            self._client = _redis_sync.from_url(self._url or resolve_default_redis_url())
        return self._client

    @property
    def ttl(self) -> int:
        return self._ttl

    def get_subscribers(
        self, channel_title: str, channel_id: Any
    ) -> Dict[str, str]:
        """Return ``{sock_key: op_id}`` for the given (title, id) pair."""
        decoded_id = _try_decode_global_id(channel_id)
        raw = self.client.hgetall(_key_main(channel_title, decoded_id))
        return {_decode(k): _decode(v) for k, v in raw.items()}

    def get_filters_bulk(
        self,
        channel_title: str,
        channel_id: Any,
        sock_keys: Iterable[str],
    ) -> Dict[str, Dict[str, str]]:
        """Pipeline ``HGETALL`` for many subscriber filter hashes in one round-trip."""
        decoded_id = _try_decode_global_id(channel_id)
        sock_keys = list(sock_keys)
        if not sock_keys:
            return {}
        pipe = self.client.pipeline()
        for sock_key in sock_keys:
            pipe.hgetall(_key_filters(channel_title, decoded_id, sock_key))
        results = pipe.execute()
        return {
            sock_key: {_decode(k): _decode(v) for k, v in raw.items()}
            for sock_key, raw in zip(sock_keys, results)
        }


# ---------------------------------------------------------------------------
# Async registry — used by the WebSocket consumer
# ---------------------------------------------------------------------------


class AsyncRedisSubscriptionRegistry:
    """Asynchronous write-side registry. Use from a Channels consumer."""

    def __init__(self, redis_url: Optional[str] = None, ttl: int = DEFAULT_TTL) -> None:
        self._url = redis_url
        self._ttl = ttl
        self._client: Optional[_redis_async.Redis] = None

    @property
    def client(self) -> _redis_async.Redis:
        if self._client is None:
            self._client = _redis_async.from_url(self._url or resolve_default_redis_url())
        return self._client

    @property
    def ttl(self) -> int:
        return self._ttl

    async def register(
        self,
        *,
        channel_title: str,
        channel_id: Any,
        sock_key: str,
        op_id: str,
        filters: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Persist a new subscription mapping and its per-subscriber filters."""
        if not channel_title or channel_id is None:
            logger.debug("register: missing channel_title/channel_id, skipping")
            return

        decoded_id = _try_decode_global_id(channel_id)
        key_main = _key_main(channel_title, decoded_id)
        key_connection = _key_connection(sock_key)
        key_filters = _key_filters(channel_title, decoded_id, sock_key)

        await asyncio.gather(
            self.client.hset(key_main, sock_key, op_id),
            self.client.expire(key_main, self._ttl),
            self.client.hset(key_connection, op_id, key_main),
            self.client.expire(key_connection, self._ttl),
        )

        cleaned = {k: v for k, v in (filters or {}).items() if v is not None}
        if cleaned:
            coros: list[Awaitable[Any]] = [
                self.client.hset(key_filters, k, str(v)) for k, v in cleaned.items()
            ]
            coros.append(self.client.expire(key_filters, self._ttl))
            await asyncio.gather(*coros)

    async def unregister_op(self, sock_key: str, op_id: str) -> None:
        """Remove subscription identified by (sock_key, op_id) and its filters."""
        key_connection = _key_connection(sock_key)
        try:
            key_main_b = await self.client.hget(key_connection, op_id)
        except Exception as exc:
            logger.debug(f"unregister_op: hget failed: {exc}")
            return

        if not key_main_b:
            logger.debug(f"unregister_op: no key for {sock_key}:{op_id}")
            return

        key_main = _decode(key_main_b)
        # Only delete the (sock_key -> op_id) mapping if it matches what we expect;
        # otherwise we may stomp another subscription that re-bound the same sock_key.
        try:
            current = await self.client.hget(key_main, sock_key)
            if _decode(current) == op_id:
                await self.client.hdel(key_main, sock_key)
                await self.client.delete(f"{key_main}:{sock_key}")
        except Exception as exc:
            logger.debug(f"unregister_op: cleanup of {key_main} failed: {exc}")

        try:
            await self.client.hdel(key_connection, op_id)
        except Exception as exc:
            logger.debug(f"unregister_op: hdel(connection) failed: {exc}")

    async def touch(self, sock_key: str) -> None:
        """Refresh TTLs on all subscriptions associated with ``sock_key``.

        Called on protocol-level pings so that idle-but-alive subscriptions
        do not expire while the connection is still open.
        """
        key_connection = _key_connection(sock_key)
        try:
            await self.client.expire(key_connection, self._ttl)
            mapping = await self.client.hgetall(key_connection)
        except Exception as exc:
            logger.debug(f"touch: read of {key_connection} failed: {exc}")
            return

        for _op_b, key_main_b in mapping.items():
            key_main = _decode(key_main_b)
            try:
                await self.client.expire(key_main, self._ttl)
                await self.client.expire(f"{key_main}:{sock_key}", self._ttl)
            except Exception as exc:
                logger.debug(f"touch: refresh of {key_main} failed: {exc}")
