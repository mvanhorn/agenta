from unittest.mock import patch

import pytest
from oss.src.dbs.redis.shared.engine import StreamsEngine
from oss.src.utils.env import env


def test_get_redis_bounds_the_client_and_caches_it(monkeypatch):
    monkeypatch.setattr(env.redis, "uri_durable", "redis://durable.example:6381/0")
    sentinel = object()

    with patch("redis.asyncio.Redis.from_url", return_value=sentinel) as from_url:
        engine = StreamsEngine()
        first = engine.get_redis()
        second = engine.get_redis()

    assert first is sentinel
    assert second is first
    from_url.assert_called_once_with(
        "redis://durable.example:6381/0",
        decode_responses=False,
        socket_timeout=15,
        socket_connect_timeout=15,
        health_check_interval=30,
        socket_keepalive=True,
    )


def test_get_redis_requires_uri_durable_before_connecting(monkeypatch):
    monkeypatch.setattr(env.redis, "uri_durable", None)

    with (
        patch("redis.asyncio.Redis.from_url") as from_url,
        pytest.raises(RuntimeError, match="REDIS_URI_DURABLE"),
    ):
        StreamsEngine().get_redis()

    from_url.assert_not_called()
