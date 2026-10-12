from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from entrypoints import worker_streams
from oss.src.dbs.redis.shared.engine import (
    STREAMS_HEALTH_CHECK_INTERVAL,
    STREAMS_SOCKET_CONNECT_TIMEOUT,
    STREAMS_SOCKET_KEEPALIVE,
    STREAMS_SOCKET_TIMEOUT,
)


async def test_relay_initialization_failure_does_not_block_durable_consumers():
    records = SimpleNamespace(
        stream_name="streams:records",
        consumer_group="worker-records",
        consumer_name="records-1",
        create_consumer_group=AsyncMock(),
        run=AsyncMock(),
    )
    live_relay = SimpleNamespace(
        stream_name="streams:session-live-frames",
        consumer_group="worker-session-live-relay",
        consumer_name="relay-1",
        create_consumer_group=AsyncMock(
            side_effect=RuntimeError("relay XGROUP failed")
        ),
        run=AsyncMock(),
    )

    with (
        patch.object(worker_streams, "_selected_streams", return_value=["records"]),
        patch.object(worker_streams, "warn_deprecated_env_vars"),
        patch.object(worker_streams, "validate_required_env_vars"),
        patch.object(worker_streams, "is_ee", return_value=False),
        patch.object(
            worker_streams.Redis, "from_url", return_value=AsyncMock()
        ) as from_url,
        patch.object(
            worker_streams,
            "_build_records_worker",
            new=AsyncMock(return_value=records),
        ),
        patch.object(
            worker_streams,
            "_build_live_relay_worker",
            new=AsyncMock(return_value=live_relay),
        ),
        patch.object(
            worker_streams,
            "prune_idle_consumers",
            new=AsyncMock(return_value=[]),
        ),
        patch.object(worker_streams.env.sessions, "shared_reader", True),
    ):
        assert await worker_streams.main_async() == 0

    records.create_consumer_group.assert_awaited_once()
    records.run.assert_awaited_once()
    live_relay.create_consumer_group.assert_awaited_once()
    live_relay.run.assert_not_awaited()
    assert STREAMS_SOCKET_TIMEOUT > 5000 / 1000
    assert STREAMS_SOCKET_CONNECT_TIMEOUT > 5000 / 1000
    from_url.assert_called_once_with(
        worker_streams.env.redis.uri_durable,
        decode_responses=False,
        socket_timeout=STREAMS_SOCKET_TIMEOUT,
        socket_connect_timeout=STREAMS_SOCKET_CONNECT_TIMEOUT,
        health_check_interval=STREAMS_HEALTH_CHECK_INTERVAL,
        socket_keepalive=STREAMS_SOCKET_KEEPALIVE,
    )
