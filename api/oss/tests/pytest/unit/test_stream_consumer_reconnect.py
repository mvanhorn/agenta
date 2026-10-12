import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import TimeoutError as RedisTimeoutError

from oss.src.tasks.asyncio.events.worker import EventsWorker
from oss.src.tasks.asyncio.shared.consumer import StreamConsumer

_STREAM = "streams:spans"
_GROUP = "worker-spans"
_REDIS_ERRORS = (RedisTimeoutError, RedisConnectionError)


def _delivery(message_id: bytes):
    return [(_STREAM, [(message_id, {b"data": b"payload"})])]


class _FakeRedis:
    def __init__(self, reads):
        self._reads = list(reads)
        self.connection_pool = SimpleNamespace(disconnect=AsyncMock())
        self.xdel = AsyncMock()
        self.xack = AsyncMock()

    async def xreadgroup(self, **_kwargs):
        item = self._reads.pop(0)
        if isinstance(item, BaseException):
            raise item
        return item


class _RecordingConsumer(StreamConsumer):
    def __init__(self, redis, *, failures=()):
        super().__init__(
            redis_client=redis,
            stream_name=_STREAM,
            consumer_group=_GROUP,
            consumer_name="span-1",
            max_batch_size=1,
        )
        self._failures = list(failures)
        self.processed = []

    async def process_batch(self, batch):
        if self._failures:
            raise self._failures.pop(0)
        self.processed.append(batch)
        return 1, [message_id for message_id, _ in batch]


def _consumer(redis, **kwargs) -> StreamConsumer:
    return StreamConsumer(
        redis_client=redis,
        stream_name=_STREAM,
        consumer_group=_GROUP,
        consumer_name="span-1",
        max_batch_size=1,
        **kwargs,
    )


@pytest.mark.parametrize("error_type", _REDIS_ERRORS)
async def test_run_reconnects_and_processes_the_next_read(error_type):
    redis = _FakeRedis(
        [error_type("dead socket"), _delivery(b"2-0"), asyncio.CancelledError()]
    )
    consumer = _RecordingConsumer(redis)

    with (
        patch("oss.src.utils.heartbeat.touch"),
        patch(
            "oss.src.tasks.asyncio.shared.consumer.asyncio.sleep",
            new_callable=AsyncMock,
        ) as sleep,
        pytest.raises(asyncio.CancelledError),
    ):
        await consumer.run()

    redis.connection_pool.disconnect.assert_awaited_once_with(inuse_connections=True)
    sleep.assert_awaited_once_with(1)
    assert consumer.processed == [[(b"2-0", {b"data": b"payload"})]]
    assert consumer.redis is redis


async def test_run_does_not_disconnect_on_an_empty_read():
    redis = _FakeRedis([None, asyncio.CancelledError()])
    consumer = _RecordingConsumer(redis)

    with (
        patch("oss.src.utils.heartbeat.touch"),
        patch(
            "oss.src.tasks.asyncio.shared.consumer.asyncio.sleep",
            new_callable=AsyncMock,
        ) as sleep,
        pytest.raises(asyncio.CancelledError),
    ):
        await consumer.run()

    redis.connection_pool.disconnect.assert_not_awaited()
    sleep.assert_not_awaited()
    assert consumer.processed == []


@pytest.mark.parametrize("error_type", _REDIS_ERRORS)
async def test_read_batch_surfaces_connection_failure(error_type):
    consumer = _consumer(_FakeRedis([error_type("dead socket")]))

    with pytest.raises(error_type):
        await consumer.read_batch()


@pytest.mark.parametrize("error_type", _REDIS_ERRORS)
@pytest.mark.parametrize("call", ["xpending_range", "xclaim"])
async def test_reclaim_batch_surfaces_connection_failure(error_type, call):
    error = error_type("dead socket")
    pending = [{"message_id": b"1-0", "times_delivered": 1}]
    redis = SimpleNamespace(
        xpending_range=(
            AsyncMock(side_effect=error)
            if call == "xpending_range"
            else AsyncMock(return_value=pending)
        ),
        xclaim=AsyncMock(side_effect=error),
    )
    consumer = _consumer(redis, reclaim_pending=True)

    with pytest.raises(error_type):
        await consumer.reclaim_batch()


async def test_read_batch_returns_empty_on_other_errors():
    consumer = _consumer(_FakeRedis([RuntimeError("parse")]))

    assert await consumer.read_batch() == []


@pytest.mark.parametrize(
    "error",
    [RuntimeError("db write failed"), TimeoutError("postgres statement timeout")],
)
async def test_run_keeps_the_pool_on_a_non_redis_failure(error):
    assert not isinstance(error, RedisTimeoutError)
    redis = _FakeRedis([_delivery(b"1-0"), _delivery(b"2-0"), asyncio.CancelledError()])
    consumer = _RecordingConsumer(redis, failures=[error])

    with (
        patch("oss.src.utils.heartbeat.touch"),
        patch(
            "oss.src.tasks.asyncio.shared.consumer.asyncio.sleep",
            new_callable=AsyncMock,
        ) as sleep,
        pytest.raises(asyncio.CancelledError),
    ):
        await consumer.run()

    redis.connection_pool.disconnect.assert_not_awaited()
    sleep.assert_awaited_once_with(1)
    assert consumer.processed == [[(b"2-0", {b"data": b"payload"})]]


def _events_worker(redis, dispatcher=None) -> EventsWorker:
    worker = EventsWorker.__new__(EventsWorker)
    worker.redis = redis
    worker.stream_name = "streams:events"
    worker.consumer_group = "worker-events"
    worker.consumer_name = "events-1"
    worker.max_batch_size = 50
    worker.webhooks_dispatcher = dispatcher
    return worker


async def test_events_worker_reconnects_after_a_redis_timeout():
    redis = _FakeRedis([])
    worker = _events_worker(redis)
    script = [
        RedisTimeoutError("dead socket"),
        [(b"1-0", {b"data": b"x"})],
        asyncio.CancelledError(),
    ]

    async def read_batch():
        item = script.pop(0)
        if isinstance(item, BaseException):
            raise item
        return item

    seen = []

    async def process_batch(batch):
        seen.append(batch)
        return 1, [batch[0][0]], []

    worker.read_batch = read_batch
    worker.process_batch = process_batch
    worker.ack_and_delete = AsyncMock()

    with (
        patch("oss.src.utils.heartbeat.touch"),
        patch(
            "oss.src.tasks.asyncio.shared.consumer.asyncio.sleep",
            new_callable=AsyncMock,
        ),
        pytest.raises(asyncio.CancelledError),
    ):
        await worker.run()

    redis.connection_pool.disconnect.assert_awaited_once_with(inuse_connections=True)
    assert seen == [[(b"1-0", {b"data": b"x"})]]
    worker.ack_and_delete.assert_awaited_once_with([b"1-0"])
    assert worker.redis is redis


async def test_events_worker_skips_ack_when_webhook_dispatch_fails():
    redis = _FakeRedis([])
    dispatcher = SimpleNamespace(
        dispatch=AsyncMock(side_effect=RuntimeError("webhook down"))
    )
    worker = _events_worker(redis, dispatcher=dispatcher)
    script = [[(b"1-0", {b"data": b"x"})], asyncio.CancelledError()]

    async def read_batch():
        item = script.pop(0)
        if isinstance(item, BaseException):
            raise item
        return item

    async def process_batch(_batch):
        return 1, [b"1-0"], [{"events": []}]

    worker.read_batch = read_batch
    worker.process_batch = process_batch
    worker.ack_and_delete = AsyncMock()

    with (
        patch("oss.src.utils.heartbeat.touch"),
        pytest.raises(asyncio.CancelledError),
    ):
        await worker.run()

    dispatcher.dispatch.assert_awaited_once()
    worker.ack_and_delete.assert_not_awaited()
    redis.connection_pool.disconnect.assert_not_awaited()
