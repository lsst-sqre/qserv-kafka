"""Tests for the rate limiter storage."""

import pytest
from testcontainers.redis import RedisContainer

from qservkafka.factory import Factory


@pytest.mark.asyncio
async def test_start_end(factory: Factory) -> None:
    concurrent = factory.create_concurrent_query_storage()
    assert await concurrent.start_query("some-user") == 1
    assert await concurrent.start_query("some-user") == 2
    assert await concurrent.start_query("other-user") == 1
    assert await concurrent.end_query("other-user") == 0
    assert await concurrent.end_query("some-user") == 1


@pytest.mark.asyncio
async def test_reconcile(factory: Factory, redis: RedisContainer) -> None:
    concurrent = factory.create_concurrent_query_storage()
    redis_client = redis.get_client()

    assert await concurrent.start_query("some-user") == 1
    assert await concurrent.start_query("some-user") == 2
    assert await concurrent.start_query("other-user") == 1
    assert await concurrent.end_query("other-user") == 0

    # Intentionally create a nonsensical rate for one user.
    assert await concurrent.end_query("bad-user") == -1

    assert redis_client.get("rate:some-user") == b"2"
    assert redis_client.get("rate:other-user") == b"0"
    assert redis_client.get("rate:third-user") is None
    assert redis_client.get("rate:bad-user") == b"-1"

    counts = {"some-user": 1, "third-user": 1, "bad-user": 1}
    await concurrent.reconcile_query_counts(counts)

    assert redis_client.get("rate:some-user") == b"1"
    assert redis_client.get("rate:other-user") is None
    assert redis_client.get("rate:third-user") is None
    assert redis_client.get("rate:bad-user") == b"0"
