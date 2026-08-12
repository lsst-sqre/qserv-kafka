"""Tests for the query status monitor."""

from datetime import UTC, datetime
from unittest.mock import call, patch

import pytest
from safir.arq import RedisArqQueue
from safir.metrics import MockEventPublisher
from testcontainers.redis import RedisContainer

from qservkafka.factory import Factory
from qservkafka.models.kafka import JobRun

from ..support.data import QservKafkaData
from ..support.qserv import MockQserv


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mock_qserv", [False, True], ids=["good", "flaky"], indirect=True
)
async def test_dispatch(
    data: QservKafkaData, factory: Factory, mock_qserv: MockQserv
) -> None:
    query_service = factory.create_query_service()
    state_store = factory.create_query_state_store()
    monitor = await factory.create_query_monitor()
    job = data.read_pydantic(JobRun, "jobs/simple")

    status = await query_service.start_query(job)
    data.assert_job_status_matches(status, "status/simple-started")

    # Check running query status and see if the count of running queries is
    # logged properly as a metric.
    await monitor.check_status()
    assert isinstance(factory.events.qserv_executing, MockEventPublisher)
    events = factory.events.qserv_executing.published
    assert len(events) == 1
    data.assert_pydantic_matches(events[0], "events/executing")

    qserv_status = mock_qserv.get_status(1)
    now = datetime.now(tz=UTC).replace(microsecond=0)
    qserv_status = data.read_qserv_status(
        "qserv/simple-completed",
        query_begin=qserv_status.query_begin,
        last_update=now,
    )
    await mock_qserv.update_status(1, qserv_status)

    query = await state_store.get_query(str(1))
    assert query
    qserv_status = mock_qserv.get_status(1)
    with patch.object(RedisArqQueue, "enqueue") as mock:
        await monitor.check_query(query, status=None)
        assert mock.call_args_list == [call("handle_finished_query", "1")]
        mock.reset_mock()

        # Running a second check on the query should notice that the query was
        # already dispatched from information stored in the state store and
        # should not dispatch it again.
        query = await state_store.get_query(str(1))
        assert query
        await monitor.check_query(query, status=None)
        assert mock.call_args_list == []


@pytest.mark.asyncio
async def test_quota(
    *,
    data: QservKafkaData,
    factory: Factory,
    mock_qserv: MockQserv,
    redis: RedisContainer,
) -> None:
    redis_client = redis.get_client()
    query_service = factory.create_query_service()
    monitor = await factory.create_query_monitor()
    job = data.read_pydantic(JobRun, "jobs/simple")
    redis_key = f"rate:{job.owner}"

    # Start a couple of jobs.
    status = await query_service.start_query(job)
    data.assert_job_status_matches(status, "status/simple-started")
    status = await query_service.start_query(job)
    data.assert_job_status_matches(
        status, "status/simple-started", execution_id="2"
    )

    # Check that the rate limit information is correct in Redis.
    assert redis_client.get(redis_key) == b"2"

    # Manually set the rate information incorrectly in Redis.
    redis_client.set(redis_key, b"3")

    # Reconciling should reduce the Redis running query information to match
    # reality.
    await monitor.reconcile_rate_limits()
    assert redis_client.get(redis_key) == b"2"

    # Now set the rate limit information too low and reconcile again. This
    # shouldn't change anything since we err on the side of leaving lower
    # numbers in the event of a race.
    redis_client.set(redis_key, b"1")
    await monitor.reconcile_rate_limits()
    assert redis_client.get(redis_key) == b"1"

    # Set the rate limit information to a negative number. This we should
    # change, but to 0, even though we think we have two running jobs.
    redis_client.set(redis_key, b"-11")
    await monitor.reconcile_rate_limits()
    assert redis_client.get(redis_key) == b"0"

    # Add a rate limit key for some other random user with no jobs and confirm
    # that reconciling removes that key.
    redis_client.set("rate:other-user", b"1")
    await monitor.reconcile_rate_limits()
    assert redis_client.get("rate:other-user") is None
