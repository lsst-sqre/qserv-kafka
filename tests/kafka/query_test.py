"""Test the Qserv Kafka bridge with a real Kafka server."""

from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime, timedelta

import pytest
from aiokafka import AIOKafkaConsumer
from asgi_lifespan import LifespanManager
from fastapi import FastAPI
from faststream.kafka import KafkaBroker
from safir.datetime import current_datetime
from testcontainers.redis import RedisContainer

from qservkafka.config import config
from qservkafka.dependencies.context import context_dependency
from qservkafka.factory import Factory
from qservkafka.models.kafka import JobRun, JobStatus
from qservkafka.models.qserv import AsyncQueryPhase, AsyncQueryStatus
from qservkafka.models.state import Query

from ..support.arq import run_arq_jobs
from ..support.data import (
    read_test_job_run,
    read_test_job_run_json,
    read_test_job_status,
    read_test_job_status_json,
)
from ..support.datetime import (
    assert_approximately_now,
    milliseconds_to_timestamp,
)
from ..support.qserv import MockQserv


async def start_query(kafka_broker: KafkaBroker, job: str) -> JobRun:
    """Send the Kafka message to start a query.

    Parameters
    ----------
    kafka_broker
        Kafka broker to use to send the message.
    job
        Name of the Kafka message to send.

    Returns
    -------
    JobRun
        Parsed version of the Kafka message.
    """
    job_model = read_test_job_run(job)
    job_json = read_test_job_run_json(job)
    await kafka_broker.publish(job_json, config.job_run_topic)
    return job_model


async def wait_for_status(
    kafka_status_consumer: AIOKafkaConsumer, status: str
) -> JobStatus:
    """Wait for a Kafka status message and check it.

    Parameters
    ----------
    kafka_status_consumer
        Consumer for the Kafka status topic.
    status
        Name to the Kafka status message to expect.

    Returns
    -------
    JobStatus
        Parsed Kafka status message.
    """
    expected = read_test_job_status_json(status)
    status_model = read_test_job_status(status)
    raw_message = await kafka_status_consumer.getone()
    message = json.loads(raw_message.value.decode())
    assert message == expected
    timestamp = milliseconds_to_timestamp(message["timestamp"])
    assert_approximately_now(timestamp)
    status_model.timestamp = timestamp
    start_time = milliseconds_to_timestamp(message["queryInfo"]["startTime"])
    assert_approximately_now(start_time)
    assert status_model.query_info
    status_model.query_info.start_time = start_time
    if message["queryInfo"].get("endTime"):
        end_time = milliseconds_to_timestamp(message["queryInfo"]["endTime"])
        assert_approximately_now(end_time)
        status_model.query_info.end_time = end_time
    return status_model


async def wait_for_dispatch(
    factory: Factory,
    query_id: int,
    *,
    timeout: timedelta = timedelta(seconds=1),
) -> None:
    """Wait for a job to be queued for the result worker.

    Parameters
    ----------
    factory
        Component factory to use.
    query_id
        Qserv query ID.
    timeout
        How long to wait for the dispatch before giving up.

    Raises
    ------
    TimeoutError
        Raised if it takes more than the timeout interval for the job to be
        dispatched to the backend worker.
    """
    state_store = factory.create_query_state_store()

    # Use polling of Redis, since subscribing to key updates in Redis is
    # complicated enough that I don't feel like writing all that code.
    poll_delay = config.qserv_poll_interval.total_seconds() / 2
    async with asyncio.timeout(timeout.total_seconds()):
        while True:
            query = await state_store.get_query(query_id)
            assert query
            if query.result_queued:
                return
            await asyncio.sleep(poll_delay)


@pytest.mark.asyncio
@pytest.mark.timeout(60)
async def test_success(
    *,
    app: FastAPI,
    kafka_broker: KafkaBroker,
    kafka_status_consumer: AIOKafkaConsumer,
    mock_qserv: MockQserv,
    redis: RedisContainer,
) -> None:
    async with LifespanManager(app):
        factory = context_dependency.create_factory()

        job = await start_query(kafka_broker, "data")
        status = await wait_for_status(kafka_status_consumer, "data-started")
        assert status.query_info
        start_time = status.query_info.start_time

        await mock_qserv.store_results(job)
        await mock_qserv.update_status(
            1,
            AsyncQueryStatus(
                query_id=1,
                status=AsyncQueryPhase.COMPLETED,
                total_chunks=10,
                completed_chunks=10,
                collected_bytes=250,
                query_begin=start_time,
                last_update=datetime.now(tz=UTC),
            ),
        )

        await wait_for_dispatch(factory, 1)

        # Run the background task queue.
        assert await run_arq_jobs() == 1
        status = await wait_for_status(kafka_status_consumer, "data-completed")
        assert status.query_info
        assert status.query_info.start_time == start_time
        assert status.query_info.end_time
        assert status.query_info.end_time >= start_time

    # Ensure all query state has been deleted.
    redis_client = redis.get_client()
    assert set(redis_client.scan_iter("query:*")) == set()


@pytest.mark.asyncio
@pytest.mark.timeout(60)
async def test_failure(
    *,
    app: FastAPI,
    kafka_broker: KafkaBroker,
    kafka_status_consumer: AIOKafkaConsumer,
    mock_qserv: MockQserv,
    redis: RedisContainer,
) -> None:
    async with LifespanManager(app):
        factory = context_dependency.create_factory()

        await start_query(kafka_broker, "simple")
        status = await wait_for_status(kafka_status_consumer, "simple-started")
        assert status.query_info
        start_time = status.query_info.start_time

        now = current_datetime()
        await mock_qserv.update_status(
            1,
            AsyncQueryStatus(
                query_id=1,
                status=AsyncQueryPhase.EXECUTING,
                total_chunks=10,
                completed_chunks=5,
                collected_bytes=150,
                query_begin=start_time,
                last_update=now,
            ),
        )
        status = await wait_for_status(kafka_status_consumer, "simple-partial")
        assert status.timestamp == now
        assert status.query_info
        assert status.query_info.start_time == start_time

        now = current_datetime()
        await mock_qserv.update_status(
            1,
            AsyncQueryStatus(
                query_id=1,
                status=AsyncQueryPhase.FAILED,
                total_chunks=10,
                completed_chunks=8,
                collected_bytes=200,
                query_begin=start_time,
                last_update=now,
            ),
        )
        await wait_for_dispatch(factory, 1)

        # Run the background tsk queue.
        assert await run_arq_jobs() == 1
        status = await wait_for_status(kafka_status_consumer, "simple-failed")
        assert status.timestamp == now
        assert status.query_info
        assert status.query_info.start_time == start_time
        assert status.query_info.end_time
        assert status.query_info.end_time >= now

    # Ensure all query state has been deleted.
    redis_client = redis.get_client()
    assert set(redis_client.scan_iter("query:*")) == set()


@pytest.mark.asyncio
@pytest.mark.timeout(60)
async def test_missing_executing(
    *,
    app: FastAPI,
    kafka_broker: KafkaBroker,
    kafka_status_consumer: AIOKafkaConsumer,
    mock_qserv: MockQserv,
    redis: RedisContainer,
) -> None:
    """Test queries that are not in the process list but still executing."""
    async with LifespanManager(app):
        factory = context_dependency.create_factory()
        await start_query(kafka_broker, "data")
        await wait_for_status(kafka_status_consumer, "data-started")

        # Remove the query from the running query list. It should be
        # dispatched to the result worker.
        await mock_qserv.remove_running_query(1)
        await wait_for_dispatch(factory, 1)

    # Run the backend worker. It should process the job and send the same
    # status update we already sent (since nothing has changed).
    assert await run_arq_jobs() == 1
    await wait_for_status(kafka_status_consumer, "data-started")

    # The query should still be active and should no longer be marked as
    # dispatched, so it will be checked again the next time through the
    # monitor loop.
    redis_client = redis.get_client()
    raw_query = redis_client.get("query:1")
    assert raw_query
    query = Query.model_validate(json.loads(raw_query))
    assert not query.result_queued
