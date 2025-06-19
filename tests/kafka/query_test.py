"""Test the Qserv Kafka bridge with a real Kafka server."""

from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime, timedelta

import pytest
from aiokafka import AIOKafkaConsumer
from fastapi import FastAPI
from faststream.kafka import KafkaBroker
from testcontainers.redis import RedisContainer

from qservkafka.config import config
from qservkafka.dependencies.context import context_dependency
from qservkafka.factory import Factory
from qservkafka.models.kafka import JobRun, JobStatus
from qservkafka.models.qserv import AsyncQueryPhase, AsyncQueryStatus

from ..support.arq import run_arq_jobs
from ..support.data import (
    read_test_job_run,
    read_test_job_status,
    read_test_job_status_json,
    read_test_json,
)
from ..support.datetime import (
    assert_approximately_now,
    milliseconds_to_timestamp,
)
from ..support.qserv import MockQserv


async def start_query(kafka_broker: KafkaBroker, job_path: str) -> JobRun:
    """Send the Kafka message to start a query.

    Parameters
    ----------
    kafka_broker
        Kafka broker to use to send the message.
    job_path
        Path to the Kafka message to send.

    Returns
    -------
    JobRun
        Parsed version of the Kafka message.
    """
    job = read_test_job_run(job_path)
    job_json = read_test_json(job_path)
    await kafka_broker.publish(job_json, config.job_run_topic)
    return job


async def wait_for_status(
    kafka_status_consumer: AIOKafkaConsumer, status_path: str
) -> JobStatus:
    """Wait for a Kafka status message and check it.

    Parameters
    ----------
    kafka_status_consumer
        Consumer for the Kafka status topic.
    status_path
        Path to the Kafka status message to expect.

    Returns
    -------
    JobStatus
        Parsed Kafka status message.
    """
    expected = read_test_job_status_json(status_path)
    status = read_test_job_status(status_path)
    raw_message = await kafka_status_consumer.getone()
    message = json.loads(raw_message.value.decode())
    assert message == expected
    timestamp = milliseconds_to_timestamp(message["timestamp"])
    assert_approximately_now(timestamp)
    status.timestamp = timestamp
    start_time = milliseconds_to_timestamp(message["queryInfo"]["startTime"])
    assert_approximately_now(start_time)
    assert status.query_info
    status.query_info.start_time = start_time
    if message["queryInfo"].get("endTime"):
        end_time = milliseconds_to_timestamp(message["queryInfo"]["endTime"])
        assert_approximately_now(end_time)
        status.query_info.end_time = end_time
    return status


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
    factory = context_dependency.create_factory()

    job = await start_query(kafka_broker, "jobs/data")
    status = await wait_for_status(
        kafka_status_consumer, "status/data-started"
    )
    assert status.query_info

    now = datetime.now(tz=UTC)
    await mock_qserv.store_results(job)
    await mock_qserv.update_status(
        1,
        AsyncQueryStatus(
            query_id=1,
            status=AsyncQueryPhase.COMPLETED,
            total_chunks=10,
            completed_chunks=10,
            collected_bytes=250,
            query_begin=status.query_info.start_time,
            last_update=now,
        ),
    )

    await wait_for_dispatch(factory, 1)
    assert await run_arq_jobs() == 1
    await wait_for_status(kafka_status_consumer, "status/data-completed")

    # Ensure all query state has been deleted.
    redis_client = redis.get_client()
    assert set(redis_client.scan_iter("query:*")) == set()
