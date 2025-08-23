"""Helper functions for running queries end-to-end with Kafka."""

from __future__ import annotations

import asyncio
import json
from datetime import timedelta

from aiokafka import AIOKafkaConsumer
from faststream.kafka import KafkaBroker

from qservkafka.config import config
from qservkafka.factory import Factory
from qservkafka.models.kafka import JobRun, JobStatus

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

__all__ = [
    "start_query",
    "wait_for_dispatch",
    "wait_for_status",
]


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
    kafka_status_consumer: AIOKafkaConsumer,
    status: str,
    *,
    execution_id: str | None = None,
) -> JobStatus:
    """Wait for a Kafka status message and check it.

    Parameters
    ----------
    kafka_status_consumer
        Consumer for the Kafka status topic.
    status
        Name to the Kafka status message to expect.
    execution_id
        If set, expect this execution ID instead of the one in the loaded JSON
        file.

    Returns
    -------
    JobStatus
        Parsed Kafka status message.
    """
    expected = read_test_job_status_json(status)
    status_model = read_test_job_status(status)
    if execution_id is not None:
        expected["executionID"] = execution_id
        status_model.execution_id = execution_id

    # Get the status message from Kafka and do the equality check
    raw_message = await kafka_status_consumer.getone()
    try:
        message = json.loads(raw_message.value.decode())
    except json.JSONDecodeError as e:
        msg = f"cannot decode message {raw_message.value.decode()}"
        raise AssertionError(msg) from e
    assert message == expected

    # Check the timestamps and update the model to match the received message.
    timestamp = milliseconds_to_timestamp(message["timestamp"])
    assert_approximately_now(timestamp)
    status_model.timestamp = timestamp
    if message.get("queryInfo"):
        start_time_milli = message["queryInfo"]["startTime"]
        start_time = milliseconds_to_timestamp(start_time_milli)
        assert_approximately_now(start_time)
        assert status_model.query_info
        status_model.query_info.start_time = start_time
        if message["queryInfo"].get("endTime"):
            end_time_milli = message["queryInfo"]["endTime"]
            end_time = milliseconds_to_timestamp(end_time_milli)
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
