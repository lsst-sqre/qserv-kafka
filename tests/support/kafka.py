"""Helper functions for running queries end-to-end with Kafka."""

import asyncio
import json
from datetime import timedelta

from aiokafka import AIOKafkaConsumer
from faststream.kafka import KafkaBroker
from pydantic import ValidationError

from qservkafka.config import config
from qservkafka.factory import Factory
from qservkafka.models.kafka import JobRun, JobStatus

from ..support.data import QservKafkaData
from ..support.datetime import assert_approximately_now

__all__ = [
    "start_query",
    "wait_for_dispatch",
    "wait_for_status",
]


async def start_query(
    data: QservKafkaData, kafka_broker: KafkaBroker, job: str
) -> JobRun:
    """Send the Kafka message to start a query.

    Parameters
    ----------
    data
        Test data.
    kafka_broker
        Kafka broker to use to send the message.
    job
        Name of the Kafka message to send.

    Returns
    -------
    JobRun
        Parsed version of the Kafka message.
    """
    job_json = data.read_json(f"jobs/{job}")
    await kafka_broker.publish(job_json, config.job_run_topic)
    return JobRun.model_validate(job_json)


async def wait_for_status(
    data: QservKafkaData,
    kafka_status_consumer: AIOKafkaConsumer,
    status: str,
    *,
    execution_id: str | None = None,
) -> JobStatus:
    """Wait for a Kafka status message and check it.

    Parameters
    ----------
    data
        Test data.
    kafka_status_consumer
        Consumer for the Kafka status topic.
    status
        Name of the Kafka status message to expect.
    execution_id
        If set, expect this execution ID instead of the one in the loaded JSON
        file.

    Returns
    -------
    JobStatus
        Parsed Kafka status message.
    """
    # Get the status message from Kafka and do the equality check
    raw_message = await kafka_status_consumer.getone()
    try:
        message = json.loads(raw_message.value.decode())
        status_model = JobStatus.model_validate(message)
    except (json.JSONDecodeError, ValidationError) as e:
        msg = f"cannot decode message {raw_message.value.decode()}"
        raise AssertionError(msg) from e
    data.assert_job_status_matches(
        status_model, f"status/{status}", execution_id=execution_id
    )

    # Check the timestamps.
    assert_approximately_now(status_model.timestamp)
    if status_model.query_info:
        assert_approximately_now(status_model.query_info.start_time)
        if status_model.query_info.end_time:
            assert_approximately_now(status_model.query_info.end_time)
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
    poll_delay = config.backend_poll_interval.total_seconds() / 2
    async with asyncio.timeout(timeout.total_seconds()):
        while True:
            query = await state_store.get_query(str(query_id))
            assert query
            if query.result_queued:
                return
            await asyncio.sleep(poll_delay)
