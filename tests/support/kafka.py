"""Helper functions for running queries end-to-end with Kafka."""

import json

from aiokafka import AIOKafkaConsumer
from faststream.kafka import KafkaBroker
from pydantic import ValidationError

from qservkafka.config import config
from qservkafka.models.kafka import JobRun, JobStatus

from ..support.data import QservKafkaData
from ..support.datetime import assert_approximately_now

__all__ = ["KafkaTestManager"]


class KafkaTestManager:
    """Manage the steps of a test that uses a real Kafka service.

    This class encapsulates the various objects used to talk to the Kafka
    service to set up a test and provides a simple API to tests to perform the
    Kafka portions of a query.

    Parameters
    ----------
    data
        Test data management class.
    kafka_broker
        Kafka broker to use to send the message.
    kafka_status_consumer
        Consumer for the Kafka status topic.
    """

    def __init__(
        self,
        data: QservKafkaData,
        kafka_broker: KafkaBroker,
        kafka_status_consumer: AIOKafkaConsumer,
    ) -> None:
        self._data = data
        self._broker = kafka_broker
        self._consumer = kafka_status_consumer

    async def start_query(self, path: str) -> JobRun:
        """Send the Kafka message to start a query.

        Parameters
        ----------
        job
            Path to the Kafka job message to send. This is a relative path to
            :file:`tests/data` without any ``.json`` extension.

        Returns
        -------
        JobRun
            Parsed version of the Kafka message.
        """
        job_json = self._data.read_json(path)
        await self._broker.publish(job_json, config.job_run_topic)
        return JobRun.model_validate(job_json)

    async def wait_for_status(
        self,
        path: str,
        *,
        execution_id: str | None = None,
    ) -> JobStatus:
        """Wait for a Kafka status message and check it.

        Parameters
        ----------
        status
            Path to the Kafka status message to expect. This is a relative
            path to :file:`tests/data` without any ``.json`` extension.
        execution_id
            If set, expect this execution ID instead of the one in the loaded
            JSON file.

        Returns
        -------
        JobStatus
            Parsed Kafka status message.
        """
        raw_message = await self._consumer.getone()
        try:
            message = json.loads(raw_message.value.decode())
            status = JobStatus.model_validate(message)
        except (json.JSONDecodeError, ValidationError) as e:
            msg = f"cannot decode message {raw_message.value.decode()}"
            raise AssertionError(msg) from e
        self._data.assert_job_status_matches(
            status, path, execution_id=execution_id
        )

        # Check the timestamps.
        assert_approximately_now(status.timestamp)
        if status.query_info:
            assert_approximately_now(status.query_info.start_time)
            if status.query_info.end_time:
                assert_approximately_now(status.query_info.end_time)
        return status
