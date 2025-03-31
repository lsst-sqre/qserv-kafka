"""Tests for the Kafka message handlers."""

from __future__ import annotations

from unittest.mock import ANY

import pytest
from fastapi import FastAPI
from faststream.kafka import KafkaBroker

from qservkafka.config import config
from qservkafka.handlers.kafka import publisher

from ..support.data import read_test_job_status, read_test_json


@pytest.mark.asyncio
async def test_job_run(app: FastAPI, kafka_broker: KafkaBroker) -> None:
    job = read_test_json("jobs/simple")
    status = read_test_job_status(
        "status/simple-started", mock_timestamps=False
    )
    expected = status.model_dump(mode="json")
    expected["timestamp"] = ANY
    expected["queryInfo"]["startTime"] = ANY

    await kafka_broker.publish(job, config.job_run_topic)
    assert publisher.mock
    publisher.mock.assert_called_once_with(expected)
