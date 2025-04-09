"""Tests for the Kafka message handlers."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from unittest.mock import ANY

import pytest
from fastapi import FastAPI
from faststream.kafka import KafkaBroker
from safir.datetime import current_datetime

from qservkafka.config import config
from qservkafka.handlers.kafka import publisher
from qservkafka.models.kafka import JobRun
from qservkafka.models.qserv import AsyncQueryPhase, AsyncQueryStatus

from ..support.data import read_test_job_status, read_test_json
from ..support.qserv import MockQserv


@pytest.mark.asyncio
async def test_job_run(
    app: FastAPI, kafka_broker: KafkaBroker, mock_qserv: MockQserv
) -> None:
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

    async_status = mock_qserv.get_status(1)
    publisher.mock.reset_mock()
    now = current_datetime()
    await mock_qserv.update_status(
        1,
        AsyncQueryStatus(
            query_id=1,
            status=AsyncQueryPhase.EXECUTING,
            total_chunks=10,
            completed_chunks=5,
            query_begin=async_status.query_begin,
            last_update=now,
        ),
    )
    await asyncio.sleep(1)
    expected["queryInfo"]["completedChunks"] = 5
    expected["queryInfo"]["startTime"] = int(
        async_status.query_begin.timestamp() * 1000
    )
    expected["timestamp"] = int(now.timestamp() * 1000)
    publisher.mock.assert_called_once_with(expected)

    publisher.mock.reset_mock()
    now = current_datetime()
    await mock_qserv.update_status(
        1,
        AsyncQueryStatus(
            query_id=1,
            status=AsyncQueryPhase.FAILED,
            total_chunks=10,
            completed_chunks=8,
            query_begin=async_status.query_begin,
            last_update=now,
        ),
    )
    await asyncio.sleep(1)
    expected["errorInfo"] = {
        "errorCode": "backend_error",
        "errorMessage": "Query failed in backend",
    }
    expected["status"] = "ERROR"
    expected["queryInfo"]["completedChunks"] = 8
    expected["queryInfo"]["endTime"] = int(now.timestamp() * 1000)
    expected["timestamp"] = int(now.timestamp() * 1000)
    publisher.mock.assert_called_once_with(expected)


@pytest.mark.asyncio
async def test_job_results(
    app: FastAPI, kafka_broker: KafkaBroker, mock_qserv: MockQserv
) -> None:
    job = read_test_json("jobs/data")
    status = read_test_job_status(
        "status/data-completed", mock_timestamps=False
    )
    expected = status.model_dump(mode="json")
    assert publisher.mock

    await kafka_broker.publish(job, config.job_run_topic)

    publisher.mock.reset_mock()
    await mock_qserv.store_results(JobRun.model_validate(job))
    async_status = mock_qserv.get_status(1)
    now = datetime.now(tz=UTC)
    await mock_qserv.update_status(
        1,
        AsyncQueryStatus(
            query_id=1,
            status=AsyncQueryPhase.COMPLETED,
            total_chunks=10,
            completed_chunks=10,
            query_begin=async_status.query_begin,
            last_update=now,
        ),
    )

    await asyncio.sleep(2)
    expected["queryInfo"]["startTime"] = int(
        async_status.query_begin.timestamp() * 1000
    )
    expected["queryInfo"]["endTime"] = int(now.timestamp() * 1000)
    expected["timestamp"] = int(now.timestamp() * 1000)
    publisher.mock.assert_called_once_with(expected)
