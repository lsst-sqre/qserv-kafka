"""Tests for the Kafka message handlers."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from unittest.mock import ANY, patch

import pytest
from fastapi import FastAPI
from faststream.kafka import KafkaBroker
from faststream.kafka.publisher.asyncapi import AsyncAPIDefaultPublisher
from httpx import Response
from safir.datetime import current_datetime
from vo_models.uws.types import ExecutionPhase

from qservkafka.config import config
from qservkafka.dependencies.context import context_dependency
from qservkafka.models.kafka import (
    JobCancel,
    JobError,
    JobErrorCode,
    JobRun,
    JobStatus,
)
from qservkafka.models.qserv import AsyncQueryPhase, AsyncQueryStatus

from ..support.arq import run_arq_jobs
from ..support.data import (
    read_test_job_run,
    read_test_job_status,
    read_test_job_status_json,
    read_test_json,
)
from ..support.qserv import MockQserv


@pytest.mark.asyncio
async def test_job_run(
    *,
    app: FastAPI,
    kafka_broker: KafkaBroker,
    status_publisher: AsyncAPIDefaultPublisher,
    mock_qserv: MockQserv,
) -> None:
    job = read_test_json("jobs/simple")
    status = read_test_job_status(
        "status/simple-started", mock_timestamps=False
    )
    expected = status.model_dump(mode="json")
    expected["timestamp"] = ANY
    expected["queryInfo"]["startTime"] = ANY

    await kafka_broker.publish(job, config.job_run_topic)
    assert status_publisher.mock
    status_publisher.mock.assert_called_once_with(expected)

    async_status = mock_qserv.get_status(1)
    status_publisher.mock.reset_mock()
    now = current_datetime()
    await mock_qserv.update_status(
        1,
        AsyncQueryStatus(
            query_id=1,
            status=AsyncQueryPhase.EXECUTING,
            total_chunks=10,
            completed_chunks=5,
            collected_bytes=150,
            query_begin=async_status.query_begin,
            last_update=now,
        ),
    )
    await asyncio.sleep(1.1)
    expected["queryInfo"]["completedChunks"] = 5
    expected["queryInfo"]["startTime"] = int(
        async_status.query_begin.timestamp() * 1000
    )
    expected["timestamp"] = int(now.timestamp() * 1000)
    status_publisher.mock.assert_called_once_with(expected)

    now = current_datetime()
    await mock_qserv.update_status(
        1,
        AsyncQueryStatus(
            query_id=1,
            status=AsyncQueryPhase.FAILED,
            total_chunks=10,
            completed_chunks=8,
            collected_bytes=200,
            query_begin=async_status.query_begin,
            last_update=now,
        ),
    )
    await asyncio.sleep(1.1)
    with patch.object(kafka_broker, "publish") as mock:
        assert await run_arq_jobs(kafka_broker) == 1
    expected["errorInfo"] = {
        "errorCode": "backend_error",
        "errorMessage": "Query failed in backend",
    }
    expected["status"] = "ERROR"
    expected["queryInfo"]["completedChunks"] = 8
    expected["queryInfo"]["endTime"] = int(now.timestamp() * 1000)
    expected["timestamp"] = int(now.timestamp() * 1000)
    mock.assert_called_once_with(
        expected,
        config.job_status_topic,
        headers={"Content-Type": "application/json"},
    )

    assert context_dependency._process_context
    state = context_dependency._process_context.state
    assert await state.get_active_queries() == set()


@pytest.mark.asyncio
async def test_job_results(
    *,
    app: FastAPI,
    kafka_broker: KafkaBroker,
    status_publisher: AsyncAPIDefaultPublisher,
    mock_qserv: MockQserv,
) -> None:
    job = read_test_json("jobs/data")
    status = read_test_job_status(
        "status/data-completed", mock_timestamps=False
    )
    expected = status.model_dump(mode="json")
    assert status_publisher.mock

    await kafka_broker.publish(job, config.job_run_topic)

    status_publisher.mock.reset_mock()
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
            collected_bytes=250,
            query_begin=async_status.query_begin,
            last_update=now,
        ),
    )

    await asyncio.sleep(1.1)
    with patch.object(kafka_broker, "publish") as mock:
        assert await run_arq_jobs(kafka_broker) == 1
    expected["queryInfo"]["startTime"] = int(
        async_status.query_begin.timestamp() * 1000
    )
    expected["queryInfo"]["endTime"] = int(now.timestamp() * 1000)
    expected["timestamp"] = int(now.timestamp() * 1000)
    mock.assert_called_once_with(
        expected,
        config.job_status_topic,
        headers={"Content-Type": "application/json"},
    )

    assert context_dependency._process_context
    state = context_dependency._process_context.state
    assert await state.get_active_queries() == set()


@pytest.mark.asyncio
async def test_job_result_error(
    *,
    app: FastAPI,
    kafka_broker: KafkaBroker,
    status_publisher: AsyncAPIDefaultPublisher,
    mock_qserv: MockQserv,
) -> None:
    """Test proper handling of an API error getting completed job status.

    An earlier version of the Qserv Kafka bridge erroneously didn't stop
    processing when the API request failed.
    """
    job = read_test_job_run("jobs/data")
    job_json = read_test_json("jobs/data")
    assert status_publisher.mock

    await kafka_broker.publish(job_json, config.job_run_topic)
    await asyncio.sleep(0.1)

    status_publisher.mock.reset_mock()
    mock_qserv.set_status_response(
        Response(
            200,
            json={"success": 0, "error": "Some error"},
        )
    )
    async_status = mock_qserv.get_status(1)
    now = datetime.now(tz=UTC)
    await mock_qserv.update_status(
        1,
        AsyncQueryStatus(
            query_id=1,
            status=AsyncQueryPhase.COMPLETED,
            total_chunks=10,
            completed_chunks=10,
            collected_bytes=250,
            query_begin=async_status.query_begin,
            last_update=now,
        ),
    )

    await asyncio.sleep(1)
    with patch.object(kafka_broker, "publish") as mock:
        assert await run_arq_jobs(kafka_broker) == 1
    expected = JobStatus(
        job_id=job.job_id,
        execution_id="1",
        timestamp=now,
        status=ExecutionPhase.ERROR,
        error=JobError(
            code=JobErrorCode.backend_error,
            message="Qserv request failed: Some error",
        ),
        metadata=job.to_job_metadata(),
    ).model_dump(mode="json")
    expected["timestamp"] = ANY
    mock.assert_called_once_with(
        expected,
        config.job_status_topic,
        headers={"Content-Type": "application/json"},
    )

    assert context_dependency._process_context
    state = context_dependency._process_context.state
    assert await state.get_active_queries() == set()


@pytest.mark.asyncio
async def test_job_cancel(
    *,
    app: FastAPI,
    kafka_broker: KafkaBroker,
    status_publisher: AsyncAPIDefaultPublisher,
    mock_qserv: MockQserv,
) -> None:
    """Test canceling a job."""
    job = read_test_job_run("jobs/simple")
    job_json = read_test_json("jobs/simple")
    status = read_test_job_status(
        "status/simple-aborted", mock_timestamps=False
    )
    assert status_publisher.mock

    await kafka_broker.publish(job_json, config.job_run_topic)
    await asyncio.sleep(0.1)

    status_publisher.mock.reset_mock()
    cancel = JobCancel(job_id=job.job_id, execution_id="1", owner="username")
    cancel_json = cancel.model_dump(mode="json")
    await kafka_broker.publish(cancel_json, config.job_cancel_topic)
    await asyncio.sleep(0.1)

    expected = status.model_dump(mode="json")
    expected["status"] = "ABORTED"
    expected["timestamp"] = ANY
    expected["queryInfo"]["startTime"] = ANY
    expected["queryInfo"]["endTime"] = ANY

    # If we're testing flaky connections, there may be a 3s delay.
    try:
        status_publisher.mock.assert_called_once_with(expected)
    except AssertionError:
        await asyncio.sleep(3)
        status_publisher.mock.assert_called_with(expected)

    assert context_dependency._process_context
    state = context_dependency._process_context.state
    assert await state.get_active_queries() == set()


@pytest.mark.asyncio
async def test_job_upload(
    *,
    app: FastAPI,
    kafka_broker: KafkaBroker,
    status_publisher: AsyncAPIDefaultPublisher,
    mock_qserv: MockQserv,
) -> None:
    """Test canceling a job."""
    job = read_test_job_run("jobs/upload")
    job_json = read_test_json("jobs/upload")
    status = read_test_job_status_json("status/upload-started")
    assert status_publisher.mock

    await kafka_broker.publish(job_json, config.job_run_topic)
    await asyncio.sleep(0.1)
    status_publisher.mock.assert_called_once_with(status)
    assert mock_qserv.get_uploaded_table() == job.upload_tables[0].table_name
