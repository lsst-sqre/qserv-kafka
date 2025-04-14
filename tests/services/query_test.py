"""Tests for creating new queries."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from unittest.mock import ANY

import pytest
from httpx import Response
from vo_models.uws.types import ExecutionPhase

from qservkafka.factory import Factory
from qservkafka.models.kafka import (
    JobError,
    JobErrorCode,
    JobQueryInfo,
    JobStatus,
)
from qservkafka.models.qserv import AsyncQueryPhase, AsyncQueryStatus

from ..support.data import read_test_job_run, read_test_job_status
from ..support.qserv import MockQserv


def assert_approximately_now(time: datetime) -> None:
    """Assert that a datetime is at most five seconds older than now."""
    now = datetime.now(tz=UTC)
    assert now - timedelta(seconds=5) <= time <= now


@pytest.mark.asyncio
async def test_start(factory: Factory) -> None:
    job = read_test_job_run("jobs/simple")
    expected_status = read_test_job_status("status/simple-started")
    query_service = factory.create_query_service()

    status = await query_service.start_query(job)
    assert status == expected_status
    assert_approximately_now(status.timestamp)
    assert status.query_info
    assert_approximately_now(status.query_info.start_time)


@pytest.mark.asyncio
async def test_start_errors(factory: Factory, mock_qserv: MockQserv) -> None:
    job = read_test_job_run("jobs/simple")
    query_service = factory.create_query_service()
    now = datetime.now(tz=UTC)

    # HTTP failure starting the job.
    mock_qserv.set_submit_response(Response(500))
    status = await query_service.start_query(job)
    expected = JobStatus(
        job_id=job.job_id,
        execution_id=None,
        timestamp=now,
        status=ExecutionPhase.ERROR,
        error=JobError(code=JobErrorCode.backend_request_error, message=""),
        metadata=job.to_job_metadata(),
    )
    expected.timestamp = ANY
    assert expected.error
    expected.error.message = ANY
    assert status == expected
    assert status.error
    assert "Status 500 from POST" in status.error.message
    assert_approximately_now(status.timestamp)

    # Invalid response from job creation endpoint.
    mock_qserv.set_submit_response(Response(200, json={"success": 1}))
    status = await query_service.start_query(job)
    expected.error.code = JobErrorCode.backend_internal_error
    assert status == expected
    assert status.error
    assert "Qserv request failed: " in status.error.message

    # Error response from job creation endpoint.
    mock_qserv.set_submit_response(
        Response(200, json={"success": 0, "error": "Some error"})
    )
    status = await query_service.start_query(job)
    expected.error.code = JobErrorCode.backend_error
    expected.error.message = "Qserv request failed: Some error"
    assert status == expected


@pytest.mark.asyncio
async def test_status_errors(factory: Factory, mock_qserv: MockQserv) -> None:
    job = read_test_job_run("jobs/simple")
    query_service = factory.create_query_service()
    now = datetime.now(tz=UTC)

    # HTTP failure getting the job status.
    mock_qserv.set_status_response(Response(500))
    status = await query_service.start_query(job)
    expected = JobStatus(
        job_id=job.job_id,
        execution_id="1",
        timestamp=now,
        status=ExecutionPhase.ERROR,
        error=JobError(code=JobErrorCode.backend_request_error, message=""),
        metadata=job.to_job_metadata(),
    )
    expected.timestamp = ANY
    assert expected.error
    expected.error.message = ANY
    assert status == expected
    assert status.error
    assert "Status 500 from GET" in status.error.message
    assert_approximately_now(status.timestamp)

    # Invalid response from the status endpoint.
    mock_qserv.set_status_response(
        Response(
            200, json={"success": 1, "status": {"queryId": 1, "status": "FOO"}}
        )
    )
    status = await query_service.start_query(job)
    expected.execution_id = "2"
    expected.error.code = JobErrorCode.backend_internal_error
    assert status == expected
    assert status.error
    assert "Qserv request failed: " in status.error.message

    # Error returned from the status endpoint.
    mock_qserv.set_status_response(
        Response(
            200,
            json={
                "success": 0,
                "error": "Some error",
                "error_ext": {"foo": "bar"},
            },
        )
    )
    status = await query_service.start_query(job)
    expected.execution_id = "3"
    expected.error.code = JobErrorCode.backend_error
    expected.error.message = (
        "Qserv request failed: Some error\n\n{'foo': 'bar'}"
    )
    assert status == expected

    # Return a normal reply from the status endpoint but mark the job as being
    # in an error state.
    query_status = AsyncQueryStatus(
        query_id=3,
        status=AsyncQueryPhase.FAILED,
        total_chunks=10,
        completed_chunks=4,
        query_begin=now,
        last_update=now,
    )
    mock_qserv.set_status_response(
        Response(
            200,
            json={
                "success": 1,
                "status": query_status.model_dump(mode="json"),
            },
        )
    )
    status = await query_service.start_query(job)
    expected.execution_id = "4"
    expected.query_info = JobQueryInfo(
        start_time=now, end_time=now, total_chunks=10, completed_chunks=4
    )
    expected.query_info.start_time = ANY
    expected.query_info.end_time = ANY
    expected.error.code = JobErrorCode.backend_error
    expected.error.message = "Query failed in backend"
    assert status == expected
    assert status.query_info
    assert_approximately_now(status.query_info.start_time)


@pytest.mark.asyncio
async def test_start_invalid(factory: Factory, mock_qserv: MockQserv) -> None:
    query_service = factory.create_query_service()
    now = datetime.now(tz=UTC)

    job = read_test_job_run("jobs/tabledata")
    status = await query_service.start_query(job)
    expected = JobStatus(
        job_id=job.job_id,
        execution_id=None,
        timestamp=now,
        status=ExecutionPhase.ERROR,
        error=JobError(
            code=JobErrorCode.invalid_request,
            message="TABLEDATA serialization not supported",
        ),
        metadata=job.to_job_metadata(),
    )
    expected.timestamp = ANY
    assert expected.error
    assert status == expected
