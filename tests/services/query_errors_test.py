"""Tests for errors during query creation or completion."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from unittest.mock import ANY, patch

import pytest
from httpx import Response
from vo_models.uws.types import ExecutionPhase

from qservkafka.config import config
from qservkafka.factory import Factory
from qservkafka.models.kafka import (
    JobError,
    JobErrorCode,
    JobQueryInfo,
    JobStatus,
)
from qservkafka.models.qserv import AsyncQueryPhase, AsyncQueryStatus
from qservkafka.storage import qserv

from ..support.data import read_test_job_run
from ..support.datetime import assert_approximately_now
from ..support.qserv import MockQserv


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

    assert await factory.query_state_store.get_active_queries() == set()


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
        query_id=4,
        status=AsyncQueryPhase.FAILED,
        total_chunks=10,
        completed_chunks=4,
        collected_bytes=150,
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
    expected.error.code = JobErrorCode.backend_error
    expected.error.message = "Query failed in backend"
    assert status == expected

    assert await factory.query_state_store.get_active_queries() == set()


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

    job = read_test_job_run("jobs/arraysize")
    status = await query_service.start_query(job)
    expected = JobStatus(
        job_id=job.job_id,
        execution_id=None,
        timestamp=now,
        status=ExecutionPhase.ERROR,
        error=JobError(
            code=JobErrorCode.invalid_request,
            message="arraysize only supported for char fields",
        ),
        metadata=job.to_job_metadata(),
    )
    expected.timestamp = ANY
    assert status == expected

    assert await factory.query_state_store.get_active_queries() == set()


@pytest.mark.asyncio
async def test_sql_failure(factory: Factory, mock_qserv: MockQserv) -> None:
    query_service = factory.create_query_service()
    job = read_test_job_run("jobs/data")
    now = datetime.now(tz=UTC)

    mock_qserv.set_immediate_success(job)
    sql = "SELECT * FROM nonexistent"
    with patch.object(qserv, "_QUERY_RESULTS_SQL_FORMAT", new=sql):
        status = await query_service.start_query(job)

    expected = JobStatus(
        job_id=job.job_id,
        execution_id="1",
        timestamp=now,
        status=ExecutionPhase.ERROR,
        error=JobError(code=JobErrorCode.backend_sql_error, message=""),
        metadata=job.to_job_metadata(),
    )
    assert expected.error
    expected.error.message = ANY
    expected.timestamp = ANY
    assert status == expected
    assert_approximately_now(status.timestamp)

    assert await factory.query_state_store.get_active_queries() == set()


@pytest.mark.asyncio
async def test_upload_timeout(
    factory: Factory, mock_qserv: MockQserv, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Test handling of a timeout during results uploading.

    This should also cover a timeout in retrieving the data from SQL.
    """
    query_service = factory.create_query_service()
    job = read_test_job_run("jobs/data")

    mock_qserv.set_immediate_success(job)
    mock_qserv.set_upload_delay(timedelta(seconds=2))
    monkeypatch.setattr(config, "result_timeout", timedelta(seconds=1))
    status = await query_service.start_query(job)

    expected = JobStatus(
        job_id=job.job_id,
        execution_id="1",
        timestamp=datetime.now(tz=UTC),
        status=ExecutionPhase.ERROR,
        error=JobError(
            code=JobErrorCode.result_timeout,
            message="Retrieving and uploading results timed out",
        ),
        metadata=job.to_job_metadata(),
    )
    expected.timestamp = ANY
    assert status == expected
    assert_approximately_now(status.timestamp)

    assert await factory.query_state_store.get_active_queries() == set()
