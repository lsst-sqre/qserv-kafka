"""Tests for errors during query creation or completion."""

from datetime import UTC, datetime, timedelta
from unittest.mock import patch

import pytest
from httpx import Response
from safir.metrics import MockEventPublisher

from qservkafka.config import config
from qservkafka.factory import Factory
from qservkafka.models.kafka import JobCancel, JobRun
from qservkafka.storage import qserv

from ..support.data import QservKafkaData
from ..support.datetime import assert_approximately_now
from ..support.qserv import MockQserv
from ..support.query import start_and_complete_immediate


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mock_qserv", [False, True], ids=["good", "flaky"], indirect=True
)
async def test_start_errors(
    data: QservKafkaData, factory: Factory, mock_qserv: MockQserv
) -> None:
    job = data.read_pydantic(JobRun, "jobs/simple")
    query_service = factory.create_query_service()
    state_store = factory.create_query_state_store()

    # HTTP failure starting the job.
    mock_qserv.set_submit_response(Response(500))
    status = await query_service.start_query(job)
    data.assert_job_status_matches(status, "status/error-submit-http")
    assert_approximately_now(status.timestamp)

    # Invalid response from job creation endpoint.
    mock_qserv.set_submit_response(Response(200, json={"success": 1}))
    status = await query_service.start_query(job)
    data.assert_job_status_matches(status, "status/error-submit-invalid")
    assert status.error
    assert "Qserv request failed: " in status.error.message

    # Error response from job creation endpoint.
    error_json = data.read_json("qserv/error")
    mock_qserv.set_submit_response(Response(200, json=error_json))
    status = await query_service.start_query(job)
    data.assert_job_status_matches(status, "status/error-submit-failed")

    assert await state_store.get_active_queries() == set()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mock_qserv", [False, True], ids=["good", "flaky"], indirect=True
)
async def test_status_errors(
    data: QservKafkaData, factory: Factory, mock_qserv: MockQserv
) -> None:
    job = data.read_pydantic(JobRun, "jobs/simple")
    query_service = factory.create_query_service()
    state_store = factory.create_query_state_store()
    now = datetime.now(tz=UTC).replace(microsecond=0)

    # HTTP failure getting the job status.
    mock_qserv.set_status_response(Response(500))
    status = await query_service.start_query(job)
    data.assert_job_status_matches(status, "status/error-status-http")
    assert_approximately_now(status.timestamp)

    # Invalid response from the status endpoint.
    error_json = data.read_json("qserv/error-invalid")
    mock_qserv.set_status_response(Response(200, json=error_json))
    status = await query_service.start_query(job)
    data.assert_job_status_matches(
        status, "status/error-status-invalid", execution_id="2"
    )
    assert status.error
    assert "Qserv request failed: " in status.error.message

    # Error returned from the status endpoint.
    error_json = data.read_json("qserv/error-ext")
    mock_qserv.set_status_response(Response(200, json=error_json))
    status = await query_service.start_query(job)
    data.assert_job_status_matches(
        status, "status/error-status-failed", execution_id="3"
    )

    # Return a normal reply from the status endpoint but mark the job as being
    # in an error state.
    start = datetime.now(tz=UTC).replace(microsecond=0)
    error_json = data.read_json("qserv/error-status")
    error_json["query_begin"] = now.isoformat(timespec="seconds")
    error_json["last_update"] = start.isoformat(timespec="seconds")
    mock_qserv.set_status_response(Response(200, json=error_json))
    status = await query_service.start_query(job)
    now = datetime.now(tz=UTC)
    data.assert_job_status_matches(
        status, "status/error-status-partial", execution_id="4"
    )
    assert status.query_info
    assert status.query_info.start_time <= now
    assert status.query_info.end_time
    assert start <= status.query_info.end_time <= now

    # This last case is the only case where a metrics event should have been
    # published. We do not publish metrics events (at least at present) when
    # starting the query fails.
    assert isinstance(factory.events.query_failure, MockEventPublisher)
    events = factory.events.query_failure.published
    assert len(events) == 1
    data.assert_pydantic_matches(events[0], "events/error")
    assert timedelta(seconds=0) < events[0].elapsed <= (now - start)

    assert await state_store.get_active_queries() == set()


@pytest.mark.asyncio
async def test_start_invalid(
    data: QservKafkaData, factory: Factory, mock_qserv: MockQserv
) -> None:
    query_service = factory.create_query_service()
    state_store = factory.create_query_state_store()

    job = data.read_pydantic(JobRun, "jobs/tabledata")
    status = await query_service.start_query(job)
    data.assert_job_status_matches(status, "status/error-tabledata")

    job = data.read_pydantic(JobRun, "jobs/arraysize")
    status = await query_service.start_query(job)
    data.assert_job_status_matches(status, "status/error-arraysize")

    assert await state_store.get_active_queries() == set()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mock_qserv", [False, True], ids=["good", "flaky"], indirect=True
)
async def test_sql_failure(
    data: QservKafkaData, factory: Factory, mock_qserv: MockQserv
) -> None:
    query_service = factory.create_query_service()
    state_store = factory.create_query_state_store()
    job = data.read_pydantic(JobRun, "jobs/data")

    mock_qserv.set_immediate_success(job)
    results_sql = "SELECT * FROM nonexistent"
    with patch.object(qserv, "_query_results_sql", return_value=results_sql):
        status = await start_and_complete_immediate(
            query_service, factory, job
        )
    data.assert_job_status_matches(status, "status/error-sql")
    assert status.error
    assert "SQL query error: " in status.error.message
    assert_approximately_now(status.timestamp)

    assert await state_store.get_active_queries() == set()


@pytest.mark.asyncio
async def test_upload_timeout(
    *,
    data: QservKafkaData,
    factory: Factory,
    mock_qserv: MockQserv,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test handling of a timeout during results uploading.

    This should also cover a timeout in retrieving the data from SQL.
    """
    query_service = factory.create_query_service()
    state_store = factory.create_query_state_store()
    job = data.read_pydantic(JobRun, "jobs/data")

    mock_qserv.set_immediate_success(job)
    mock_qserv.set_upload_delay(timedelta(seconds=2))
    monkeypatch.setattr(config, "result_timeout", timedelta(seconds=1))
    status = await start_and_complete_immediate(query_service, factory, job)
    data.assert_job_status_matches(status, "status/error-upload-timeout")
    assert_approximately_now(status.timestamp)

    assert await state_store.get_active_queries() == set()


@pytest.mark.asyncio
async def test_cancel_unknown(data: QservKafkaData, factory: Factory) -> None:
    """Test canceling an unknown job."""
    query_service = factory.create_query_service()
    cancel = data.read_pydantic(JobCancel, "cancel/simple")

    assert await query_service.cancel_query(cancel) is None
