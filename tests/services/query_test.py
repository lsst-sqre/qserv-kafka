"""Tests for creating new queries."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from pydantic import SecretStr

from qservkafka.config import config
from qservkafka.factory import Factory
from qservkafka.models.kafka import JobRun, JobStatus
from qservkafka.services.query import QueryService

from ..support.data import (
    read_test_job_cancel,
    read_test_job_run,
    read_test_job_status,
)
from ..support.datetime import assert_approximately_now
from ..support.qserv import MockQserv


async def assert_query_successful(
    *,
    query_service: QueryService,
    mock_qserv: MockQserv,
    job: JobRun,
    expected_status: JobStatus,
) -> None:
    """Run a query to completion with immediate results.

    Parameters
    ----------
    query_service
        Query service to test.
    mock_qserv
        Qserv mock.
    job
        Model of job to run.
    expected_status
        Model of status to expect.
    """
    mock_qserv.set_immediate_success(job)
    status = await query_service.start_query(job)
    assert status == expected_status
    assert_approximately_now(status.timestamp)
    assert status.query_info
    assert_approximately_now(status.query_info.start_time)
    assert_approximately_now(status.query_info.end_time)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mock_qserv", [False, True], ids=["good", "flaky"], indirect=True
)
async def test_start(factory: Factory) -> None:
    job = read_test_job_run("simple")
    expected_status = read_test_job_status("simple-started")
    query_service = factory.create_query_service()
    state_store = factory.create_query_state_store()

    status = await query_service.start_query(job)
    assert status == expected_status
    assert_approximately_now(status.timestamp)
    assert status.query_info
    assert_approximately_now(status.query_info.start_time)

    assert await state_store.get_active_queries() == {1}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mock_qserv", [False, True], ids=["good", "flaky"], indirect=True
)
async def test_immediate(factory: Factory, mock_qserv: MockQserv) -> None:
    """Test a job that completes immediately."""
    query_service = factory.create_query_service()
    job = read_test_job_run("data")
    expected_status = read_test_job_status("data-completed")
    state_store = factory.create_query_state_store()

    await assert_query_successful(
        query_service=query_service,
        mock_qserv=mock_qserv,
        job=job,
        expected_status=expected_status,
    )

    # It should be possible to immediately run the same query again. This
    # tests that the results were deleted from the database, and thus can be
    # re-added.
    assert expected_status.execution_id
    expected_status.execution_id = str(int(expected_status.execution_id) + 1)
    await assert_query_successful(
        query_service=query_service,
        mock_qserv=mock_qserv,
        job=job,
        expected_status=expected_status,
    )

    assert await state_store.get_active_queries() == set()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mock_qserv", [False, True], ids=["good", "flaky"], indirect=True
)
async def test_cancel(factory: Factory) -> None:
    job = read_test_job_run("simple")
    started_status = read_test_job_status("simple-started")
    cancel = read_test_job_cancel("simple")
    canceled_status = read_test_job_status("simple-aborted")
    query_service = factory.create_query_service()
    state_store = factory.create_query_state_store()

    status: JobStatus | None = await query_service.start_query(job)
    assert status == started_status
    assert_approximately_now(status.timestamp)
    assert status.query_info
    assert_approximately_now(status.query_info.start_time)
    start_time = status.query_info.start_time

    assert await state_store.get_active_queries() == {1}

    now = datetime.now(tz=UTC)
    status = await query_service.cancel_query(cancel)
    assert status == canceled_status
    assert_approximately_now(status.timestamp)
    assert status.query_info
    assert status.query_info.start_time == start_time
    assert status.query_info.end_time
    assert status.query_info.end_time >= now


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mock_qserv", [False, True], ids=["good", "flaky"], indirect=True
)
async def test_maxrec(factory: Factory, mock_qserv: MockQserv) -> None:
    query_service = factory.create_query_service()
    job = read_test_job_run("data-maxrec")
    expected_status = read_test_job_status("data-maxrec-completed")

    await assert_query_successful(
        query_service=query_service,
        mock_qserv=mock_qserv,
        job=job,
        expected_status=expected_status,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mock_qserv", [False, True], ids=["good", "flaky"], indirect=True
)
async def test_maxrec_zero(factory: Factory, mock_qserv: MockQserv) -> None:
    """Test a query with MAXREC set to zero."""
    query_service = factory.create_query_service()
    job = read_test_job_run("data-zero")
    expected_status = read_test_job_status("data-zero-completed")

    await assert_query_successful(
        query_service=query_service,
        mock_qserv=mock_qserv,
        job=job,
        expected_status=expected_status,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mock_qserv", [False, True], ids=["good", "flaky"], indirect=True
)
async def test_no_api_version(
    factory: Factory, mock_qserv: MockQserv, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Test disabling sending the API version in Qserv requests."""
    monkeypatch.setattr(config, "qserv_rest_send_api_version", False)
    query_service = factory.create_query_service()
    job = read_test_job_run("data")
    expected_status = read_test_job_status("data-completed")
    state_store = factory.create_query_state_store()

    await assert_query_successful(
        query_service=query_service,
        mock_qserv=mock_qserv,
        job=job,
        expected_status=expected_status,
    )

    # Also test starting a job with table upload, since that tests an
    # additional API endpoint.
    job = read_test_job_run("upload")
    expected_status = read_test_job_status("upload-started")
    expected_status.execution_id = "2"

    mock_qserv.set_immediate_success(None)
    status = await query_service.start_query(job)
    assert status == expected_status
    assert_approximately_now(status.timestamp)
    assert status.query_info
    assert_approximately_now(status.query_info.start_time)

    assert await state_store.get_active_queries() == {2}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mock_qserv", [False, True], ids=["good", "flaky"], indirect=True
)
async def test_auth(
    factory: Factory, mock_qserv: MockQserv, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Test authenticating to the Qserv REST API."""
    monkeypatch.setattr(config, "qserv_rest_username", "someuser")
    monkeypatch.setattr(config, "qserv_rest_password", SecretStr("password"))
    query_service = factory.create_query_service()
    state_store = factory.create_query_state_store()
    job = read_test_job_run("data")
    expected_status = read_test_job_status("data-completed")

    await assert_query_successful(
        query_service=query_service,
        mock_qserv=mock_qserv,
        job=job,
        expected_status=expected_status,
    )

    # Also test starting a job with table upload, since that tests an
    # additional API endpoint.
    job = read_test_job_run("upload")
    expected_status = read_test_job_status("upload-started")
    expected_status.execution_id = "2"

    mock_qserv.set_immediate_success(None)
    status = await query_service.start_query(job)
    assert status == expected_status
    assert_approximately_now(status.timestamp)
    assert status.query_info
    assert_approximately_now(status.query_info.start_time)

    assert await state_store.get_active_queries() == {2}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mock_qserv", [False, True], ids=["good", "flaky"], indirect=True
)
async def test_upload(factory: Factory, mock_qserv: MockQserv) -> None:
    """Test temporary table upload."""
    query_service = factory.create_query_service()
    state_store = factory.create_query_state_store()
    job = read_test_job_run("upload")
    completed_status = read_test_job_status("upload-completed")
    started_status = read_test_job_status("upload-started")

    await assert_query_successful(
        query_service=query_service,
        mock_qserv=mock_qserv,
        job=job,
        expected_status=completed_status,
    )
    assert mock_qserv.get_uploaded_table() is None

    mock_qserv.set_immediate_success(None)
    status = await query_service.start_query(job)
    started_status.execution_id = "2"
    assert status == started_status
    assert mock_qserv.get_uploaded_table() == job.upload_tables[0].table_name

    assert await state_store.get_active_queries() == {2}
