"""Tests for creating new queries."""

from datetime import UTC, datetime, timedelta
from unittest.mock import ANY

import pytest
from pydantic import SecretStr
from safir.datetime import current_datetime
from safir.metrics import MockEventPublisher
from safir.testing.slack import MockSlackWebhook

from qservkafka.config import config
from qservkafka.factory import Factory
from qservkafka.models.kafka import JobRun, JobStatus
from qservkafka.services.query import QueryService

from ..support.data import (
    read_test_data,
    read_test_job_cancel,
    read_test_job_run,
    read_test_job_status,
    read_test_qserv_status,
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
    kafka_timestamp = datetime.now(tz=UTC) - timedelta(seconds=10)
    status = await query_service.start_query(job, kafka_timestamp)
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

    assert await state_store.get_active_queries() == {"1"}


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

    start = datetime.now(tz=UTC)
    await assert_query_successful(
        query_service=query_service,
        mock_qserv=mock_qserv,
        job=job,
        expected_status=expected_status,
    )
    finish = datetime.now(tz=UTC)
    elapsed = finish - start

    # Check that the results were cleaned up.
    assert not mock_qserv.results_stored

    # Check that the correct metrics event was sent.
    assert isinstance(factory.events.qserv_success, MockEventPublisher)
    events = factory.events.qserv_success.published
    assert len(events) == 1
    success_event = events[0]
    assert success_event.model_dump(mode="json") == {
        "job_id": job.job_id,
        "username": job.owner,
        "elapsed": ANY,
        "kafka_elapsed": ANY,
        "qserv_elapsed": ANY,
        "result_elapsed": ANY,
        "submit_elapsed": ANY,
        "delete_elapsed": ANY,
        "rows": 2,
        "qserv_size": 250,
        "qserv_rate": None,
        "encoded_size": len(read_test_data("results/data.binary2")),
        "result_size": (
            len(read_test_data("results/data.binary2"))
            + len(job.result_format.envelope.header)
            + len(job.result_format.envelope.footer)
        ),
        "rate": (
            success_event.encoded_size / success_event.elapsed.total_seconds()
        ),
        "result_rate": (
            success_event.encoded_size
            / success_event.result_elapsed.total_seconds()
        ),
        "upload_tables": 0,
        "immediate": True,
    }

    # These time fields should include the fake Kafka delay of 10s.
    for field in ("elapsed", "kafka_elapsed"):
        timestamp = getattr(success_event, field)
        assert timedelta(seconds=10) <= timestamp
        assert timestamp <= elapsed + timedelta(seconds=10)

    # These time fields shouldn't include the Kafka delay.
    for field in (
        "qserv_elapsed",
        "result_elapsed",
        "submit_elapsed",
        "delete_elapsed",
    ):
        assert timedelta(seconds=0) <= getattr(success_event, field) <= elapsed

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

    # If Qserv was configured to intermittently fail, check that we logged
    # metrics events recording the failures.
    if mock_qserv.flaky:
        assert isinstance(factory.events.qserv_failure, MockEventPublisher)
        factory.events.qserv_failure.published.assert_published(
            [{"protocol": "HTTP"}, {"protocol": "SQL"}]
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mock_qserv", [False, True], ids=["good", "flaky"], indirect=True
)
async def test_no_delete(
    factory: Factory, mock_qserv: MockQserv, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Test that deleting results from Qserv can be configured."""
    monkeypatch.setattr(config, "qserv_delete_queries", False)
    query_service = factory.create_query_service()
    job = read_test_job_run("data")
    expected_status = read_test_job_status("data-completed")

    await assert_query_successful(
        query_service=query_service,
        mock_qserv=mock_qserv,
        job=job,
        expected_status=expected_status,
    )

    # Check that the query was not deleted.
    assert mock_qserv.results_stored

    # Check that the correct metrics events were sent.
    assert isinstance(factory.events.qserv_success, MockEventPublisher)
    events = factory.events.qserv_success.published
    assert len(events) == 1
    assert events[0].model_dump(mode="json") == {
        "job_id": job.job_id,
        "username": job.owner,
        "elapsed": ANY,
        "kafka_elapsed": ANY,
        "qserv_elapsed": ANY,
        "result_elapsed": ANY,
        "submit_elapsed": ANY,
        "delete_elapsed": None,
        "rows": 2,
        "qserv_size": 250,
        "qserv_rate": ANY,
        "encoded_size": len(read_test_data("results/data.binary2")),
        "result_size": ANY,
        "rate": ANY,
        "result_rate": ANY,
        "upload_tables": 0,
        "immediate": True,
    }


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
    start_time = status.query_info.start_time
    assert_approximately_now(start_time)

    assert await state_store.get_active_queries() == {"1"}

    now = datetime.now(tz=UTC)
    status = await query_service.cancel_query(cancel)
    finish = datetime.now(tz=UTC)
    assert status == canceled_status
    assert_approximately_now(status.timestamp)
    assert status.query_info
    assert status.query_info.start_time == start_time
    assert status.query_info.end_time
    assert status.query_info.end_time >= now

    # Check that the correct metrics event was sent.
    assert isinstance(factory.events.query_abort, MockEventPublisher)
    events = factory.events.query_abort.published
    assert len(events) == 1
    assert events[0].model_dump(mode="json") == {
        "job_id": job.job_id,
        "username": job.owner,
        "elapsed": ANY,
    }
    assert timedelta(seconds=0) < events[0].elapsed <= (finish - start_time)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mock_qserv", [False, True], ids=["good", "flaky"], indirect=True
)
async def test_cancel_completed(
    factory: Factory, mock_qserv: MockQserv, mock_slack: MockSlackWebhook
) -> None:
    query_service = factory.create_query_service()
    job = read_test_job_run("simple")
    expected_status = read_test_job_status("simple-started")
    cancel = read_test_job_cancel("simple")

    # Start the query.
    start_time = current_datetime()
    assert await query_service.start_query(job) == expected_status

    # Mark the query complete in the mock behind the back of the bridge.
    qserv_status = read_test_qserv_status(
        "simple-completed",
        query_begin=start_time,
        last_update=current_datetime(),
    )
    await mock_qserv.update_status(1, qserv_status)

    # Canceling a completed query should quietly do nothing.
    assert await query_service.cancel_query(cancel) is None
    assert isinstance(factory.events.query_abort, MockEventPublisher)
    events = factory.events.query_abort.published
    assert len(events) == 0
    assert mock_slack.messages == []


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

    assert await state_store.get_active_queries() == {"2"}


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

    assert await state_store.get_active_queries() == {"2"}


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

    start = datetime.now(tz=UTC)
    await assert_query_successful(
        query_service=query_service,
        mock_qserv=mock_qserv,
        job=job,
        expected_status=completed_status,
    )
    finish = datetime.now(tz=UTC)
    assert mock_qserv.get_uploaded_table() is None
    assert mock_qserv.get_uploaded_database() is None

    # Check that the correct metrics events were sent.
    assert isinstance(factory.events.qserv_success, MockEventPublisher)
    events = factory.events.qserv_success.published
    assert len(events) == 1
    assert events[0].model_dump(mode="json") == {
        "job_id": job.job_id,
        "username": job.owner,
        "elapsed": ANY,
        "kafka_elapsed": ANY,
        "qserv_elapsed": ANY,
        "result_elapsed": ANY,
        "submit_elapsed": ANY,
        "delete_elapsed": ANY,
        "rows": 2,
        "qserv_size": 250,
        "qserv_rate": ANY,
        "encoded_size": len(read_test_data("results/data.binary2")),
        "result_size": ANY,
        "rate": ANY,
        "result_rate": ANY,
        "upload_tables": 1,
        "immediate": True,
    }
    assert isinstance(factory.events.temporary_table, MockEventPublisher)
    events = factory.events.temporary_table.published
    assert len(events) == 1
    upload_event = events[0]
    assert upload_event.model_dump(mode="json") == {
        "job_id": job.job_id,
        "username": job.owner,
        "size": len(mock_qserv._UPLOAD_CSV),
        "elapsed": ANY,
    }
    assert timedelta(seconds=0) < upload_event.elapsed <= (finish - start)

    # Start another upload query, but this time don't let it complete
    # immediately. In this case, the uploaded table should still be present
    # (not yet deleted) since the query is still running.
    mock_qserv.set_immediate_success(None)
    status = await query_service.start_query(job)
    started_status.execution_id = "2"
    assert status == started_status
    assert mock_qserv.get_uploaded_table() == job.upload_tables[0].table_name
    assert mock_qserv.get_uploaded_database() == job.upload_tables[0].database

    # Only the second query should be active.
    assert await state_store.get_active_queries() == {"2"}
