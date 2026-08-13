"""Tests for creating new queries."""

from datetime import UTC, datetime, timedelta
from unittest.mock import call, patch

import pytest
from pydantic import SecretStr
from safir.arq import RedisArqQueue
from safir.metrics import MockEventPublisher
from safir.testing.slack import MockSlackWebhook

from qservkafka.config import config
from qservkafka.factory import Factory
from qservkafka.models.kafka import JobCancel, JobRun
from qservkafka.models.state import Query
from qservkafka.services.query import QueryService

from ..support.data import QservKafkaData
from ..support.datetime import assert_approximately_now
from ..support.kafka import read_status_message
from ..support.qserv import MockQserv
from ..support.query import start_and_complete_immediate


async def assert_query_successful(
    *,
    data: QservKafkaData,
    factory: Factory,
    query_service: QueryService,
    mock_qserv: MockQserv,
    job: JobRun,
    status: str,
    execution_id: str | None = None,
) -> None:
    """Run a query to completion with immediate results.

    Parameters
    ----------
    data
        Test data management.
    factory
        Component factory to use.
    query_service
        Query service to test.
    mock_qserv
        Qserv mock.
    job
        Model of job to run.
    status
        Path to status to expect.
    execution_id
        Expected execution ID for the job status.
    """
    mock_qserv.set_immediate_success(job)
    kafka_timestamp = datetime.now(tz=UTC) - timedelta(seconds=10)
    result = await start_and_complete_immediate(
        factory, job, kafka_start=kafka_timestamp
    )
    data.assert_job_status_matches(result, status, execution_id=execution_id)
    assert_approximately_now(result.timestamp)
    assert result.query_info
    assert_approximately_now(result.query_info.start_time)
    assert_approximately_now(result.query_info.end_time)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mock_qserv", [False, True], ids=["good", "flaky"], indirect=True
)
async def test_start(data: QservKafkaData, factory: Factory) -> None:
    job = data.read_pydantic(JobRun, "jobs/simple")
    query_service = factory.create_query_service()
    state_store = factory.create_query_state_store()

    await query_service.handle_query(job)
    status = read_status_message(factory)
    data.assert_job_status_matches(status, "status/simple-started")
    assert_approximately_now(status.timestamp)
    assert status.query_info
    assert_approximately_now(status.query_info.start_time)

    assert await state_store.get_active_queries() == {"1"}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mock_qserv", [False, True], ids=["good", "flaky"], indirect=True
)
async def test_immediate(
    data: QservKafkaData, factory: Factory, mock_qserv: MockQserv
) -> None:
    """Test a job that completes immediately."""
    query_service = factory.create_query_service()
    job = data.read_pydantic(JobRun, "jobs/data")
    state_store = factory.create_query_state_store()

    start = datetime.now(tz=UTC)
    await assert_query_successful(
        data=data,
        factory=factory,
        query_service=query_service,
        mock_qserv=mock_qserv,
        job=job,
        status="status/data-completed",
    )
    finish = datetime.now(tz=UTC)
    elapsed = finish - start

    # Check that the results were cleaned up.
    assert not mock_qserv.results_stored

    # Check that the correct metrics event was sent.
    assert isinstance(factory.events.query_success, MockEventPublisher)
    events = factory.events.query_success.published
    assert len(events) == 1
    event = events[0]
    data.assert_pydantic_matches(event, "events/success")

    # These time fields should include the fake Kafka delay of 10s.
    for field in ("elapsed", "kafka_elapsed"):
        timestamp = getattr(event, field)
        assert timedelta(seconds=10) <= timestamp
        assert timestamp <= elapsed + timedelta(seconds=10)

    # These time fields shouldn't include the Kafka delay.
    for field in ("qserv_elapsed", "result_elapsed", "submit_elapsed"):
        assert timedelta(seconds=0) <= getattr(event, field) <= elapsed

    # Check the calculated rates.
    assert event.rate == event.encoded_size / event.elapsed.total_seconds()
    assert event.result_rate == (
        event.encoded_size / event.result_elapsed.total_seconds()
    )

    # It should be possible to immediately run the same query again. This
    # tests that the results were deleted from the database, and thus can be
    # re-added.
    await assert_query_successful(
        data=data,
        factory=factory,
        query_service=query_service,
        mock_qserv=mock_qserv,
        job=job,
        status="status/data-completed",
        execution_id="2",
    )

    # All queries should have completed.
    assert await state_store.get_active_queries() == set()

    # If Qserv was configured to intermittently fail, check that we logged
    # metrics events recording the failures.
    if mock_qserv.flaky:
        assert isinstance(factory.events.query_api_failure, MockEventPublisher)
        factory.events.query_api_failure.published.assert_published(
            [{"protocol": "HTTP"}, {"protocol": "SQL"}]
        )


@pytest.mark.asyncio
async def test_immediate_dispatch(
    data: QservKafkaData, factory: Factory, mock_qserv: MockQserv
) -> None:
    """Test correct dispatch of a query that finishes immediately.

    Test that a job that completed immediately is dispatched to a worker,
    returns an executing status, and properly marked as queued.
    """
    query_service = factory.create_query_service()
    state_store = factory.create_query_state_store()
    job = data.read_pydantic(JobRun, "jobs/data")
    mock_qserv.set_immediate_success(job)

    with patch.object(RedisArqQueue, "enqueue") as mock:
        await query_service.handle_query(job)
        assert mock.call_args_list == [call("finish_query", "1")]

    status = read_status_message(factory)
    data.assert_job_status_matches(status, "status/simple-immediate")
    query = await state_store.get_query("1")
    assert query
    assert query.result_queued


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mock_qserv", [False, True], ids=["good", "flaky"], indirect=True
)
async def test_no_delete(
    *,
    data: QservKafkaData,
    factory: Factory,
    mock_qserv: MockQserv,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test that deleting results from Qserv can be configured."""
    monkeypatch.setattr(config, "qserv_delete_queries", False)
    query_service = factory.create_query_service()
    job = data.read_pydantic(JobRun, "jobs/data")

    await assert_query_successful(
        data=data,
        factory=factory,
        query_service=query_service,
        mock_qserv=mock_qserv,
        job=job,
        status="status/data-completed",
    )

    # Check that the query was not deleted.
    assert mock_qserv.results_stored

    # Check that the correct metrics events were sent.
    assert isinstance(factory.events.query_success, MockEventPublisher)
    events = factory.events.query_success.published
    assert len(events) == 1
    data.assert_pydantic_matches(events[0], "events/success-no-delete")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mock_qserv", [False, True], ids=["good", "flaky"], indirect=True
)
async def test_cancel_completed(
    *,
    data: QservKafkaData,
    factory: Factory,
    mock_qserv: MockQserv,
    mock_slack: MockSlackWebhook,
) -> None:
    query_service = factory.create_query_service()
    job = data.read_pydantic(JobRun, "jobs/simple")
    cancel = data.read_pydantic(JobCancel, "cancel/simple")

    # Start the query.
    start_time = datetime.now(tz=UTC).replace(microsecond=0)
    await query_service.handle_query(job)
    start_status = read_status_message(factory)
    data.assert_job_status_matches(start_status, "status/simple-started")

    # Mark the query complete in the mock behind the back of the bridge.
    qserv_status = data.read_qserv_status(
        "qserv/simple-completed",
        query_begin=start_time,
        last_update=datetime.now(tz=UTC).replace(microsecond=0),
    )
    await mock_qserv.update_status(1, qserv_status)

    # Canceling a completed query should quietly do nothing. The mock Qserv
    # returns an error if we try to cancel a completed job, so the lack of a
    # reported error indicates correct behavior.
    await query_service.cancel_query(cancel)
    assert isinstance(factory.events.query_abort, MockEventPublisher)
    events = factory.events.query_abort.published
    assert len(events) == 0
    assert mock_slack.messages == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mock_qserv", [False, True], ids=["good", "flaky"], indirect=True
)
async def test_maxrec(
    data: QservKafkaData, factory: Factory, mock_qserv: MockQserv
) -> None:
    query_service = factory.create_query_service()
    job = data.read_pydantic(JobRun, "jobs/data-maxrec")

    await assert_query_successful(
        data=data,
        factory=factory,
        query_service=query_service,
        mock_qserv=mock_qserv,
        job=job,
        status="status/data-maxrec-completed",
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mock_qserv", [False, True], ids=["good", "flaky"], indirect=True
)
async def test_maxrec_zero(
    data: QservKafkaData, factory: Factory, mock_qserv: MockQserv
) -> None:
    """Test a query with MAXREC set to zero."""
    query_service = factory.create_query_service()
    job = data.read_pydantic(JobRun, "jobs/data-zero")

    await assert_query_successful(
        data=data,
        factory=factory,
        query_service=query_service,
        mock_qserv=mock_qserv,
        job=job,
        status="status/data-zero-completed",
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mock_qserv", [False, True], ids=["good", "flaky"], indirect=True
)
async def test_no_api_version(
    *,
    data: QservKafkaData,
    factory: Factory,
    mock_qserv: MockQserv,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test disabling sending the API version in Qserv requests."""
    monkeypatch.setattr(config, "qserv_rest_send_api_version", False)
    query_service = factory.create_query_service()
    job = data.read_pydantic(JobRun, "jobs/data")
    state_store = factory.create_query_state_store()

    await assert_query_successful(
        data=data,
        factory=factory,
        query_service=query_service,
        mock_qserv=mock_qserv,
        job=job,
        status="status/data-completed",
    )

    # Also test starting a job with table upload, since that tests an
    # additional API endpoint.
    job = data.read_pydantic(JobRun, "jobs/upload")
    mock_qserv.set_immediate_success(None)
    await query_service.start_query(Query(job=job))
    status = read_status_message(factory)
    data.assert_job_status_matches(
        status, "status/upload-started", execution_id="2"
    )
    assert_approximately_now(status.timestamp)
    assert status.query_info
    assert_approximately_now(status.query_info.start_time)

    assert await state_store.get_active_queries() == {"2"}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mock_qserv", [False, True], ids=["good", "flaky"], indirect=True
)
async def test_auth(
    *,
    data: QservKafkaData,
    factory: Factory,
    mock_qserv: MockQserv,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test authenticating to the Qserv REST API."""
    monkeypatch.setattr(config, "qserv_rest_username", "someuser")
    monkeypatch.setattr(config, "qserv_rest_password", SecretStr("password"))
    query_service = factory.create_query_service()
    state_store = factory.create_query_state_store()
    job = data.read_pydantic(JobRun, "jobs/data")

    await assert_query_successful(
        data=data,
        factory=factory,
        query_service=query_service,
        mock_qserv=mock_qserv,
        job=job,
        status="status/data-completed",
    )

    # Also test starting a job with table upload, since that tests an
    # additional API endpoint.
    job = data.read_pydantic(JobRun, "jobs/upload")
    mock_qserv.set_immediate_success(None)
    await query_service.start_query(Query(job=job))
    status = read_status_message(factory)
    data.assert_job_status_matches(
        status, "status/upload-started", execution_id="2"
    )
    assert_approximately_now(status.timestamp)
    assert status.query_info
    assert_approximately_now(status.query_info.start_time)

    assert await state_store.get_active_queries() == {"2"}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mock_qserv", [False, True], ids=["good", "flaky"], indirect=True
)
async def test_upload(
    data: QservKafkaData, factory: Factory, mock_qserv: MockQserv
) -> None:
    """Test temporary table upload."""
    query_service = factory.create_query_service()
    state_store = factory.create_query_state_store()
    job = data.read_pydantic(JobRun, "jobs/upload")

    start = datetime.now(tz=UTC)
    await assert_query_successful(
        data=data,
        factory=factory,
        query_service=query_service,
        mock_qserv=mock_qserv,
        job=job,
        status="status/upload-completed",
    )
    finish = datetime.now(tz=UTC)
    assert mock_qserv.get_uploaded_table() is None
    assert mock_qserv.get_uploaded_database() is None

    # Check that the correct metrics events were sent.
    assert isinstance(factory.events.query_success, MockEventPublisher)
    events = factory.events.query_success.published
    assert len(events) == 1
    data.assert_pydantic_matches(events[0], "events/success-upload")
    assert isinstance(factory.events.temporary_table, MockEventPublisher)
    events = factory.events.temporary_table.published
    assert len(events) == 1
    data.assert_pydantic_matches(events[0], "events/upload")
    assert timedelta(seconds=0) < events[0].elapsed <= (finish - start)

    # Start another upload query, but this time don't let it complete
    # immediately. In this case, the uploaded table should still be present
    # (not yet deleted) since the query is still running.
    mock_qserv.set_immediate_success(None)
    await query_service.start_query(Query(job=job))
    status = read_status_message(factory)
    data.assert_job_status_matches(
        status, "status/upload-started", execution_id="2"
    )
    assert mock_qserv.get_uploaded_table() == job.upload_tables[0].table_name
    assert mock_qserv.get_uploaded_database() == job.upload_tables[0].database

    # Only the second query should be active.
    assert await state_store.get_active_queries() == {"2"}


@pytest.mark.asyncio
async def test_upload_director(
    data: QservKafkaData, factory: Factory, mock_qserv: MockQserv
) -> None:
    """Test upload of a spatially-partitioned (director) table."""
    query_service = factory.create_query_service()
    job = data.read_pydantic(JobRun, "jobs/upload-director")

    await assert_query_successful(
        data=data,
        factory=factory,
        query_service=query_service,
        mock_qserv=mock_qserv,
        job=job,
        status="status/upload-completed",
    )
    assert mock_qserv.get_uploaded_table() is None
    assert mock_qserv.get_uploaded_database() is None


@pytest.mark.asyncio
async def test_upload_dependent(
    data: QservKafkaData, factory: Factory, mock_qserv: MockQserv
) -> None:
    """Test upload of a dependent (FK-partitioned) table."""
    query_service = factory.create_query_service()
    job = data.read_pydantic(JobRun, "jobs/upload-dependent")

    await assert_query_successful(
        data=data,
        factory=factory,
        query_service=query_service,
        mock_qserv=mock_qserv,
        job=job,
        status="status/upload-completed",
    )
    assert mock_qserv.get_uploaded_table() is None
    assert mock_qserv.get_uploaded_database() is None
