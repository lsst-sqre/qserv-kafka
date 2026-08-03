"""Test the Qserv Kafka bridge with a real Kafka server."""

import json
from datetime import UTC, datetime

import pytest
from asgi_lifespan import LifespanManager
from fastapi import FastAPI
from faststream.kafka import KafkaBroker
from httpx import Response
from rubin.gafaelfawr import (
    GafaelfawrQuota,
    GafaelfawrTapQuota,
    GafaelfawrUserInfo,
    MockGafaelfawr,
)
from safir.metrics import MockEventPublisher
from safir.testing.slack import MockSlackWebhook
from testcontainers.redis import RedisContainer

from qservkafka.config import config
from qservkafka.dependencies.context import context_dependency
from qservkafka.models.kafka import JobRun
from qservkafka.models.state import RunningQuery
from qservkafka.workers.main import UploadWorkerSettings

from ..support.arq import (
    create_arq_worker,
    run_worker_until_processed,
    wait_for_dispatch,
)
from ..support.data import QservKafkaData
from ..support.kafka import KafkaTestManager
from ..support.qserv import MockQserv


@pytest.mark.asyncio
@pytest.mark.timeout(40)
@pytest.mark.parametrize(
    "mock_qserv", [False, True], ids=["good", "flaky"], indirect=True
)
async def test_success(
    *,
    data: QservKafkaData,
    app: FastAPI,
    kafka_manager: KafkaTestManager,
    mock_qserv: MockQserv,
    redis: RedisContainer,
) -> None:
    async with LifespanManager(app):
        factory = context_dependency.create_factory()
        arq_worker = create_arq_worker(factory._context)

        start = datetime.now(tz=UTC)
        job = await kafka_manager.start_query("jobs/data")
        status = await kafka_manager.wait_for_status("status/data-started")
        assert status.query_info
        start_time = status.query_info.start_time

        await mock_qserv.store_results(job)
        qserv_status = data.read_qserv_status(
            "qserv/data-completed",
            query_begin=start_time,
            last_update=datetime.now(tz=UTC).replace(microsecond=0),
        )
        await mock_qserv.update_status(1, qserv_status)
        await wait_for_dispatch(factory, 1)

        # Run the background task queue.
        assert await arq_worker.run_check() == 1
        status = await kafka_manager.wait_for_status("status/data-completed")
        assert status.query_info
        assert status.query_info.start_time == start_time
        assert status.query_info.end_time
        assert status.query_info.end_time >= start_time

    # Ensure all query state has been deleted.
    redis_client = redis.get_client()
    assert set(redis_client.scan_iter("query:*")) == set()

    # Check that the correct metrics event was sent.
    assert isinstance(factory.events.qserv_success, MockEventPublisher)
    events = factory.events.qserv_success.published
    assert len(events) == 1
    data.assert_pydantic_matches(events[0], "events/success-kafka")
    assert events[0].qserv_size == qserv_status.collected_bytes
    assert events[0].kafka_elapsed <= start_time - start


@pytest.mark.asyncio
@pytest.mark.timeout(40)
async def test_immediate(
    *,
    data: QservKafkaData,
    app: FastAPI,
    kafka_manager: KafkaTestManager,
    mock_qserv: MockQserv,
    redis: RedisContainer,
) -> None:
    """Test a query that completes before its first status check."""
    job = data.read_pydantic(JobRun, "jobs/data")
    mock_qserv.set_immediate_success(job)

    async with LifespanManager(app):
        factory = context_dependency.create_factory()
        arq_worker = create_arq_worker(factory._context)

        await kafka_manager.start_query("jobs/data")
        status = await kafka_manager.wait_for_status(
            "status/data-immediate-started"
        )
        assert status.query_info
        start_time = status.query_info.start_time

        await wait_for_dispatch(factory, 1)

        assert await arq_worker.run_check() == 1
        status = await kafka_manager.wait_for_status("status/data-completed")
        assert status.query_info
        assert status.query_info.start_time == start_time
        assert status.query_info.end_time
        assert status.query_info.end_time >= start_time

    redis_client = redis.get_client()
    assert set(redis_client.scan_iter("query:*")) == set()
    assert isinstance(factory.events.qserv_success, MockEventPublisher)
    events = factory.events.qserv_success.published
    assert len(events) == 1
    assert events[0].immediate is True


@pytest.mark.asyncio
@pytest.mark.timeout(60)
async def test_failure(
    *,
    data: QservKafkaData,
    app: FastAPI,
    kafka_manager: KafkaTestManager,
    mock_qserv: MockQserv,
    redis: RedisContainer,
) -> None:
    async with LifespanManager(app):
        factory = context_dependency.create_factory()
        arq_worker = create_arq_worker(factory._context)

        await kafka_manager.start_query("jobs/simple")
        status = await kafka_manager.wait_for_status("status/simple-started")
        assert status.query_info
        start_time = status.query_info.start_time

        now = datetime.now(tz=UTC).replace(microsecond=0)
        qserv_status = data.read_qserv_status(
            "qserv/simple-partial", query_begin=start_time, last_update=now
        )
        await mock_qserv.update_status(1, qserv_status)
        status = await kafka_manager.wait_for_status("status/simple-partial")
        assert status.timestamp == now
        assert status.query_info
        assert status.query_info.start_time == start_time

        now = datetime.now(tz=UTC).replace(microsecond=0)
        qserv_status = data.read_qserv_status(
            "qserv/simple-failed", query_begin=start_time, last_update=now
        )
        await mock_qserv.update_status(1, qserv_status)
        await wait_for_dispatch(factory, 1)

        # Run the background tsk queue.
        assert await arq_worker.run_check() == 1
        status = await kafka_manager.wait_for_status("status/simple-failed")
        assert status.timestamp == now
        assert status.query_info
        assert status.query_info.start_time == start_time
        assert status.query_info.end_time
        assert status.query_info.end_time >= now

    # Ensure all query state has been deleted.
    redis_client = redis.get_client()
    assert set(redis_client.scan_iter("query:*")) == set()


@pytest.mark.asyncio
@pytest.mark.timeout(60)
async def test_too_large(
    *,
    data: QservKafkaData,
    app: FastAPI,
    kafka_manager: KafkaTestManager,
    mock_qserv: MockQserv,
    redis: RedisContainer,
) -> None:
    async with LifespanManager(app):
        factory = context_dependency.create_factory()
        arq_worker = create_arq_worker(factory._context)

        await kafka_manager.start_query("jobs/simple")
        status = await kafka_manager.wait_for_status("status/simple-started")
        assert status.query_info
        start_time = status.query_info.start_time

        now = datetime.now(tz=UTC).replace(microsecond=0)
        qserv_status = data.read_qserv_status(
            "qserv/simple-large", query_begin=start_time, last_update=now
        )
        await mock_qserv.update_status(1, qserv_status)
        await wait_for_dispatch(factory, 1)

        # Run the background tsk queue.
        assert await arq_worker.run_check() == 1
        status = await kafka_manager.wait_for_status("status/simple-large")
        assert status.timestamp == now
        assert status.query_info
        assert status.query_info.start_time == start_time
        assert status.query_info.end_time
        assert status.query_info.end_time >= now

    # Ensure all query state has been deleted.
    redis_client = redis.get_client()
    assert set(redis_client.scan_iter("query:*")) == set()


@pytest.mark.asyncio
@pytest.mark.timeout(60)
async def test_qserv_error(
    *,
    data: QservKafkaData,
    app: FastAPI,
    kafka_manager: KafkaTestManager,
    mock_qserv: MockQserv,
    redis: RedisContainer,
) -> None:
    """Test proper handling of an API error getting completed job status.

    An earlier version of the Qserv Kafka bridge erroneously didn't stop
    processing when the API request failed.
    """
    async with LifespanManager(app):
        factory = context_dependency.create_factory()
        arq_worker = create_arq_worker(factory._context)

        await kafka_manager.start_query("jobs/simple")
        status = await kafka_manager.wait_for_status("status/simple-started")
        assert status.query_info
        start_time = status.query_info.start_time

        error_json = data.read_json("qserv/error")
        mock_qserv.set_status_response(Response(200, json=error_json))
        qserv_status = data.read_qserv_status(
            "qserv/simple-completed",
            query_begin=start_time,
            last_update=datetime.now(tz=UTC).replace(microsecond=0),
        )
        await mock_qserv.update_status(1, qserv_status)
        await wait_for_dispatch(factory, 1)

        # Run the background tsk queue.
        assert await arq_worker.run_check() == 1
        status = await kafka_manager.wait_for_status("status/simple-error")

    # Ensure all query state has been deleted.
    redis_client = redis.get_client()
    assert set(redis_client.scan_iter("query:*")) == set()


@pytest.mark.asyncio
@pytest.mark.timeout(60)
async def test_missing_executing(
    *,
    data: QservKafkaData,
    app: FastAPI,
    kafka_manager: KafkaTestManager,
    mock_qserv: MockQserv,
    redis: RedisContainer,
) -> None:
    """Test queries that are not in the process list but still executing."""
    async with LifespanManager(app):
        factory = context_dependency.create_factory()
        await kafka_manager.start_query("jobs/data")
        await kafka_manager.wait_for_status("status/data-started")

        # Remove the query from the running query list. It should be
        # dispatched to the result worker.
        await mock_qserv.remove_running_query(1)
        await wait_for_dispatch(factory, 1)

    # Run the backend worker. It should process the job and send the same
    # status update we already sent (since nothing has changed).
    arq_worker = create_arq_worker()
    assert await arq_worker.run_check() == 1
    await kafka_manager.wait_for_status("status/data-started")

    # The query should still be active and should no longer be marked as
    # dispatched, so it will be checked again the next time through the
    # monitor loop.
    redis_client = redis.get_client()
    raw_query = redis_client.get("query:1")
    assert raw_query
    query = RunningQuery.model_validate(json.loads(raw_query))
    assert not query.result_queued


@pytest.mark.asyncio
@pytest.mark.timeout(60)
async def test_cancel(
    *,
    data: QservKafkaData,
    app: FastAPI,
    kafka_manager: KafkaTestManager,
    kafka_broker: KafkaBroker,
    redis: RedisContainer,
) -> None:
    async with LifespanManager(app):
        await kafka_manager.start_query("jobs/simple")
        status = await kafka_manager.wait_for_status("status/simple-started")
        assert status.query_info
        start_time = status.query_info.start_time

        sent_time = datetime.now(tz=UTC)
        cancel = data.read_json("cancel/simple")
        await kafka_broker.publish(cancel, config.job_cancel_topic)

        status = await kafka_manager.wait_for_status("status/simple-aborted")
        assert status.query_info
        assert status.query_info.start_time == start_time
        assert status.query_info.end_time
        assert sent_time <= status.query_info.end_time <= datetime.now(tz=UTC)

    # Ensure all query state has been deleted.
    redis_client = redis.get_client()
    assert set(redis_client.scan_iter("query:*")) == set()


@pytest.mark.asyncio
@pytest.mark.timeout(60)
async def test_upload(
    *,
    data: QservKafkaData,
    app: FastAPI,
    kafka_manager: KafkaTestManager,
    mock_qserv: MockQserv,
    redis: RedisContainer,
) -> None:
    async with LifespanManager(app):
        factory = context_dependency.create_factory()
        upload_worker = create_arq_worker(
            factory._context, settings=UploadWorkerSettings
        )

        job = await kafka_manager.start_query("jobs/upload")
        table_name = job.upload_tables[0].table_name
        database_name = job.upload_tables[0].database

        # Jobs with table uploads are dispatched to a separate arq worker
        # pool so that worker has to run before the job is actually started.
        assert await run_worker_until_processed(upload_worker) == 1
        status = await kafka_manager.wait_for_status("status/upload-started")
        assert status.query_info
        start_time = status.query_info.start_time
        assert mock_qserv.get_uploaded_table() == table_name
        assert mock_qserv.get_uploaded_database() == database_name

        qserv_status = data.read_qserv_status(
            "qserv/upload-failed",
            query_begin=start_time,
            last_update=datetime.now(tz=UTC).replace(microsecond=0),
        )
        await mock_qserv.update_status(1, qserv_status)
        await wait_for_dispatch(factory, 1)

    # Before the backend worker runs, the database and table should still
    # exist.
    assert mock_qserv.get_uploaded_table() == table_name
    assert mock_qserv.get_uploaded_database() == database_name

    # Run the backend worker.
    arq_worker = create_arq_worker()
    assert await arq_worker.run_check() == 1
    await kafka_manager.wait_for_status("status/upload-failed")

    # Now that results have been processed, the table should be deleted.
    assert mock_qserv.get_uploaded_database() is None
    assert mock_qserv.get_uploaded_table() is None

    # Ensure all query state has been deleted.
    redis_client = redis.get_client()
    assert set(redis_client.scan_iter("query:*")) == set()


@pytest.mark.asyncio
@pytest.mark.timeout(60)
async def test_quota(
    *,
    data: QservKafkaData,
    app: FastAPI,
    kafka_manager: KafkaTestManager,
    mock_gafaelfawr: MockGafaelfawr,
    mock_qserv: MockQserv,
    redis: RedisContainer,
) -> None:
    redis_client = redis.get_client()
    job = data.read_pydantic(JobRun, "jobs/data")
    quota = GafaelfawrQuota(
        tap={config.tap_service: GafaelfawrTapQuota(concurrent=2)}
    )
    user_info = GafaelfawrUserInfo(username=job.owner, quota=quota)
    mock_gafaelfawr.set_user_info(job.owner, user_info)

    async with LifespanManager(app):
        factory = context_dependency.create_factory()
        arq_worker = create_arq_worker(factory._context)

        # Start a couple of queries.
        await kafka_manager.start_query("jobs/data")
        status = await kafka_manager.wait_for_status("status/data-started")
        assert status.query_info
        start_time = status.query_info.start_time
        job = await kafka_manager.start_query("jobs/data")
        await kafka_manager.wait_for_status(
            "status/data-started", execution_id="2"
        )

        # This should have exhausted the user's quota, and starting a third
        # job should be rejected with an error.
        await kafka_manager.start_query("jobs/data")
        await kafka_manager.wait_for_status("status/data-overquota")

        # Make sure that we decremented the counter of running queries again
        # when rejecting that job.
        assert redis_client.get(f"rate:{job.owner}") == b"2"

        # Let the first query finish.
        await mock_qserv.store_results(job)
        qserv_status = data.read_qserv_status(
            "qserv/data-completed",
            query_begin=start_time,
            last_update=datetime.now(tz=UTC).replace(microsecond=0),
        )
        await mock_qserv.update_status(1, qserv_status)
        await wait_for_dispatch(factory, 1)

        # Run the background task queue.
        assert await arq_worker.run_check() == 1
        await kafka_manager.wait_for_status("status/data-completed")

        # Now, it should be possible to start a new query.
        await kafka_manager.start_query("jobs/data")
        await kafka_manager.wait_for_status(
            "status/data-started", execution_id="3"
        )
        assert redis_client.get(f"rate:{job.owner}") == b"2"


@pytest.mark.asyncio
@pytest.mark.timeout(40)
async def test_wrong_schema(
    *,
    data: QservKafkaData,
    app: FastAPI,
    kafka_manager: KafkaTestManager,
    mock_slack: MockSlackWebhook,
    mock_qserv: MockQserv,
    redis: RedisContainer,
) -> None:
    async with LifespanManager(app):
        factory = context_dependency.create_factory()
        arq_worker = create_arq_worker(factory._context)

        job = await kafka_manager.start_query("jobs/data-wrong-schema")
        status = await kafka_manager.wait_for_status("status/data-started")
        assert status.query_info
        start_time = status.query_info.start_time

        await mock_qserv.store_results(job)
        qserv_status = data.read_qserv_status(
            "qserv/data-completed",
            query_begin=start_time,
            last_update=datetime.now(tz=UTC).replace(microsecond=0),
        )
        await mock_qserv.update_status(1, qserv_status)
        await wait_for_dispatch(factory, 1)

        # Run the background task queue.
        assert await arq_worker.run_check() == 1
        await kafka_manager.wait_for_status("status/data-wrong-schema")

    # Ensure all query state has been deleted.
    redis_client = redis.get_client()
    assert set(redis_client.scan_iter("query:*")) == set()

    # Check that a Slack message was posted about the encoding error.
    data.assert_json_matches(mock_slack.messages, "slack/wrong-schema")
