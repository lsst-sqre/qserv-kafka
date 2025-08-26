"""Test the Qserv Kafka bridge with a real Kafka server."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from unittest.mock import ANY

import pytest
from aiokafka import AIOKafkaConsumer
from asgi_lifespan import LifespanManager
from fastapi import FastAPI
from faststream.kafka import KafkaBroker
from httpx import Response
from safir.datetime import current_datetime
from safir.metrics import MockEventPublisher
from testcontainers.redis import RedisContainer

from qservkafka.config import config
from qservkafka.dependencies.context import context_dependency
from qservkafka.models.gafaelfawr import GafaelfawrQuota, GafaelfawrTapQuota
from qservkafka.models.state import RunningQuery

from ..support.arq import create_arq_worker
from ..support.data import (
    read_test_job_cancel,
    read_test_job_run,
    read_test_qserv_status,
)
from ..support.gafaelfawr import MockGafaelfawr
from ..support.kafka import start_query, wait_for_dispatch, wait_for_status
from ..support.qserv import MockQserv


@pytest.mark.asyncio
@pytest.mark.timeout(60)
@pytest.mark.parametrize(
    "mock_qserv", [False, True], ids=["good", "flaky"], indirect=True
)
async def test_success(
    *,
    app: FastAPI,
    kafka_broker: KafkaBroker,
    kafka_status_consumer: AIOKafkaConsumer,
    mock_qserv: MockQserv,
    redis: RedisContainer,
) -> None:
    async with LifespanManager(app):
        factory = context_dependency.create_factory()
        arq_worker = create_arq_worker(factory._context)

        job = await start_query(kafka_broker, "data")
        status = await wait_for_status(kafka_status_consumer, "data-started")
        assert status.query_info
        start_time = status.query_info.start_time

        await mock_qserv.store_results(job)
        qserv_status = read_test_qserv_status(
            "data-completed",
            query_begin=start_time,
            last_update=current_datetime(),
        )
        await mock_qserv.update_status(1, qserv_status)

        await wait_for_dispatch(factory, 1)

        # Run the background task queue.
        assert await arq_worker.run_check() == 1
        status = await wait_for_status(kafka_status_consumer, "data-completed")
        assert status.query_info
        assert status.query_info.start_time == start_time
        assert status.query_info.end_time
        assert status.query_info.end_time >= start_time

    # Ensure all query state has been deleted.
    redis_client = redis.get_client()
    assert set(redis_client.scan_iter("query:*")) == set()

    # Check that the correct metrics event was sent.
    assert isinstance(factory.events.query_success, MockEventPublisher)
    events = factory.events.query_success.published
    assert len(events) == 1
    assert events[0].model_dump(mode="json") == {
        "job_id": job.job_id,
        "username": job.owner,
        "elapsed": ANY,
        "qserv_elapsed": ANY,
        "result_elapsed": ANY,
        "submit_elapsed": ANY,
        "rows": 2,
        "qserv_size": qserv_status.collected_bytes,
        "encoded_size": ANY,
        "result_size": ANY,
        "rate": ANY,
        "qserv_rate": ANY,
        "result_rate": ANY,
        "upload_tables": 0,
        "immediate": False,
    }


@pytest.mark.asyncio
@pytest.mark.timeout(60)
async def test_failure(
    *,
    app: FastAPI,
    kafka_broker: KafkaBroker,
    kafka_status_consumer: AIOKafkaConsumer,
    mock_qserv: MockQserv,
    redis: RedisContainer,
) -> None:
    async with LifespanManager(app):
        factory = context_dependency.create_factory()
        arq_worker = create_arq_worker(factory._context)

        await start_query(kafka_broker, "simple")
        status = await wait_for_status(kafka_status_consumer, "simple-started")
        assert status.query_info
        start_time = status.query_info.start_time

        now = current_datetime()
        qserv_status = read_test_qserv_status(
            "simple-partial", query_begin=start_time, last_update=now
        )
        await mock_qserv.update_status(1, qserv_status)
        status = await wait_for_status(kafka_status_consumer, "simple-partial")
        assert status.timestamp == now
        assert status.query_info
        assert status.query_info.start_time == start_time

        now = current_datetime()
        qserv_status = read_test_qserv_status(
            "simple-failed", query_begin=start_time, last_update=now
        )
        await mock_qserv.update_status(1, qserv_status)
        await wait_for_dispatch(factory, 1)

        # Run the background tsk queue.
        assert await arq_worker.run_check() == 1
        status = await wait_for_status(kafka_status_consumer, "simple-failed")
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
    app: FastAPI,
    kafka_broker: KafkaBroker,
    kafka_status_consumer: AIOKafkaConsumer,
    mock_qserv: MockQserv,
    redis: RedisContainer,
) -> None:
    async with LifespanManager(app):
        factory = context_dependency.create_factory()
        arq_worker = create_arq_worker(factory._context)

        await start_query(kafka_broker, "simple")
        status = await wait_for_status(kafka_status_consumer, "simple-started")
        assert status.query_info
        start_time = status.query_info.start_time

        now = current_datetime()
        qserv_status = read_test_qserv_status(
            "simple-large", query_begin=start_time, last_update=now
        )
        await mock_qserv.update_status(1, qserv_status)
        await wait_for_dispatch(factory, 1)

        # Run the background tsk queue.
        assert await arq_worker.run_check() == 1
        status = await wait_for_status(kafka_status_consumer, "simple-large")
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
    app: FastAPI,
    kafka_broker: KafkaBroker,
    kafka_status_consumer: AIOKafkaConsumer,
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

        await start_query(kafka_broker, "simple")
        status = await wait_for_status(kafka_status_consumer, "simple-started")
        assert status.query_info
        start_time = status.query_info.start_time

        error_json = {"success": 0, "error": "Some error"}
        mock_qserv.set_status_response(Response(200, json=error_json))
        qserv_status = read_test_qserv_status(
            "simple-completed",
            query_begin=start_time,
            last_update=current_datetime(),
        )
        await mock_qserv.update_status(1, qserv_status)
        await wait_for_dispatch(factory, 1)

        # Run the background tsk queue.
        assert await arq_worker.run_check() == 1
        status = await wait_for_status(kafka_status_consumer, "simple-error")

    # Ensure all query state has been deleted.
    redis_client = redis.get_client()
    assert set(redis_client.scan_iter("query:*")) == set()


@pytest.mark.asyncio
@pytest.mark.timeout(60)
async def test_missing_executing(
    *,
    app: FastAPI,
    kafka_broker: KafkaBroker,
    kafka_status_consumer: AIOKafkaConsumer,
    mock_qserv: MockQserv,
    redis: RedisContainer,
) -> None:
    """Test queries that are not in the process list but still executing."""
    async with LifespanManager(app):
        factory = context_dependency.create_factory()
        await start_query(kafka_broker, "data")
        await wait_for_status(kafka_status_consumer, "data-started")

        # Remove the query from the running query list. It should be
        # dispatched to the result worker.
        await mock_qserv.remove_running_query(1)
        await wait_for_dispatch(factory, 1)

    # Run the backend worker. It should process the job and send the same
    # status update we already sent (since nothing has changed).
    arq_worker = create_arq_worker()
    assert await arq_worker.run_check() == 1
    await wait_for_status(kafka_status_consumer, "data-started")

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
    app: FastAPI,
    kafka_broker: KafkaBroker,
    kafka_status_consumer: AIOKafkaConsumer,
    redis: RedisContainer,
) -> None:
    async with LifespanManager(app):
        await start_query(kafka_broker, "simple")
        status = await wait_for_status(kafka_status_consumer, "simple-started")
        assert status.query_info
        start_time = status.query_info.start_time

        sent_time = datetime.now(tz=UTC)
        cancel_json = read_test_job_cancel("simple").model_dump(mode="json")
        await kafka_broker.publish(cancel_json, config.job_cancel_topic)

        status = await wait_for_status(kafka_status_consumer, "simple-aborted")
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
    app: FastAPI,
    kafka_broker: KafkaBroker,
    kafka_status_consumer: AIOKafkaConsumer,
    mock_qserv: MockQserv,
    redis: RedisContainer,
) -> None:
    async with LifespanManager(app):
        factory = context_dependency.create_factory()

        job = await start_query(kafka_broker, "upload")
        table_name = job.upload_tables[0].table_name
        database_name = job.upload_tables[0].database
        status = await wait_for_status(kafka_status_consumer, "upload-started")
        assert status.query_info
        start_time = status.query_info.start_time
        assert mock_qserv.get_uploaded_table() == table_name
        assert mock_qserv.get_uploaded_database() == database_name

        qserv_status = read_test_qserv_status(
            "upload-failed",
            query_begin=start_time,
            last_update=current_datetime(),
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
    await wait_for_status(kafka_status_consumer, "upload-failed")

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
    app: FastAPI,
    kafka_broker: KafkaBroker,
    kafka_status_consumer: AIOKafkaConsumer,
    mock_gafaelfawr: MockGafaelfawr,
    mock_qserv: MockQserv,
    redis: RedisContainer,
) -> None:
    redis_client = redis.get_client()
    job = read_test_job_run("data")
    quota = GafaelfawrQuota(
        tap={config.tap_service: GafaelfawrTapQuota(concurrent=2)}
    )
    mock_gafaelfawr.set_quota(job.owner, quota)

    async with LifespanManager(app):
        factory = context_dependency.create_factory()
        arq_worker = create_arq_worker(factory._context)

        # Start a couple of queries.
        await start_query(kafka_broker, "data")
        status = await wait_for_status(kafka_status_consumer, "data-started")
        assert status.query_info
        start_time = status.query_info.start_time
        job = await start_query(kafka_broker, "data")
        await wait_for_status(
            kafka_status_consumer, "data-started", execution_id="2"
        )

        # This should have exhausted the user's quota, and starting a third
        # job should be rejected with an error.
        await start_query(kafka_broker, "data")
        await wait_for_status(kafka_status_consumer, "data-overquota")

        # Make sure that we decremented the counter of running queries again
        # when rejecting that job.
        assert redis_client.get(f"rate:{job.owner}") == b"2"

        # Let the first query finish.
        await mock_qserv.store_results(job)
        qserv_status = read_test_qserv_status(
            "data-completed",
            query_begin=start_time,
            last_update=current_datetime(),
        )
        await mock_qserv.update_status(1, qserv_status)
        await wait_for_dispatch(factory, 1)

        # Run the background task queue.
        assert await arq_worker.run_check() == 1
        await wait_for_status(kafka_status_consumer, "data-completed")

        # Now, it should be possible to start a new query.
        await start_query(kafka_broker, "data")
        await wait_for_status(
            kafka_status_consumer, "data-started", execution_id="3"
        )
        assert redis_client.get(f"rate:{job.owner}") == b"2"
