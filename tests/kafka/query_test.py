"""Test the Qserv Kafka bridge with a real Kafka server."""

from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime, timedelta

import pytest
from aiokafka import AIOKafkaConsumer
from asgi_lifespan import LifespanManager
from faststream.kafka import KafkaBroker
from pydantic import RedisDsn
from safir.arq import ArqMode
from safir.kafka import KafkaConnectionSettings
from testcontainers.redis import RedisContainer

from qservkafka.config import config
from qservkafka.main import create_app
from qservkafka.models.qserv import AsyncQueryPhase, AsyncQueryStatus

from ..support.arq import run_arq_jobs
from ..support.data import (
    read_test_job_run,
    read_test_job_status_json,
    read_test_json,
)
from ..support.qserv import MockQserv


@pytest.mark.asyncio
@pytest.mark.timeout(60)
async def test_success(
    *,
    kafka_connection_settings: KafkaConnectionSettings,
    kafka_broker: KafkaBroker,
    kafka_status_consumer: AIOKafkaConsumer,
    mock_qserv: MockQserv,
    redis: RedisContainer,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    redis_host = redis.get_container_host_ip()
    redis_port = redis.get_exposed_port(6379)
    redis_url = RedisDsn(f"redis://{redis_host}:{redis_port}/0")
    monkeypatch.setattr(config, "arq_mode", ArqMode.production)
    monkeypatch.setattr(config, "redis_url", redis_url)
    monkeypatch.setattr(config, "kafka", kafka_connection_settings)

    job = read_test_job_run("jobs/data")
    job_json = read_test_json("jobs/data")
    status_started = read_test_job_status_json("status/data-started")
    status_completed = read_test_job_status_json("status/data-completed")

    mock_qserv.set_upload_delay(timedelta(seconds=1))
    app = create_app()
    async with LifespanManager(app):
        await kafka_broker.publish(job_json, config.job_run_topic)
        await mock_qserv.store_results(job)

        raw_message = await kafka_status_consumer.getone()
        message = json.loads(raw_message.value.decode())
        assert message == status_started

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

    assert await run_arq_jobs() == 1
    raw_message = await kafka_status_consumer.getone()
    message = json.loads(raw_message.value.decode())
    if message["status"] == "EXECUTING":
        raw_message = await kafka_status_consumer.getone()
        message = json.loads(raw_message.value.decode())
    assert message == status_completed

    # Ensure all query state has been deleted.
    redis_client = redis.get_client()
    assert set(redis_client.scan_iter("query:*")) == set()
