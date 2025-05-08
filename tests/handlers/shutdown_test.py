"""Test result processing during application shutdown."""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncGenerator, Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import ANY

import pytest
import pytest_asyncio
from aiokafka import AIOKafkaConsumer
from asgi_lifespan import LifespanManager
from faststream.kafka import KafkaBroker
from pydantic import RedisDsn
from safir.arq import ArqMode
from safir.kafka import KafkaConnectionSettings, SecurityProtocol
from testcontainers.core.container import Network
from testcontainers.redis import RedisContainer

from qservkafka.config import config
from qservkafka.main import create_app
from qservkafka.models.qserv import AsyncQueryPhase, AsyncQueryStatus

from ..support.arq import run_arq_jobs
from ..support.data import (
    read_test_job_run,
    read_test_job_status,
    read_test_json,
)
from ..support.kafka.container import FullKafkaContainer
from ..support.qserv import MockQserv


@pytest.fixture(scope="session")
def kafka_docker_network() -> Iterator[Network]:
    """Provide a network object to link session-scoped testcontainers."""
    with Network() as network:
        yield network


@pytest.fixture(scope="session")
def kafka_cert_path(tmp_path_factory: pytest.TempPathFactory) -> Path:
    return tmp_path_factory.mktemp("kafka-certs")


@pytest.fixture(scope="session")
def global_kafka_container(
    kafka_docker_network: Network, kafka_cert_path: Path
) -> Iterator[FullKafkaContainer]:
    """Provide a session-scoped kafka container.

    You proably want one of the dependent test-scoped fixtures that clears
    kafka data, like:
    * ``kafka_container``
    * ``kafka_broker``
    * ``kafka_consumer``
    """
    container = FullKafkaContainer(limit_broker_to_first_host=True)
    container.with_network(kafka_docker_network)
    container.with_network_aliases("kafka")
    with container as kafka:
        for filename in ("ca.crt", "client.crt", "client.key"):
            contents = container.get_secret_file_contents("ca.crt")
            (kafka_cert_path / filename).write_text(contents)
        yield kafka


@pytest.fixture
def kafka_container(
    global_kafka_container: FullKafkaContainer,
) -> Iterator[FullKafkaContainer]:
    """Yield the global kafka container, but rid it of data post-test."""
    yield global_kafka_container
    global_kafka_container.reset()


@pytest.fixture
def kafka_connection_settings(
    kafka_container: FullKafkaContainer, monkeypatch: pytest.MonkeyPatch
) -> KafkaConnectionSettings:
    """Provide a url to a session-scoped kafka container."""
    monkeypatch.delenv("KAFKA_BOOTSTRAP_SERVERS")
    monkeypatch.delenv("KAFKA_SECURITY_PROTOCOL")
    return KafkaConnectionSettings(
        bootstrap_servers=kafka_container.get_bootstrap_server(),
        security_protocol=SecurityProtocol.PLAINTEXT,
    )


@pytest_asyncio.fixture
async def kafka_status_consumer(
    kafka_connection_settings: KafkaConnectionSettings,
) -> AsyncGenerator[AIOKafkaConsumer]:
    """Provide an AOIKafkaConsumer pointed at a session-scoped kafka container.

    All data is cleared from the kafka instance at the end of the test.
    """
    consumer = AIOKafkaConsumer(
        config.job_status_topic,
        **kafka_connection_settings.to_aiokafka_params(),
        client_id="pytest-consumer",
    )
    await consumer.start()
    yield consumer
    await consumer.stop()


@pytest_asyncio.fixture
async def kafka_broker(
    kafka_connection_settings: KafkaConnectionSettings,
) -> AsyncGenerator[KafkaBroker]:
    """Provide a fast stream KafkaBroker pointed at a session-scoped kafka
    container.

    All data is cleared from the kafka instance at the end of the test.
    """
    broker = KafkaBroker(
        **kafka_connection_settings.to_faststream_params(),
        client_id="pytest-broker",
    )
    await broker.start()
    yield broker
    await broker.close()


@pytest.mark.asyncio
@pytest.mark.timeout(60)
async def test_shutdown(
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
    status = read_test_job_status(
        "status/data-completed", mock_timestamps=False
    )
    expected = status.model_dump(mode="json")

    mock_qserv.set_upload_delay(timedelta(seconds=1))
    app = create_app()
    async with LifespanManager(app):
        await kafka_broker.publish(job_json, config.job_run_topic)
        await asyncio.sleep(0.1)
        await kafka_status_consumer.getone()

        await mock_qserv.store_results(job)
        async_status = mock_qserv.get_status(1)
        now = datetime.now(tz=UTC)
        await mock_qserv.update_status(
            1,
            AsyncQueryStatus(
                query_id=1,
                status=AsyncQueryPhase.COMPLETED,
                total_chunks=10,
                completed_chunks=10,
                query_begin=async_status.query_begin,
                last_update=now,
            ),
        )
        await asyncio.sleep(1.1)

    expected["queryInfo"]["startTime"] = ANY
    expected["queryInfo"]["endTime"] = ANY
    expected["timestamp"] = ANY
    assert await run_arq_jobs() == 1
    raw_message = await kafka_status_consumer.getone()
    message = json.loads(raw_message.value.decode())
    assert message == expected

    redis_client = redis.get_client()
    assert set(redis_client.scan_iter("query:*")) == set()
