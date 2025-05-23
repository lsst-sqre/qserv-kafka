"""Supplemental fixtures required to use a real Kafka server."""

from __future__ import annotations

from collections.abc import AsyncGenerator, Iterator

import pytest
import pytest_asyncio
from aiokafka import AIOKafkaConsumer
from faststream.kafka import KafkaBroker
from safir.kafka import KafkaConnectionSettings, SecurityProtocol
from safir.testing.containers import FullKafkaContainer
from testcontainers.core.network import Network

from qservkafka.config import config


@pytest.fixture(scope="session")
def kafka_docker_network() -> Iterator[Network]:
    with Network() as network:
        yield network


@pytest.fixture(scope="session")
def global_kafka_container(
    kafka_docker_network: Network,
) -> Iterator[FullKafkaContainer]:
    container = FullKafkaContainer()
    container.with_network(kafka_docker_network)
    container.with_network_aliases("kafka")
    with container as kafka:
        yield kafka


@pytest.fixture
def kafka_container(
    global_kafka_container: FullKafkaContainer,
) -> FullKafkaContainer:
    global_kafka_container.reset()
    return global_kafka_container


@pytest.fixture
def kafka_connection_settings(
    kafka_container: FullKafkaContainer, monkeypatch: pytest.MonkeyPatch
) -> KafkaConnectionSettings:
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
    broker = KafkaBroker(
        **kafka_connection_settings.to_faststream_params(),
        client_id="pytest-broker",
    )
    await broker.start()
    yield broker
    await broker.close()
