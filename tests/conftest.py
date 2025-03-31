"""Test fixtures for qserv-kafka tests."""

from __future__ import annotations

from collections.abc import AsyncGenerator

import pytest_asyncio
from asgi_lifespan import LifespanManager
from fastapi import FastAPI
from faststream.kafka import KafkaBroker, TestKafkaBroker
from httpx import ASGITransport, AsyncClient

from qservkafka import main
from qservkafka.handlers.kafka import kafka_router


@pytest_asyncio.fixture
async def app(kafka_broker: KafkaBroker) -> AsyncGenerator[FastAPI]:
    """Return a configured test application.

    Wraps the application in a lifespan manager so that startup and shutdown
    events are sent during test execution.
    """
    async with LifespanManager(main.app):
        yield main.app


@pytest_asyncio.fixture
async def client(app: FastAPI) -> AsyncGenerator[AsyncClient]:
    """Return an ``httpx.AsyncClient`` configured to talk to the test app."""
    async with AsyncClient(
        base_url="https://example.com/", transport=ASGITransport(app=app)
    ) as client:
        yield client


@pytest_asyncio.fixture
async def kafka_broker() -> AsyncGenerator[KafkaBroker]:
    """Provide a Kafka producer pointing to the test Kafka."""
    async with TestKafkaBroker(kafka_router.broker) as broker:
        yield broker
