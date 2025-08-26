"""Test for memory leaks in query processing.

Each test repeats the leak tracing setup, rather than using a fixture, to
ensure that the setup code is not included in the traced memory.
"""

from __future__ import annotations

import gc
import sys
import tracemalloc
from datetime import UTC, datetime

import pytest
import respx
from aiokafka import AIOKafkaConsumer
from arq import Worker
from asgi_lifespan import LifespanManager
from fastapi import FastAPI
from faststream.kafka import KafkaBroker
from safir.logging import LogLevel
from safir.metrics import metrics_configuration_factory

from qservkafka.config import config
from qservkafka.dependencies.context import context_dependency
from qservkafka.factory import Factory

from ..support.arq import create_arq_worker
from ..support.data import read_test_qserv_status
from ..support.kafka import start_query, wait_for_dispatch, wait_for_status
from ..support.qserv import MockQserv


@pytest.fixture(autouse=True)
def disable_metrics(monkeypatch: pytest.MonkeyPatch) -> None:
    """Disable the metrics events mock.

    Disable metrics events logging entirely, rather than using the mock.
    Otherwise, we accomulate metrics events in memory, which causes false
    positives for memory leaks.
    """
    monkeypatch.delenv("METRICS_MOCK")
    monkeypatch.setattr(config, "metrics", metrics_configuration_factory())


@pytest.fixture(autouse=True)
def set_log_level(monkeypatch: pytest.MonkeyPatch) -> None:
    """Reduce log noise.

    The leak test runs a bunch of queries, so cut down on logging. Debug-level
    logging also appears to allocate a lot of memory. Hopefully this is not a
    real memory leak.
    """
    monkeypatch.setattr(config, "log_level", LogLevel.WARNING)


async def run_job(
    *,
    factory: Factory,
    kafka_broker: KafkaBroker,
    kafka_status_consumer: AIOKafkaConsumer,
    mock_qserv: MockQserv,
    arq_worker: Worker,
    execution_id: int,
) -> None:
    """Run a single job end-to-end."""
    job = await start_query(kafka_broker, "data")
    status = await wait_for_status(
        kafka_status_consumer, "data-started", execution_id=str(execution_id)
    )
    assert status.query_info
    start_time = status.query_info.start_time
    await mock_qserv.store_results(job)
    qserv_status = read_test_qserv_status(
        "data-completed",
        query_id=execution_id,
        query_begin=start_time,
        last_update=datetime.now(tz=UTC),
    )
    await mock_qserv.update_status(execution_id, qserv_status)
    await wait_for_dispatch(factory, execution_id)
    assert await arq_worker.run_check() == execution_id
    await wait_for_status(
        kafka_status_consumer, "data-completed", execution_id=str(execution_id)
    )


@pytest.mark.asyncio
async def test_leak(
    *,
    app: FastAPI,
    kafka_broker: KafkaBroker,
    kafka_status_consumer: AIOKafkaConsumer,
    mock_qserv: MockQserv,
    respx_mock: respx.Router,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test for memory leaks in a full end-to-end Kafka flow."""
    async with LifespanManager(app):
        factory = context_dependency.create_factory()
        arq_worker = create_arq_worker(factory._context)

        # Run a single job to force any memory allocations that only happen
        # once, during the first job.
        await run_job(
            factory=factory,
            kafka_broker=kafka_broker,
            kafka_status_consumer=kafka_status_consumer,
            mock_qserv=mock_qserv,
            arq_worker=arq_worker,
            execution_id=1,
        )

        # Start tracing memory.
        gc.collect()
        tracemalloc.start()
        start_usage = tracemalloc.get_traced_memory()[0]

        # Run 100 jobs through the system end to end.
        for i in range(2, 102):
            await run_job(
                factory=factory,
                kafka_broker=kafka_broker,
                kafka_status_consumer=kafka_status_consumer,
                mock_qserv=mock_qserv,
                arq_worker=arq_worker,
                execution_id=i,
            )

        # Delete as much known stored data as possible, force garbage
        # collection, and then stop tracing memory and gather usage.
        del arq_worker
        mock_qserv.reset()
        respx_mock.reset()
        gc.collect()
        end_usage = tracemalloc.get_traced_memory()[0]

        # In practice memory usage change is never zero because Python and its
        # libraries aggressively cache a lot of objects. Fail only if more
        # than 300KiB was leaked.
        limit = 300_000
        if end_usage - start_usage >= limit:
            snapshot = tracemalloc.take_snapshot()
            top_stats = snapshot.statistics("lineno")
            for stat in top_stats[:10]:
                sys.stdout.write(str(stat) + "\n")
            assert end_usage - start_usage < limit
        tracemalloc.stop()
