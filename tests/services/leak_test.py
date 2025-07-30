"""Test for memory leaks in query processing.

Each test repeats the leak tracing setup, rather than using a fixture, to
ensure that the setup code is not included in the traced memory.
"""

from __future__ import annotations

import gc
import sys
import tracemalloc

import pytest
import pytest_asyncio
import respx
from safir.logging import LogLevel
from safir.metrics import metrics_configuration_factory

from qservkafka.config import config
from qservkafka.factory import Factory

from ..support.data import read_test_job_run, read_test_job_status
from ..support.qserv import MockQserv


@pytest_asyncio.fixture(autouse=True)
async def disable_metrics_mock(monkeypatch: pytest.MonkeyPatch) -> None:
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


@pytest.mark.asyncio
async def test_success(
    *,
    factory: Factory,
    mock_qserv: MockQserv,
    respx_mock: respx.Router,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test for memory leaks in the immediate job processing flow."""
    query_service = factory.create_query_service()
    state_store = factory.create_query_state_store()
    job = read_test_job_run("data")
    expected_status = read_test_job_status("data-completed")
    mock_qserv.set_immediate_success(job)

    # Run one query first to set up the various internal Python caches.
    status = await query_service.start_query(job)
    expected_status.execution_id = "1"
    assert status == expected_status

    # Start tracing memory.
    gc.collect()
    tracemalloc.start()
    start_usage = tracemalloc.get_traced_memory()[0]

    # Run 100 more tasks with memory tracing.
    for i in range(2, 102):
        status = await query_service.start_query(job)
        expected_status.execution_id = str(i)
        assert status == expected_status

    # Ensure all the queries have been processed.
    assert await state_store.get_active_queries() == set()

    # Delete as much known stored data as possible, force garbage collection,
    # and then stop tracing memory and gather usage.
    mock_qserv.reset()
    respx_mock.reset()
    gc.collect()
    end_usage = tracemalloc.get_traced_memory()[0]

    # In practice memory usage change is never zero because Python and its
    # libraries aggressively cache a lot of objects. Fail only if more than
    # 200KB was leaked.
    limit = 200_000
    if end_usage - start_usage >= limit:
        snapshot = tracemalloc.take_snapshot()
        top_stats = snapshot.statistics("lineno")
        for stat in top_stats[:10]:
            sys.stdout.write(str(stat) + "\n")
        assert end_usage - start_usage < limit
    tracemalloc.stop()
