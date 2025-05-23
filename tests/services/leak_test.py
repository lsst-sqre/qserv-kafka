"""Test for memory leaks in query processing.

Each test repeats the leak tracing setup, rather than using a fixture, to
ensure that the setup code is not included in the traced memory.
"""

from __future__ import annotations

import gc
import sys
import tracemalloc

import pytest
import respx
from safir.logging import Profile, configure_logging
from safir.metrics import metrics_configuration_factory

from qservkafka.config import config
from qservkafka.events import Events
from qservkafka.factory import Factory

from ..support.data import read_test_job_run, read_test_job_status
from ..support.qserv import MockQserv


@pytest.mark.asyncio
async def test_success(
    *,
    factory: Factory,
    mock_qserv: MockQserv,
    respx_mock: respx.Router,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test for memory leaks in the immediate job processing flow."""
    configure_logging(
        profile=Profile.production, log_level="WARNING", name="qservkafka"
    )

    # Switch to a no-op metrics implementation.
    monkeypatch.delenv("METRICS_MOCK")
    config.metrics = metrics_configuration_factory()
    event_manager = config.metrics.make_manager()
    await event_manager.initialize()
    events = Events()
    await events.initialize(event_manager)
    await factory._context.event_manager.aclose()
    factory._context.event_manager = event_manager
    factory._context.events = events

    query_service = factory.create_query_service()
    job = read_test_job_run("jobs/data")
    expected_status = read_test_job_status("status/data-completed")
    mock_qserv.set_immediate_success(job)

    tracemalloc.start()
    gc.collect()
    start_usage = tracemalloc.get_traced_memory()[0]

    for i in range(1, 1000):
        status = await query_service.start_query(job)
        expected_status.execution_id = str(i)
        assert status == expected_status

    assert await factory.query_state_store.get_active_queries() == set()

    mock_qserv.reset()
    respx_mock.reset()
    gc.collect()
    end_usage = tracemalloc.get_traced_memory()[0]

    # In practice memory usage change is never zero, so fail only if more than
    # 300KB was leaked.
    if end_usage - start_usage >= 10_000:
        snapshot = tracemalloc.take_snapshot()
        top_stats = snapshot.statistics("lineno")
        for stat in top_stats[:10]:
            sys.stdout.write(str(stat) + "\n")
        assert end_usage - start_usage < 300_000
    tracemalloc.stop()
