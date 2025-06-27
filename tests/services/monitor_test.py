"""Tests for the query status monitor."""

from __future__ import annotations

from unittest.mock import call, patch

import pytest
from safir.arq import RedisArqQueue
from safir.datetime import current_datetime

from qservkafka.factory import Factory
from qservkafka.models.qserv import AsyncQueryPhase, AsyncQueryStatus

from ..support.data import read_test_job_run, read_test_job_status
from ..support.qserv import MockQserv


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mock_qserv", [False, True], ids=["good", "flaky"], indirect=True
)
async def test_dispatch(factory: Factory, mock_qserv: MockQserv) -> None:
    query_service = factory.create_query_service()
    state_store = factory.create_query_state_store()
    monitor = await factory.create_query_monitor()
    job = read_test_job_run("simple")
    expected_status = read_test_job_status("simple-started")

    status = await query_service.start_query(job)
    assert status == expected_status

    qserv_status = mock_qserv.get_status(1)
    now = current_datetime()
    await mock_qserv.update_status(
        1,
        AsyncQueryStatus(
            query_id=1,
            status=AsyncQueryPhase.COMPLETED,
            total_chunks=10,
            completed_chunks=10,
            collected_bytes=250,
            query_begin=qserv_status.query_begin,
            last_update=now,
        ),
    )

    query = await state_store.get_query(1)
    assert query
    qserv_status = mock_qserv.get_status(1)
    with patch.object(RedisArqQueue, "enqueue") as mock:
        assert await monitor.check_query(query, qserv_status) is None
        assert mock.call_args_list == [call("handle_finished_query", 1)]
        mock.reset_mock()

        # Running a second check on the query should notice that the query was
        # already dispatched from information stored in the state store and
        # should not dispatch it again.
        query = await state_store.get_query(1)
        assert query
        assert await monitor.check_query(query, qserv_status) is None
        assert mock.call_args_list == []
