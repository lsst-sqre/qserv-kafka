"""Tests for the Qserv storage layer."""

import pytest

from qservkafka.factory import Factory
from qservkafka.models.progress import ChunkProgress
from qservkafka.models.query import AsyncQueryPhase, ProcessStatus

from ..support.data import read_test_job_run, read_test_job_status
from ..support.qserv import MockQserv


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mock_qserv", [False, True], ids=["good", "flaky"], indirect=True
)
async def test_list_running_queries(
    factory: Factory, mock_qserv: MockQserv
) -> None:
    qserv = factory.create_backend_client()
    query_service = factory.create_query_service()
    job = read_test_job_run("simple")
    expected_status = read_test_job_status("simple-started")

    processes = await qserv.list_running_queries()
    assert processes == {}

    status = await query_service.start_query(job)
    assert status == expected_status
    processes = await qserv.list_running_queries()
    qserv_status = mock_qserv.get_status(1)
    expected_process_status = ProcessStatus(
        status=AsyncQueryPhase.EXECUTING,
        progress=ChunkProgress(
            total_chunks=qserv_status.total_chunks,
            completed_chunks=qserv_status.completed_chunks,
        ),
        last_update=qserv_status.last_update,
    )
    assert len(processes) == 1
    assert "1" in processes
    actual = processes["1"]
    assert actual.status == expected_process_status.status
    assert actual.progress == expected_process_status.progress
