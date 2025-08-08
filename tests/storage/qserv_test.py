"""Tests for the Qserv storage layer."""

from __future__ import annotations

import pytest

from qservkafka.factory import Factory

from ..support.data import read_test_job_run, read_test_job_status
from ..support.qserv import MockQserv


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mock_qserv", [False, True], ids=["good", "flaky"], indirect=True
)
async def test_list_running_queries(
    factory: Factory, mock_qserv: MockQserv
) -> None:
    qserv = factory.create_qserv_client()
    query_service = factory.create_query_service()
    job = read_test_job_run("simple")
    expected_status = read_test_job_status("simple-started")

    processes = await qserv.list_running_queries()
    assert processes == {}

    status = await query_service.start_query(job)
    assert status == expected_status
    processes = await qserv.list_running_queries()
    assert processes == {1: mock_qserv.get_status(1).to_process_status()}
