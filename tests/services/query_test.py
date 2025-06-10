"""Tests for creating new queries."""

from __future__ import annotations

import pytest

from qservkafka.factory import Factory
from qservkafka.models.kafka import JobStatus

from ..support.data import (
    read_test_job_cancel,
    read_test_job_run,
    read_test_job_status,
)
from ..support.datetime import assert_approximately_now
from ..support.qserv import MockQserv


@pytest.mark.asyncio
async def test_start(factory: Factory) -> None:
    job = read_test_job_run("jobs/simple")
    expected_status = read_test_job_status("status/simple-started")
    query_service = factory.create_query_service()

    status = await query_service.start_query(job)
    assert status == expected_status
    assert_approximately_now(status.timestamp)
    assert status.query_info
    assert_approximately_now(status.query_info.start_time)

    assert await factory.query_state_store.get_active_queries() == {1}


@pytest.mark.asyncio
async def test_immediate(factory: Factory, mock_qserv: MockQserv) -> None:
    """Test a job that completes immediately."""
    query_service = factory.create_query_service()
    job = read_test_job_run("jobs/data")
    expected_status = read_test_job_status("status/data-completed")

    mock_qserv.set_immediate_success(job)
    status = await query_service.start_query(job)
    assert status == expected_status
    assert_approximately_now(status.timestamp)
    assert status.query_info
    assert_approximately_now(status.query_info.start_time)
    assert_approximately_now(status.query_info.end_time)

    # It should be possible to immediately run the same query again. This
    # tests that the results were deleted from the database, and thus can be
    # re-added.
    assert expected_status.execution_id
    expected_status.execution_id = str(int(expected_status.execution_id) + 1)
    mock_qserv.set_immediate_success(job)
    status = await query_service.start_query(job)
    assert status == expected_status

    assert await factory.query_state_store.get_active_queries() == set()


@pytest.mark.asyncio
async def test_start_cancel(factory: Factory) -> None:
    job = read_test_job_run("jobs/simple")
    started_status = read_test_job_status("status/simple-started")
    cancel = read_test_job_cancel("cancel/simple")
    canceled_status = read_test_job_status("status/simple-aborted")
    query_service = factory.create_query_service()

    status: JobStatus | None = await query_service.start_query(job)
    assert status == started_status
    assert_approximately_now(status.timestamp)
    assert status.query_info
    assert_approximately_now(status.query_info.start_time)
    start_time = status.query_info.start_time

    assert await factory.query_state_store.get_active_queries() == {1}

    status = await query_service.cancel_query(cancel)
    assert status == canceled_status
    assert_approximately_now(status.timestamp)
    assert status.query_info
    assert status.query_info.start_time == start_time
    assert_approximately_now(status.query_info.end_time)
