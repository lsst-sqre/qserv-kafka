"""Tests for the DatabaseBackend abstract interface."""

from __future__ import annotations

from collections.abc import AsyncGenerator
from datetime import UTC, datetime, timedelta
from typing import Any, override

import pytest
from pydantic import HttpUrl, TypeAdapter

from qservkafka.models.kafka import (
    JobResultConfig,
    JobResultEnvelope,
    JobResultFormat,
    JobResultSerialization,
    JobResultType,
    JobRun,
    JobTableUpload,
)
from qservkafka.models.progress import ByteProgress, ChunkProgress
from qservkafka.models.qserv import TableUploadStats
from qservkafka.models.query import (
    AsyncQueryPhase,
    BigQueryQueryStatus,
    ProcessStatus,
    QservQueryStatus,
    QueryStatus,
)
from qservkafka.storage.backend import DatabaseBackend


@pytest.fixture
def sample_job_run() -> JobRun:
    """Create a sample JobRun for testing."""
    return JobRun(
        job_id="test-job-123",
        owner="testuser",
        query="SELECT * FROM dataset.table LIMIT 10",
        database="test-dataset",
        result_url=HttpUrl("https://example.com/results"),
        result_format=JobResultConfig(
            format=JobResultFormat(
                type=JobResultType.VOTable,
                serialization=JobResultSerialization.BINARY2,
            ),
            envelope=JobResultEnvelope(
                header="<VOTABLE>",
                footer="</VOTABLE>",
                footer_overflow="</VOTABLE>",
            ),
            column_types=[],
        ),
        timeout=timedelta(minutes=5),
    )


class MockDatabaseBackend(DatabaseBackend):
    """Mock implementation of DatabaseBackend for testing."""

    def __init__(self) -> None:
        self.submitted_queries: list[str] = []
        self.cancelled_queries: list[str] = []
        self.deleted_results: list[str] = []
        self.deleted_databases: list[str] = []
        self.uploaded_tables: list[JobTableUpload] = []

    @override
    async def submit_query(self, job: JobRun) -> str:
        """Mock query submission."""
        query_id = f"mock-{len(self.submitted_queries)}"
        self.submitted_queries.append(query_id)
        return query_id

    @override
    async def cancel_query(self, query_id: str) -> None:
        """Mock query cancellation."""
        self.cancelled_queries.append(query_id)

    @override
    async def get_query_status(self, query_id: str) -> QservQueryStatus:
        """Mock status retrieval returning QservQueryStatus."""
        now = datetime.now(tz=UTC)
        return QservQueryStatus(
            backend_type="Qserv",
            query_id=query_id,
            status=AsyncQueryPhase.EXECUTING,
            query_begin=now,
            last_update=now,
            collected_bytes=1000,
            chunk_progress=ChunkProgress(
                total_chunks=100, completed_chunks=50
            ),
        )

    @override
    async def list_running_queries(self) -> dict[str, ProcessStatus]:
        """Mock query listing."""
        now = datetime.now(tz=UTC)
        return {
            "mock-0": ProcessStatus(
                status=AsyncQueryPhase.EXECUTING,
                progress=ChunkProgress(total_chunks=100, completed_chunks=50),
                last_update=now,
            )
        }

    @override
    async def get_query_results_gen(
        self, query_id: str
    ) -> AsyncGenerator[tuple[Any, ...]]:
        """Mock result streaming."""
        for i in range(3):
            yield (i, f"row{i}")

    @override
    async def delete_result(self, query_id: str) -> None:
        """Mock result deletion."""
        self.deleted_results.append(query_id)

    @override
    async def upload_table(self, upload: JobTableUpload) -> TableUploadStats:
        """Mock table upload."""
        self.uploaded_tables.append(upload)
        return TableUploadStats(size=1000, elapsed=timedelta(seconds=1))

    @override
    async def delete_database(self, database: str) -> None:
        """Mock database deletion."""
        self.deleted_databases.append(database)


@pytest.mark.asyncio
async def test_backend_interface_can_be_implemented() -> None:
    backend = MockDatabaseBackend()
    assert isinstance(backend, DatabaseBackend)


@pytest.mark.asyncio
async def test_backend_submit_query(sample_job_run: JobRun) -> None:
    backend = MockDatabaseBackend()
    query_id = await backend.submit_query(sample_job_run)

    assert isinstance(query_id, str)
    assert query_id == "mock-0"
    assert len(backend.submitted_queries) == 1


@pytest.mark.asyncio
async def test_backend_cancel_query() -> None:
    backend = MockDatabaseBackend()
    await backend.cancel_query("test-query-123")

    assert "test-query-123" in backend.cancelled_queries


@pytest.mark.asyncio
async def test_backend_get_query_status() -> None:
    backend = MockDatabaseBackend()
    status = await backend.get_query_status("test-query-123")

    assert isinstance(status, QservQueryStatus)
    assert status.query_id == "test-query-123"
    assert status.status == AsyncQueryPhase.EXECUTING
    assert status.error is None
    assert status.chunk_progress is not None
    assert status.chunk_progress.total_chunks == 100


@pytest.mark.asyncio
async def test_backend_list_running_queries() -> None:
    backend = MockDatabaseBackend()
    queries = await backend.list_running_queries()

    assert isinstance(queries, dict)
    assert "mock-0" in queries
    assert isinstance(queries["mock-0"], ProcessStatus)


@pytest.mark.asyncio
async def test_backend_get_query_results_gen() -> None:
    backend = MockDatabaseBackend()

    rows = [
        row async for row in backend.get_query_results_gen("test-query-123")
    ]

    assert len(rows) == 3
    assert rows[0] == (0, "row0")
    assert rows[2] == (2, "row2")


@pytest.mark.asyncio
async def test_backend_delete_result() -> None:
    backend = MockDatabaseBackend()
    await backend.delete_result("test-query-123")

    assert "test-query-123" in backend.deleted_results


@pytest.mark.asyncio
async def test_backend_upload_table() -> None:
    backend = MockDatabaseBackend()
    upload = JobTableUpload(
        table_name="user_test.temp_table",
        source_url="https://example.com/data.csv",
        schema_url="https://example.com/schema.json",
    )

    stats = await backend.upload_table(upload)

    assert isinstance(stats, TableUploadStats)
    assert stats.size == 1000
    assert len(backend.uploaded_tables) == 1


@pytest.mark.asyncio
async def test_backend_delete_database() -> None:
    backend = MockDatabaseBackend()
    await backend.delete_database("test_database")

    assert "test_database" in backend.deleted_databases


@pytest.mark.asyncio
async def test_backend_interface_requires_all_methods() -> None:
    class IncompleteBackend(DatabaseBackend):
        pass

    with pytest.raises(TypeError) as exc_info:
        IncompleteBackend()  # type: ignore[abstract]

    error_msg = str(exc_info.value)
    assert "abstract" in error_msg.lower()


def test_qserv_query_status_serialization() -> None:
    now = datetime.now(tz=UTC)
    original = QservQueryStatus(
        backend_type="Qserv",
        query_id="123",
        status=AsyncQueryPhase.EXECUTING,
        query_begin=now,
        last_update=now,
        collected_bytes=5000000,
        chunk_progress=ChunkProgress(total_chunks=100, completed_chunks=75),
        czar_id=42,
        czar_type="shared",
    )

    serialized = original.model_dump()
    deserialized = QservQueryStatus.model_validate(serialized)

    assert deserialized.query_id == "123"
    assert deserialized.backend_type == "Qserv"
    assert deserialized.chunk_progress is not None
    assert deserialized.chunk_progress.total_chunks == 100
    assert deserialized.chunk_progress.completed_chunks == 75
    assert deserialized.czar_id == 42


def test_bigquery_query_status_serialization() -> None:
    now = datetime.now(tz=UTC)
    original = BigQueryQueryStatus(
        backend_type="BigQuery",
        query_id="bq-job-uuid-123",
        status=AsyncQueryPhase.COMPLETED,
        query_begin=now,
        last_update=now,
        collected_bytes=2000000,
        final_rows=42,
        byte_progress=ByteProgress(
            bytes_processed=2000000, bytes_billed=1500000, cached=True
        ),
    )

    serialized = original.model_dump()
    deserialized = BigQueryQueryStatus.model_validate(serialized)

    assert deserialized.query_id == "bq-job-uuid-123"
    assert deserialized.backend_type == "BigQuery"
    assert deserialized.byte_progress is not None
    assert deserialized.byte_progress.bytes_processed == 2000000
    assert deserialized.byte_progress.bytes_billed == 1500000
    assert deserialized.byte_progress.cached is True


def test_query_status_to_qserv() -> None:
    now = datetime.now(tz=UTC)
    adapter: TypeAdapter = TypeAdapter(QueryStatus)

    data = {
        "backend_type": "Qserv",
        "query_id": "123",
        "status": "EXECUTING",
        "query_begin": now.isoformat(),
        "chunk_progress": {"total_chunks": 100, "completed_chunks": 50},
    }

    status = adapter.validate_python(data)
    assert isinstance(status, QservQueryStatus)
    assert status.query_id == "123"


def test_query_status_to_bigquery() -> None:
    now = datetime.now(tz=UTC)
    adapter: TypeAdapter = TypeAdapter(QueryStatus)

    data = {
        "backend_type": "BigQuery",
        "query_id": "bq-uuid-123",
        "status": "COMPLETED",
        "query_begin": now.isoformat(),
        "byte_progress": {"bytes_processed": 2000000},
    }

    status = adapter.validate_python(data)
    assert isinstance(status, BigQueryQueryStatus)
    assert status.query_id == "bq-uuid-123"


def test_query_status_is_different_with_process_status() -> None:
    now = datetime.now(tz=UTC)
    old_status = QservQueryStatus(
        backend_type="Qserv",
        query_id="123",
        status=AsyncQueryPhase.EXECUTING,
        query_begin=now,
        last_update=now,
        chunk_progress=ChunkProgress(total_chunks=100, completed_chunks=50),
    )

    same_process = ProcessStatus(
        status=AsyncQueryPhase.EXECUTING,
        last_update=now,
        progress=ChunkProgress(total_chunks=100, completed_chunks=50),
    )
    assert not old_status.is_different_than(same_process)

    updated_process = ProcessStatus(
        status=AsyncQueryPhase.EXECUTING,
        last_update=now,
        progress=ChunkProgress(total_chunks=100, completed_chunks=75),
    )
    assert old_status.is_different_than(updated_process)

    byte_process = ProcessStatus(
        status=AsyncQueryPhase.EXECUTING,
        last_update=now,
        progress=ByteProgress(bytes_processed=1000000),
    )
    assert old_status.is_different_than(byte_process)


def test_query_status_update_from_process_status() -> None:
    now = datetime.now(tz=UTC)
    later = datetime.now(tz=UTC)

    status = QservQueryStatus(
        backend_type="Qserv",
        query_id="123",
        status=AsyncQueryPhase.EXECUTING,
        query_begin=now,
        last_update=now,
        chunk_progress=ChunkProgress(total_chunks=100, completed_chunks=50),
    )

    updated_process = ProcessStatus(
        status=AsyncQueryPhase.EXECUTING,
        last_update=later,
        progress=ChunkProgress(total_chunks=100, completed_chunks=75),
    )
    status.update_from(updated_process)

    assert status.chunk_progress is not None
    assert status.chunk_progress.total_chunks == 100
    assert status.chunk_progress.completed_chunks == 75
    assert status.last_update == later
