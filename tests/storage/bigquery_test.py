"""Tests for the BigQuery storage layer."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any
from unittest.mock import MagicMock, Mock

import google.cloud.bigquery as google_bigquery
import pytest
from google.api_core.exceptions import GoogleAPIError
from google.cloud.bigquery import DatasetReference, QueryJob
from httpx import AsyncClient
from pydantic import HttpUrl
from structlog.stdlib import BoundLogger

from qservkafka import config as config_module
from qservkafka.events import Events
from qservkafka.exceptions import (
    BackendNotImplementedError,
    BigQueryApiError,
    BigQueryApiProtocolError,
)
from qservkafka.models.kafka import (
    JobResultConfig,
    JobResultEnvelope,
    JobResultFormat,
    JobResultSerialization,
    JobResultType,
    JobRun,
    JobTableUpload,
)
from qservkafka.models.progress import ByteProgress
from qservkafka.models.query import AsyncQueryPhase
from qservkafka.storage.bigquery import BigQueryClient


@pytest.fixture
def mock_bq_client() -> MagicMock:
    """Create a mock BigQuery client."""
    return MagicMock()


@pytest.fixture
def bigquery_client(
    mock_bq_client: MagicMock,
    logger: BoundLogger,
    monkeypatch: pytest.MonkeyPatch,
) -> BigQueryClient:
    """Create a BigQueryClient with mocked dependencies."""
    mock_client_class = MagicMock(return_value=mock_bq_client)
    monkeypatch.setattr(google_bigquery, "Client", mock_client_class)

    http_client = Mock(spec=AsyncClient)
    events = Mock(spec=Events)
    slack_client = None

    client = BigQueryClient(
        project="test-project",
        location="US",
        http_client=http_client,
        events=events,
        slack_client=slack_client,
        logger=logger,
    )

    client._client = mock_bq_client

    return client


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


@pytest.mark.asyncio
async def test_submit_query_success(
    bigquery_client: BigQueryClient,
    sample_job_run: JobRun,
    mock_bq_client: MagicMock,
) -> None:
    mock_job = MagicMock(spec=QueryJob)
    mock_job.job_id = "bq-job-uuid-123"
    mock_bq_client.query.return_value = mock_job

    job_id = await bigquery_client.submit_query(sample_job_run)

    assert job_id == "bq-job-uuid-123"

    mock_bq_client.query.assert_called_once()
    call_args = mock_bq_client.query.call_args
    assert call_args[0][0] == sample_job_run.query
    dataset_ref = call_args[1]["job_config"].default_dataset
    assert isinstance(dataset_ref, DatasetReference)
    assert dataset_ref.project == "test-project"
    assert dataset_ref.dataset_id == "test-dataset"


@pytest.mark.asyncio
async def test_submit_query_with_max_bytes_billed(
    bigquery_client: BigQueryClient,
    sample_job_run: JobRun,
    mock_bq_client: MagicMock,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mock_job = MagicMock(spec=QueryJob)
    mock_job.job_id = "bq-job-uuid-456"
    mock_bq_client.query.return_value = mock_job

    monkeypatch.setattr(
        config_module.config, "bigquery_max_bytes_billed", 1000000000
    )

    job_id = await bigquery_client.submit_query(sample_job_run)

    assert job_id == "bq-job-uuid-456"

    mock_bq_client.query.assert_called_once()
    call_args = mock_bq_client.query.call_args
    job_config = call_args[1]["job_config"]
    assert job_config.maximum_bytes_billed == 1000000000


@pytest.mark.asyncio
async def test_submit_query_exceeds_max_bytes(
    bigquery_client: BigQueryClient,
    sample_job_run: JobRun,
    mock_bq_client: MagicMock,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    error_msg = "Query exceeded maximum bytes billed limit"
    mock_bq_client.query.side_effect = GoogleAPIError(error_msg)

    monkeypatch.setattr(config_module.config, "bigquery_max_bytes_billed", 100)

    with pytest.raises(BigQueryApiProtocolError) as exc_info:
        await bigquery_client.submit_query(sample_job_run)

    assert error_msg in str(exc_info.value)


@pytest.mark.asyncio
async def test_submit_query_failure(
    bigquery_client: BigQueryClient,
    sample_job_run: JobRun,
    mock_bq_client: MagicMock,
) -> None:
    mock_bq_client.query.side_effect = GoogleAPIError("BigQuery error")

    with pytest.raises(BigQueryApiProtocolError) as exc_info:
        await bigquery_client.submit_query(sample_job_run)

    assert "BigQuery error" in str(exc_info.value)


@pytest.mark.asyncio
async def test_cancel_query(
    bigquery_client: BigQueryClient,
    mock_bq_client: MagicMock,
) -> None:
    mock_job = MagicMock(spec=QueryJob)
    mock_bq_client.get_job.return_value = mock_job

    await bigquery_client.cancel_query("test-job-id")

    mock_bq_client.get_job.assert_called_once_with("test-job-id")
    mock_job.cancel.assert_called_once()


@pytest.mark.asyncio
async def test_get_query_status_executing(
    bigquery_client: BigQueryClient,
    mock_bq_client: MagicMock,
) -> None:
    mock_job = MagicMock(spec=QueryJob)
    mock_job.job_id = "test-job-id"
    mock_job.done.return_value = False
    mock_job.created = datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)
    mock_job.total_bytes_processed = 1000000
    mock_job.total_bytes_billed = None
    mock_bq_client.get_job.return_value = mock_job

    status = await bigquery_client.get_query_status("test-job-id")

    assert status.query_id == "test-job-id"
    assert status.status == AsyncQueryPhase.EXECUTING
    assert status.error is None
    assert isinstance(status.progress, ByteProgress)
    assert status.progress.bytes_processed == 1000000
    assert status.collected_bytes == 1000000


@pytest.mark.asyncio
async def test_get_query_status_completed(
    bigquery_client: BigQueryClient,
    mock_bq_client: MagicMock,
) -> None:
    mock_result = MagicMock()
    mock_result.total_rows = 42

    mock_job = MagicMock(spec=QueryJob)
    mock_job.job_id = "test-job-id"
    mock_job.done.return_value = True
    mock_job.error_result = None
    mock_job.created = datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)
    mock_job.total_bytes_processed = 2000000
    mock_job.total_bytes_billed = 2000000
    mock_job.result.return_value = mock_result
    mock_bq_client.get_job.return_value = mock_job

    status = await bigquery_client.get_query_status("test-job-id")

    assert status.query_id == "test-job-id"
    assert status.status == AsyncQueryPhase.COMPLETED
    assert status.error is None
    assert status.final_rows == 42
    assert status.collected_bytes == 2000000


@pytest.mark.asyncio
async def test_get_query_status_failed(
    bigquery_client: BigQueryClient,
    mock_bq_client: MagicMock,
) -> None:
    mock_job = MagicMock(spec=QueryJob)
    mock_job.job_id = "test-job-id"
    mock_job.done.return_value = True
    mock_job.error_result = {"message": "Syntax error in SQL"}
    mock_job.created = datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)
    mock_job.total_bytes_processed = 0
    mock_job.total_bytes_billed = 0
    mock_bq_client.get_job.return_value = mock_job

    status = await bigquery_client.get_query_status("test-job-id")

    assert status.query_id == "test-job-id"
    assert status.status == AsyncQueryPhase.FAILED
    assert status.error == "Syntax error in SQL"
    assert status.collected_bytes == 0


@pytest.mark.asyncio
async def test_list_running_queries(
    bigquery_client: BigQueryClient,
) -> None:
    queries = await bigquery_client.list_running_queries()
    assert queries == {}


@pytest.mark.asyncio
async def test_get_query_results_gen(
    bigquery_client: BigQueryClient,
    mock_bq_client: MagicMock,
) -> None:
    class MockBQRow:
        def __init__(self, fields: list[str], values_tuple: tuple) -> None:
            self._fields = fields
            self._values_tuple = values_tuple

        def values(self) -> tuple:
            return self._values_tuple

        def items(self) -> list[tuple[str, Any]]:
            return list(zip(self._fields, self._values_tuple, strict=True))

    mock_row1 = MockBQRow(["id", "name", "value"], (1, "test1", 100.5))
    mock_row2 = MockBQRow(["id", "name", "value"], (2, "test2", 200.5))

    mock_result = MagicMock()
    mock_result.__iter__.return_value = iter([mock_row1, mock_row2])

    mock_job = MagicMock(spec=QueryJob)
    mock_job.done.return_value = True
    mock_job.error_result = None
    mock_job.result.return_value = mock_result
    mock_bq_client.get_job.return_value = mock_job

    rows = [
        row
        async for row in bigquery_client.get_query_results_gen("test-job-id")
    ]

    assert len(rows) == 2
    assert len(rows[0]) == 3
    assert rows[0] == (1, "test1", 100.5)
    assert rows[1] == (2, "test2", 200.5)


@pytest.mark.asyncio
async def test_get_query_results_gen_not_done(
    bigquery_client: BigQueryClient,
    mock_bq_client: MagicMock,
) -> None:
    mock_job = MagicMock(spec=QueryJob)
    mock_job.done.return_value = False
    mock_bq_client.get_job.return_value = mock_job

    with pytest.raises(BigQueryApiError):
        async for _ in bigquery_client.get_query_results_gen("test-job-id"):
            pass


@pytest.mark.asyncio
async def test_delete_result(
    bigquery_client: BigQueryClient,
    logger: BoundLogger,
) -> None:
    await bigquery_client.delete_result("test-job-id")


@pytest.mark.asyncio
async def test_upload_table_not_implemented(
    bigquery_client: BigQueryClient,
) -> None:
    upload = JobTableUpload(
        table_name="user_test.temp_table",
        source_url="https://example.com/data.csv",
        schema_url="https://example.com/schema.json",
    )

    with pytest.raises(BackendNotImplementedError) as exc_info:
        await bigquery_client.upload_table(upload)

    assert "not supported" in str(exc_info.value).lower()
