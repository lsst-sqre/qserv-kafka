"""Tests for the BigQuery storage layer."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, Mock

import pyarrow as pa
import pytest
from google.api_core.exceptions import (
    BadRequest,
    Forbidden,
    GoogleAPIError,
    InternalServerError,
)
from google.cloud.bigquery import (
    DatasetReference,
    QueryJob,
    SchemaField,
    SourceFormat,
    TableReference,
)
from google.cloud.storage import Blob
from httpx import AsyncClient
from pydantic import HttpUrl
from structlog.stdlib import BoundLogger

from qservkafka import config as config_module
from qservkafka.events import Events
from qservkafka.exceptions import (
    BigQueryApiError,
    BigQueryApiFailedError,
    BigQueryApiNetworkError,
    BigQueryApiProtocolError,
)
from qservkafka.models.kafka import (
    JobResultConfig,
    JobResultEnvelope,
    JobResultFormat,
    JobResultSerialization,
    JobResultType,
    JobRun,
    ReplicatedTableUpload,
)
from qservkafka.models.progress import ByteProgress
from qservkafka.models.query import AsyncQueryPhase
from qservkafka.storage.bigquery import BigQueryClient

UPLOAD_TABLE = "upload-project.tap_uploads.user_test_uws123_mytable"

UPLOAD_SCHEMA = (
    '[{"name": "ra", "type": "FLOAT64"}, {"name": "id", "type": "INT64"}]'
)


@pytest.fixture
def mock_bq_client() -> MagicMock:
    """Create a mock BigQuery client."""
    return MagicMock()


@pytest.fixture
def mock_storage_client() -> MagicMock:
    """Create a mock BigQuery Storage client."""
    return MagicMock()


@pytest.fixture
def bigquery_client(
    mock_bq_client: MagicMock,
    mock_storage_client: MagicMock,
    logger: BoundLogger,
) -> BigQueryClient:
    """Create a BigQueryClient with mocked dependencies."""
    http_client = Mock(spec=AsyncClient)
    events = Mock(spec=Events)
    slack_client = None

    return BigQueryClient(
        project="test-project",
        location="US",
        bigquery_client=mock_bq_client,
        bigquery_read_client=mock_storage_client,
        gcs_client=MagicMock(),
        http_client=http_client,
        events=events,
        slack_client=slack_client,
        logger=logger,
    )


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


@pytest.fixture
def upload_config(monkeypatch: pytest.MonkeyPatch) -> None:
    """Configure the BigQuery upload dataset."""
    config = config_module.config
    monkeypatch.setattr(config, "bigquery_upload_project", "upload-project")
    monkeypatch.setattr(config, "bigquery_upload_dataset", "tap_uploads")


@pytest.fixture
def mock_schema_blob(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    """Mock reading the schema of an uploaded table from GCS."""
    blob = MagicMock()
    blob.download_as_text.return_value = UPLOAD_SCHEMA
    from_uri = MagicMock(return_value=blob)
    monkeypatch.setattr(Blob, "from_uri", from_uri)
    return from_uri


@pytest.fixture
def fast_retry(monkeypatch: pytest.MonkeyPatch) -> None:
    """Make retries of BigQuery API calls fast."""
    config = config_module.config
    monkeypatch.setattr(config, "backend_retry_delay", timedelta(0))


def _upload(table_name: str = UPLOAD_TABLE) -> ReplicatedTableUpload:
    return ReplicatedTableUpload(
        table_name=table_name,
        source_url="gs://uploads/uws123/mytable.csv",
        schema_url="gs://uploads/uws123/mytable.schema.json",
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
    assert status.result_bytes == 0


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
    mock_bq_client.get_table.return_value.num_bytes = 1500000

    status = await bigquery_client.get_query_status("test-job-id")

    assert status.query_id == "test-job-id"
    assert status.status == AsyncQueryPhase.COMPLETED
    assert status.error is None
    assert status.final_rows == 42
    assert status.progress
    assert status.progress.bytes_processed == 2000000
    assert status.result_bytes == 1500000
    mock_bq_client.get_table.assert_called_once_with(mock_job.destination)


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
    assert status.result_bytes == 0


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
    schema = pa.schema(
        [
            pa.field("id", pa.int64()),
            pa.field("name", pa.string()),
            pa.field("value", pa.float64()),
        ]
    )
    batch = pa.record_batch(
        [
            pa.array([1, 2], type=pa.int64()),
            pa.array(["test1", "test2"], type=pa.string()),
            pa.array([100.5, 200.5], type=pa.float64()),
        ],
        schema=schema,
    )

    mock_result = MagicMock()
    mock_result.to_arrow_iterable.return_value = iter([batch])

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
@pytest.mark.usefixtures("upload_config")
async def test_upload_table(
    bigquery_client: BigQueryClient,
    mock_bq_client: MagicMock,
    mock_schema_blob: MagicMock,
) -> None:
    load_job = MagicMock()
    load_job.error_result = None
    load_job.input_file_bytes = 1234
    mock_bq_client.load_table_from_uri.return_value = load_job

    stats = await bigquery_client.upload_table(_upload())

    assert stats.size == 1234
    assert mock_schema_blob.call_args[0][0] == (
        "gs://uploads/uws123/mytable.schema.json"
    )
    args = mock_bq_client.load_table_from_uri.call_args
    assert args[0][0] == "gs://uploads/uws123/mytable.csv"
    job_config = args[1]["job_config"]
    assert job_config.schema == [
        SchemaField("ra", "FLOAT64"),
        SchemaField("id", "INT64"),
    ]
    assert job_config.source_format == SourceFormat.CSV
    assert job_config.null_marker == "\\N"
    assert not job_config.skip_leading_rows


@pytest.mark.asyncio
@pytest.mark.usefixtures("upload_config", "mock_schema_blob")
async def test_upload_table_rejected(
    bigquery_client: BigQueryClient,
    mock_bq_client: MagicMock,
) -> None:
    load_job = MagicMock()
    load_job.result.side_effect = BadRequest("Error while reading data")
    load_job.error_result = {
        "reason": "invalid",
        "message": "Error while reading data",
    }
    load_job.errors = [
        {"message": "Could not parse 'abc' as INT64 for field id"},
    ]
    mock_bq_client.load_table_from_uri.return_value = load_job

    with pytest.raises(BigQueryApiFailedError) as exc_info:
        await bigquery_client.upload_table(_upload())

    assert "user_test_uws123_mytable" in str(exc_info.value)
    assert "Could not parse 'abc' as INT64" in str(exc_info.value)


@pytest.mark.asyncio
@pytest.mark.usefixtures("upload_config", "mock_schema_blob")
async def test_upload_table_load_failed(
    bigquery_client: BigQueryClient,
    mock_bq_client: MagicMock,
) -> None:
    load_job = MagicMock()
    load_job.result.side_effect = Forbidden("Access Denied")
    load_job.error_result = {
        "reason": "accessDenied",
        "message": "Access Denied",
    }
    mock_bq_client.load_table_from_uri.return_value = load_job

    with pytest.raises(BigQueryApiFailedError, match="Access Denied"):
        await bigquery_client.upload_table(_upload())

    mock_bq_client.load_table_from_uri.assert_called_once()


@pytest.mark.asyncio
@pytest.mark.usefixtures("upload_config", "mock_schema_blob", "fast_retry")
async def test_upload_table_load_retried(
    bigquery_client: BigQueryClient,
    mock_bq_client: MagicMock,
) -> None:
    load_job = MagicMock()
    load_job.result.side_effect = InternalServerError("Backend error")
    load_job.error_result = {
        "reason": "backendError",
        "message": "Backend error",
    }
    mock_bq_client.load_table_from_uri.return_value = load_job
    bigquery_client.events = MagicMock()
    bigquery_client.events.query_api_failure.publish = AsyncMock()

    with pytest.raises(BigQueryApiNetworkError):
        await bigquery_client.upload_table(_upload())

    count = config_module.config.backend_retry_count
    assert mock_bq_client.load_table_from_uri.call_count == count


@pytest.mark.asyncio
@pytest.mark.usefixtures("upload_config", "mock_schema_blob")
async def test_upload_table_timeout(
    bigquery_client: BigQueryClient,
    mock_bq_client: MagicMock,
) -> None:
    load_job = MagicMock()
    load_job.result.side_effect = TimeoutError()
    mock_bq_client.load_table_from_uri.return_value = load_job

    with pytest.raises(BigQueryApiFailedError, match="Timed out"):
        await bigquery_client.upload_table(_upload())

    load_job.cancel.assert_called_once()
    mock_bq_client.load_table_from_uri.assert_called_once()


@pytest.mark.asyncio
@pytest.mark.usefixtures("upload_config")
async def test_upload_table_invalid_schema(
    bigquery_client: BigQueryClient,
    mock_bq_client: MagicMock,
    mock_schema_blob: MagicMock,
) -> None:
    blob = mock_schema_blob.return_value
    blob.download_as_text.return_value = '[{"name": "ra"}]'

    with pytest.raises(BigQueryApiProtocolError, match="Invalid schema"):
        await bigquery_client.upload_table(_upload())

    mock_bq_client.load_table_from_uri.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.usefixtures("upload_config", "mock_schema_blob")
async def test_upload_table_outside_dataset(
    bigquery_client: BigQueryClient,
    mock_bq_client: MagicMock,
) -> None:
    upload = _upload("upload-project.ppdb_public.user_test_mytable")

    with pytest.raises(BigQueryApiProtocolError):
        await bigquery_client.upload_table(upload)

    mock_bq_client.load_table_from_uri.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.usefixtures("upload_config")
async def test_delete_upload_resource(
    bigquery_client: BigQueryClient,
    mock_bq_client: MagicMock,
) -> None:
    await bigquery_client.delete_upload_resource(UPLOAD_TABLE)

    mock_bq_client.delete_table.assert_called_once()
    args = mock_bq_client.delete_table.call_args
    assert args[0][0] == TableReference.from_string(UPLOAD_TABLE)
    assert args[1]["not_found_ok"] is True

    outside = "upload-project.ppdb_public.user_test_uws123_bad"
    with pytest.raises(BigQueryApiProtocolError):
        await bigquery_client.delete_upload_resource(outside)


@pytest.mark.asyncio
async def test_submit_query_with_timeout(
    bigquery_client: BigQueryClient,
    sample_job_run: JobRun,
    mock_bq_client: MagicMock,
) -> None:
    mock_job = MagicMock(spec=QueryJob)
    mock_job.job_id = "bq-job-uuid-789"
    mock_bq_client.query.return_value = mock_job

    sample_job_run.timeout = timedelta(seconds=90)
    await bigquery_client.submit_query(sample_job_run)

    job_config = mock_bq_client.query.call_args[1]["job_config"]
    assert str(job_config.job_timeout_ms) == "90000"


@pytest.mark.asyncio
async def test_submit_query_without_timeout(
    bigquery_client: BigQueryClient,
    sample_job_run: JobRun,
    mock_bq_client: MagicMock,
) -> None:
    mock_job = MagicMock(spec=QueryJob)
    mock_job.job_id = "bq-job-uuid-790"
    mock_bq_client.query.return_value = mock_job

    sample_job_run.timeout = None
    await bigquery_client.submit_query(sample_job_run)

    job_config = mock_bq_client.query.call_args[1]["job_config"]
    assert job_config.job_timeout_ms is None
