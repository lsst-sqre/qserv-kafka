"""Client for the BigQuery database backend."""

from __future__ import annotations

import asyncio
import re
from collections.abc import (
    AsyncGenerator,
    Callable,
    Coroutine,
    Generator,
    Iterator,
    Sequence,
)
from concurrent.futures import TimeoutError as FutureTimeoutError
from datetime import UTC, datetime
from functools import wraps
from typing import Any, Concatenate, Protocol, cast, override

import pyarrow as pa
from google.api_core.exceptions import ClientError, TooManyRequests
from google.cloud import bigquery, bigquery_storage
from google.cloud.bigquery import (
    CreateDisposition,
    DatasetReference,
    LoadJob,
    LoadJobConfig,
    QueryJob,
    QueryJobConfig,
    SchemaField,
    SourceFormat,
    TableReference,
    WriteDisposition,
)
from google.cloud.storage import Blob
from google.cloud.storage import Client as GCSClient
from httpx import AsyncClient
from pydantic import ValidationError
from safir.slack.webhook import SlackWebhookClient
from structlog.stdlib import BoundLogger

from ..config import config
from ..events import BigQueryApiFailureEvent, Events, QueryApiFailureEvent
from ..exceptions import (
    BackendNotImplementedError,
    BigQueryApiError,
    BigQueryApiFailedError,
    BigQueryApiNetworkError,
    BigQueryApiProtocolError,
)
from ..models.bigquery import BigQueryUploadSchema
from ..models.kafka import JobRun, JobTableUpload
from ..models.progress import ByteProgress
from ..models.query import (
    AsyncQueryPhase,
    BigQueryQueryStatus,
    ProcessStatus,
    TableUploadStats,
)
from .backend import DatabaseBackend

__all__ = ["BigQueryClient"]

_RESULT_BATCH_SIZE = 1000

_UPLOAD_NULL_MARKER = "\\N"
"""String used for null values in uploaded CSV files."""

_UPLOAD_MAX_ERRORS = 5
"""Maximum number of load errors to include in the error for the user."""

_UPLOAD_GS_URL = re.compile(r"^gs://[^/]+/.+")
"""Pattern for the GCS URLs of uploaded tables and their schemas."""


def _upload_error(
    method: str, project: str, exc: Exception
) -> BigQueryApiError:
    """Convert an upload exception, only retrying transient errors."""
    if isinstance(exc, ClientError) and not isinstance(exc, TooManyRequests):
        return BigQueryApiFailedError.from_exception(method, project, exc)
    return BigQueryApiNetworkError.from_exception(method, project, exc)


def _next_record_batch(
    batch_iter: Iterator[pa.RecordBatch],
) -> pa.RecordBatch | None:
    try:
        return next(batch_iter)
    except StopIteration:
        return None


class _BigQueryClientProtocol(Protocol):
    """Protocol used by the retry decorator."""

    events: Events
    logger: BoundLogger


type _BigQueryClientMethod[**P, T, C: _BigQueryClientProtocol] = Callable[
    Concatenate[C, P], Coroutine[None, None, T]
]
"""The type of a method in the `BigQueryClient` class."""


def _retry[**P, T, C: _BigQueryClientProtocol](
    __func: _BigQueryClientMethod[P, T, C], /
) -> _BigQueryClientMethod[P, T, C]:
    """Retry a failed BigQuery API action.

    If the wrapped method fails with a transient error, retry it up to
    ``backend_retry_count`` times. Any method with this decorator must be
    idempotent, since it may be re-run multiple times.
    """

    @wraps(__func)
    async def retry_wrapper(client: C, *args: P.args, **kwargs: P.kwargs) -> T:
        for _ in range(1, config.backend_retry_count):
            try:
                return await __func(client, *args, **kwargs)
            except BigQueryApiNetworkError:
                delay = config.backend_retry_delay.total_seconds()
                msg = f"BigQuery API call failed, retrying after {delay}s"
                client.logger.exception(msg)
                event = BigQueryApiFailureEvent()
                await client.events.query_api_failure.publish(event)
                await asyncio.sleep(delay)

        # Fell through so failed max_tries - 1 times. Try one last time,
        # re-raising the exception.
        try:
            return await __func(client, *args, **kwargs)
        except BigQueryApiNetworkError:
            event = BigQueryApiFailureEvent()
            await client.events.query_api_failure.publish(event)
            raise

    return retry_wrapper


class BigQueryClient(DatabaseBackend):
    """Client for the BigQuery database backend.

    Implements the `DatabaseBackend` interface for Google BigQuery, providing
    query submission, monitoring, and result retrieval.

    The BigQuery client library is synchronous so all operations are wrapped
    with asyncio.to_thread() to avoid blocking.

    Parameters
    ----------
    project
        GCP project ID containing the BigQuery datasets.
    location
        BigQuery processing location.
    bigquery_client
        Shared synchronous BigQuery client for the jobs/REST API.
    bigquery_read_client
        Shared BigQuery Storage Read API client used to stream results.
    gcs_client
        Shared Google Cloud Storage client used to read the schemas of
        uploaded tables.
    http_client
        HTTP client for making API calls.
    events
        Metrics events publishers.
    slack_client
        Slack client to send errors to.
    logger
        Logger to use.

    Attributes
    ----------
    events
        Metrics events publishers.
    slack_client
        Slack client to send errors to.
    logger
        Logger to use.

    Notes
    -----
    BigQuery differs from Qserv in a few ways: Query IDs are UUIDs (strings)
    instead of int, and progress is reported in bytes processed. Uploaded
    tables are loaded straight from GCS into a shared upload dataset.
    Results are cached for 24 hours and auto-expire, and query listing is
    limited.
    """

    def __init__(
        self,
        *,
        project: str,
        location: str,
        bigquery_client: bigquery.Client,
        bigquery_read_client: bigquery_storage.BigQueryReadClient,
        gcs_client: GCSClient,
        http_client: AsyncClient,
        events: Events,
        slack_client: SlackWebhookClient | None,
        logger: BoundLogger,
    ) -> None:
        self.events = events
        self.slack_client = slack_client
        self.logger = logger

        self._project = project
        self._location = location
        self._http_client = http_client
        self._client = bigquery_client
        self._storage_client = bigquery_read_client
        self._gcs_client = gcs_client

    def _validate_query_job(self, job: Any, query_id: str) -> QueryJob:
        """Validate and return a `QueryJob`, raising errors for invalid states.

        Parameters
        ----------
        job
            Job object from BigQuery client.
        query_id
            BigQuery job ID for error messages.

        Returns
        -------
        QueryJob
            Validated query job.

        Raises
        ------
        TypeError
            If job is not a QueryJob.
        ValueError
            If query is not complete or failed.
        """
        if not isinstance(job, QueryJob):
            raise TypeError(f"Expected QueryJob, got {type(job).__name__}")

        if not job.done():
            raise ValueError(f"Query {query_id} is not yet complete")

        if job.error_result:
            msg = job.error_result.get("message")
            raise ValueError(f"Query {query_id} failed: {msg}")

        return job

    @override
    @_retry
    async def cancel_query(self, query_id: str) -> None:
        def _cancel() -> None:
            job = self._client.get_job(query_id)
            job.cancel()

        try:
            await asyncio.to_thread(_cancel)
        except Exception as e:
            exc = BigQueryApiNetworkError.from_exception(
                "cancel", self._project, e
            )
            msg = "Failed to cancel BigQuery query"
            self.logger.exception(msg, **exc.to_logging_context())
            raise exc from e

        self.logger.info("Cancelled BigQuery query", bigquery_job_id=query_id)

    @override
    @_retry
    async def delete_upload_resource(self, name: str) -> None:
        table = self._get_upload_table(name)
        timeout = config.backend_api_timeout.total_seconds()
        try:
            await asyncio.to_thread(
                self._client.delete_table,
                table,
                not_found_ok=True,
                timeout=timeout,
            )
        except Exception as e:
            exc = _upload_error("delete_table", table.project, e)
            msg = "Failed to delete uploaded table"
            self.logger.exception(msg, **exc.to_logging_context())
            raise exc from e

    @override
    async def delete_result(self, query_id: str) -> None:
        pass  # BigQuery results auto-expire after 24 hours

    @override
    async def get_query_results_gen(
        self, query_id: str
    ) -> AsyncGenerator[Sequence[Any]]:
        def _setup_stream() -> Generator[pa.RecordBatch]:
            job = self._client.get_job(query_id)
            validated_job = self._validate_query_job(job, query_id)

            # to_arrow_iterable claims to return an Iterator, but it's a
            # generator holding a thread pool and gRPC streams. Narrow the
            # type so we can call close() on it.
            return cast(
                "Generator[pa.RecordBatch]",
                validated_job.result().to_arrow_iterable(
                    bqstorage_client=self._storage_client,
                ),
            )

        try:
            batch_iter = await asyncio.to_thread(_setup_stream)
        except Exception as e:
            exc = BigQueryApiNetworkError.from_exception(
                "get_results", self._project, e
            )
            msg = "Failed to retrieve BigQuery results"
            self.logger.exception(msg, **exc.to_logging_context())
            raise exc from e

        try:
            while True:
                try:
                    batch = await asyncio.to_thread(
                        _next_record_batch, batch_iter
                    )
                except Exception as e:
                    exc = BigQueryApiNetworkError.from_exception(
                        "get_results", self._project, e
                    )
                    self.logger.exception(
                        "Failed to stream BigQuery results",
                        **exc.to_logging_context(),
                    )
                    raise exc from e
                if batch is None:
                    break
                for offset in range(0, len(batch), _RESULT_BATCH_SIZE):
                    length = min(_RESULT_BATCH_SIZE, len(batch) - offset)
                    sliced = batch.slice(offset, length)
                    columns = [col.to_pylist() for col in sliced.columns]
                    if columns:
                        for row in zip(*columns, strict=True):
                            yield row
        finally:
            try:
                await asyncio.to_thread(batch_iter.close)
            except Exception:
                self.logger.warning(
                    "Failed to close BigQuery result stream", exc_info=True
                )

    @override
    @_retry
    async def get_query_status(self, query_id: str) -> BigQueryQueryStatus:
        def _get_status() -> tuple[QueryJob, ByteProgress, int | None]:
            """Retrieve status synchronously for async wrapping."""
            job = self._client.get_job(query_id)

            if not isinstance(job, QueryJob):
                raise TypeError(f"Expected QueryJob, got {type(job).__name__}")

            progress = ByteProgress(
                bytes_processed=job.total_bytes_processed or 0,
                bytes_billed=job.total_bytes_billed,
                cached=job.cache_hit or False,
            )

            final_rows = None
            if job.done() and not job.error_result:
                final_rows = job.result().total_rows

            return job, progress, final_rows

        try:
            job, progress, final_rows = await asyncio.to_thread(_get_status)
        except Exception as e:
            exc = BigQueryApiNetworkError.from_exception(
                "get_status", self._project, e
            )
            msg = "Failed to get BigQuery query status"
            self.logger.exception(msg, **exc.to_logging_context())
            raise exc from e

        error = None
        if job.done():
            if job.error_result:
                phase = AsyncQueryPhase.FAILED
                error = job.error_result.get(
                    "message", "Unknown BigQuery error"
                )
            else:
                phase = AsyncQueryPhase.COMPLETED
        else:
            phase = AsyncQueryPhase.EXECUTING

        result_bytes = None
        if phase == AsyncQueryPhase.COMPLETED and job.destination is not None:
            try:
                result_bytes = await asyncio.to_thread(
                    lambda: self._client.get_table(job.destination).num_bytes
                )
            except Exception:
                self.logger.exception(
                    "Failed to retrieve BigQuery result table size"
                )
                await self.events.query_api_failure.publish(
                    BigQueryApiFailureEvent()
                )

        return BigQueryQueryStatus(
            backend_type="BigQuery",
            query_id=query_id,
            status=phase,
            error=error,
            byte_progress=progress,
            query_begin=job.created,
            last_update=datetime.now(tz=UTC),
            result_bytes=result_bytes or 0,
            final_rows=final_rows,
        )

    @override
    def get_upload_resources(
        self, uploads: Sequence[JobTableUpload]
    ) -> list[str]:
        return [u.table_name for u in uploads]

    @override
    @_retry
    async def list_running_queries(self) -> dict[str, ProcessStatus]:
        def _list_running() -> dict[str, ProcessStatus]:
            processes: dict[str, ProcessStatus] = {}
            try:
                for job in self._client.list_jobs(state_filter="RUNNING"):
                    # Only include QueryJob instances
                    if not isinstance(job, QueryJob):
                        continue

                    progress = ByteProgress(
                        bytes_processed=job.total_bytes_processed or 0,
                        bytes_billed=job.total_bytes_billed,
                        cached=job.cache_hit or False,
                    )

                    processes[job.job_id] = ProcessStatus(
                        status=AsyncQueryPhase.EXECUTING,
                        progress=progress,
                        last_update=datetime.now(tz=UTC),
                    )
            except Exception as e:
                exc = BigQueryApiNetworkError.from_exception(
                    "list_jobs", self._project, e
                )
                msg = "Failed to list running BigQuery jobs"
                self.logger.exception(msg, **exc.to_logging_context())
                raise exc from e

            return processes

        return await asyncio.to_thread(_list_running)

    @override
    def result_api_failure_event(self) -> QueryApiFailureEvent:
        return BigQueryApiFailureEvent()

    @override
    async def submit_query(self, job: JobRun) -> str:
        def _submit() -> str:
            default_dataset = None
            if job.database:
                default_dataset = DatasetReference(self._project, job.database)

            job_config_kwargs: dict[str, Any] = {
                "default_dataset": default_dataset,
            }
            if config.bigquery_max_bytes_billed is not None:
                job_config_kwargs["maximum_bytes_billed"] = (
                    config.bigquery_max_bytes_billed
                )

            if job.timeout is not None:
                job_config_kwargs["job_timeout_ms"] = int(
                    job.timeout.total_seconds() * 1000
                )

            job_config = QueryJobConfig(**job_config_kwargs)
            query_job = self._client.query(job.query, job_config=job_config)
            return query_job.job_id

        try:
            job_id = await asyncio.to_thread(_submit)
        except Exception as e:
            exc = BigQueryApiProtocolError.from_exception(
                "query", self._project, e
            )
            msg = "Failed to submit query to BigQuery"
            self.logger.exception(msg, **exc.to_logging_context())
            raise exc from e

        self.logger.info(
            "Submitted query to BigQuery",
            job_id=job.job_id,
            bigquery_job_id=job_id,
            max_bytes_billed=config.bigquery_max_bytes_billed,
        )
        return job_id

    @override
    @_retry
    async def upload_table(self, upload: JobTableUpload) -> TableUploadStats:
        table = self._get_upload_table(upload.table_name)
        for url in (upload.source_url, upload.schema_url):
            if not _UPLOAD_GS_URL.match(url):
                msg = f"{url} for {upload.table_name} is not a gs:// URL"
                raise BigQueryApiProtocolError("upload", table.project, msg)
        start = datetime.now(tz=UTC)
        timeout = config.bigquery_upload_timeout.total_seconds()
        schema = await self._read_upload_schema(upload, table)

        job_config = LoadJobConfig(
            schema=schema,
            source_format=SourceFormat.CSV,
            null_marker=_UPLOAD_NULL_MARKER,
            allow_quoted_newlines=True,
            create_disposition=CreateDisposition.CREATE_IF_NEEDED,
            write_disposition=WriteDisposition.WRITE_TRUNCATE,
        )

        try:
            load_job = await asyncio.to_thread(
                self._client.load_table_from_uri,
                upload.source_url,
                table,
                job_config=job_config,
            )
        except Exception as e:
            exc = _upload_error("load_table", table.project, e)
            msg = "Failed to start BigQuery load job"
            self.logger.exception(msg, **exc.to_logging_context())
            raise exc from e

        # A failed job raises the exception matching its error reason.
        job_id = load_job.job_id
        try:
            await asyncio.to_thread(load_job.result, timeout=timeout)
        except Exception as e:
            project = table.project
            error_result = load_job.error_result or {}
            if isinstance(e, FutureTimeoutError):
                await self._cancel_load(load_job)
                error = f"Timed out loading {table.table_id} after {timeout}s"
                exc = BigQueryApiFailedError("load_table", project, error)
            elif error_result.get("reason") == "invalid":
                errors = load_job.errors or [error_result]
                details = "; ".join(
                    str(err.get("message", err))
                    for err in errors[:_UPLOAD_MAX_ERRORS]
                )
                error = f"Unable to load uploaded table {table.table_id}"
                exc = BigQueryApiFailedError(
                    "load_table", project, f"{error}: {details}"
                )
            else:
                exc = _upload_error("load_table", project, e)
            msg = "Failed to load uploaded table into BigQuery"
            context = exc.to_logging_context()
            self.logger.exception(msg, bigquery_job_id=job_id, **context)
            raise exc from e

        elapsed = datetime.now(tz=UTC) - start
        self.logger.info(
            "Loaded uploaded table into BigQuery",
            table_name=upload.table_name,
            bigquery_job_id=job_id,
            rows=load_job.output_rows,
            elapsed=elapsed.total_seconds(),
        )
        return TableUploadStats(
            size=load_job.input_file_bytes, elapsed=elapsed
        )

    async def _cancel_load(self, load_job: LoadJob) -> None:
        """Cancel a load job that timed out, logging any failure.

        Parameters
        ----------
        load_job
            Load job to cancel.
        """
        try:
            await asyncio.to_thread(load_job.cancel)
        except Exception as e:
            self.logger.warning(
                "Unable to cancel BigQuery load job",
                bigquery_job_id=load_job.job_id,
                error=str(e),
            )

    def _get_upload_table(self, table_name: str) -> TableReference:
        """Parse and check the destination of an uploaded table.

        The TAP service passes in the fully qualified table name. Reject any
        table that is not in the configured upload dataset.

        Parameters
        ----------
        table_name
            Fully qualified name of the uploaded table.

        Returns
        -------
        TableReference
            Reference to the destination table.

        Raises
        ------
        BackendNotImplementedError
            Raised if table uploads are not configured.
        BigQueryApiProtocolError
            Raised if the table is not in the upload dataset.
        """
        project = config.bigquery_upload_project
        if not project:
            msg = "Table upload is not configured for the BigQuery backend"
            raise BackendNotImplementedError(msg)
        dataset = config.bigquery_upload_dataset
        try:
            table = TableReference.from_string(table_name)
        except ValueError as e:
            msg = f"Invalid upload table name {table_name}: {e}"
            raise BigQueryApiProtocolError("upload", project, msg) from e
        if table.project != project or table.dataset_id != dataset:
            msg = (
                f"Upload table {table_name} is not in upload dataset"
                f" {project}.{dataset}"
            )
            raise BigQueryApiProtocolError("upload", project, msg)
        return table

    async def _read_upload_schema(
        self, upload: JobTableUpload, table: TableReference
    ) -> list[SchemaField]:
        """Read the BigQuery schema of an uploaded table from GCS.

        Parameters
        ----------
        upload
            Uploaded table.
        table
            Destination table, used for error reporting.

        Returns
        -------
        list of SchemaField
            Schema of the uploaded table.

        Raises
        ------
        BigQueryApiFailedError
            Raised if GCS refused the request.
        BigQueryApiNetworkError
            Raised if the schema could not be retrieved from GCS because of a
            transient error.
        BigQueryApiProtocolError
            Raised if the schema is invalid.
        """
        timeout = config.backend_api_timeout.total_seconds()
        blob = Blob.from_uri(upload.schema_url, client=self._gcs_client)
        try:
            data = await asyncio.to_thread(
                blob.download_as_text, timeout=timeout
            )
        except Exception as e:
            exc = _upload_error("get_schema", table.project, e)
            msg = "Failed to read uploaded table schema from GCS"
            self.logger.exception(msg, **exc.to_logging_context())
            raise exc from e

        try:
            schema = BigQueryUploadSchema.model_validate_json(data)
        except ValidationError as e:
            error = f"Invalid schema for {upload.table_name}: {e}"
            exc = BigQueryApiProtocolError("get_schema", table.project, error)
            msg = "Invalid uploaded table schema"
            self.logger.exception(msg, **exc.to_logging_context())
            raise exc from e
        return [SchemaField(f.name, f.type) for f in schema.root]
