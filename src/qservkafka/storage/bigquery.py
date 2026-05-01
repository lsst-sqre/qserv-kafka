"""Client for the BigQuery database backend."""

from __future__ import annotations

import asyncio
from collections.abc import (
    AsyncGenerator,
    Callable,
    Coroutine,
    Iterator,
    Sequence,
)
from datetime import UTC, datetime
from functools import wraps
from typing import Any, Concatenate, Protocol, override

import pyarrow as pa
from google.cloud import bigquery, bigquery_storage
from google.cloud.bigquery import DatasetReference, QueryJob, QueryJobConfig
from httpx import AsyncClient
from safir.slack.webhook import SlackWebhookClient
from structlog.stdlib import BoundLogger

from ..config import config
from ..events import BigQueryFailureEvent, Events
from ..exceptions import (
    BackendNotImplementedError,
    BigQueryApiNetworkError,
    BigQueryApiProtocolError,
)
from ..models.kafka import JobRun, JobTableUpload
from ..models.progress import ByteProgress
from ..models.qserv import TableUploadStats
from ..models.query import AsyncQueryPhase, BigQueryQueryStatus, ProcessStatus
from .backend import DatabaseBackend

__all__ = ["BigQueryClient"]

_RESULT_BATCH_SIZE = 1000


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
                event = BigQueryFailureEvent()
                await client.events.bigquery_failure.publish(event)
                await asyncio.sleep(delay)

        # Fell through so failed max_tries - 1 times. Try one last time,
        # re-raising the exception.
        try:
            return await __func(client, *args, **kwargs)
        except BigQueryApiNetworkError:
            event = BigQueryFailureEvent()
            await client.events.bigquery_failure.publish(event)
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
    BigQuery differs from Qserv in a few ways:
    Query IDs are UUIDs (strings) instead of int, progress is reported
    in bytes processed, there is no temp table upload. Also results are
    cached for 24 hours and auto-expire, and query listing is limited.
    """

    def __init__(
        self,
        *,
        project: str,
        location: str,
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
        self._client = bigquery.Client(project=project, location=location)
        self._storage_client = bigquery_storage.BigQueryReadClient()

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
    async def delete_database(self, database: str) -> None:
        raise BackendNotImplementedError(
            "BigQuery doesn't use temporary databases, no deletion needed"
        )

    @override
    async def delete_result(self, query_id: str) -> None:
        pass  # BigQuery results auto-expire after 24 hours

    @override
    async def get_query_results_gen(
        self, query_id: str
    ) -> AsyncGenerator[Sequence[Any]]:
        def _setup_stream() -> Iterator[pa.RecordBatch]:
            job = self._client.get_job(query_id)
            validated_job = self._validate_query_job(job, query_id)
            return validated_job.result().to_arrow_iterable(
                bqstorage_client=self._storage_client,
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

        while True:
            try:
                batch = await asyncio.to_thread(_next_record_batch, batch_iter)
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

        return BigQueryQueryStatus(
            backend_type="BigQuery",
            query_id=query_id,
            status=phase,
            error=error,
            byte_progress=progress,
            query_begin=job.created,
            last_update=datetime.now(tz=UTC),
            collected_bytes=job.total_bytes_processed or 0,
            final_rows=final_rows,
        )

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
    async def upload_table(self, upload: JobTableUpload) -> TableUploadStats:
        raise BackendNotImplementedError(
            "Table upload is not supported for BigQuery backend"
        )
