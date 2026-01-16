"""Client for the BigQuery database backend."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator, Sequence
from datetime import UTC, datetime
from typing import Any, override

from google.api_core.exceptions import GoogleAPIError
from google.cloud import bigquery
from google.cloud.bigquery import DatasetReference, QueryJob, QueryJobConfig
from httpx import AsyncClient
from safir.slack.webhook import SlackWebhookClient
from structlog.stdlib import BoundLogger

from ..config import config
from ..events import Events
from ..exceptions import (
    BackendNotImplementedError,
    BigQueryApiError,
    BigQueryApiProtocolError,
)
from ..models.kafka import JobRun, JobTableUpload
from ..models.progress import ByteProgress
from ..models.qserv import TableUploadStats
from ..models.query import AsyncQueryPhase, BigQueryQueryStatus, ProcessStatus
from .backend import DatabaseBackend

__all__ = ["BigQueryClient"]

# Batch size for streaming result rows from BigQuery.
_RESULT_BATCH_SIZE = 1000

_RETRYABLE_HTTP = {429, 500, 503}
_RETRYABLE_GRPC = {
    "UNAVAILABLE",
    "DEADLINE_EXCEEDED",
    "RESOURCE_EXHAUSTED",
}


def _extract_google_error_details(exc: Exception) -> dict[str, Any]:
    """Extract detailed information from a Google API error.

    Parameters
    ----------
    exc
        Exception to extract details from.

    Returns
    -------
    dict
        Dictionary containing error details suitable for logging.
    """
    details: dict[str, Any] = {
        "error": str(exc),
        "error_type": type(exc).__name__,
    }
    if not isinstance(exc, GoogleAPIError):
        return details

    code = getattr(exc, "code", None)
    if code is not None:
        details["code"] = code

    errors = getattr(exc, "errors", None)
    if errors:
        details["errors"] = [
            {
                "reason": err.get("reason"),
                "message": err.get("message"),
                "location": err.get("location"),
            }
            for err in errors
        ]

    return details


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

    @override
    async def submit_query(self, job: JobRun) -> str:
        """Submit a query to BigQuery for execution.

        Parameters
        ----------
        job
            Query job run request from the user via Kafka.

        Returns
        -------
        str
            BigQuery job ID (UUID string).

        Raises
        ------
        BigQueryApiProtocolError
            Raised if query submission fails.
        """

        def _submit() -> str:
            """Submit query synchronously for async wrapping."""
            default_dataset = (
                DatasetReference(self._project, job.database)
                if job.database
                else None
            )
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
        except (TypeError, ValueError, AttributeError, GoogleAPIError) as e:
            error_details = _extract_google_error_details(e)
            self.logger.exception(
                "Failed to submit query to BigQuery", **error_details
            )
            raise BigQueryApiProtocolError(
                method="query",
                project=self._project,
                error=str(e),
            ) from e

        self.logger.info(
            "Submitted query to BigQuery",
            job_id=job.job_id,
            bigquery_job_id=job_id,
            max_bytes_billed=config.bigquery_max_bytes_billed,
        )
        return job_id

    @override
    async def cancel_query(self, query_id: str) -> None:
        """Cancel a running BigQuery query.

        Parameters
        ----------
        query_id
            BigQuery job ID.

        Raises
        ------
        BigQueryApiError
            Raised if cancellation fails.
        """

        def _cancel() -> None:
            """Cancel query synchronously for async wrapping."""
            job = self._client.get_job(query_id)
            job.cancel()

        try:
            await asyncio.to_thread(_cancel)
        except (TypeError, ValueError, AttributeError, GoogleAPIError) as e:
            error_details = _extract_google_error_details(e)
            self.logger.exception(
                "Failed to cancel BigQuery query",
                bigquery_job_id=query_id,
                **error_details,
            )
            raise BigQueryApiError(
                f"Failed to cancel BigQuery query: {e}"
            ) from e

        self.logger.info("Cancelled BigQuery query", bigquery_job_id=query_id)

    @override
    async def get_query_status(self, query_id: str) -> BigQueryQueryStatus:
        """Get the current status of a BigQuery query.

        Parameters
        ----------
        query_id
            BigQuery job ID.

        Returns
        -------
        BigQueryQueryStatus
            Current status of the query.

        Raises
        ------
        BigQueryApiError
            Raised if status retrieval fails.
        """

        def _get_status() -> tuple[QueryJob, ByteProgress]:
            """Retrieve status synchronously for async wrapping."""
            job = self._client.get_job(query_id)

            if not isinstance(job, QueryJob):
                raise TypeError(f"Expected QueryJob, got {type(job).__name__}")

            progress = ByteProgress(
                bytes_processed=job.total_bytes_processed or 0,
                bytes_billed=job.total_bytes_billed,
                cached=job.cache_hit or False,
            )

            return job, progress

        try:
            job, progress = await asyncio.to_thread(_get_status)
        except Exception as e:
            self.logger.exception(
                "Failed to get BigQuery query status",
                bigquery_job_id=query_id,
                error=str(e),
            )
            raise BigQueryApiError(
                f"Failed to get BigQuery query status: {e}"
            ) from e

        if job.done():
            if job.error_result:
                phase = AsyncQueryPhase.FAILED
                error = job.error_result.get(
                    "message", "Unknown BigQuery error"
                )
            else:
                phase = AsyncQueryPhase.COMPLETED
                error = None
        else:
            phase = AsyncQueryPhase.EXECUTING
            error = None

        return BigQueryQueryStatus(
            query_id=query_id,
            status=phase,
            error=error,
            byte_progress=progress,
            query_begin=job.created,
            last_update=datetime.now(tz=UTC),
            collected_bytes=job.total_bytes_processed or 0,
            final_rows=job.result().total_rows
            if (job.done() and not job.error_result)
            else None,
        )

    @override
    async def list_running_queries(self) -> dict[str, ProcessStatus]:
        """List all currently running BigQuery queries.

        Uses BigQuery's list_jobs API with state_filter="RUNNING" to
        efficiently retrieve all running queries in a single API call.

        Returns
        -------
        dict of ProcessStatus
            Mapping from query ID to process status.

        Raises
        ------
        BigQueryApiError
            Raised if listing jobs fails.
        """

        def _list_running() -> dict[str, ProcessStatus]:
            """List running jobs synchronously for async wrapping."""
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
                self.logger.exception(
                    "Failed to list running BigQuery jobs",
                    error=str(e),
                )
                raise BigQueryApiError(
                    f"Failed to list running jobs: {e}"
                ) from e

            return processes

        return await asyncio.to_thread(_list_running)

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
    async def get_query_results_gen(
        self, query_id: str
    ) -> AsyncGenerator[Sequence[Any]]:
        """Stream the results of a completed BigQuery query.

        Parameters
        ----------
        query_id
            BigQuery job ID.

        Yields
        ------
        Sequence
            Individual rows as tuples of column values.

        Raises
        ------
        BigQueryApiError
            Raised if result retrieval fails during iteration.
        ValueError
            Raised if the query is not complete or failed.
        """

        def _get_result() -> bigquery.table.RowIterator:
            """Get validated job and return result iterator."""
            job = self._client.get_job(query_id)
            validated_job = self._validate_query_job(job, query_id)
            return validated_job.result()

        def _fetch_batch(
            result_iter: Any, batch_size: int = _RESULT_BATCH_SIZE
        ) -> list[tuple[Any, ...]]:
            """Fetch a batch of rows."""
            batch = []
            for _ in range(batch_size):
                try:
                    bq_row = next(result_iter)
                    batch.append(tuple(bq_row.values()))
                except StopIteration:
                    break
            return batch

        try:
            result = await asyncio.to_thread(_get_result)
        except (TypeError, ValueError, AttributeError, GoogleAPIError) as e:
            error_details = _extract_google_error_details(e)
            self.logger.exception(
                "Failed to retrieve BigQuery results",
                bigquery_job_id=query_id,
                **error_details,
            )
            raise BigQueryApiError(
                f"Failed to retrieve BigQuery results: {e}"
            ) from e

        result_iter = iter(result)
        while True:
            batch = await asyncio.to_thread(_fetch_batch, result_iter)
            if not batch:
                break

            for row_values in batch:
                yield row_values

    @override
    async def delete_result(self, query_id: str) -> None:
        """Delete the results of a BigQuery query.

        BigQuery query results are cached for 24 hours and auto-expire,
        so no explicit deletion is needed.

        Parameters
        ----------
        query_id
            BigQuery job ID.
        """

    @override
    async def upload_table(self, upload: JobTableUpload) -> TableUploadStats:
        """Upload a temporary table to BigQuery.

        Parameters
        ----------
        upload
            Table upload specification.

        Raises
        ------
        BackendNotImplementedError
            This operation is not supported for BigQuery in the initial
            implementation.
        """
        raise BackendNotImplementedError(
            "Table upload is not supported for BigQuery backend"
        )

    @override
    async def delete_database(self, database: str) -> None:
        """Delete a temporary database.

        Parameters
        ----------
        database
            Name of the database to delete.

        Raises
        ------
        BackendNotImplementedError
            Always raised; this operation is not supported for BigQuery.
        """
        raise BackendNotImplementedError(
            "BigQuery doesn't use temporary databases, no deletion needed"
        )
