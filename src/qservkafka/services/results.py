"""Processing of completed queries."""

import asyncio
from dataclasses import asdict
from datetime import UTC, datetime

from safir.sentry import report_exception
from safir.slack.webhook import SlackWebhookClient
from structlog.stdlib import BoundLogger

from ..config import config
from ..events import Events
from ..exceptions import (
    BackendApiError,
    BackendApiTransientError,
    QueryError,
    UploadTimeoutError,
    UploadWebError,
)
from ..models.kafka import JobRun
from ..models.query import AsyncQueryPhase
from ..models.state import RunningQuery, StartedQuery
from ..models.votable import UploadStats
from ..storage.backend import DatabaseBackend
from ..storage.rate import RateLimitStore
from ..storage.state import QueryStateStore
from ..storage.votable import VOTableWriter
from .status import StatusPublisher

__all__ = ["ResultProcessor"]


class ResultProcessor:
    """Process the results of a completed query.

    Parameters
    ----------
    status_publisher
        Publisher for status events and Kafka messages.
    backend
        Database backend client (Qserv, BigQuery, etc.).
    state_store
        Storage for query state.
    votable_writer
        Writer for VOTable output.
    rate_limit_store
        Storage for rate limiting.
    events
        Metrics events publishers.
    slack_client
        Client to send errors to Slack
    logger
        Logger to use.
    """

    def __init__(
        self,
        *,
        status_publisher: StatusPublisher,
        backend: DatabaseBackend,
        state_store: QueryStateStore,
        votable_writer: VOTableWriter,
        rate_limit_store: RateLimitStore,
        events: Events,
        slack_client: SlackWebhookClient | None,
        logger: BoundLogger,
    ) -> None:
        self._status = status_publisher
        self._backend = backend
        self._state = state_store
        self._votable = votable_writer
        self._rate_store = rate_limit_store
        self._events = events
        self._slack_client = slack_client
        self._logger = logger

    async def delete_query_data(self, query: StartedQuery) -> None:
        """Delete stored information for the query.

        Remove the query from the Kafka bridge state storage and delete
        any uploaded temporary tables.

        Parameters
        ----------
        query
            Query metadata.
        logger
            Logger to use.
        """
        logger = self._logger.bind(**query.to_logging_context())
        logger.debug("Cleaning up query data")

        # Remove internal tracking. Everything after this point will not be
        # retried on failure, just orphaned.
        await self._rate_store.end_query(query.job.owner)
        await self._state.delete_query(query.query_id)

        # Delete the uploaded tables and the results if configured to do so.
        await self.delete_uploaded_databases(query.job)
        if query.has_results() and config.qserv_delete_queries:
            try:
                await self._backend.delete_result(query.query_id)
            except BackendApiError as e:
                e.user = query.job.owner
                await report_exception(e, slack_client=self._slack_client)
                logger.exception("Cannot delete results")

    async def delete_uploaded_databases(
        self,
        job: JobRun,
        databases: set[str] | None = None,
    ) -> None:
        """Delete any temporary databases created for uploaded tables.

        Parameters
        ----------
        job
            Job metadata.
        databases
            If given, the databases to delete. Otherwise, all databases in the
            list of tables to upload will be deleted.
        """
        logger = self._logger.bind(**job.to_logging_context())
        if databases is None:
            databases = {t.database for t in job.upload_tables}
        logger.debug(
            "Deleting upload databases",
            upload_table_count=len(job.upload_tables),
            upload_table_types=[type(t).__name__ for t in job.upload_tables],
            databases=list(databases),
        )
        for database in databases:
            try:
                await self._backend.delete_database(database)
                logger.debug("Deleted upload database", database_name=database)
            except BackendApiError as e:
                e.user = job.owner
                await report_exception(e, slack_client=self._slack_client)
                logger.exception(
                    "Unable to delete temporary database, orphaning it",
                    error=str(e),
                    database_name=database,
                )

    async def get_running_query(self, query: StartedQuery) -> RunningQuery:
        """Retrieve the query status and construct a running query record.

        Parameters
        ----------
        query
            Metadata about the query, possibly without the backend status.

        Returns
        -------
        RunningQuery
            Query enhanced with backend status.
        """
        try:
            status = await self._backend.get_query_status(query.query_id)
        except BackendApiError as e:
            e.user = query.job.owner
            await report_exception(e, slack_client=self._slack_client)
            logger = self._logger.bind(**query.to_logging_context())
            logger.exception("Unable to get job status", error=str(e))
            raise
        return RunningQuery.from_started_query(query, status)

    async def process_query(self, query: RunningQuery) -> None:
        """Publish a Kafka status message for the query.

        If the query has already completed, retrieve the results if successful
        and return an appropriate final status. Otherwise, return a status
        message indicating that the job is executing.

        Parameters
        ----------
        query
            Metadata about the query without the Qserv status.

        Returns
        -------
        JobStatus
            Job status to report to Kafka.
        """
        logger = self._logger.bind(**query.to_logging_context())
        match query.status.status:
            case AsyncQueryPhase.ABORTED:
                await self._status.publish_aborted(query)
            case AsyncQueryPhase.EXECUTING:
                await self._status.publish_executing(query)
                return
            case AsyncQueryPhase.COMPLETED:
                try:
                    stats = await self._upload_results_retry(query, logger)
                except QueryError as e:
                    await report_exception(e, slack_client=self._slack_client)
                    logger.exception(e.description, **e.to_logging_context())
                    await self._status.publish_exception(query, e)
                else:
                    await self._status.publish_completed(query, stats)
            case AsyncQueryPhase.FAILED:
                await self._status.publish_failed(query)

        # If the query was still executing, the code above returned early, so
        # the query completed in some fashion and can be cleaned up.
        await self.delete_query_data(query)

    async def _upload_results(self, query: RunningQuery) -> UploadStats:
        """Retrieve and upload the results.

        Parameters
        ----------
        query
            Query metadata.

        Returns
        -------
        UploadStats
            Statistics about the upload.

        Raises
        ------
        BackendApiTransientError
            Raised if there was a transient failure retrieving results from
            the backend.
        UploadWebError
            Raised if there was a failure to upload the results.
        TimeoutError
            Raised if the processing and upload did not complete within the
            configured timeout.
        """
        start = datetime.now(tz=UTC)
        results = self._backend.get_query_results_gen(query.query_id)
        timeout = config.result_timeout.total_seconds()
        try:
            async with asyncio.timeout(timeout):
                size = await self._votable.store(
                    query.job.result_url,
                    query.job.result_format,
                    results,
                    maxrec=query.job.maxrec,
                )
            elapsed = datetime.now(tz=UTC) - start
            return UploadStats(elapsed=elapsed, **asdict(size))
        except TimeoutError as e:
            elapsed = datetime.now(tz=UTC) - start
            raise UploadTimeoutError(elapsed) from e

    async def _upload_results_retry(
        self, query: RunningQuery, logger: BoundLogger
    ) -> UploadStats:
        """Retrieve and upload the results, with retries.

        Retry the attempt to retrieve and upload the results on SQL or HTTP
        error to work around flaky connections to backend (and the occasional
        GCS hiccup). This cannot use the retry logic at the storage level
        since the SQL call and the HTTP call have to be coordinated.

        Parameters
        ----------
        query
            Query metadata.
        logger
            Logger to use.

        Returns
        -------
        UploadStats
            Statistics about the upload.

        Raises
        ------
        BackendApiTransientError
            Raised if there was a transient failure retrieving results from
            the backend.
        UploadWebError
            Raised if there was a failure to upload the results.
        TimeoutError
            Raised if the processing and upload did not complete within the
            configured timeout.
        """
        logger.debug("Processing job completion")
        for _ in range(1, config.backend_retry_count):
            try:
                return await self._upload_results(query)
            except (BackendApiTransientError, UploadWebError) as e:
                delay = config.backend_retry_delay.total_seconds()
                if isinstance(e, BackendApiTransientError):
                    event = self._backend.result_api_failure_event()
                    await self._events.query_api_failure.publish(event)
                    msg = f"Backend call failed, retrying after {delay}s"
                else:
                    msg = f"Upload of results failed, retrying after {delay}s"

                # We don't want to notify Sentry or Slack about exceptions
                # here because we are going to retry.
                logger.exception(msg)
                await asyncio.sleep(delay)

        # Fell through, so failed max_tries - 1 times. Try one more time,
        # re-raising the exception.
        try:
            return await self._upload_results(query)
        except BackendApiTransientError as e:
            e.user = query.job.owner
            event = self._backend.result_api_failure_event()
            await self._events.query_api_failure.publish(event)
            raise
