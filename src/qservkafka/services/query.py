"""Service to create new queries."""

import asyncio
from dataclasses import asdict
from datetime import UTC, datetime

from safir.arq import ArqQueue
from safir.sentry import report_exception
from safir.slack.webhook import SlackWebhookClient
from structlog.stdlib import BoundLogger

from ..config import config
from ..events import Events, TemporaryTableUploadEvent
from ..exceptions import (
    BackendApiError,
    BackendApiFailedError,
    BackendApiTransientError,
    QueryError,
    TableUploadWebError,
    UploadTimeoutError,
    UploadWebError,
)
from ..models.kafka import (
    JobCancel,
    JobResultSerialization,
    JobResultType,
    JobRun,
)
from ..models.query import AsyncQueryPhase
from ..models.state import Query, RunningQuery, StartedQuery
from ..models.votable import UploadStats
from ..storage.backend import DatabaseBackend
from ..storage.gafaelfawr import GafaelfawrStorage
from ..storage.rate import RateLimitStore
from ..storage.state import QueryStateStore
from ..storage.votable import VOTableWriter
from .status import StatusPublisher

__all__ = ["QueryService"]


class QueryService:
    """Start or cancel queries.

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
    gafaelfawr_storage
        Storage for quota information.
    arq_queue_fast
        Client to dispatch fast jobs to arq workers.
    arq_queue_slow
        Client to dispatch slow jobs, such as result processing, to arq
        workers.
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
        votable_writer: VOTableWriter,
        state_store: QueryStateStore,
        rate_limit_store: RateLimitStore,
        gafaelfawr_storage: GafaelfawrStorage,
        arq_queue_fast: ArqQueue,
        arq_queue_slow: ArqQueue,
        events: Events,
        slack_client: SlackWebhookClient | None,
        logger: BoundLogger,
    ) -> None:
        self._status = status_publisher
        self._backend = backend
        self._votable = votable_writer
        self._state = state_store
        self._rate_store = rate_limit_store
        self._gafaelfawr = gafaelfawr_storage
        self._arq_fast = arq_queue_fast
        self._arq_slow = arq_queue_slow
        self._events = events
        self._slack_client = slack_client
        self._logger = logger

    async def cancel_query(self, cancel: JobCancel) -> None:
        """Cancel a running query.

        Parameters
        ----------
        cancel
            Request to cancel the query.
        """
        logger = self._logger.bind(**cancel.to_logging_context())
        query_id = cancel.execution_id
        query = await self._state.get_query(query_id)
        if not query:
            logger.info("Ignoring cancel of unknown or completed job")
            return

        # Cancel the query. If this fails, check to see if it only failed
        # because the job finished and, if so, quietly do nothing. Otherwise,
        # log an exception, which is the best we can do since we don't have a
        # way of returning a cancelation error to the TAP server.
        try:
            await self._backend.cancel_query(query_id)
        except BackendApiError as e:
            e.user = cancel.owner
            try:
                status = await self._backend.get_query_status(query_id)
                if status.status != AsyncQueryPhase.EXECUTING:
                    return
            except BackendApiError:
                pass
            await report_exception(e, self._slack_client)
            logger.exception("Failed to cancel query", error=str(e))

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
        await self._delete_uploaded_databases(query.job)
        if query.has_results() and config.qserv_delete_queries:
            try:
                await self._backend.delete_result(query.query_id)
            except BackendApiError as e:
                e.user = query.job.owner
                await report_exception(e, slack_client=self._slack_client)
                logger.exception("Cannot delete results")

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

    async def handle_query(
        self, job: JobRun, kafka_start: datetime | None = None
    ) -> None:
        """Handle an incoming request to run a query.

        Start the query and publish the status of the query as a Kafka
        message.

        Parameters
        ----------
        job
            Query job to start.
        kafka_start
            Time at which the Kafka message for the job was queued.
        """
        query = Query(queued=kafka_start, job=job, start=datetime.now(tz=UTC))

        # Verify that the job is valid.
        result_type = job.result_format.format.type
        serialization = job.result_format.format.serialization
        if result_type == JobResultType.VOTable:
            if not serialization:
                msg = "VOTable format requires serialization"
                await self._status.publish_invalid_request(job, msg)
                return
            if serialization != JobResultSerialization.BINARY2:
                msg = f"{serialization} serialization not supported"
                await self._status.publish_invalid_request(job, msg)
                return
        for column in job.result_format.column_types:
            if not column.is_string() and column.arraysize is not None:
                m = "arraysize only supported for char and unicodeChar fields"
                await self._status.publish_invalid_request(job, m)
                return

        # Jobs with table uploads are dispatched to an arq worker instead,
        # since an upload can take a long time and would otherwise block
        # consumption of the job-run topic. Otherwise, start the query and get
        # its initial status directly in the handler.
        if job.upload_tables:
            await self._arq_fast.enqueue("start_query", query)
        else:
            await self.start_query(query)

    async def start_query(self, query: Query) -> None:
        """Start a new query and publish its initial status.

        Parameters
        ----------
        query
            Query to start.
        """
        job = query.job

        # Increment the user's running queries and make sure they have space
        # to start a new query.
        quota = await self._gafaelfawr.get_user_quota(job.owner)
        count = await self._rate_store.start_query(job.owner)
        if quota and quota.concurrent < count:
            await self._rate_store.end_query(job.owner)
            await self._status.publish_quota_exceeded(job, count - 1, quota)
            return

        # Set up the logger.
        logger = self._logger.bind(
            quota=quota.to_logging_context() if quota else None,
            running=count,
            **query.to_logging_context(),
        )

        # Start the query.
        try:
            started_query = await self._start_query_internal(query, logger)
        except QueryError as e:
            await self._rate_store.end_query(job.owner)
            await self._status.publish_start_exception(query, e)
            return
        except Exception:
            await self._rate_store.end_query(job.owner)
            raise

        # Publish an initial status update to Kafka.
        await self._publish_initial_status(started_query, logger)

    async def update_query(self, query: RunningQuery) -> None:
        """Update the status of a query.

        If the query has already completed, retrieve the results if
        successful. Publish a status update for the query, and then clean up
        the query if needed.

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

    async def _delete_uploaded_databases(
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

    async def _publish_initial_status(
        self, started_query: StartedQuery, logger: BoundLogger
    ) -> None:
        """Store the running query and construct the initial status.

        Store the running query in Redis for further tracking and construct
        the initial status update to return to Kafka.

        Parameters
        ----------
        started_query
            Started query.
        logger
            Logger to use.
        """
        try:
            query = await self.get_running_query(started_query)
        except QueryError as e:
            await self.delete_query_data(started_query)
            await self._status.publish_exception(started_query, e)
            return

        # If the query has already completed successfully, immediately
        # dispatch it to the result worker and and send an executing status.
        # Otherwise, let the result service build the status, which handles
        # executing, aborted, and failed queries.
        if query.status.status == AsyncQueryPhase.COMPLETED:
            query.result_queued = True
            await self._state.store_query(query)
            await self._arq_slow.enqueue("finish_query", query.query_id)
            logger.info("Dispatched immediately completed query to worker")
            await self._status.publish_executing(query)
        else:
            if query.status.status == AsyncQueryPhase.EXECUTING:
                await self._state.store_query(query)
            await self.update_query(query)

    async def _start_query_internal(
        self, query: Query, logger: BoundLogger
    ) -> StartedQuery:
        """Start a query by dispatching it to the backend.

        Parameters
        ----------
        query
            Query to start.
        logger
            Logger to use for any messages.

        Returns
        -------
        StartedQuery
            Started query with backend job ID.
        """
        # Upload any tables.
        uploaded = set()
        try:
            for upload in query.job.upload_tables:
                stats = await self._backend.upload_table(upload)
                uploaded.add(upload.database)
                logger.info("Uploaded table", table_name=upload.table_name)
                event = TemporaryTableUploadEvent(
                    job_id=query.job.job_id,
                    username=query.job.owner,
                    size=stats.size,
                    elapsed=stats.elapsed,
                )
                await self._events.temporary_table.publish(event)
        except (BackendApiError, TableUploadWebError) as e:
            e.user = query.job.owner
            await self._rate_store.end_query(query.job.owner)
            if isinstance(e, TableUploadWebError):
                msg = "Unable to retrieve table to upload"
            else:
                msg = "Unable to upload table"
            await report_exception(e, self._slack_client)
            logger.exception(msg, error=str(e))
            await self._delete_uploaded_databases(query.job, uploaded)
            raise

        # Start the query.
        query_id = None
        try:
            query_id = await self._backend.submit_query(query.job)
        except BackendApiError as e:
            await self._rate_store.end_query(query.job.owner)
            if isinstance(e, BackendApiFailedError):
                logger = logger.bind(**e.to_logging_context())
                logger.info("Query rejected by backend")
            else:
                await report_exception(e, self._slack_client)
                logger.exception("Unable to start query", error=str(e))
            await self._delete_uploaded_databases(query.job)
            raise

        # Return the corresponding StartedQuery object.
        logger.info("Started query", backend_id=query_id)
        return StartedQuery.from_query(query, query_id)

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
