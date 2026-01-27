"""Client for the Qserv REST API."""

import asyncio
from collections.abc import (
    AsyncGenerator,
    Callable,
    Coroutine,
    Mapping,
    Sequence,
)
from copy import copy
from datetime import UTC, datetime, timedelta
from functools import wraps
from typing import Any, Concatenate, Protocol, overload, override

from httpx import AsyncClient, HTTPError, Response
from pydantic import BaseModel, ValidationError
from safir.database import datetime_from_db
from safir.datetime import format_datetime_for_logging
from safir.slack.blockkit import SlackWebException
from safir.slack.webhook import SlackWebhookClient
from sqlalchemy import Row, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import async_sessionmaker
from structlog.stdlib import BoundLogger

from ..config import config
from ..events import Events, QservFailureEvent, QservProtocol
from ..exceptions import (
    QservApiFailedError,
    QservApiProtocolError,
    QservApiSqlError,
    QservApiWebError,
    TableUploadWebError,
)
from ..models.kafka import JobRun, JobTableUpload
from ..models.progress import ChunkProgress
from ..models.qserv import (
    AsyncSubmitRequest,
    AsyncSubmitResponse,
    BaseResponse,
    QservStatusResponse,
    TableUploadStats,
)
from ..models.query import AsyncQueryPhase, ProcessStatus, QservQueryStatus
from .backend import DatabaseBackend

API_VERSION = 51
"""Version of the REST API that this client requests."""

__all__ = ["API_VERSION", "QservClient"]


def _query_list_sql() -> str:
    """Generate SQL query to get a list of running queries.

    This is overridden by the test suite since it queries an internal MySQL
    namespace when talking to actual Qserv that's difficult to mock. It is
    defined as a function so that it can be mocked in a way that alternates
    successes and failures.
    """
    return """
        SELECT
          ID AS id,
          SUBMITTED AS submitted,
          UPDATED AS updated,
          CHUNKS AS chunks,
          CHUNKS_COMP as chunks_comp
        FROM information_schema.processlist
    """.strip()


def _query_results_sql() -> str:
    """Generate the SQL query to get Qserv query results.

    Returns
    -------
    str
        SQL that returns the results of that query. The ``:id`` bind variable
        must be set to the query ID.
    """
    return "SELECT * FROM qserv_result(:id)"


class _QservClientProtocol(Protocol):
    """Protocol used by the retry decorator.

    This avoids a circular dependency between the definition of ``_retry`` and
    the definition of `QservClient`.
    """

    events: Events
    slack_client: SlackWebhookClient | None
    logger: BoundLogger


type _QservClientMethod[**P, T, C: _QservClientProtocol] = Callable[
    Concatenate[C, P], Coroutine[None, None, T]
]
"""The type of a method in the `QservClient` class."""


@overload
def _retry[**P, T, C: _QservClientProtocol](
    __func: _QservClientMethod[P, T, C], /
) -> _QservClientMethod[P, T, C]: ...


@overload
def _retry[**P, T, C: _QservClientProtocol](
    *,
    qserv: bool = True,
    qserv_protocol: QservProtocol = QservProtocol.HTTP,
) -> Callable[[_QservClientMethod[P, T, C]], _QservClientMethod[P, T, C]]: ...


def _retry[**P, T, C: _QservClientProtocol](
    __func: _QservClientMethod[P, T, C] | None = None,
    /,
    *,
    qserv: bool = True,
    qserv_protocol: QservProtocol = QservProtocol.HTTP,
) -> (
    _QservClientMethod
    | Callable[[_QservClientMethod[P, T, C]], _QservClientMethod[P, T, C]]
):
    """Retry a failed HTTP action.

    If the wrapped method fails with a transient error, retry it up to
    ``max_tries`` times. Any method with this decorator must be idempotent,
    since it may be re-run multiple times.

    Parameters
    ----------
    qserv
        Set to `False` if this call is not a call to Qserv and therefore
        should not generate metrics events.
    qserv_protocol
        Protocol of Qserv API that is being retried, for metrics purposes.
    """

    def retry_decorator(
        f: _QservClientMethod[P, T, C],
    ) -> _QservClientMethod[P, T, C]:
        @wraps(f)
        async def retry_wrapper(
            client: C, *args: P.args, **kwargs: P.kwargs
        ) -> T:
            for _ in range(1, config.backend_retry_count):
                try:
                    return await f(client, *args, **kwargs)
                except (QservApiSqlError, SlackWebException):
                    delay = config.backend_retry_delay.total_seconds()
                    msg = f"Qserv API call failed, retrying after {delay}s"

                    # We don't want to notify Sentry or Slack about exceptions
                    # here because we are going to retry.
                    client.logger.exception(msg)
                    event = QservFailureEvent(protocol=qserv_protocol)
                    await client.events.qserv_failure.publish(event)
                    await asyncio.sleep(delay)

            # Fell through so failed max_tries - 1 times. Try one last time,
            # re-raising the exception.
            try:
                return await f(client, *args, **kwargs)
            except (QservApiSqlError, SlackWebException):
                event = QservFailureEvent(protocol=qserv_protocol)
                await client.events.qserv_failure.publish(event)
                raise

        return retry_wrapper

    if __func is not None:
        return retry_decorator(__func)
    else:
        return retry_decorator


class QservClient(DatabaseBackend):
    """Client for the Qserv API.

    Only the routes and queries needed by the Qserv Kafka bridge are
    implemented.

    Parameters
    ----------
    sessionmaker
        Factory for database sessions.
    http_client
        HTTP client to use.
    events
        Metrics events publishers.
    slack_client
        Client to send errors to Slack
    logger
        Logger to use.

    Attributes
    ----------
    events
        Metrics events publishers. This is a public attribute so that it can
        be used by the retry decorator.
    slack_client
        Client to send errors to Slack. This is a public attribute so that it
        can be used by the retry decorator
    logger
        Logger to use. This is a public attribute so that it can be used by
        the retry decorator.
    """

    def __init__(
        self,
        *,
        sessionmaker: async_sessionmaker,
        http_client: AsyncClient,
        events: Events,
        slack_client: SlackWebhookClient | None,
        logger: BoundLogger,
    ) -> None:
        self.events = events
        self.slack_client = slack_client
        self.logger = logger

        self._sessionmaker = sessionmaker
        self._client = http_client

    @override
    async def cancel_query(self, query_id: str) -> None:
        await self._delete(f"/query-async/{query_id}")

    @override
    async def delete_result(self, query_id: str) -> None:
        """Delete the results of a query.

        Notes
        -----
        This should be called after the results have been successfully
        retrieved, although it is not a disaster if it's not called. The
        results will be automatically garbage-collected after some time.
        """
        await self._delete(f"/query-async/result/{query_id}")

    async def delete_table(self, database: str, table: str) -> None:
        await self._delete(f"/ingest/table/{database}/{table}")

    @override
    async def delete_database(self, database: str) -> None:
        """Delete a user database.

        Notes
        -----
        We delete the entire user database for each job rather than deleting
        individual tables because we've now moved to creating a new database
        for each new job. This is because a failed upload can currently leave
        Qserv in a state where the user can no longer upload tables to that
        database. Also, if a user attempts two simultaneous uploads, that
        could trigger a similar problem.

        With a short lived database for each upload, we can delete the entire
        temporary database oncd the job is completed and not have to worry
        about interactions with other queries.
        """
        await self._delete(f"/ingest/database/{database}")

    @override
    async def get_query_results_gen(
        self, query_id: str
    ) -> AsyncGenerator[Row[Any]]:
        """Get an async iterator for the results of a query.

        Notes
        -----
        Qserv discards the results after they're retrieved, so be aware that
        the results may not be available once this method has been called once
        for a given query.
        """
        stmt = text(_query_results_sql())
        results = None
        try:
            async with self._sessionmaker() as session:
                async with session.begin():
                    results = await session.stream(stmt, {"id": int(query_id)})
                    results = results.yield_per(100)
                    try:
                        async for result in results:
                            yield result
                    finally:
                        await results.close()
        except SQLAlchemyError as e:
            raise QservApiSqlError.from_exception(e) from e

    @override
    async def get_query_status(self, query_id: str) -> QservQueryStatus:
        url = f"/query-async/status/{query_id}"
        result = await self._get(url, {}, QservStatusResponse)
        return result.status.to_query_status()

    @override
    @_retry(qserv_protocol=QservProtocol.SQL)
    async def list_running_queries(self) -> dict[str, ProcessStatus]:
        try:
            async with self._sessionmaker() as session:
                async with session.begin():
                    result = await session.stream(text(_query_list_sql()))
                    processes = {}
                    try:
                        async for row in result:
                            msg = "Saw running query"
                            self.logger.debug(msg, query=row._asdict())
                            processes[str(row.id)] = ProcessStatus(
                                status=AsyncQueryPhase.EXECUTING,
                                progress=ChunkProgress(
                                    total_chunks=row.chunks,
                                    completed_chunks=row.chunks_comp,
                                ),
                                last_update=datetime_from_db(row.updated),
                            )
                    finally:
                        await result.close()
        except SQLAlchemyError as e:
            raise QservApiSqlError.from_exception(e) from e
        self.logger.debug("Listed running queries", count=len(processes))
        return processes

    @override
    async def submit_query(self, job: JobRun) -> str:
        request = AsyncSubmitRequest(query=job.query, database=job.database)
        result = await self._post("/query-async", request, AsyncSubmitResponse)
        return str(result.query_id)

    @override
    async def upload_table(self, upload: JobTableUpload) -> TableUploadStats:
        schema = await self._get_table(upload.schema_url)
        source = await self._get_table(upload.source_url)
        start = datetime.now(tz=UTC)
        await self._post_multipart(
            "/ingest/csv",
            data={
                "database": upload.database,
                "table": upload.table,
                "fields_terminated_by": ",",
                "charset_name": "utf8mb4",
                "collation_name": "utf8mb4_uca1400_ai_ci",
                "timeout": int(config.qserv_upload_timeout.total_seconds()),
            },
            files=(
                ("schema", ("schema.json", schema, "application/json")),
                ("rows", ("table.csv", source, "text/csv")),
            ),
            timeout=config.qserv_upload_timeout + timedelta(seconds=1),
        )
        return TableUploadStats(
            size=len(source), elapsed=datetime.now(tz=UTC) - start
        )

    @_retry
    async def _delete(self, route: str) -> None:
        """Send a DELETE request to the Qserv REST API.

        Parameters
        ----------
        route
            Route to which to send the request.

        Raises
        ------
        QservApiError
            Raised if something failed when issuing the DELETE request.
        """
        if config.qserv_rest_send_api_version:
            params = {"version": str(API_VERSION)}
        else:
            params = None
        url = str(config.qserv_rest_url).rstrip("/") + route
        start = datetime.now(tz=UTC)
        try:
            r = await self._client.delete(
                url, params=params, auth=config.rest_authentication
            )
            r.raise_for_status()
            self.logger.debug(
                "Qserv API reply",
                method="DELETE",
                url=url,
                result=r.json(),
                start=format_datetime_for_logging(start),
                elapsed=(datetime.now(tz=UTC) - start).total_seconds(),
            )
            self._parse_response("DELETE", url, r, BaseResponse)
        except HTTPError as e:
            raise QservApiWebError.from_exception(e) from e

    @_retry
    async def _get[T: BaseResponse](
        self, route: str, params: dict[str, str], result_type: type[T]
    ) -> T:
        """Send a GET request to the Qserv REST API.

        Parameters
        ----------
        route
            Route to which to send the request.
        params
            Query parameters to send.
        result_type
            Expected type of the response.

        Returns
        -------
        BaseResponse
            Parsed response from the GET request.

        Raises
        ------
        QservApiError
            Raised if something failed when issuing the GET request.
        """
        params_with_version = copy(params)
        if config.qserv_rest_send_api_version:
            params_with_version["version"] = str(API_VERSION)
        url = str(config.qserv_rest_url).rstrip("/") + route
        try:
            r = await self._client.get(
                url,
                params=params_with_version,
                auth=config.rest_authentication,
            )
            r.raise_for_status()
            self.logger.debug(
                "Qserv API reply", method="GET", url=url, result=r.json()
            )
            return self._parse_response("GET", url, r, result_type)
        except HTTPError as e:
            raise QservApiWebError.from_exception(e) from e

    @_retry(qserv=False)
    async def _get_table(self, url: str) -> bytes:
        """Retrieve user table upload data.

        Parameters
        ----------
        url
            Full URL to the data to retrieve.

        Returns
        -------
        bytes
            Contents of the file.

        Raises
        ------
        TableUploadWebError
            Raised if retrieving the file failed.
        """
        try:
            r = await self._client.get(url)
            r.raise_for_status()
        except HTTPError as e:
            raise TableUploadWebError.from_exception(e) from e
        else:
            return r.content

    def _parse_response[T: BaseResponse](
        self, method: str, url: str, response: Response, result_type: type[T]
    ) -> T:
        """Parse a response from a Qserv REST API endpoint.

        Parameters
        ----------
        method
            Method of the request.
        url
            URL of the request.
        response
            Raw response from the HTTP client.
        result_type
            Expected type of the response.

        Returns
        -------
        BaseResponse
            Parsed response.

        Raises
        ------
        QservApiError
            Raised if the response was an error or did not validate.
        """
        try:
            json_result = response.json()
            base_result = BaseResponse.model_validate(json_result)
            if not base_result.is_success():
                raise QservApiFailedError(method, url, base_result)
            return result_type.model_validate(json_result)
        except ValidationError as e:
            raise QservApiProtocolError(method, url, str(e)) from e

    @_retry
    async def _post[T: BaseResponse](
        self, route: str, body: BaseModel, result_type: type[T]
    ) -> T:
        """Send a POST request to the Qserv REST API.

        Parameters
        ----------
        route
            Route to which to send the request.
        body
            Body of the request.
        result_type
            Expected type of the response.

        Returns
        -------
        BaseResponse
            Parsed response from the POST request.

        Raises
        ------
        QservApiError
            Raised if something failed when submitting the POST request.
        """
        params = {}
        if config.qserv_rest_send_api_version:
            params["version"] = str(API_VERSION)
        body_dict = body.model_dump(mode="json", exclude_none=True)
        url = str(config.qserv_rest_url).rstrip("/") + route
        start = datetime.now(tz=UTC)
        try:
            r = await self._client.post(
                url,
                params=params,
                json=body_dict,
                auth=config.rest_authentication,
            )
            r.raise_for_status()
            self.logger.debug(
                "Qserv API reply",
                method="POST",
                url=url,
                result=r.json(),
                start=format_datetime_for_logging(start),
                elapsed=(datetime.now(tz=UTC) - start).total_seconds(),
            )
            return self._parse_response("POST", url, r, result_type)
        except HTTPError as e:
            raise QservApiWebError.from_exception(e) from e

    @_retry
    async def _post_multipart(
        self,
        route: str,
        *,
        data: Mapping[str, str | int],
        files: Sequence[tuple[str, tuple[str, bytes, str]]],
        timeout: timedelta,
    ) -> None:
        """Send a POST request to the Qserv REST API.

        Parameters
        ----------
        route
            Route to which to send the request.
        data
            Key/value pairs to send.
        files
            Files to upload.
        timeout
            Timeout for the request.

        Raises
        ------
        QservApiError
            Raised if something failed when submitting the POST request.
        """
        params = {}
        if config.qserv_rest_send_api_version:
            params["version"] = str(API_VERSION)
        url = str(config.qserv_rest_url).rstrip("/") + route
        try:
            r = await self._client.post(
                url,
                params=params,
                data=data,
                files=files,
                timeout=timeout.total_seconds(),
                auth=config.rest_authentication,
            )
            r.raise_for_status()
            self.logger.debug(
                "Qserv API reply", method="POST", url=url, result=r.json()
            )
            self._parse_response("POST", url, r, BaseResponse)
        except HTTPError as e:
            raise QservApiWebError.from_exception(e) from e
