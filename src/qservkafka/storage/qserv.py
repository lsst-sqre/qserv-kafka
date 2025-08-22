"""Client for the Qserv REST API."""

from __future__ import annotations

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
from typing import Any, Concatenate, overload

from httpx import AsyncClient, HTTPError, Response
from pydantic import BaseModel, ValidationError
from safir.database import datetime_from_db
from safir.slack.blockkit import SlackWebException
from sqlalchemy import Row, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import async_scoped_session
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
from ..models.qserv import (
    AsyncProcessStatus,
    AsyncQueryPhase,
    AsyncQueryStatus,
    AsyncStatusResponse,
    AsyncSubmitRequest,
    AsyncSubmitResponse,
    BaseResponse,
    TableUploadStats,
)

API_VERSION = 47
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


def _query_results_sql(query_id: int) -> str:
    """Generate the SQL query to get Qserv query results.

    Parameters
    ----------
    query_id
        Qserv query ID.

    Returns
    -------
    str
        SQL that returns the results of that query.
    """
    if not isinstance(query_id, int):
        raise TypeError(f'query_id "{query_id}" is not int')
    return f"SELECT * FROM qserv_result({query_id!s})"  # noqa: S608


@overload
def _retry[**P, T](
    __func: Callable[Concatenate[QservClient, P], Coroutine[None, None, T]], /
) -> Callable[Concatenate[QservClient, P], Coroutine[None, None, T]]: ...


@overload
def _retry[**P, T](
    *,
    qserv: bool = True,
    qserv_protocol: QservProtocol = QservProtocol.HTTP,
) -> Callable[
    [Callable[Concatenate[QservClient, P], Coroutine[None, None, T]]],
    Callable[Concatenate[QservClient, P], Coroutine[None, None, T]],
]: ...


def _retry[**P, T](
    __func: (
        Callable[Concatenate[QservClient, P], Coroutine[None, None, T]] | None
    ) = None,
    /,
    *,
    qserv: bool = True,
    qserv_protocol: QservProtocol = QservProtocol.HTTP,
) -> (
    Callable[Concatenate[QservClient, P], Coroutine[None, None, T]]
    | Callable[
        [Callable[Concatenate[QservClient, P], Coroutine[None, None, T]]],
        Callable[Concatenate[QservClient, P], Coroutine[None, None, T]],
    ]
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
        f: Callable[Concatenate[QservClient, P], Coroutine[None, None, T]],
    ) -> Callable[Concatenate[QservClient, P], Coroutine[None, None, T]]:
        @wraps(f)
        async def retry_wrapper(
            client: QservClient, *args: P.args, **kwargs: P.kwargs
        ) -> T:
            for _ in range(1, config.qserv_retry_count):
                try:
                    return await f(client, *args, **kwargs)
                except (QservApiSqlError, SlackWebException):
                    delay = config.qserv_retry_delay.total_seconds()
                    msg = f"Qserv API call failed, retrying after {delay}s"
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


class QservClient:
    """Client for the Qserv API.

    Only the routes and queries needed by the Qserv Kafka bridge are
    implemented.

    Parameters
    ----------
    session
        Database session.
    http_client
        HTTP client to use.
    events
        Metrics events publishers.
    logger
        Logger to use.

    Attributes
    ----------
    events
        Metrics events publishers. This is a public attribute so that it can
        be used by the retry decorator.
    logger
        Logger to use. This is a public attribute so that it can be used by
        the retry decorator.
    """

    def __init__(
        self,
        *,
        session: async_scoped_session,
        http_client: AsyncClient,
        events: Events,
        logger: BoundLogger,
    ) -> None:
        self.events = events
        self.logger = logger

        self._session = session
        self._client = http_client

    async def cancel_query(self, query_id: int) -> None:
        """Cancel a running query.

        Parameters
        ----------
        query_id
            Identifier of the query.

        Raises
        ------
        QservApiError
            Raised if there was some error canceling the query.
        """
        await self._delete(f"/query-async/{query_id}")

    async def delete_result(self, query_id: int) -> None:
        """Delete the results of a query.

        This should be called after the results have been successfully
        retrieved, although it is not a disaster if it's not called. The
        results will be automatically garbage-collected after some time.

        Parameters
        ----------
        query_id
            Identifier of the query.

        Raises
        ------
        QservApiError
            Raised if there was some error deleting the results.
        """
        await self._delete(f"/query-async/result/{query_id}")

    async def delete_table(self, database: str, table: str) -> None:
        """Delete a user table.

        Parameters
        ----------
        database
            Name of the database (normally :samp:`user_{username}`).
        table
            Name of the table.

        Raises
        ------
        QservApiError
            Raised if there was some error deleting the table.
        """
        await self._delete(f"/ingest/table/{database}/{table}")

    async def delete_database(self, database: str) -> None:
        """Delete a user database.

        Parameters
        ----------
        database
            Name of the database.

        Notes
        -----
        We delete the entire user database for each job rather than deleting
        individual tables because we've now moved to creating a new database
        for each new job.
        The reason for this change is that in the current implementation of
        QServ a failed upload can lead to a state where the user can no
        longer upload tables to that database.
        Also if a user attempts two simultaneous uploads, this could also
        trigger a similar problem leaving the database in a problematic state.
        So with a short lived database for each upload, it is now simpler
        to just delete the whole temporary database once the job is
        completed.


        Raises
        ------
        QservApiError
            Raised if there was some error deleting the database.
        """
        await self._delete(f"/ingest/database/{database}")

    async def get_query_results_gen(
        self, query_id: int
    ) -> AsyncGenerator[Row[Any]]:
        """Get an async iterator for the results of a query.

        Qserv discards the results after they're retrieved, so be aware that
        the results may not be available once this method has been called once
        for a given query.

        Parameters
        ----------
        query_id
            Identifier of the query.

        Returns
        -------
        collections.abc.AsyncIterator
            Iterator over the rows of the query results.

        Raises
        ------
        QservApiSqlError
            Raised if there was some error retrieving results.
        """
        stmt = text(_query_results_sql(query_id))
        results = None
        try:
            async with self._session.begin():
                results = await self._session.stream(stmt)
                results = results.yield_per(100)
                try:
                    async for result in results:
                        yield result
                finally:
                    await results.close()
        except SQLAlchemyError as e:
            raise QservApiSqlError.from_exception(e) from e

    async def get_query_status(self, query_id: int) -> AsyncQueryStatus:
        """Query for the status of an async job.

        Parameters
        ----------
        query_id
            Identifier of the query.

        Returns
        -------
        AsyncStatusResponse
            Status of the query.
        """
        url = f"/query-async/status/{query_id}"
        result = await self._get(url, {}, AsyncStatusResponse)
        return result.status

    @_retry(qserv_protocol=QservProtocol.SQL)
    async def list_running_queries(self) -> dict[int, AsyncProcessStatus]:
        """Return information about all running queries.

        Returns
        -------
        dict of AsyncQueryStatus
            Mapping from query ID to information about a running query.

        Raises
        ------
        QservApiSqlError
            Raised if there was some error retrieving status.
        """
        try:
            async with self._session.begin():
                result = await self._session.stream(text(_query_list_sql()))
                processes = {}
                try:
                    async for row in result:
                        msg = "Saw running query"
                        self.logger.debug(msg, query=row._asdict())
                        processes[row.id] = AsyncProcessStatus(
                            query_id=row.id,
                            status=AsyncQueryPhase.EXECUTING,
                            total_chunks=row.chunks,
                            completed_chunks=row.chunks_comp,
                            query_begin=datetime_from_db(row.submitted),
                            last_update=datetime_from_db(row.updated),
                        )
                finally:
                    await result.close()
        except SQLAlchemyError as e:
            raise QservApiSqlError.from_exception(e) from e
        self.logger.debug("Listed running queries", count=len(processes))
        return processes

    async def submit_query(self, job: JobRun) -> int:
        """Submit an async query to Qserv.

        Parameters
        ----------
        job
            Query job run request from the user via Kafka.

        Returns
        -------
        int
            Qserv identifier of the query.

        Raises
        ------
        QservApiError
            Raised if something failed when attempting to submit the job.
        """
        request = AsyncSubmitRequest(query=job.query, database=job.database)
        result = await self._post("/query-async", request, AsyncSubmitResponse)
        return result.query_id

    async def upload_table(self, upload: JobTableUpload) -> TableUploadStats:
        """Upload a table to Qserv.

        Parameters
        ----------
        upload
            Table to upload.

        Returns
        -------
        TableUploadStats
            Statistics about the uploaded table.

        Raises
        ------
        QservApiError
            Raised if something failed when uploading the table.
        TableUploadWebError
            Raised if retrieving the uploaded table or schema failed.
        """
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
                "collation_name": "utf8mb4_unicode_520_ci",
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
        try:
            r = await self._client.delete(
                url, params=params, auth=config.rest_authentication
            )
            r.raise_for_status()
            self.logger.debug(
                "Qserv API reply", method="DELETE", url=url, result=r.json()
            )
            self._parse_response(url, r, BaseResponse)
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
            return self._parse_response(url, r, result_type)
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
        self, url: str, response: Response, result_type: type[T]
    ) -> T:
        """Parse a response from a Qserv REST API endpoint.

        Parameters
        ----------
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
                raise QservApiFailedError(url, base_result)
            return result_type.model_validate(json_result)
        except ValidationError as e:
            raise QservApiProtocolError(url, str(e)) from e

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
        try:
            r = await self._client.post(
                url,
                params=params,
                json=body_dict,
                auth=config.rest_authentication,
            )
            r.raise_for_status()
            self.logger.debug(
                "Qserv API reply", method="POST", url=url, result=r.json()
            )
            return self._parse_response(url, r, result_type)
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
            self._parse_response(url, r, BaseResponse)
        except HTTPError as e:
            raise QservApiWebError.from_exception(e) from e
