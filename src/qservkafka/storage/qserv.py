"""Client for the Qserv REST API."""

import asyncio
from collections.abc import (
    AsyncGenerator,
    AsyncIterator,
    Callable,
    Coroutine,
    Mapping,
    Sequence,
)
from copy import copy
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from functools import wraps
from typing import Any, Concatenate, Protocol, overload, override

from aiohttp import (
    ClientError,
    ClientSession,
    ClientTimeout,
    MultipartWriter,
    encode_basic_auth,
)
from httpx import AsyncClient, HTTPError, Response
from pydantic import BaseModel, ValidationError
from safir.database import datetime_from_db
from safir.slack.blockkit import SlackWebException
from safir.slack.webhook import SlackWebhookClient
from sqlalchemy import Row, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from structlog.stdlib import BoundLogger

from ..config import config
from ..events import (
    Events,
    QservApiFailureEvent,
    QservProtocol,
    QueryApiFailureEvent,
)
from ..exceptions import (
    QservApiError,
    QservApiFailedError,
    QservApiProtocolError,
    QservApiSqlError,
    QservApiUploadWebError,
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

API_VERSION = 60
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


@dataclass
class _UploadFile:
    """Specification for a file in an upload request."""

    field: str
    """Name of the field in the form data."""

    data: bytes | AsyncIterator[bytes]
    """Bytes to upload, possibly as an iterator."""

    mime_type: str
    """MIME type of the file."""

    filename: str
    """Filename of the file."""


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
                except QservApiSqlError, SlackWebException:
                    delay = config.backend_retry_delay.total_seconds()
                    msg = f"Qserv API call failed, retrying after {delay}s"

                    # We don't want to notify Sentry or Slack about exceptions
                    # here because we are going to retry.
                    client.logger.exception(msg)
                    event = QservApiFailureEvent(protocol=qserv_protocol)
                    await client.events.query_api_failure.publish(event)
                    await asyncio.sleep(delay)

            # Fell through so failed max_tries - 1 times. Try one last time,
            # re-raising the exception.
            try:
                return await f(client, *args, **kwargs)
            except QservApiSqlError, SlackWebException:
                event = QservApiFailureEvent(protocol=qserv_protocol)
                await client.events.query_api_failure.publish(event)
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
    upload_http_client
        HTTP client to use for uploads.
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
        upload_http_client: ClientSession,
        events: Events,
        slack_client: SlackWebhookClient | None,
        logger: BoundLogger,
    ) -> None:
        self.events = events
        self.slack_client = slack_client
        self.logger = logger

        self._sessionmaker = sessionmaker
        self._client = http_client
        self._upload_client = upload_http_client

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

    @override
    async def delete_database(self, database: str) -> None:
        """Delete a user database.

        Parameters
        ----------
        database
            Name of the database to delete.

        Notes
        -----
        We delete the entire user database for each job rather than deleting
        individual tables because we create a new database for each new job.
        This is done because a failed upload or two simultaneous uploads can
        currently leave Qserv in a state where the user can no longer upload
        tables to that database.

        With a short lived database for each upload, we can delete the entire
        temporary database oncd the job is completed and not have to worry
        about interactions with other queries.
        """
        await self._delete(
            f"/ingest/database/{database}",
            timeout=config.qserv_upload_delete_timeout,
        )

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
                    processes = await self._list_processes(session)
        except SQLAlchemyError as e:
            raise QservApiSqlError.from_exception(e) from e
        self.logger.debug("Listed running queries", count=len(processes))
        return processes

    @override
    def result_api_failure_event(self) -> QueryApiFailureEvent:
        return QservApiFailureEvent(protocol=QservProtocol.SQL)

    @override
    async def submit_query(self, job: JobRun) -> str:
        request = AsyncSubmitRequest(query=job.query, database=job.database)
        result = await self._post("/query-async", request, AsyncSubmitResponse)
        return str(result.query_id)

    @override
    @_retry
    async def upload_table(self, upload: JobTableUpload) -> TableUploadStats:
        start = datetime.now(tz=UTC)
        data = {
            "database": upload.database,
            "table": upload.table,
            "fields_terminated_by": ",",
            "charset_name": "utf8mb4",
            "collation_name": "utf8mb4_uca1400_ai_ci",
            "timeout": str(int(config.qserv_upload_timeout.total_seconds())),
        }
        data.update(upload.to_ingest_fields())

        # Construct the table upload request.
        try:
            async with (
                self._client.stream("GET", upload.schema_url) as schema,
                self._client.stream("GET", upload.source_url) as source,
            ):
                schema.raise_for_status()
                source.raise_for_status()
                try:
                    size = int(source.headers["Content-Length"])
                except KeyError, ValueError:
                    size = None

                # Perform the upload.
                await self._upload(
                    "/ingest/csv",
                    data=data,
                    files=[
                        _UploadFile(
                            field="schema",
                            data=schema.aiter_bytes(),
                            mime_type="application/json",
                            filename="schema.json",
                        ),
                        _UploadFile(
                            field="rows",
                            data=source.aiter_bytes(),
                            mime_type="text/csv",
                            filename="table.csv",
                        ),
                    ],
                    timeout=config.qserv_upload_timeout + timedelta(seconds=1),
                )
        except HTTPError as e:
            try:
                await self._delete_table(upload.database, upload.table)
            except QservApiError:
                self.logger.exception(
                    "Cannot delete failed table upload",
                    database=upload.database,
                    table=upload.table,
                )
            raise TableUploadWebError.from_exception(e) from e

        # Return the statistics.
        elapsed = datetime.now(tz=UTC) - start
        return TableUploadStats(size=size, elapsed=elapsed)

    @_retry
    async def _delete(
        self, route: str, *, timeout: timedelta | None = None
    ) -> None:
        """Send a DELETE request to the Qserv REST API.

        Parameters
        ----------
        route
            Route to which to send the request.
        timeout
            Timeout for the request.

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
        if not timeout:
            timeout = config.backend_api_timeout
        logger = self.logger.bind(method="DELETE", url=url)

        start = datetime.now(tz=UTC)
        try:
            r = await self._client.delete(
                url,
                params=params,
                auth=config.rest_authentication,
                timeout=timeout.total_seconds(),
            )
            if r.status_code == 404:
                logger.info("Ignoring 404 from DELETE", result=r.json())
                return
            r.raise_for_status()
            elapsed = round((datetime.now(tz=UTC) - start).total_seconds(), 2)
            logger.debug("Qserv API reply", result=r.json(), elapsed=elapsed)
            self._parse_response("DELETE", url, r, BaseResponse)
        except HTTPError as e:
            raise QservApiWebError.from_exception(e) from e

    async def _delete_table(self, database: str, table: str) -> None:
        """Delete a user table.

        Parameters
        ----------
        database
            Name of the database.
        table
            Name of the table to delete.
        """
        await self._delete(f"/ingest/table/{database}/{table}")

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
        logger = self.logger.bind(method="GET", url=url)

        try:
            r = await self._client.get(
                url,
                params=params_with_version,
                auth=config.rest_authentication,
            )
            r.raise_for_status()
            logger.debug("Qserv API reply", result=r.json())
            return self._parse_response("GET", url, r, result_type)
        except HTTPError as e:
            raise QservApiWebError.from_exception(e) from e

    async def _list_processes(
        self, session: AsyncSession
    ) -> dict[str, ProcessStatus]:
        """Get process status of running Qserv queries.

        Parameters
        ----------
        session
            Database session with an open transaction.

        Returns
        -------
        dict of ProcessStatus
            Qserv process information for running queries.
        """
        result = await session.stream(text(_query_list_sql()))
        processes = {}
        try:
            async for row in result:
                self.logger.debug("Saw running query", query=row._asdict())
                processes[str(row.id)] = ProcessStatus(
                    status=AsyncQueryPhase.EXECUTING,
                    progress=ChunkProgress(
                        total_chunks=row.chunks or 0,
                        completed_chunks=row.chunks_comp or 0,
                    ),
                    last_update=datetime_from_db(row.updated),
                )
        finally:
            await result.close()
        return processes

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
        logger = self.logger.bind(method="POST", url=url)

        start = datetime.now(tz=UTC)
        try:
            r = await self._client.post(
                url,
                params=params,
                json=body_dict,
                auth=config.rest_authentication,
            )
            r.raise_for_status()
            elapsed = round((datetime.now(tz=UTC) - start).total_seconds(), 2)
            logger.debug("Qserv API reply", result=r.json(), elapsed=elapsed)
            return self._parse_response("POST", url, r, result_type)
        except HTTPError as e:
            raise QservApiWebError.from_exception(e) from e

    async def _upload(
        self,
        route: str,
        *,
        data: Mapping[str, str],
        files: Sequence[_UploadFile],
        timeout: timedelta,
    ) -> None:
        """Send a multipart file upload request to Qserv.

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
        start = datetime.now(tz=UTC)
        params = {}
        if config.qserv_rest_send_api_version:
            params["version"] = str(API_VERSION)
        url = str(config.qserv_rest_url).rstrip("/") + route
        logger = self.logger.bind(method="POST", url=url)
        client_timeout = ClientTimeout(total=timeout.total_seconds())

        # Construct the authentication headers.
        headers = None
        if config.qserv_rest_username and config.qserv_rest_password:
            headers = {
                "Authorization": encode_basic_auth(
                    config.qserv_rest_username,
                    config.qserv_rest_password.get_secret_value(),
                )
            }

        # Construct the POST body.
        with MultipartWriter("form-data") as mpwriter:
            for key, value in data.items():
                part = mpwriter.append(value)
                part.set_content_disposition("form-data", name=key)
            for upload in files:
                upload_headers = {"Content-Type": upload.mime_type}
                filename = upload.filename
                part = mpwriter.append(upload.data, upload_headers)
                part.set_content_disposition(
                    "form-data", name=upload.field, filename=filename
                )

            # Make the request.
            try:
                async with self._upload_client.post(
                    url,
                    params=params,
                    data=mpwriter,
                    timeout=client_timeout,
                    headers=headers,
                    allow_redirects=False,
                ) as r:
                    r.raise_for_status()
                    json_result = await r.json()
                    base_result = BaseResponse.model_validate(json_result)
                    if not base_result.is_success():
                        raise QservApiFailedError("POST", url, base_result)
            except ClientError as e:
                raise QservApiUploadWebError.from_aiohttp_exception(e) from e
            except ValidationError as e:
                raise QservApiProtocolError("POST", url, str(e)) from e

        # Upload succeeded. Log the results.
        elapsed = round((datetime.now(tz=UTC) - start).total_seconds(), 2)
        logger.debug("Qserv API reply", result=json_result, elapsed=elapsed)
