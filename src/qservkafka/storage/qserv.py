"""Client for the Qserv REST API."""

from __future__ import annotations

from collections.abc import AsyncGenerator
from copy import copy
from typing import Any

from httpx import AsyncClient, HTTPError, Response
from pydantic import BaseModel, ValidationError
from safir.database import datetime_from_db
from sqlalchemy import Row, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import async_scoped_session
from structlog.stdlib import BoundLogger

from ..config import config
from ..exceptions import (
    QservApiFailedError,
    QservApiProtocolError,
    QservApiSqlError,
    QservApiWebError,
    TableUploadWebError,
)
from ..models.kafka import JobRun, JobTableUpload
from ..models.qserv import (
    AsyncQueryPhase,
    AsyncQueryStatus,
    AsyncStatusResponse,
    AsyncSubmitRequest,
    AsyncSubmitResponse,
    BaseResponse,
)

API_VERSION = 39
"""Version of the REST API that this client requests."""

_QUERY_LIST_SQL = """
    SELECT
      ID AS id,
      SUBMITTED AS submitted,
      UPDATED AS updated,
      CHUNKS AS chunks,
      CHUNKS_COMP as chunks_comp
    FROM information_schema.processlist
"""
"""SQL query to get a list of running queries.

This is overridden by the test suite since it queries an internal MySQL
namespace when talking to actual Qserv that's difficult to mock.
"""

_QUERY_RESULTS_SQL_FORMAT = "SELECT * FROM qserv_result({})"
"""Format used to generate the SQL query to get Qserv query results.

This format takes one parameter, the Qserv query ID.
"""

__all__ = ["API_VERSION", "QservClient"]


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
    logger
        Logger to use.
    """

    def __init__(
        self,
        session: async_scoped_session,
        http_client: AsyncClient,
        logger: BoundLogger,
    ) -> None:
        self._session = session
        self._client = http_client
        self._logger = logger

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
        stmt = text(_QUERY_RESULTS_SQL_FORMAT.format(query_id))
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

    async def list_running_queries(self) -> dict[int, AsyncQueryStatus]:
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
                result = await self._session.stream(text(_QUERY_LIST_SQL))
                processes = {}
                try:
                    async for row in result:
                        msg = "Saw running query"
                        self._logger.debug(msg, query=row._asdict())
                        processes[row.id] = AsyncQueryStatus(
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
        self._logger.debug("Listed running queries", count=len(processes))
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

    async def upload_table(self, upload: JobTableUpload) -> int:
        """Upload a table to Qserv.

        Parameters
        ----------
        upload
            Table to upload.

        Returns
        -------
        int
            Size of the uploaded CSV data for the table in bytes.

        Raises
        ------
        QservApiError
            Raised if something failed when uploading the table.
        TableUploadWebError
            Raised if retrieving the uploaded table or schema failed.
        """
        try:
            r = await self._client.get(upload.schema_url)
            r.raise_for_status()
            schema = r.text
            r = await self._client.get(upload.source_url)
            r.raise_for_status()
            source = r.content
        except HTTPError as e:
            raise TableUploadWebError.from_exception(e) from e
        url = str(config.qserv_rest_url).rstrip("/") + "/ingest/csv"
        timeout = int(config.qserv_upload_timeout.total_seconds())
        try:
            r = await self._client.post(
                url,
                data={
                    "database": upload.database,
                    "table": upload.table,
                    "fields_terminated_by": ",",
                    "charset_name": "utf8",
                    "timeout": timeout,
                    "version": API_VERSION,
                },
                files=(
                    ("schema", ("schema.json", schema, "application/json")),
                    ("rows", ("table.csv", source, "text/csv")),
                ),
                timeout=timeout + 1,
            )
            self._logger.debug(
                "Qserv API reply", method="POST", url=url, result=r.json()
            )
            self._parse_response(url, r, BaseResponse)
        except HTTPError as e:
            raise QservApiWebError.from_exception(e) from e
        return len(source)

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
        params = {"version": str(API_VERSION)}
        url = str(config.qserv_rest_url).rstrip("/") + route
        try:
            r = await self._client.delete(url, params=params)
            r.raise_for_status()
            self._logger.debug(
                "Qserv API reply", method="DELETE", url=url, result=r.json()
            )
            self._parse_response(url, r, BaseResponse)
        except HTTPError as e:
            raise QservApiWebError.from_exception(e) from e

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
        params_with_version["version"] = str(API_VERSION)
        url = str(config.qserv_rest_url).rstrip("/") + route
        try:
            r = await self._client.get(url, params=params_with_version)
            r.raise_for_status()
            self._logger.debug(
                "Qserv API reply", method="GET", url=url, result=r.json()
            )
            return self._parse_response(url, r, result_type)
        except HTTPError as e:
            raise QservApiWebError.from_exception(e) from e

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
        body_dict = body.model_dump(mode="json", exclude_none=True)
        body_dict["version"] = API_VERSION
        url = str(config.qserv_rest_url).rstrip("/") + route
        try:
            r = await self._client.post(url, json=body_dict)
            r.raise_for_status()
            self._logger.debug(
                "Qserv API reply", method="POST", url=url, result=r.json()
            )
            return self._parse_response(url, r, result_type)
        except HTTPError as e:
            raise QservApiWebError.from_exception(e) from e
