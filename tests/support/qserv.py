"""Mocks for testing code that talks to Qserv."""

from __future__ import annotations

import asyncio
import json
import re
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from io import BytesIO
from unittest.mock import patch
from urllib.parse import parse_qs, urlparse

import respx
from httpx import Request, Response
from multipart import MultipartParser, parse_options_header
from safir.database import (
    create_async_session,
    datetime_to_db,
    initialize_database,
)
from safir.datetime import current_datetime
from sqlalchemy import BigInteger, Double, String, delete, select
from sqlalchemy.dialects.mysql import DATETIME
from sqlalchemy.ext.asyncio import AsyncEngine, async_scoped_session
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from structlog import get_logger
from structlog.stdlib import BoundLogger

from qservkafka.config import config
from qservkafka.models.kafka import JobRun
from qservkafka.models.qserv import (
    AsyncQueryPhase,
    AsyncQueryStatus,
    AsyncStatusResponse,
    AsyncSubmitRequest,
    BaseResponse,
)
from qservkafka.storage import qserv
from qservkafka.storage.qserv import API_VERSION

from .data import read_test_data, read_test_job_run, read_test_json

__all__ = ["MockQserv", "register_mock_qserv"]

_QUERY_LIST_SQL = """
    SELECT
      id,
      submitted,
      updated,
      chunks,
      chunks_comp
    FROM processlist
"""
"""SQL query to get a list of running queries."""

_QUERY_RESULT_SQL = "SELECT * FROM results"
"""SQL query to get results."""


class _SchemaBase(DeclarativeBase):
    """Declarative base for the test MySQL schema."""


class _Process(_SchemaBase):
    """Simulation of the process list table."""

    __tablename__ = "processlist"

    id: Mapped[int] = mapped_column(primary_key=True)
    submitted: Mapped[datetime]
    updated: Mapped[datetime]
    chunks: Mapped[int]
    chunks_comp: Mapped[int]


class _Result(_SchemaBase):
    """Simulation of the results table."""

    __tablename__ = "results"

    id: Mapped[int] = mapped_column(primary_key=True)
    a: Mapped[bool | None]
    b: Mapped[str | None] = mapped_column(String(1))
    c: Mapped[str | None] = mapped_column(String(10))
    d: Mapped[str | None] = mapped_column(String(256))
    e: Mapped[float | None] = mapped_column(Double)
    f: Mapped[float | None]
    g: Mapped[int | None]
    h: Mapped[int | None] = mapped_column(BigInteger)
    i: Mapped[str | None] = mapped_column(String(256))
    j: Mapped[datetime | None] = mapped_column(DATETIME(fsp=6))
    k: Mapped[int | None]


class MockQserv:
    """Mock Qserv that simulates the REST API."""

    _UPLOAD_CSV = "one\ntwo\n"
    """Static table data to return for user table upload."""

    _UPLOAD_SCHEMA = '[{"name":"col_0","type":"VARCHAR(32)"}]'
    """Static schema to return for user table upload."""

    def __init__(
        self, session: async_scoped_session, respx_mock: respx.Router
    ) -> None:
        self._session = session
        self._respx_mock = respx_mock

        self._expected_job: JobRun | None
        self._immediate_success: JobRun | None
        self._next_query_id: int
        self._override_status: Response | None
        self._override_submit: Response | None
        self._queries: dict[int, AsyncQueryStatus]
        self._results_stored: bool
        self._upload_delay: timedelta | None
        self._uploaded_table: str | None
        self.reset()

    @classmethod
    async def initialize(
        cls, engine: AsyncEngine, logger: BoundLogger
    ) -> None:
        """Initialize the MySQL database."""
        await initialize_database(
            engine, logger, schema=_SchemaBase.metadata, reset=True
        )

    def get_status(self, query_id: int) -> AsyncQueryStatus:
        """Return the current stored status.

        This is used by tests that need to poke at the mock directly.

        Parameters
        ----------
        query_id
            Query ID.

        Returns
        -------
        AsyncQueryStatus
            Current stored status for that query ID.
        """
        return self._queries[query_id]

    def get_uploaded_table(self) -> str | None:
        """Get the name of the uploaded table, if any.

        Returns
        -------
        str or None
            The name of any uploaded table, or `None` if no table has been
            uploaded.
        """
        return self._uploaded_table

    def reset(self) -> None:
        """Reset the mock to its initial state."""
        self._expected_job = None
        self._immediate_success = None
        self._next_query_id = 1
        self._override_status = None
        self._override_submit = None
        self._queries = {}
        self._results_stored = False
        self._upload_delay = None
        self._uploaded_table = None

    def set_immediate_success(self, job: JobRun | None) -> None:
        """Configure whether to mark the job completed immediately.

        Parameters
        ----------
        job
            Job for which to mock the upload URL, or `None` to restore normal
            behavior.
        """
        self._immediate_success = job

    def set_status_response(self, response: Response | None) -> None:
        """Override the normal status reponse handling.

        Parameters
        ----------
        response
            Response to return for any request, or `None` to return to normal
            behavior.
        """
        self._override_status = response

    def set_submit_response(self, response: Response | None) -> None:
        """Override the normal submit reponse handling.

        Parameters
        ----------
        response
            Response to return for any request, or `None` to return to normal
            behavior.
        """
        self._override_submit = response

    def set_upload_delay(self, delay: timedelta | None) -> None:
        """Set the delay before the upload handler returns.

        Parameters
        ----------
        delay
            Delay, or `None` to return to default behavior.
        """
        self._upload_delay = delay

    def cancel(self, request: Request, *, query_id: str) -> Response:
        """Cancel a running job.

        Parameters
        ----------
        request
            Incoming request.
        query_id
            Query ID (as a string) from the request URL.

        Returns
        -------
        httpx.Response
            Returns 200 with the results of canceling the query.
        """
        self._check_version(request)
        status = self._queries.get(int(query_id))
        if not status:
            return Response(
                200,
                json={"success": 0, "error": f"Query {query_id} not found"},
                request=request,
            )
        if status.status != AsyncQueryPhase.EXECUTING:
            return Response(
                200,
                json={"success": 0, "error": f"Query {query_id} completed"},
                request=request,
            )
        status.status = AsyncQueryPhase.ABORTED
        status.last_update = datetime.now(tz=UTC)
        return Response(200, json={"success": 1}, request=request)

    async def delete_results(
        self, request: Request, query_id: str
    ) -> Response:
        """Delete the stored results from the database.

        Parameters
        ----------
        request
            Incoming request.

        Returns
        -------
        httpx.Response
            Returns 200 with the static schema string.
        """
        self._check_version(request)
        assert self._results_stored
        async with self._session.begin():
            await self._session.execute(delete(_Result))
        self._results_stored = False
        return Response(200, json={"success": 1}, request=request)

    def get_upload_schema(self, request: Request) -> Response:
        """Return the stored schema for table upload.

        Parameters
        ----------
        request
            Incoming request.

        Returns
        -------
        httpx.Response
            Returns 200 with the static schema string.
        """
        return Response(200, content=self._UPLOAD_SCHEMA.encode())

    def get_upload_source(self, request: Request) -> Response:
        """Return the stored data for table upload.

        Parameters
        ----------
        request
            Incoming request.

        Returns
        -------
        httpx.Response
            Returns 200 with the static data string.
        """
        return Response(200, content=self._UPLOAD_CSV.encode())

    def status(self, request: Request, *, query_id: str) -> Response:
        """Mock a request for job status.

        Parameters
        ----------
        request
            Incoming request.
        query_id
            Query ID (as a string) from the request URL.

        Returns
        -------
        httpx.Response
            Returns 200 with the details of the query.
        """
        self._check_version(request)
        if self._override_status:
            return self._override_status
        status = self._queries.get(int(query_id))
        if not status:
            return Response(
                200,
                json={"success": 0, "error": f"Query {query_id} not found"},
                request=request,
            )
        result = AsyncStatusResponse(success=1, status=status)
        return Response(
            200,
            json=result.model_dump(mode="json", exclude_none=True),
            request=request,
        )

    async def store_results(self, job: JobRun) -> None:
        """Store mock results in the database and mock the upload.

        After this is called, an attempt to retrieve results and upload them
        should work and the uploaded VOTable will be checked against the
        properties of the job. Any calls to this method after the first will
        not repeat the MySQL work, but will change the mock and expected job.

        Parameters
        ----------
        job
            Query request.
        """
        url = str(job.result_url)
        self._respx_mock.put(url).mock(side_effect=self.upload)
        assert not self._results_stored
        data = read_test_json("results/data")
        async with self._session.begin():
            for row in data:
                if row["j"] is not None:
                    row["j"] = datetime.fromisoformat(row["j"] + "Z")
                result = _Result(**row)
                self._session.add(result)
        self._results_stored = True
        self._expected_job = job

    async def submit(self, request: Request) -> Response:
        """Mock a request to submit an async job.

        Parameters
        ----------
        request
            Incoming request.

        Returns
        -------
        httpx.Response
            Returns 200 with the details of the query.
        """
        body_raw = json.loads(request.content.decode())
        assert body_raw["version"] == API_VERSION
        AsyncSubmitRequest.model_validate(body_raw)
        if self._override_submit:
            return self._override_submit
        query_id = self._next_query_id
        self._next_query_id += 1
        now = current_datetime()
        if self._immediate_success:
            self._queries[query_id] = AsyncQueryStatus(
                query_id=query_id,
                status=AsyncQueryPhase.COMPLETED,
                total_chunks=10,
                completed_chunks=10,
                collected_bytes=250,
                query_begin=now,
                last_update=now,
            )
            await self.store_results(self._immediate_success)
        else:
            self._queries[query_id] = AsyncQueryStatus(
                query_id=query_id,
                status=AsyncQueryPhase.EXECUTING,
                total_chunks=10,
                completed_chunks=0,
                query_begin=now,
            )
            async with self._session.begin():
                process = _Process(
                    id=query_id,
                    submitted=now,
                    updated=now,
                    chunks=10,
                    chunks_comp=0,
                )
                self._session.add(process)
        return Response(
            200, json={"success": 1, "query_id": query_id}, request=request
        )

    async def update_status(
        self, query_id: int, status: AsyncQueryStatus
    ) -> None:
        """Update the status of a query for future requests.

        Parameters
        ----------
        query_id
            Identifier of the query.
        status
            New query status.
        """
        assert query_id in self._queries
        async with self._session.begin():
            if status.status == AsyncQueryPhase.EXECUTING:
                stmt = select(_Process).where(_Process.id == query_id)
                results = await self._session.execute(stmt)
                process = results.scalars().first()
                assert process
                assert status.last_update
                process.updated = datetime_to_db(status.last_update)
                process.chunks_comp = status.completed_chunks
            else:
                dstmt = delete(_Process).where(_Process.id == query_id)
                await self._session.execute(dstmt)
        self._queries[query_id] = status

    async def upload(self, request: Request) -> Response:
        """Mock a request to upload the VOTable of results.

        Parameters
        ----------
        request
            Incoming request.

        Returns
        -------
        httpx.Response
            Returns 200 with the details of the query.
        """
        expected = read_test_data("results/data.binary2")
        assert self._expected_job
        header = self._expected_job.result_format.envelope.header
        footer = self._expected_job.result_format.envelope.footer
        assert request.content.decode() == header + expected + footer
        if self._upload_delay:
            await asyncio.sleep(self._upload_delay.total_seconds())
        self._expected_job = None
        return Response(201)

    async def upload_table(self, request: Request) -> Response:
        """Mock a request to upload a table.

        Parameters
        ----------
        request
            Incoming request.

        Returns
        -------
        httpx.Response
            Returns 200 with the details of the query.
        """
        body = BytesIO(request.content)
        content_type_header = request.headers["Content-Type"]
        content_type, options = parse_options_header(content_type_header)
        assert content_type == "multipart/form-data"
        assert "boundary" in options
        parser = MultipartParser(body, options["boundary"])
        data = {}
        files = []
        for part in parser:
            if part.filename:
                file_info = (part.filename, part.value, part.content_type)
                files.append((part.name, file_info))
            else:
                data[part.name] = part.value
        body.close()

        # Check the request is correct.
        expected_job = read_test_job_run("jobs/upload")
        upload_table = expected_job.upload_tables[0]
        assert data == {
            "database": upload_table.table_name.split(".", 1)[0],
            "table": upload_table.table_name.split(".", 1)[1],
            "fields_terminated_by": ",",
            "charset_name": "utf8",
            "timeout": str(int(config.qserv_upload_timeout.total_seconds())),
            "version": str(API_VERSION),
        }
        assert files == [
            (
                "schema",
                ("schema.json", self._UPLOAD_SCHEMA, "application/json"),
            ),
            ("rows", ("table.csv", self._UPLOAD_CSV, "text/csv")),
        ]
        assert not self._uploaded_table, "Too many tables uploaded"
        self._uploaded_table = upload_table.table_name
        return Response(200, json=BaseResponse(success=1).model_dump())

    def _check_version(self, request: Request) -> None:
        """Check that the correct API version was added to the parameters."""
        url = urlparse(str(request.url))
        query = parse_qs(url.query)
        assert query["version"] == [str(API_VERSION)]


@asynccontextmanager
async def register_mock_qserv(
    respx_mock: respx.Router, base_url: str, engine: AsyncEngine
) -> AsyncGenerator[MockQserv]:
    """Mock out the Qserv REST API.

    Parameters
    ----------
    respx_mock
        Mock router.
    base_url
        Base URL on which the mock API should appear to listen.

    Returns
    -------
    MockQserv
        Mock Qserv API object.
    """
    session = await create_async_session(engine, get_logger("qservkafka"))
    mock = MockQserv(session, respx_mock)
    base_url = str(base_url).rstrip("/")
    respx_mock.post(f"{base_url}/query-async").mock(side_effect=mock.submit)
    ingest_url = f"{base_url}/ingest/csv"
    respx_mock.post(ingest_url).mock(side_effect=mock.upload_table)
    base_escaped = re.escape(base_url)
    regex = rf"{base_escaped}/query-async/(?P<query_id>[0-9]+)\?"
    respx_mock.delete(url__regex=regex).mock(side_effect=mock.cancel)
    regex = rf"{base_escaped}/query-async/result/(?P<query_id>[0-9]+)\?"
    respx_mock.delete(url__regex=regex).mock(side_effect=mock.delete_results)
    regex = rf"{base_escaped}/query-async/status/(?P<query_id>[0-9]+)\?"
    respx_mock.get(url__regex=regex).mock(side_effect=mock.status)

    upload_job = read_test_job_run("jobs/upload")
    for upload_table in upload_job.upload_tables:
        url = upload_table.source_url
        respx_mock.get(url).mock(side_effect=mock.get_upload_source)
        url = upload_table.schema_url
        respx.mock.get(url).mock(side_effect=mock.get_upload_schema)

    sql = _QUERY_RESULT_SQL
    with patch.object(qserv, "_QUERY_RESULTS_SQL_FORMAT", new=sql):
        with patch.object(qserv, "_QUERY_LIST_SQL", new=_QUERY_LIST_SQL):
            yield mock
