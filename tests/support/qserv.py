"""Mocks for testing code that talks to Qserv."""

import asyncio
import inspect
import io
import json
import re
from base64 import b64encode
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from itertools import cycle
from typing import Any, override
from unittest.mock import MagicMock, Mock, patch
from urllib.parse import parse_qs, urlsplit

import respx
from aiohttp import ClientResponse, ClientTimeout, MultipartWriter
from aioresponses import CallbackResult, aioresponses
from httpx import AsyncByteStream, Request, Response
from multipart import MultipartParser
from safir.database import datetime_to_db, initialize_database
from sqlalchemy import BigInteger, Double, String, delete, select
from sqlalchemy.dialects.mysql import DATETIME
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from structlog.stdlib import BoundLogger

from qservkafka.config import config
from qservkafka.models.kafka import JobRun
from qservkafka.models.qserv import (
    AsyncSubmitRequest,
    BaseResponse,
    QservAsyncStatusData,
    QservQueryPhase,
    QservStatusResponse,
)
from qservkafka.storage import qserv
from qservkafka.storage.qserv import API_VERSION

from .data import QservKafkaData

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


# Taken from https://github.com/j7an/dep-rank/pull/123
#
# aiohttp 3.14 added a required keyword-only ``stream_writer`` argument to
# ``ClientResponse.__init__``. aioresponses (<=0.7.8) builds mocked responses
# without it, so every mocked request raises ``TypeError: ... missing 1
# required keyword-only argument: 'stream_writer'``. aiohttp only reads
# ``stream_writer.output_size``, so a ``Mock(output_size=0)`` suffices.
#
# This mirrors the upstream fix (aioresponses#288, tracking aioresponses#289).
# The signature guard makes it a no-op on aiohttp < 3.14 and once aioresponses
# ships a release that supplies the argument itself; remove this shim then.
_response_init = ClientResponse.__init__
if "stream_writer" in inspect.signature(_response_init).parameters:

    def _patched_response_init(
        self: ClientResponse, *args: Any, **kwargs: Any
    ) -> Any:
        kwargs.setdefault("stream_writer", Mock(output_size=0))
        _response_init(self, *args, **kwargs)

    ClientResponse.__init__ = _patched_response_init  # type: ignore[method-assign]


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


class _UploadedTable(AsyncByteStream):
    """Provides a stream of data as a mock uploaded table."""

    def __init__(self, content: bytes) -> None:
        self._content = [bytes([b]) for b in content]

    @override
    async def __aiter__(self) -> AsyncGenerator[bytes]:
        for b in self._content:
            yield b


class MockQserv:
    """Mock Qserv that simulates the REST API.

    Parameters
    ----------
    data
        Test data management class.
    sessionmaker
        Factory for database sessions.
    respx_mock
        Router for HTTP mocks.
    flaky
        Whether to fail every other request.

    Attributes
    ----------
    flaky
        Whether Qserv API requests intermittently fail.
    """

    _UPLOAD_CSV = "one\ntwo\n"
    """Static table data to return for user table upload."""

    _UPLOAD_SCHEMA = '[{"name":"col_0","type":"VARCHAR(32)"}]'
    """Static schema to return for user table upload."""

    def __init__(
        self,
        data: QservKafkaData,
        sessionmaker: async_sessionmaker,
        respx_mock: respx.Router,
        *,
        flaky: bool = False,
    ) -> None:
        self.flaky = flaky

        self._data = data
        self._sessionmaker = sessionmaker
        self._respx_mock = respx_mock

        self._expected_job: JobRun | None
        self._immediate_success: JobRun | None
        self._intermittent_failure: int | None
        self._mocks: list[MagicMock] = []
        self._next_query_id: int
        self._override_status: Response | None
        self._override_submit: Response | None
        self._queries: dict[int, QservAsyncStatusData]
        self._results_stored: bool
        self._upload_delay: timedelta | None
        self._upload_failure: int | None
        self._uploaded_table: str | None = None
        self._uploaded_database: str | None = None
        self.reset()

    @classmethod
    async def initialize(
        cls, engine: AsyncEngine, logger: BoundLogger
    ) -> None:
        """Initialize the MySQL database."""
        await initialize_database(
            engine, logger, schema=_SchemaBase.metadata, reset=True
        )

    @property
    def results_stored(self) -> bool:
        """Whether results are currently stored."""
        return self._results_stored

    def get_status(self, query_id: int) -> QservAsyncStatusData:
        """Return the current stored status.

        This is used by tests that need to poke at the mock directly.

        Parameters
        ----------
        query_id
            Query ID.

        Returns
        -------
        QservAsyncStatusData
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

    def get_uploaded_database(self) -> str | None:
        """Get the set of uploaded database names.

        Returns
        -------
        str | None
            The uploaded database name or None.
        """
        return self._uploaded_database

    def register_mocks(self, mocks: list[MagicMock]) -> None:
        """Register additional magic mocks to clear on `reset`.

        This helps clean up memory usage for leak testing.
        """
        self._mocks = mocks

    async def remove_running_query(self, query_id: int) -> None:
        """Remove a running query from the process list.

        Normally this is done as part of `update_status` with a completed
        status, but allow it to be done separately to test handling of
        still-executing queries that no longer appear in the process list.

        Parameters
        ----------
        query_id
            Qserv query ID.
        """
        async with self._sessionmaker() as session:
            async with session.begin():
                stmt = delete(_Process).where(_Process.id == query_id)
                await session.execute(stmt)

    def reset(self) -> None:
        """Reset the mock to its initial state."""
        self._expected_job = None
        self._immediate_success = None
        self._intermittent_failure = 0 if self.flaky else None
        self._next_query_id = 1
        self._override_status = None
        self._override_submit = None
        self._queries = {}
        self._results_stored = False
        self._upload_delay = None
        self._upload_failure = 0 if self.flaky else None
        self._uploaded_table = None
        self._uploaded_database = None
        for mock in self._mocks:
            mock.reset_mock()

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

    async def cancel(self, request: Request, *, query_id: str) -> Response:
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
        if self._should_fail():
            return Response(500, text="Something failed")
        self._check_auth(request)
        self._check_version(request)
        status = self._queries.get(int(query_id))
        if not status:
            return Response(
                200,
                json={"success": 0, "error": f"Query {query_id} not found"},
                request=request,
            )
        if status.status != QservQueryPhase.EXECUTING:
            return Response(
                200,
                json={"success": 0, "error": f"Query {query_id} completed"},
                request=request,
            )
        status.status = QservQueryPhase.ABORTED
        status.last_update = datetime.now(tz=UTC)
        await self.remove_running_query(int(query_id))
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
        if self._should_fail():
            return Response(500, text="Something failed")
        self._check_auth(request)
        self._check_version(request)
        assert self._results_stored
        async with self._sessionmaker() as session:
            async with session.begin():
                await session.execute(delete(_Result))
        self._results_stored = False
        return Response(200, json={"success": 1}, request=request)

    async def delete_database(
        self, request: Request, database: str
    ) -> Response:
        """Delete an uploaded database and all its tables.

        Parameters
        ----------
        request
            Incoming request.
        database
            Name of the database.

        Returns
        -------
        httpx.Response
            Returns 200 on successful deletion.
        """
        if self._should_fail():
            return Response(500, text="Something failed")
        self._check_auth(request)
        self._check_version(request)
        assert database == self._uploaded_database
        self._uploaded_database = None
        self._uploaded_table = None
        return Response(200, json={"success": 1}, request=request)

    async def delete_table(
        self, request: Request, database: str, table: str
    ) -> Response:
        """Delete an uploaded database table.

        This API is only used in a failure fallback, so should always succeed
        even if flaky APIs are enabled.

        Parameters
        ----------
        request
            Incoming request.
        database
            Name of the database.

        Returns
        -------
        httpx.Response
            Returns 200 on successful deletion.
        """
        self._check_auth(request)
        self._check_version(request)
        if not self._uploaded_database:
            return Response(404, json={"success": 0})
        assert database == self._uploaded_database
        self._uploaded_table = None
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
        if self._should_fail(upload=True):
            return Response(500, text="Something failed")
        content = self._UPLOAD_SCHEMA.encode()
        return Response(
            200, content=content, headers={"Content-Length": str(len(content))}
        )

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
        if self._should_fail(upload=True):
            return Response(500, text="Something failed")
        return Response(
            200,
            stream=_UploadedTable(self._UPLOAD_CSV.encode()),
            headers={"Content-Length": str(len(self._UPLOAD_CSV.encode()))},
        )

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
        self._check_auth(request)
        self._check_version(request)
        if self._override_status:
            return self._override_status
        if self._should_fail():
            return Response(500, text="Something failed")
        status = self._queries.get(int(query_id))
        if not status:
            return Response(
                200,
                json={"success": 0, "error": f"Query {query_id} not found"},
                request=request,
            )
        result = QservStatusResponse(success=1, status=status)
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
        data = self._data.read_json("results/data")
        async with self._sessionmaker() as session:
            async with session.begin():
                for row in data:
                    if row["j"] is not None:
                        row["j"] = datetime.fromisoformat(row["j"] + "Z")
                    result = _Result(**row)
                    session.add(result)
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
        self._check_auth(request)
        self._check_version(request)
        body_raw = json.loads(request.content.decode())
        AsyncSubmitRequest.model_validate(body_raw)
        if self._override_submit:
            return self._override_submit
        if self._should_fail():
            return Response(500, text="Something failed")
        query_id = self._next_query_id
        self._next_query_id += 1
        now = datetime.now(tz=UTC).replace(microsecond=0)
        if self._immediate_success:
            self._queries[query_id] = self._data.read_qserv_status(
                "qserv/data-completed",
                query_id=query_id,
                query_begin=now,
                last_update=now,
            )
            await self.store_results(self._immediate_success)
        else:
            status = self._data.read_qserv_status(
                "qserv/data-executing",
                query_id=query_id,
                query_begin=now,
                last_update=now,
            )
            self._queries[query_id] = status
            async with self._sessionmaker() as session:
                async with session.begin():
                    process = _Process(
                        id=query_id,
                        submitted=now,
                        updated=now,
                        chunks=status.total_chunks,
                        chunks_comp=status.completed_chunks,
                    )
                    session.add(process)
        return Response(
            200, json={"success": 1, "query_id": query_id}, request=request
        )

    async def update_status(
        self, query_id: int, status: QservAsyncStatusData
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
        if status.status == QservQueryPhase.EXECUTING:
            async with self._sessionmaker() as session:
                async with session.begin():
                    stmt = select(_Process).where(_Process.id == query_id)
                    results = await session.execute(stmt)
                    process = results.scalars().first()
                    assert process
                    assert status.last_update
                    process.updated = datetime_to_db(status.last_update)
                    process.chunks_comp = status.completed_chunks
        else:
            await self.remove_running_query(query_id)
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
        assert self._expected_job
        header = self._expected_job.result_format.envelope.header
        if self._expected_job.maxrec == 1:
            expected = self._data.read_text("results/data-maxrec.binary2")
            footer = self._expected_job.result_format.envelope.footer_overflow
        elif self._expected_job.maxrec == 0:
            expected = "\n"
            footer = self._expected_job.result_format.envelope.footer_overflow
        else:
            expected = self._data.read_text("results/data.binary2")
            footer = self._expected_job.result_format.envelope.footer
        assert request.content.decode() == header + expected + footer
        if self._upload_delay:
            await asyncio.sleep(self._upload_delay.total_seconds())
        self._expected_job = None
        return Response(201)

    async def upload_table(
        self,
        url: str,
        *,
        params: dict[str, str],
        data: MultipartWriter,
        timeout: ClientTimeout | None = None,
        headers: dict[str, str] | None = None,
        allow_redirects: bool = False,
    ) -> CallbackResult:
        """Mock a request to upload a table.

        Parameters
        ----------
        url
            URL of request.
        params
            GET parameters for the request.
        data
            Body of the request.
        timeout
            Timeout of the request, if any.
        headers
            Headers of the request if any.
        allow_redirects
            Whether redirects are allowed.

        Returns
        -------
        httpx.Response
            Returns 200 with the details of the query.
        """
        if config.qserv_rest_username and config.qserv_rest_password:
            assert headers is not None
            password = config.qserv_rest_password.get_secret_value()
            auth = f"{config.qserv_rest_username}:{password}"
            encoded_auth = b64encode(auth.encode()).decode()
            method, seen = headers["Authorization"].split(" ", 1)
            assert method.lower() == "basic"
            assert seen == encoded_auth
        else:
            assert not headers
        if config.qserv_rest_send_api_version:
            assert params == {"version": str(API_VERSION)}
        else:
            assert not params

        if self._should_fail():
            return CallbackResult(
                method="POST", status=500, reason="Something failed"
            )

        body = io.BytesIO(await data.as_bytes())
        parser = MultipartParser(body, data.boundary)
        fields = {}
        files = []
        for part in parser:
            if part.filename:
                file_info = (part.filename, part.value, part.content_type)
                files.append((part.name, file_info))
            else:
                fields[part.name] = part.value

        # Check the request is correct.
        if self._immediate_success:
            expected_job = self._immediate_success
        else:
            expected_job = self._data.read_pydantic(JobRun, "jobs/upload")
        upload_table = expected_job.upload_tables[0]
        expected = {
            "database": upload_table.database,
            "table": upload_table.table,
            "fields_terminated_by": ",",
            "charset_name": "utf8mb4",
            "collation_name": "utf8mb4_uca1400_ai_ci",
            "timeout": str(int(config.qserv_upload_timeout.total_seconds())),
        }
        for key, value in upload_table.to_ingest_fields().items():
            expected[key] = str(value)
        assert fields == expected
        assert files == [
            (
                "schema",
                ("schema.json", self._UPLOAD_SCHEMA, "application/json"),
            ),
            ("rows", ("table.csv", self._UPLOAD_CSV, "text/csv")),
        ]
        database = upload_table.database

        if self._uploaded_database is not None:
            assert database == self._uploaded_database, (
                f"Multiple databases in single job: expected "
                f"'{self._uploaded_database}', "
                f"got '{database}'"
            )

        assert not self._uploaded_table, "Too many tables uploaded"
        self._uploaded_table = upload_table.table_name
        self._uploaded_database = database
        result_body = BaseResponse(success=1).model_dump_json()
        return CallbackResult(method="POST", status=200, body=result_body)

    def _check_auth(self, request: Request) -> None:
        """Check that authentication credentials were added, if configured."""
        if config.qserv_rest_username and config.qserv_rest_password:
            password = config.qserv_rest_password.get_secret_value()
            auth = f"{config.qserv_rest_username}:{password}"
            expected = b64encode(auth.encode()).decode()
            method, seen = request.headers["Authorization"].split(" ", 1)
            assert method.lower() == "basic"
            assert seen == expected

    def _check_version(self, request: Request) -> None:
        """Check that the correct API version was added to the parameters."""
        url = urlsplit(str(request.url))
        query = parse_qs(url.query)
        if config.qserv_rest_send_api_version:
            assert query["version"] == [str(API_VERSION)]
        else:
            assert "version" not in query

    def _should_fail(self, *, upload: bool = False) -> bool:
        """Check whether to return an intermittent failure.

        Different logic is required for user table uploads than for regular
        calls, since a user table upload is three separate HTTP calls: one to
        download the schema, one to download the data, and one to upload the
        table. If all three failed every other request, the request will never
        complete successfully since at least one of them fails.

        Therefore, for the two requests to download the schema and data, only
        fail every seven requests instead of every other. This means the first
        attempt will fail getting the schema, the second attempt will succeed
        in getting both files but will fail in the upload, and the third
        attempt will succeed in all three stages. Then, the next time through,
        the schema will succeed and the data download will fail.
        """
        if upload:
            if self._upload_failure is not None:
                self._upload_failure += 1
                return self._upload_failure % 7 == 1
            return False
        if self._intermittent_failure is not None:
            self._intermittent_failure += 1
            return self._intermittent_failure % 2 == 1
        else:
            return False


@asynccontextmanager
async def register_mock_qserv(
    data: QservKafkaData,
    respx_mock: respx.Router,
    aioresponses_mock: aioresponses,
    *,
    engine: AsyncEngine,
    base_url: str,
    flaky: bool = False,
) -> AsyncGenerator[MockQserv]:
    """Mock out the Qserv REST API.

    Parameters
    ----------
    data
        Test data management class.
    respx_mock
        Mock router for HTTPX.
    aiorespones_mock
        Mock router for aiohttp.
    engine
        Database engine.
    base_url
        Base URL on which the mock API should appear to listen.
    flaky
        Whether to simulate a flaky Qserv by returning SQL or HTTP errors
        every other request.

    Returns
    -------
    MockQserv
        Mock Qserv API object.
    """
    sessionmaker = async_sessionmaker(engine, expire_on_commit=False)
    mock = MockQserv(data, sessionmaker, respx_mock, flaky=flaky)
    base = re.escape(str(base_url).rstrip("/"))
    regex = rf"{base}/query-async"
    respx_mock.post(url__regex=regex).mock(side_effect=mock.submit)
    regex = rf"{base}/ingest/csv"
    aioresponses_mock.post(
        re.compile(regex), callback=mock.upload_table, repeat=True
    )
    regex = rf"{base}/ingest/database/(?P<database>[^/?]+)(?:\?|$)"
    respx_mock.delete(url__regex=regex).mock(side_effect=mock.delete_database)
    regex = rf"{base}/ingest/table/(?P<database>[^/?]+)/(?P<table>[^/?]+)"
    respx_mock.delete(url__regex=regex).mock(side_effect=mock.delete_table)
    regex = rf"{base}/query-async/(?P<query_id>[0-9]+)"
    respx_mock.delete(url__regex=regex).mock(side_effect=mock.cancel)
    regex = rf"{base}/query-async/result/(?P<query_id>[0-9]+)"
    respx_mock.delete(url__regex=regex).mock(side_effect=mock.delete_results)
    regex = rf"{base}/query-async/status/(?P<query_id>[0-9]+)"
    respx_mock.get(url__regex=regex).mock(side_effect=mock.status)

    upload_job = data.read_pydantic(JobRun, "jobs/upload")
    for upload_table in upload_job.upload_tables:
        url = upload_table.source_url
        respx_mock.get(url).mock(side_effect=mock.get_upload_source)
        url = upload_table.schema_url
        respx.mock.get(url).mock(side_effect=mock.get_upload_schema)

    bad_sql = "SELECT * FROM nonexistent"
    with patch.object(qserv, "_query_results_sql") as results_mock:
        if flaky:
            results_mock.side_effect = cycle((bad_sql, _QUERY_RESULT_SQL))
        else:
            results_mock.return_value = _QUERY_RESULT_SQL
        with patch.object(qserv, "_query_list_sql") as list_mock:
            if flaky:
                list_mock.side_effect = cycle((bad_sql, _QUERY_LIST_SQL))
            else:
                list_mock.return_value = _QUERY_LIST_SQL
            mock.register_mocks([results_mock, list_mock])
            yield mock
