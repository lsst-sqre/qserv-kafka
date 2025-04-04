"""Mocks for testing code that talks to Qserv."""

from __future__ import annotations

import json
import re
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from datetime import datetime
from unittest.mock import patch
from urllib.parse import parse_qs, urlparse

import respx
from httpx import Request, Response
from safir.database import (
    create_async_session,
    datetime_to_db,
    initialize_database,
)
from safir.datetime import current_datetime
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncEngine, async_scoped_session
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from structlog import get_logger
from structlog.stdlib import BoundLogger

from qservkafka.models.qserv import (
    AsyncQueryPhase,
    AsyncQueryStatus,
    AsyncStatusResponse,
    AsyncSubmitRequest,
)
from qservkafka.storage import qserv
from qservkafka.storage.qserv import API_VERSION

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


class MockQserv:
    """Mock Qserv that simulates the REST API."""

    def __init__(self, session: async_scoped_session) -> None:
        self._session = session
        self._next_query_id = 1
        self._queries: dict[int, AsyncQueryStatus] = {}
        self._override_status: Response | None = None
        self._override_submit: Response | None = None

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
        url = urlparse(str(request.url))
        query = parse_qs(url.query)
        assert query["version"] == [str(API_VERSION)]
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
    mock = MockQserv(session)
    base_url = str(base_url).rstrip("/")
    respx_mock.post(f"{base_url}/query-async").mock(side_effect=mock.submit)
    base_escaped = re.escape(base_url)
    url_regex = rf"{base_escaped}/query-async/status/(?P<query_id>[0-9]+)\?"
    respx_mock.get(url__regex=url_regex).mock(side_effect=mock.status)
    with patch.object(qserv, "_QUERY_LIST_SQL", new=_QUERY_LIST_SQL):
        yield mock
