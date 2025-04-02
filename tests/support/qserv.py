"""Mocks for testing code that talks to Qserv."""

from __future__ import annotations

import json
import re
from datetime import UTC, datetime
from urllib.parse import parse_qs, urlparse

import respx
from httpx import Request, Response

from qservkafka.models.qserv import (
    AsyncQueryPhase,
    AsyncQueryStatus,
    AsyncStatusResponse,
    AsyncSubmitRequest,
)
from qservkafka.storage.rest import API_VERSION

__all__ = ["MockQserv", "register_mock_qserv"]


class MockQserv:
    """Mock Qserv that simulates the REST API."""

    def __init__(self) -> None:
        self._next_query_id = 1
        self._queries: dict[int, AsyncQueryStatus] = {}
        self._override_status: Response | None = None
        self._override_submit: Response | None = None

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
        status.last_update = datetime.now(tz=UTC)
        result = AsyncStatusResponse(success=1, status=status)
        return Response(
            200,
            json=result.model_dump(mode="json", exclude_none=True),
            request=request,
        )

    def submit(self, request: Request) -> Response:
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
        self._queries[query_id] = AsyncQueryStatus(
            query_id=query_id,
            status=AsyncQueryPhase.EXECUTING,
            total_chunks=10,
            completed_chunks=0,
            query_begin=datetime.now(tz=UTC),
        )
        return Response(
            200, json={"success": 1, "query_id": query_id}, request=request
        )


def register_mock_qserv(respx_mock: respx.Router, base_url: str) -> MockQserv:
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
    mock = MockQserv()
    base_url = str(base_url).rstrip("/")
    respx_mock.post(f"{base_url}/query-async").mock(side_effect=mock.submit)
    base_escaped = re.escape(base_url)
    url_regex = rf"{base_escaped}/query-async/status/(?P<query_id>[0-9]+)\?"
    respx_mock.get(url__regex=url_regex).mock(side_effect=mock.status)
    return mock
