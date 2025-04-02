"""Client for the Qserv REST API."""

from __future__ import annotations

import copy

from httpx import AsyncClient, HTTPError, Response
from pydantic import BaseModel, ValidationError

from ..config import config
from ..exceptions import (
    QservApiFailedError,
    QservApiProtocolError,
    QservApiWebError,
)
from ..models.kafka import JobRun
from ..models.qserv import (
    AsyncQueryStatus,
    AsyncStatusResponse,
    AsyncSubmitRequest,
    AsyncSubmitResponse,
    BaseResponse,
)

API_VERSION = 39
"""Version of the REST API that this client requests."""

__all__ = ["API_VERSION", "QservRestClient"]


class QservRestClient:
    """Client for the Qserv REST API.

    Only the routes needed by the Qserv Kafka bridge are implemented.

    Parameters
    ----------
    http_client
        HTTP client to use.
    """

    def __init__(self, http_client: AsyncClient) -> None:
        self._client = http_client

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

        Raises
        ------
        QservApiError
            Raised if something failed when attempting to submit the job.
        """
        params_with_version = copy.copy(params)
        params_with_version["version"] = str(API_VERSION)
        url = str(config.qserv_rest_url).rstrip("/") + route
        try:
            r = await self._client.get(url, params=params_with_version)
            r.raise_for_status()
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

        Raises
        ------
        QservApiError
            Raised if something failed when attempting to submit the job.
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

        Raises
        ------
        QservApiError
            Raised if something failed when attempting to submit the job.
        """
        body_dict = body.model_dump(mode="json", exclude_none=True)
        body_dict["version"] = API_VERSION
        url = str(config.qserv_rest_url).rstrip("/") + route
        try:
            r = await self._client.post(url, json=body_dict)
            r.raise_for_status()
            return self._parse_response(url, r, result_type)
        except HTTPError as e:
            raise QservApiWebError.from_exception(e) from e
