"""Mock for Gafaelfawr's quota API."""

from __future__ import annotations

import respx
from httpx import Request, Response

from qservkafka.config import config
from qservkafka.models.gafaelfawr import GafaelfawrQuota

__all__ = ["MockGafaelfawr", "register_mock_gafaelfawr"]


class MockGafaelfawr:
    """Mock Gafaelfawr that returns preconfigured quota information."""

    def __init__(self) -> None:
        self._errors: dict[str, bool] = {}
        self._quotas: dict[str, GafaelfawrQuota | None] = {}

    def clear_quota_error(self, username: str) -> None:
        """Clear the quota error flag for a user.

        Parameters
        ----------
        username
            Username for which to clear the error flag.
        """
        self._errors[username] = False

    def get_user_info(self, request: Request, username: str) -> Response:
        """Mock user information requests.

        Parameters
        ----------
        request
            Incoming request.

        Returns
        -------
        httpx.Response
            Returns 200 with the quota details if some quota information is
            set, even if it's `None`, or 404 if no quota information is set
            for that user.
        """
        authorization = request.headers["Authorization"]
        scheme, token = authorization.split(None, 1)
        assert scheme.lower() == "bearer"
        assert token == config.gafaelfawr_token.get_secret_value()
        if self._errors.get(username):
            return Response(500)
        if username in self._quotas:
            quota_model = self._quotas[username]
            if quota_model:
                quota = quota_model.model_dump(mode="json")
            else:
                quota = None
            return Response(200, json={"username": username, "quota": quota})
        else:
            return Response(404)

    def set_quota(self, username: str, quota: GafaelfawrQuota | None) -> None:
        """Set the Gafaelfawr quota for a given user.

        Parameters
        ----------
        username
            Username for which to set a quota.
        quota
            Quota to set for that user or `None` to return an entry without
            a quota.
        """
        self._quotas[username] = quota

    def set_quota_error(self, username: str) -> None:
        """Mark a user to return an error on quota requests.

        Parameters
        ----------
        username
            Username for which to set the error flag.
        """
        self._errors[username] = True


def register_mock_gafaelfawr(
    respx_mock: respx.Router, base_url: str
) -> MockGafaelfawr:
    """Mock out Gafaelfawr.

    Parameters
    ----------
    respx_mock
        Mock router.
    base_url
        Base URL on which the mock API should appear to listen.

    Returns
    -------
    MockGafaelfawr
        Mock Gafaelfawr API object.
    """
    mock = MockGafaelfawr()

    regex = rf"{base_url.rstrip('/')}/auth/api/v1/users/(?P<username>[^/]+)$"
    respx_mock.get(url__regex=regex).mock(side_effect=mock.get_user_info)

    return mock
