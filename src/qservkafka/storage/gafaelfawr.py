"""Client for Gafaelfawr user information."""

from __future__ import annotations

import asyncio
from types import EllipsisType

from cachetools import TTLCache
from httpx import AsyncClient, HTTPError
from pydantic import ValidationError
from safir.sentry import report_exception
from safir.slack.webhook import SlackWebhookClient
from structlog.stdlib import BoundLogger

from ..config import config
from ..constants import GAFAELFAWR_CACHE_LIFETIME, GAFAELFAWR_CACHE_SIZE
from ..models.gafaelfawr import GafaelfawrTapQuota, GafaelfawrUserInfo

__all__ = ["GafaelfawrClient"]


class GafaelfawrClient:
    """Get quota information from Gafaelfawr.

    Parameters
    ----------
    http_client
        Shared HTTP client.
    slack_client
        Client to send errors to Slack
    logger
        Logger for messages.
    """

    def __init__(
        self,
        http_client: AsyncClient,
        slack_client: SlackWebhookClient | None,
        logger: BoundLogger,
    ) -> None:
        self._http_client = http_client
        self._slack_client = slack_client
        self._logger = logger

        base_url = str(config.gafaelfawr_base_url).rstrip("/")
        self._url = f"{base_url}/auth/api/v1/users"
        token = config.gafaelfawr_token.get_secret_value()
        self._headers = {"Authorization": f"bearer {token}"}

        # Cache and cache lock. Ellipsis is used as the data when we
        # successfully got the quota information from Gafaelfawr and the user
        # has no quota. This allows us to distinguish that from the None case
        # where we have never looked up the user or their cached information
        # has expired.
        self._cache: TTLCache[str, GafaelfawrTapQuota | EllipsisType | None]
        self._cache = TTLCache(
            GAFAELFAWR_CACHE_SIZE, GAFAELFAWR_CACHE_LIFETIME.total_seconds()
        )
        self._lock = asyncio.Lock()

    async def clear_cache(self) -> None:
        """Invalidate the cache."""
        async with self._lock:
            self._cache = TTLCache(
                GAFAELFAWR_CACHE_SIZE,
                GAFAELFAWR_CACHE_LIFETIME.total_seconds(),
            )

    async def get_user_quota(self, username: str) -> GafaelfawrTapQuota | None:
        """Get quota information for the user, with caching.

        Parameters
        ----------
        username
            Username of user.

        Returns
        -------
        GafaelfawrTapQuota or None
            Quota information for the user, or `None` if the user has no quota
            or if there was any problem talking to Gafaelfawr.
        """
        quota = self._cache.get(username)
        if quota is None:
            async with self._lock:
                quota = self._cache.get(username)
                if quota is None:
                    quota = await self._get_quota(username)
                    self._cache[username] = quota
        return None if quota is Ellipsis else quota

    async def _get_quota(
        self, username: str
    ) -> GafaelfawrTapQuota | EllipsisType | None:
        """Get quota information for the user from Gafaelfawr.

        Parameters
        ----------
        username
            Username of user.

        Returns
        -------
        GafaelfawrTapQuota or Ellipsis or None
            Quota information for the user, `Ellipsis` if the user has no
            quota, or `None` if there was any problem talking to Gafaelfawr.
        """
        url = self._url + f"/{username}"
        try:
            r = await self._http_client.get(url, headers=self._headers)
        except HTTPError as e:
            await report_exception(e, slack_client=self._slack_client)
            msg = "Cannot contact Gafaelfawr for quota information"
            self._logger.exception(msg)
            return None
        if r.status_code == 404:
            return Ellipsis
        try:
            r.raise_for_status()
            data = r.json()
            self._logger.debug(
                "Got user information from Gafaelfawr",
                username=username,
                info=data,
            )
            userinfo = GafaelfawrUserInfo.model_validate(data)
        except (HTTPError, ValidationError) as e:
            await report_exception(e, slack_client=self._slack_client)
            msg = "Invalid user information response from Gafaelfawr"
            self._logger.exception(msg)
            return None
        if not userinfo.quota:
            return Ellipsis
        return userinfo.quota.tap.get(config.tap_service) or Ellipsis
