"""Client for Gafaelfawr user information."""

from httpx import AsyncClient
from rubin.gafaelfawr import (
    GafaelfawrClient,
    GafaelfawrError,
    GafaelfawrNotFoundError,
    GafaelfawrTapQuota,
)
from rubin.repertoire import DiscoveryClient
from safir.sentry import report_exception
from safir.slack.webhook import SlackWebhookClient
from structlog.stdlib import BoundLogger

from ..config import config

__all__ = ["GafaelfawrStorage"]


class GafaelfawrStorage:
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
        *,
        http_client: AsyncClient,
        discovery_client: DiscoveryClient,
        slack_client: SlackWebhookClient | None,
        logger: BoundLogger,
    ) -> None:
        self._slack_client = slack_client
        self._logger = logger

        self._gafaelfawr = GafaelfawrClient(
            http_client, discovery_client=discovery_client
        )
        self._token = config.gafaelfawr_token.get_secret_value()

    async def clear_cache(self) -> None:
        """Invalidate the cache."""
        await self._gafaelfawr.clear_cache()

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
        try:
            info = await self._gafaelfawr.get_user_info(self._token, username)
        except GafaelfawrNotFoundError:
            return None
        except GafaelfawrError as e:
            await report_exception(e, slack_client=self._slack_client)
            msg = "Cannot contact Gafaelfawr for quota information"
            self._logger.exception(msg)
            return None
        if not info.quota:
            return None
        return info.quota.tap.get(config.tap_service)
