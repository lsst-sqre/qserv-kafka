"""Storage for rate limiting of simultaneous queries."""

from datetime import UTC, datetime

import redis.asyncio as redis
from limits import RateLimitItemPerDay
from limits.aio.strategies import FixedWindowRateLimiter
from rubin.gafaelfawr import (
    GafaelfawrClient,
    GafaelfawrError,
    GafaelfawrNotFoundError,
    GafaelfawrTapQuota,
)
from safir.sentry import report_exception
from safir.slack.webhook import SlackWebhookClient
from structlog.stdlib import BoundLogger

from ..config import config
from ..exceptions import RateLimitError
from ..models.query import QueryStatus
from ..models.rate import RateLimitStatus

__all__ = ["RateLimitStore"]


class RateLimitStore:
    """Storage for tracking rate limits.

    Currently, there are two supported rate limits: one on bytes billed, and
    one on concurrent queries.

    The concurrent query approach to rate limiting is not robust against race
    conditions due to a few problems:

    #. The bridge may lose contact with Redis after query completion and may
       not be able to decrement the counter properly.
    #. The bridge may increment the counter and then die before starting the
       query or decrementing it again.
    #. arq may re-run a result handling job in a way that causes us to
       decrement the counter for a user twice.

    Rather than trying to make the system fully robust, which seems complex,
    these races are addressed by attempting self-healing to eventually restore
    consistency. The caller should periodically count all running queries by
    other means and then call `reconcile_query_counts`. This will reset each
    counter to the counted number of queries in progress, as long as that
    count is less than the current count, and also set the counter to zero if
    it is negative.

    Parameters
    ----------
    gafaelfawr_client
        Shared Gafaelfawr client.
    limiter
        Fixed-window rate limit manager, currently only used for bytes billed
        rate limits.
    redis
        A Redis client configured to talk to the backend store.
    slack_client
        Client to send errors to Slack
    logger
        Logger for messages.
    """

    def __init__(
        self,
        *,
        gafaelfawr_client: GafaelfawrClient,
        limiter: FixedWindowRateLimiter | None,
        redis: redis.Redis,
        slack_client: SlackWebhookClient | None,
        logger: BoundLogger,
    ) -> None:
        self._gafaelfawr = gafaelfawr_client
        self._limiter = limiter
        self._redis = redis
        self._slack_client = slack_client
        self._logger = logger

    async def end_query(
        self, username: str, status: QueryStatus | None = None
    ) -> None:
        """Decrement the number of active queries and return the count.

        Must be called once a query ends for any reason, including errors.

        Parameters
        ----------
        username
            User running the query.
        status
            Status of the query if the query was started.

        Raises
        ------
        RateLimitError
            Raised if there was a failure contacting the rate limiting
            backend.
        """
        await self._redis.decr(self._key(username))
        if not self._limiter or not status or not status.bytes_billed:
            return
        quota = await self._get_quota(username)
        if not quota or quota.bytes_billed is None:
            return
        limit = RateLimitItemPerDay(quota.bytes_billed)
        used = status.bytes_billed
        try:
            await self._limiter.hit(limit, username, cost=used - 1)
        except Exception as e:
            self._logger.exception(
                "Error updating quota",
                username=username,
                **status.to_logging_context(),
            )
            exc = RateLimitError.from_exception(e, username)
            await report_exception(exc, self._slack_client)

    async def reconcile_query_counts(self, counts: dict[str, int]) -> None:
        """Reconcile active query counts against a snapshot.

        Takes a snapshot of running query counts by user gathered at some
        point in time (possibly with modifications happening while the
        snapshot is gathered), and try to clean up any inconsistencies in
        query counts. Err on the side of counting fewer queries by resetting
        the count only if the snapshot showed fewer running queries than
        expected. This should err on the side of allowing extra queries if the
        caller lost a race and has an inconsistent count.

        Parameters
        ----------
        counts
            Number of running queries per user.
        """
        for username, count in counts.items():
            key = self._key(username)

            # Don't bother to use pipelines here since all they accomplish is
            # to decrease the window for race conditions. They cannot
            # eliminate it since we're still working from a snapshot.
            data = await self._redis.get(key)
            current = 0 if data is None else int(data)
            if current > count:
                await self._redis.set(key, count)
            if current < 0:
                await self._redis.set(key, 0)

        # Remove any rate counters for users with no running queries. This
        # keeps the Redis size down and reduces the work involved in future
        # reconciles. Likewise don't bother to use pipelines here since we
        # still have a race condition window.
        async for key in self._redis.scan_iter(match="rate:*"):
            username = key[len("rate:") :].decode()
            if username not in counts:
                await self._redis.delete(key)

    async def start_query(self, username: str) -> RateLimitStatus:
        """Increment the number of active queries and return the count.

        If the caller chooses to not start a query, it must decrement the
        count again. There is some danger, therefore, that the count will get
        out of sync with the number of executing queries, which should be
        handled via a periodic reconciliation task.

        Parameters
        ----------
        username
            User running the query.

        Returns
        -------
        RateLimitStatus
            Status information about the user's quota, used to enhance logging
            when starting the quer.

        Raises
        ------
        RateLimitError
            Raised if there was a failure contacting the rate limiting
            backend.
        """
        quota = await self._get_quota(username)

        # Track simultaneous running queries even if there is no quota so that
        # a quota can be imposed later and will take into account the
        # currently running queries. Don't increment the running count before
        # checking the bytes limit if there is a quota, though.
        if not quota:
            running = await self._redis.incr(self._key(username))
            return RateLimitStatus(allowed=True, running=running)

        # Check any bytes billed quota. We don't know in advance how many
        # bytes the query will bill, so we test the quota assuming it will
        # bill one byte. When the query is complete, we will deduct the actual
        # cost. A user can therefore always exceed their quota with queries
        # started before one that puts them over quota finishes, but then
        # subsequent queries will be rejected.
        if self._limiter and quota.bytes_billed is not None:
            limit = RateLimitItemPerDay(quota.bytes_billed)
            try:
                allowed = await self._limiter.hit(limit, username)
            except Exception as e:
                raise RateLimitError.from_exception(e, username) from e
            if not allowed:
                running = await self._redis.get(self._key(username))
                stats = await self._limiter.get_window_stats(limit, username)
                reset = datetime.fromtimestamp(stats.reset_time, tz=UTC)
                return RateLimitStatus(
                    allowed=False,
                    running=running,
                    bytes_billed_reset=reset,
                    quota=quota,
                )

        # Now increment the running query count and check against the quota.
        running = await self._redis.incr(self._key(username))
        if quota.concurrent < running:
            await self._redis.decr(self._key(username))
            running -= 1
            return RateLimitStatus(allowed=False, running=running, quota=quota)

        # Return success.
        return RateLimitStatus(allowed=True, running=running, quota=quota)

    async def _get_quota(self, username: str) -> GafaelfawrTapQuota | None:
        """Get quota information for the user.

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
        token = config.gafaelfawr_token.get_secret_value()
        try:
            info = await self._gafaelfawr.get_user_info(token, username)
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

    @staticmethod
    def _key(username: str) -> str:
        """Construct the Redis key for the given user."""
        return f"rate:{username}"
