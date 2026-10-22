"""Rate limiting and quota enforcement for queries."""

from collections import Counter

from safir.sentry import report_exception
from safir.slack.webhook import SlackWebhookClient
from structlog.stdlib import BoundLogger

from ..exceptions import RateLimitError
from ..models.query import QueryStatus
from ..models.rate import RateLimitStatus
from ..storage.gafaelfawr import GafaelfawrStorage
from ..storage.rate import ConcurrentQueryStorage, RateLimitStorage
from ..storage.state import QueryStateStore

__all__ = ["RateLimitService"]


class RateLimitService:
    """Storage for tracking rate limits.

    Currently, there are two supported rate limits: one on bytes billed, and
    one on concurrent queries. This service handles both via methods for
    starting a new query, which returns rate limit information including
    whether the query is allowed, and ending a query.

    Parameters
    ----------
    concurrent_storage
        Concurrent query storage.
    rate_limit_storage
        Rate limit storage.
    gafaelfawr_storage
        Shared Gafaelfawr client.
    state_store
        Storage for query state.
    slack_client
        Client to send errors to Slack
    logger
        Logger for messages.
    """

    def __init__(
        self,
        *,
        concurrent_storage: ConcurrentQueryStorage,
        gafaelfawr_storage: GafaelfawrStorage,
        rate_limit_storage: RateLimitStorage,
        state_store: QueryStateStore,
        slack_client: SlackWebhookClient | None,
        logger: BoundLogger,
    ) -> None:
        self._concurrent = concurrent_storage
        self._gafaelfawr = gafaelfawr_storage
        self._rate_limit = rate_limit_storage
        self._state = state_store
        self._slack_client = slack_client
        self._logger = logger

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
        quota = await self._gafaelfawr.get_user_quota(username)

        # Track simultaneous running queries even if there is no quota so that
        # a quota can be imposed later and will take into account the
        # currently running queries. Don't increment the running count before
        # checking the bytes limit if there is a quota, though.
        if not quota:
            running = await self._concurrent.start_query(username)
            return RateLimitStatus(allowed=True, running=running)

        # Check any bytes billed quota. We don't know in advance how many
        # bytes the query will bill, so we test the quota assuming it will
        # bill one byte. When the query is complete, we will deduct the actual
        # cost. A user can therefore always exceed their quota with queries
        # started before one that puts them over quota finishes, but then
        # subsequent queries will be rejected.
        if quota.bytes_billed is not None:
            limit = quota.bytes_billed
            status = await self._rate_limit.get_daily(username, limit)
            if status and status.remaining < 1:
                running = await self._concurrent.count(username)
                return RateLimitStatus(
                    allowed=False,
                    running=running,
                    bytes_billed_reset=status.reset_time,
                    quota=quota,
                )

        # Now increment the running query count and check against the quota.
        running = await self._concurrent.start_query(username)
        if quota.concurrent < running:
            running = await self._concurrent.end_query(username)
            return RateLimitStatus(allowed=False, running=running, quota=quota)

        # Return success.
        return RateLimitStatus(allowed=True, running=running, quota=quota)

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
        await self._concurrent.end_query(username)
        if not status or not status.bytes_billed:
            return
        quota = await self._gafaelfawr.get_user_quota(username)
        if not quota or quota.bytes_billed is None:
            return
        maximum = quota.bytes_billed
        used = status.bytes_billed
        try:
            await self._rate_limit.update_daily(username, maximum, used)
        except RateLimitError as e:
            self._logger.exception(
                "Error updating quota",
                username=username,
                **status.to_logging_context(),
            )
            await report_exception(e, self._slack_client)

    async def reconcile_rate_limits(self) -> None:
        """Reconcile rate limits against currently running queries."""
        active_queries = await self._state.get_active_queries()
        counts: Counter[str] = Counter()
        for backend_id in active_queries:
            query = await self._state.get_query(backend_id)
            if query:
                counts[query.job.owner] += 1
        await self._concurrent.reconcile_query_counts(counts)

    @staticmethod
    def _key(username: str) -> str:
        """Construct the Redis key for the given user."""
        return f"rate:{username}"
