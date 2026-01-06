"""Storage for rate limiting of simultaneous queries."""

import redis.asyncio as redis

__all__ = ["RateLimitStore"]


class RateLimitStore:
    """Storage for tracking concurrent queries for rate limiting.

    This approach to rate limiting is not robust against race conditions due
    to a few problems:

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
    redis
        A Redis client configured to talk to the backend store.
    """

    def __init__(self, redis: redis.Redis) -> None:
        self._redis = redis

    async def end_query(self, username: str) -> int:
        """Decrement the number of active queries and return the count.

        Must be called once a query ends for any reason, including errors.

        Parameters
        ----------
        username
            User running the query.

        Returns
        -------
        int
            Number of active queries for that user after subtracting this
            query.
        """
        return await self._redis.decr(self._key(username))

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

    async def start_query(self, username: str) -> int:
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
        int
            Count of running queries for that user, including the one
            indicated by this call.
        """
        return await self._redis.incr(self._key(username))

    @staticmethod
    def _key(username: str) -> str:
        """Construct the Redis key for the given user."""
        return f"rate:{username}"
