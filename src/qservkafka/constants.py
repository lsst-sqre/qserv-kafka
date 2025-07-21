"""Constants that (currently) do not need to be configurable."""

from __future__ import annotations

from datetime import timedelta

__all__ = [
    "ARQ_TIMEOUT_GRACE",
    "GAFAELFAWR_CACHE_LIFETIME",
    "GAFAELFAWR_CACHE_SIZE",
    "MAXIMUM_QUERY_LIFETIME",
    "RATE_LIMIT_RECONCILE_INTERVAL",
    "REDIS_BACKOFF_MAX",
    "REDIS_BACKOFF_START",
    "REDIS_POOL_SIZE",
    "REDIS_POOL_TIMEOUT",
    "REDIS_RETRIES",
    "REDIS_TIMEOUT",
    "UPLOAD_BUFFER_SIZE",
]

ARQ_TIMEOUT_GRACE = timedelta(seconds=5)
"""Additional grace period to allow on top of result processing timeout.

This should be long enough to allow for asking the REST API for the status,
but still shorter than the additional grace period Kubernetes is configured
to give the worker pod.
"""

GAFAELFAWR_CACHE_LIFETIME = timedelta(minutes=5)
"""How long to cache quota information for users, retrieved from Gafaelfawr.

Gafaelfawr policy says that this should not be any longer than five minutes so
that changes to the user's groups are picked up correctly.
"""

GAFAELFAWR_CACHE_SIZE = 1000
"""Maximum number of users whose quota information is cached in memory."""

MAXIMUM_QUERY_LIFETIME = timedelta(days=1)
"""How long before we forget about a query entirely.

Various bugs and other issues may result in stranding a query in Redis with
the flag set to indicate it has been dispatched by arq but without any active
arq job processing it. As a last resort, tell Redis to forget about these
queries after this interval. This will strand unretrieved results in Qserv
that will have to be pruned by Qserv garbage collection.
"""

RATE_LIMIT_RECONCILE_INTERVAL = timedelta(hours=1)
"""How frequently to reconcile rate limits against running queries.

The rate limiting algorithm can get out of sync with running queries in
various situations, particularly pod restarts, pod crashes, Redis crashes, or
network problems. Run a background reconcile task at this interval to try to
self-heal inconsistencies. Unfortunately, this too can create its own
inconsistencies since it reconciles against a data snapshot, so running it too
frequently can create more problems than it solves.
"""

REDIS_BACKOFF_MAX = 1.0
"""Maximum delay (in seconds) to wait after a Redis failure."""

REDIS_BACKOFF_START = 0.2
"""How long (in seconds) to initially wait after a Redis failure.

Exponential backoff will be used for subsequent retries, up to
`REDIS_BACKOFF_MAX` total delay.
"""

REDIS_POOL_SIZE = 5
"""Size of the ephemeral Redis connection pool (without rate limiting.)"""

REDIS_POOL_TIMEOUT = 30
"""Seconds to wait for a connection from the pool before giving up."""

REDIS_RETRIES = 10
"""How many times to try to connect to Redis before giving up."""

REDIS_TIMEOUT = 5
"""Timeout in seconds for a Redis network operation (including connecting)."""

UPLOAD_BUFFER_SIZE = 64 * 1024
"""Size of the internal buffer for HTTP PUT.

Writes to the signed URL are buffered to avoid too many small writes and
context switches. This is the size of the buffer.
"""
