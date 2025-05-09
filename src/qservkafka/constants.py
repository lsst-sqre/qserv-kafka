"""Constants that (currently) do not need to be configurable."""

from __future__ import annotations

from datetime import timedelta

__all__ = [
    "ARQ_TIMEOUT_GRACE",
    "REDIS_BACKOFF_MAX",
    "REDIS_BACKOFF_START",
    "REDIS_POOL_SIZE",
    "REDIS_POOL_TIMEOUT",
    "REDIS_RETRIES",
    "REDIS_TIMEOUT",
]

ARQ_TIMEOUT_GRACE = timedelta(seconds=2)
"""Additional grace period to allow on top of result processing timeout.

This should be long enough to allow for asking the REST API for the status,
but still shorter than the additional grace period Kubernetes is configured
to give the worker pod.
"""

MAXIMUM_QUERY_LIFETIME = timedelta(days=1)
"""How long before we forget about a query entirely.

Various bugs and other issues may result in stranding a query in Redis with
the flag set to indicate it has been dispatched by arq but without any active
arq job processing it. As a last resort, tell Redis to forget about these
queries after this interval. This will strand unretrieved results in Qserv
that will have to be pruned by Qserv garbage collection.
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
