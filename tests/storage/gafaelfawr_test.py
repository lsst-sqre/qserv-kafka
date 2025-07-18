"""Tests for the caching Gafaelfawr quota client."""

from __future__ import annotations

import pytest

from qservkafka.config import config
from qservkafka.factory import Factory
from qservkafka.models.gafaelfawr import GafaelfawrQuota, GafaelfawrTapQuota

from ..support.gafaelfawr import MockGafaelfawr


@pytest.mark.asyncio
async def test_caching(
    factory: Factory, mock_gafaelfawr: MockGafaelfawr
) -> None:
    client = factory.gafaelfawr_client
    mock_gafaelfawr.set_quota(
        "one",
        GafaelfawrQuota(
            tap={
                config.tap_service: GafaelfawrTapQuota(concurrent=1),
                "ssotap": GafaelfawrTapQuota(concurrent=2),
            }
        ),
    )
    mock_gafaelfawr.set_quota("none", None)
    mock_gafaelfawr.set_quota_error("error")

    quota = await client.get_user_quota("one")
    assert quota
    assert quota.concurrent == 1
    assert await client.get_user_quota("none") is None
    assert await client.get_user_quota("unknown") is None
    assert await client.get_user_quota("error") is None

    # Change the quota for a user and check that the old value is cached.
    mock_gafaelfawr.set_quota(
        "one",
        GafaelfawrQuota(
            tap={config.tap_service: GafaelfawrTapQuota(concurrent=2)}
        ),
    )
    quota = await client.get_user_quota("one")
    assert quota
    assert quota.concurrent == 1

    # Clear the cache and we should see the new value.
    await client.clear_cache()
    quota = await client.get_user_quota("one")
    assert quota
    assert quota.concurrent == 2

    # Both the none and unknown users should be cached, but the error user
    # should not be.
    assert await client.get_user_quota("none") is None
    assert await client.get_user_quota("unknown") is None
    assert await client.get_user_quota("error") is None
    mock_gafaelfawr.clear_quota_error("error")
    for user in ("none", "unknown", "error"):
        mock_gafaelfawr.set_quota(
            user,
            GafaelfawrQuota(
                tap={config.tap_service: GafaelfawrTapQuota(concurrent=2)}
            ),
        )
    assert await client.get_user_quota("none") is None
    assert await client.get_user_quota("unknown") is None
    quota = await client.get_user_quota("error")
    assert quota
    assert quota.concurrent == 2

    # Clearing the cache should return the new information for everything.
    await client.clear_cache()
    for user in ("none", "unknown", "error"):
        quota = await client.get_user_quota(user)
        assert quota
        assert quota.concurrent == 2
