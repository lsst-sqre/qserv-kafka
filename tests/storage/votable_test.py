"""Test the VOTable writer."""

from __future__ import annotations

from base64 import b64encode
from collections.abc import AsyncGenerator
from typing import Any

import pytest
from sqlalchemy import Row

from qservkafka.models.kafka import (
    JobResultColumnType,
    JobResultConfig,
    JobResultEnvelope,
    JobResultFormat,
    JobResultSerialization,
    JobResultType,
)
from qservkafka.storage.votable import VOTableEncoder


async def data_generator(
    data: list[tuple[Any]],
) -> AsyncGenerator[Row[Any] | tuple[Any]]:
    """Turn a list of rows into an async generator of rows."""
    for row in data:
        yield row


async def assert_encoded_value(
    column: dict[str, Any], data: Any, expected: bytes
) -> None:
    """Encode a single data element and check the result.

    Parameters
    ----------
    column
        Column type definition.
    data
        Input data.
    expected
        Expected bytes before base64 encoding.
    """
    config = JobResultConfig(
        format=JobResultFormat(
            type=JobResultType.votable,
            serialization=JobResultSerialization.BINARY2,
        ),
        column_types=[JobResultColumnType.model_validate(column)],
        envelope=JobResultEnvelope(header="", footer=""),
    )
    rows = [(data,)]
    encoder = VOTableEncoder(config)
    encoded = b""
    async for blob in encoder.encode(data_generator(rows)):
        encoded += blob
    assert encoded == b64encode(expected) + b"\n", str(column)


@pytest.mark.asyncio
async def test_type_encoder() -> None:
    tests = [
        (
            {"name": "a", "datatype": "int"},
            1,
            b"\x00\x00\x00\x00\x01",
        ),
        (
            {"name": "d", "datatype": "char", "arraysize": "*"},
            "something",
            b"\x00\x00\x00\x00\x09something",
        ),
    ]
    for column, data, expected in tests:
        await assert_encoded_value(column, data, expected)
