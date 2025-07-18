"""Test the VOTable writer."""

from __future__ import annotations

from base64 import b64encode
from collections.abc import AsyncGenerator
from typing import Any

import pytest
from sqlalchemy import Row
from structlog import get_logger

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
            type=JobResultType.VOTable,
            serialization=JobResultSerialization.BINARY2,
        ),
        column_types=[JobResultColumnType.model_validate(column)],
        envelope=JobResultEnvelope(header="", footer="", footer_overflow=""),
    )
    rows = [(data,)]
    logger = get_logger(__name__)
    encoder = VOTableEncoder(config, logger)
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


@pytest.mark.asyncio
async def test_unicode_char_variable_arrays() -> None:
    tests = [
        (
            {"name": "text", "datatype": "unicodeChar", "arraysize": "*"},
            "hello",
            b"\x00\x00\x00\x00\x05\x00h\x00e\x00l\x00l\x00o",
        ),
        (
            {"name": "text", "datatype": "unicodeChar", "arraysize": "*"},
            "",
            b"\x00\x00\x00\x00\x00",
        ),
        (
            {"name": "emoji", "datatype": "unicodeChar", "arraysize": "*"},
            "😀",
            b"\x00\x00\x00\x00\x02\xd8=\xde\x00",
        ),
        (
            {"name": "accents", "datatype": "unicodeChar", "arraysize": "*"},
            "café",
            b"\x00\x00\x00\x00\x04\x00c\x00a\x00f\x00\xe9",
        ),
    ]

    for column, data, expected in tests:
        await assert_encoded_value(column, data, expected)


@pytest.mark.asyncio
async def test_unicode_char_fixed_arrays() -> None:
    tests = [
        (
            {"name": "short", "datatype": "unicodeChar", "arraysize": "5"},
            "hi",
            b"\x00\x00h\x00i\x00\x00\x00\x00\x00\x00",
        ),
        (
            {"name": "exact", "datatype": "unicodeChar", "arraysize": "3"},
            "abc",
            b"\x00\x00a\x00b\x00c",
        ),
        (
            {"name": "long", "datatype": "unicodeChar", "arraysize": "3"},
            "hello",
            b"\x00\x00h\x00e\x00l",
        ),
        (
            {"name": "empty", "datatype": "unicodeChar", "arraysize": "2"},
            "",
            b"\x00\x00\x00\x00\x00",
        ),
        (
            {"name": "single", "datatype": "unicodeChar", "arraysize": "3"},
            "A",
            b"\x00\x00A\x00\x00\x00\x00",
        ),
        (
            {
                "name": "split_surrogate",
                "datatype": "unicodeChar",
                "arraysize": "3",
            },
            "AB😀",
            b"\x00\x00A\x00B\x00\x00",
        ),
        (
            {
                "name": "emoji_too_big",
                "datatype": "unicodeChar",
                "arraysize": "1",
            },
            "😀",
            b"\x00\x00\x00",
        ),
    ]

    for column, data, expected in tests:
        await assert_encoded_value(column, data, expected)


@pytest.mark.asyncio
async def test_unicode_char_bounded_variable_arrays() -> None:
    tests = [
        (
            {"name": "short", "datatype": "unicodeChar", "arraysize": "5*"},
            "hi",
            b"\x00\x00\x00\x00\x02\x00h\x00i",
        ),
        (
            {"name": "exact", "datatype": "unicodeChar", "arraysize": "3*"},
            "abc",
            b"\x00\x00\x00\x00\x03\x00a\x00b\x00c",
        ),
        (
            {
                "name": "truncated",
                "datatype": "unicodeChar",
                "arraysize": "3*",
            },
            "hello world",
            b"\x00\x00\x00\x00\x03\x00h\x00e\x00l",
        ),
        (
            {"name": "empty", "datatype": "unicodeChar", "arraysize": "5*"},
            "",
            b"\x00\x00\x00\x00\x00",
        ),
        (
            {
                "name": "emoji_byte_limit",
                "datatype": "unicodeChar",
                "arraysize": "1*",
            },
            "😀",
            b"\x00\x00\x00\x00\x00",
        ),
    ]

    for column, data, expected in tests:
        await assert_encoded_value(column, data, expected)


@pytest.mark.asyncio
async def test_unicode_char_surrogate_pair_truncation() -> None:
    tests = [
        (
            {"name": "safe", "datatype": "unicodeChar", "arraysize": "2"},
            "AB😀C",
            b"\x00\x00A\x00B",
        ),
        (
            {"name": "safe_var", "datatype": "unicodeChar", "arraysize": "2*"},
            "AB😀C",
            b"\x00\x00\x00\x00\x02\x00A\x00B",
        ),
        (
            {
                "name": "emoji_var",
                "datatype": "unicodeChar",
                "arraysize": "1*",
            },
            "😀",
            b"\x00\x00\x00\x00\x00",
        ),
    ]

    for column, data, expected in tests:
        await assert_encoded_value(column, data, expected)
