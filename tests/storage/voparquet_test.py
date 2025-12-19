"""Test the Parquet encoder."""

from __future__ import annotations

import tempfile
from collections.abc import AsyncGenerator
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from rubin.repertoire import DiscoveryClient
from sqlalchemy import Row
from structlog.stdlib import get_logger

from qservkafka.models.kafka import (
    JobResultColumnType,
    JobResultConfig,
    JobResultEnvelope,
    JobResultFormat,
    JobResultSerialization,
    JobResultType,
)
from qservkafka.storage.votable import VOParquetEncoder


async def data_generator(
    data: list[tuple[Any, ...]],
) -> AsyncGenerator[Row[Any] | tuple[Any]]:
    """Turn a list of rows into an async generator of rows."""
    for row in data:
        yield row


async def assert_parquet_data(
    columns: list[dict[str, Any]],
    data: list[tuple[Any, ...]],
    expected_rows: int,
    expected_columns: list[str],
    expected_types: dict[str, str] | None = None,
    *,
    overflow: bool = False,
) -> pa.Table:
    """Encode data to Parquet and check the result.

    Parameters
    ----------
    columns
        Column type definitions.
    data
        Input rows.
    expected_rows
        Expected number of rows.
    expected_columns
        Expected columns.
    expected_types
        Expected types (optional).
    overflow
        Whether to simulate an overflow condition.
    """
    config = JobResultConfig(
        format=JobResultFormat(
            type=JobResultType.VOTable,
            serialization=JobResultSerialization.BINARY2,
        ),
        column_types=[
            JobResultColumnType.model_validate(col) for col in columns
        ],
        envelope=JobResultEnvelope(
            header='<?xml version="1.0"?><VOTABLE><RESOURCE><TABLE>'
            '<FIELD name="test" datatype="int"/>',
            footer="</TABLE></RESOURCE></VOTABLE>",
            footer_overflow="</TABLE>"
            '<INFO name="QUERY_STATUS" value="OVERFLOW"/>'
            "</RESOURCE></VOTABLE>",
        ),
    )

    local_logger = get_logger(__name__)
    encoder = VOParquetEncoder(
        config, DiscoveryClient(), local_logger, overflow=overflow
    )

    parquet_data = b""
    async for chunk in encoder.encode(data_generator(data)):
        parquet_data += chunk

    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
        tmp.write(parquet_data)
        temp_path = Path(tmp.name)

    try:
        table = pq.read_table(temp_path)

        assert len(table) == expected_rows
        assert list(table.column_names) == expected_columns

        if expected_types:
            for col, expected_type in expected_types.items():
                actual_type = str(table.column(col).type)
                assert actual_type == expected_type, (
                    f"Column {col}: expected {expected_type}, "
                    f"got {actual_type}"
                )

        parquet_file = pq.ParquetFile(temp_path)
        metadata = parquet_file.metadata.metadata

        assert b"IVOA.VOTable-Parquet.version" in metadata, (
            "Missing VOParquet version"
        )
        assert metadata[b"IVOA.VOTable-Parquet.version"].decode() == "1.0", (
            "Wrong VOParquet version"
        )
        assert b"IVOA.VOTable-Parquet.content" in metadata, (
            "Missing VOParquet content"
        )

        votable_xml = metadata[b"IVOA.VOTable-Parquet.content"].decode()
        assert "<FIELD" in votable_xml, (
            "VOTable metadata missing field definitions"
        )
        assert "<DATA>" not in votable_xml, (
            "VOTable metadata should not contain data"
        )

        return table
    finally:
        temp_path.unlink()


@pytest.mark.asyncio
async def test_basic_types() -> None:
    columns = [
        {"name": "id", "datatype": "int"},
        {"name": "value", "datatype": "double"},
        {"name": "name", "datatype": "char"},
        {"name": "flag", "datatype": "boolean"},
    ]

    data = [
        (1, 1.1, "test", True),
        (2, 2.2, "ra", False),
        (3, 3.3, "dec", True),
    ]

    df = await assert_parquet_data(
        columns,
        data,
        expected_rows=3,
        expected_columns=["id", "value", "name", "flag"],
        expected_types={
            "id": "int32",
            "value": "double",
            "name": "string",
            "flag": "bool",
        },
    )

    assert df["id"][0].as_py() == 1
    assert df["value"][0].as_py() == 1.1
    assert df["name"][0].as_py() == "test"
    assert df["flag"][0].as_py() is True

    assert df["id"][2].as_py() == 3
    assert df["name"][2].as_py() == "dec"


@pytest.mark.asyncio
async def test_null_values() -> None:
    columns = [
        {"name": "id", "datatype": "int"},
        {"name": "text", "datatype": "char"},
        {"name": "num", "datatype": "double"},
    ]

    data = [
        (1, "ra", 1.0),
        (None, "dec", None),
        (3, None, 3.0),
    ]

    table = await assert_parquet_data(
        columns, data, expected_rows=3, expected_columns=["id", "text", "num"]
    )

    assert not table["id"][1].is_valid
    assert not table["num"][1].is_valid
    assert not table["text"][2].is_valid

    assert table["id"][0].is_valid
    assert table["id"][2].is_valid


@pytest.mark.asyncio
async def test_empty_result() -> None:
    columns = [
        {"name": "id", "datatype": "int"},
        {"name": "obj_name", "datatype": "char"},
    ]

    data: list[tuple[Any]] = []

    await assert_parquet_data(
        columns, data, expected_rows=0, expected_columns=["id", "obj_name"]
    )


@pytest.mark.asyncio
async def test_single_row() -> None:
    columns = [
        {"name": "value", "datatype": "double"},
    ]

    data = [(1.0,)]

    table = await assert_parquet_data(
        columns, data, expected_rows=1, expected_columns=["value"]
    )

    assert table["value"][0].as_py() == 1.0


@pytest.mark.asyncio
async def test_large_strings() -> None:
    columns = [
        {"name": "obj_id", "datatype": "int"},
        {"name": "obj_name", "datatype": "char"},
    ]

    large_string = "z" * 10000
    data = [
        (1, "short"),
        (2, large_string),
        (3, "normal"),
    ]

    table = await assert_parquet_data(
        columns, data, expected_rows=3, expected_columns=["obj_id", "obj_name"]
    )
    assert table["obj_name"][1].as_py() == large_string
    assert len(table["obj_name"][1].as_py()) == 10000


@pytest.mark.asyncio
async def test_different_numeric_types() -> None:
    """Test different numeric VOTable types."""
    columns = [
        {"name": "small", "datatype": "short"},
        {"name": "medium", "datatype": "int"},
        {"name": "large", "datatype": "long"},
        {"name": "double_val", "datatype": "double"},
        {"name": "float_val", "datatype": "float"},
    ]

    data = [
        (100, 50000, 9999999999, 1.12345, 2.5),
        (-100, -50000, -9999999999, -1.23456789, -1.5),
    ]

    table = await assert_parquet_data(
        columns,
        data,
        expected_rows=2,
        expected_columns=[
            "small",
            "medium",
            "large",
            "double_val",
            "float_val",
        ],
    )

    assert abs(table["double_val"][0].as_py() - 1.12345) < 1e-15
    assert table["large"][1].as_py() == -9999999999


@pytest.mark.asyncio
async def test_maxrec_limit() -> None:
    columns = [
        {"name": "obj_id", "datatype": "int"},
        {"name": "obj_name", "datatype": "char"},
    ]

    data = [(i, f"row_{i}") for i in range(10)]

    config = JobResultConfig(
        format=JobResultFormat(
            type=JobResultType.VOTable,
            serialization=JobResultSerialization.BINARY2,
        ),
        column_types=[
            JobResultColumnType.model_validate(col) for col in columns
        ],
        envelope=JobResultEnvelope(
            header='<?xml version="1.0"?><VOTABLE><RESOURCE><TABLE>'
            '<FIELD name="obj_id" datatype="int"/><FIELD name="obj_name" '
            'datatype="char"/>',
            footer="</TABLE></RESOURCE></VOTABLE>",
            footer_overflow="</TABLE>"
            '<INFO name="QUERY_STATUS" value="OVERFLOW"/>'
            "</RESOURCE></VOTABLE>",
        ),
    )

    local_logger = get_logger(__name__)
    encoder = VOParquetEncoder(
        config, DiscoveryClient(), local_logger, overflow=True
    )

    parquet_data = b""
    async for chunk in encoder.encode(data_generator(data), maxrec=3):
        parquet_data += chunk

    with tempfile.NamedTemporaryFile(suffix=".parquet") as tmp:
        tmp.write(parquet_data)
        tmp.flush()
        table = pq.read_table(tmp.name)

        assert len(table) == 3
        assert table["obj_id"][0].as_py() == 0
        assert table["obj_id"][2].as_py() == 2

        parquet_file = pq.ParquetFile(tmp.name)
        metadata = parquet_file.metadata.metadata
        votable_xml = metadata[b"IVOA.VOTable-Parquet.content"].decode()
        assert "OVERFLOW" in votable_xml, (
            "Expected overflow marker in VOTable metadata"
        )


@pytest.mark.asyncio
async def test_encoder_properties() -> None:
    columns = [{"name": "obj_id", "datatype": "int"}]
    data = [(i,) for i in range(5)]

    config = JobResultConfig(
        format=JobResultFormat(
            type=JobResultType.VOTable,
            serialization=JobResultSerialization.BINARY2,
        ),
        column_types=[
            JobResultColumnType.model_validate(col) for col in columns
        ],
        envelope=JobResultEnvelope(
            header="<test>", footer="</test>", footer_overflow="</test>"
        ),
    )

    local_logger = get_logger(__name__)
    encoder = VOParquetEncoder(config, DiscoveryClient(), local_logger)

    chunks = [chunk async for chunk in encoder.encode(data_generator(data))]

    assert encoder.total_rows == 5
    assert encoder.encoded_size > 0

    total_size = sum(len(chunk) for chunk in chunks)
    assert encoder.encoded_size == total_size
