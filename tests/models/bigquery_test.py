"""Tests for BigQuery type mappings."""

from __future__ import annotations

import pytest

from qservkafka.models.bigquery import BIGQUERY_TO_VOTABLE, get_votable_type
from qservkafka.models.votable import VOTablePrimitive


def test_type_mappings() -> None:
    assert BIGQUERY_TO_VOTABLE["INTEGER"] == VOTablePrimitive.long
    assert BIGQUERY_TO_VOTABLE["FLOAT"] == VOTablePrimitive.double
    assert BIGQUERY_TO_VOTABLE["NUMERIC"] == VOTablePrimitive.double
    assert BIGQUERY_TO_VOTABLE["BOOL"] == VOTablePrimitive.boolean
    assert BIGQUERY_TO_VOTABLE["BOOLEAN"] == VOTablePrimitive.boolean
    assert BIGQUERY_TO_VOTABLE["STRING"] == VOTablePrimitive.char
    assert BIGQUERY_TO_VOTABLE["JSON"] == VOTablePrimitive.char
    assert BIGQUERY_TO_VOTABLE["BYTES"] == VOTablePrimitive.char
    assert BIGQUERY_TO_VOTABLE["DATE"] == VOTablePrimitive.char
    assert BIGQUERY_TO_VOTABLE["TIME"] == VOTablePrimitive.char
    assert BIGQUERY_TO_VOTABLE["TIMESTAMP"] == VOTablePrimitive.char
    assert BIGQUERY_TO_VOTABLE["GEOGRAPHY"] == VOTablePrimitive.char
    assert get_votable_type("INT64") == VOTablePrimitive.long
    assert get_votable_type("STRING") == VOTablePrimitive.char
    assert get_votable_type("BOOL") == VOTablePrimitive.boolean
    assert get_votable_type("FLOAT64") == VOTablePrimitive.double
    assert get_votable_type("int64") == VOTablePrimitive.long
    assert get_votable_type("INT64") == VOTablePrimitive.long
    assert get_votable_type("string") == VOTablePrimitive.char
    assert get_votable_type("STRING") == VOTablePrimitive.char
    assert get_votable_type("  INT64  ") == VOTablePrimitive.long
    assert get_votable_type("\tSTRING\n") == VOTablePrimitive.char
    assert get_votable_type("NUMERIC(10, 2)") == VOTablePrimitive.double
    assert get_votable_type("STRING(100)") == VOTablePrimitive.char


def test_get_votable_type_complex_types_not_supported() -> None:
    with pytest.raises(ValueError, match="not supported"):
        get_votable_type("ARRAY")


def test_get_votable_type_complex_types_case_insensitive() -> None:
    with pytest.raises(ValueError, match="not supported"):
        get_votable_type("Array")


def test_get_votable_type_unknown_type() -> None:
    with pytest.raises(ValueError, match="Unknown BigQuery type"):
        get_votable_type("UNKNOWN_TYPE")


def test_get_votable_type_error_lists_supported_types() -> None:
    with pytest.raises(ValueError, match="Unknown BigQuery type"):
        get_votable_type("INVALID")


def test_type_mapping_consistency() -> None:
    assert BIGQUERY_TO_VOTABLE["INT64"] == BIGQUERY_TO_VOTABLE["INTEGER"]
    assert BIGQUERY_TO_VOTABLE["FLOAT64"] == BIGQUERY_TO_VOTABLE["FLOAT"]
    assert BIGQUERY_TO_VOTABLE["BOOL"] == BIGQUERY_TO_VOTABLE["BOOLEAN"]
