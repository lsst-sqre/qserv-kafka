"""Tests for VOTable models."""

from __future__ import annotations

import struct

import pytest
from pydantic import BaseModel, ValidationError

from qservkafka.models.votable import (
    VOTableArraySize,
    VOTablePrimitive,
    VOTableSize,
)


def test_votable_array_size() -> None:
    class TestModel(BaseModel):
        size: VOTableArraySize

    model = TestModel.model_validate({"size": "*"})
    assert model.size == VOTableSize(limit=None, variable=True)
    assert model == TestModel.model_validate(model)
    model = TestModel.model_validate({"size": "10"})
    assert model.size == VOTableSize(limit=10, variable=False)
    model = TestModel.model_validate({"size": "4*"})
    assert model.size == VOTableSize(limit=4, variable=True)

    with pytest.raises(ValidationError):
        TestModel.model_validate({"size": 14})


def test_votable_primitive() -> None:
    class TestModel(BaseModel):
        type: VOTablePrimitive

    model = TestModel.model_validate({"type": "char"})
    assert model.type == VOTablePrimitive.char
    assert model.type.pack("foo") == b"f"
    assert model.model_dump(mode="json") == {"type": "char"}
    model = TestModel.model_validate({"type": "double"})
    assert model.type == VOTablePrimitive.double
    assert model.type.pack(13.71) == struct.pack(">d", 13.71)
    assert model.model_dump(mode="json") == {"type": "double"}


def test_votable_primitive_unicode_char() -> None:
    class TestModel(BaseModel):
        type: VOTablePrimitive

    model = TestModel.model_validate({"type": "unicodeChar"})
    assert model.type == VOTablePrimitive.unicode_char
    assert model.model_dump(mode="json") == {"type": "unicodeChar"}

    result = model.type.pack("h")
    expected = "h".encode("utf-16-be")
    assert result == expected

    result = model.type.pack("hello")
    expected = "h".encode("utf-16-be")
    assert result == expected


def test_votable_primitive_char_vs_unicode_char() -> None:
    char_type = VOTablePrimitive.char
    unicode_type = VOTablePrimitive.unicode_char

    test_char = "h"

    char_result = char_type.pack(test_char)
    unicode_result = unicode_type.pack(test_char)

    assert char_result == struct.pack("c", test_char.encode()[:1])
    assert unicode_result == test_char.encode("utf-16-be")

    assert char_type.pack("hello") == b"h"
    assert unicode_type.pack("hello") == b"\x00h"
