"""Tests for VOTable models."""

from __future__ import annotations

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
    assert model.type.pack == "1s"
    assert model.model_dump(mode="json") == {"type": "char"}
    model = TestModel.model_validate({"type": "double"})
    assert model.type == VOTablePrimitive.double
    assert model.type.pack == ">d"
    assert model.model_dump(mode="json") == {"type": "double"}
