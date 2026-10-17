"""Models used for talking to BigQuery."""

from typing import Annotated

from pydantic import BaseModel, Field, RootModel

__all__ = [
    "BigQueryUploadField",
    "BigQueryUploadSchema",
]


class BigQueryUploadField(BaseModel):
    """One column in the schema of an uploaded table, as written by TAP."""

    name: Annotated[str, Field(title="Column name")]

    type: Annotated[str, Field(title="BigQuery column type")]


class BigQueryUploadSchema(RootModel[list[BigQueryUploadField]]):
    """Schema of an uploaded table, as written by TAP."""
