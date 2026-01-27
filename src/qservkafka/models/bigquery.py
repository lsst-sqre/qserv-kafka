"""BigQuery-specific data models and type mappings."""

from __future__ import annotations

from ..models.votable import VOTablePrimitive

__all__ = ["BIGQUERY_TO_VOTABLE"]


# Type mapping from BigQuery types to VOTable datatypes
# https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types
BIGQUERY_TO_VOTABLE: dict[str, VOTablePrimitive] = {
    "INT64": VOTablePrimitive.long,
    "INTEGER": VOTablePrimitive.long,
    "NUMERIC": VOTablePrimitive.double,
    "BIGNUMERIC": VOTablePrimitive.double,
    "FLOAT64": VOTablePrimitive.double,
    "FLOAT": VOTablePrimitive.double,
    "BOOL": VOTablePrimitive.boolean,
    "BOOLEAN": VOTablePrimitive.boolean,
    "STRING": VOTablePrimitive.char,
    "BYTES": VOTablePrimitive.char,
    "DATE": VOTablePrimitive.char,
    "DATETIME": VOTablePrimitive.char,
    "TIME": VOTablePrimitive.char,
    "TIMESTAMP": VOTablePrimitive.char,
    "GEOGRAPHY": VOTablePrimitive.char,
    "JSON": VOTablePrimitive.char,
}


def get_votable_type(bigquery_type: str) -> VOTablePrimitive:
    """Convert a BigQuery type to a VOTable primitive type.

    Parameters
    ----------
    bigquery_type
        BigQuery type name

    Returns
    -------
    VOTablePrimitive
        VOTable primitive type

    Raises
    ------
    ValueError
        If the BigQuery type is not supported

    Notes
    -----
    Complex types (STRUCT, ARRAY) are not supported and will raise an error.
    """
    normalized_type = bigquery_type.upper().strip()

    # Complex types not supported for now
    if normalized_type in ("STRUCT", "ARRAY", "RECORD"):
        raise ValueError(
            f"Complex BigQuery type '{bigquery_type}' is not supported. "
        )

    if normalized_type in BIGQUERY_TO_VOTABLE:
        return BIGQUERY_TO_VOTABLE[normalized_type]

    # Parameterized types (e.g., "NUMERIC(10, 2)")
    try:
        base_type = normalized_type.split("(")[0].strip()
        if base_type in BIGQUERY_TO_VOTABLE:
            return BIGQUERY_TO_VOTABLE[base_type]
    except Exception as e:
        raise ValueError(
            f"Failed to parse BigQuery type '{bigquery_type}': {e}"
        ) from e

    raise ValueError(
        f"Unknown BigQuery type '{bigquery_type}'. "
        "Supported types: " + ", ".join(sorted(BIGQUERY_TO_VOTABLE.keys()))
    )
