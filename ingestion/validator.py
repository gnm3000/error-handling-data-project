from __future__ import annotations

from typing import Mapping

import polars as pl
from polars.datatypes import DataType

from .exceptions import InvalidSchemaError

FrameLike = pl.DataFrame | pl.LazyFrame


def _get_schema(frame: FrameLike) -> Mapping[str, DataType]:
    """
    Unified schema extractor.
    LazyFrame: uses collect_schema() (does not materialize data).
    DataFrame: uses .schema directly.
    """
    if isinstance(frame, pl.LazyFrame):
        return frame.collect_schema()
    return frame.schema


def validate_columns(
    df: FrameLike,
    required: Mapping[str, DataType],
    *,
    allow_subtypes: bool = False,
) -> bool:
    """
    Validate presence + dtype compatibility of required columns.

    Parameters:
        df: DataFrame | LazyFrame.
        required: mapping {column_name: Polars DataType}.
        allow_subtypes: 
            If True, allows logical subtype compatibility (e.g., Int8 ≈ Int64 upgrade).

    Raises:
        InvalidSchemaError: missing or incompatible schema definitions.

    Returns:
        True on valid schema.
    """
    schema = _get_schema(df)

    for col, expected in required.items():
        # 1. Columna faltante
        if col not in schema:
            raise InvalidSchemaError(f"Missing column '{col}'.")

        actual = schema[col]

        # 2. Tipos estrictos
        if not allow_subtypes and actual != expected:
            raise InvalidSchemaError(
                f"Invalid dtype for '{col}': expected {expected}, got {actual}."
            )

        # 3. Tipos compatibles (opcional)
        if allow_subtypes and not _is_dtype_compatible(actual, expected):
            raise InvalidSchemaError(
                f"Incompatible dtype for '{col}': expected {expected}, got {actual}."
            )

    return True


def _is_dtype_compatible(actual: DataType, expected: DataType) -> bool:
    """
    Define a minimal compatibility rule-set.
    Extend if your domain requires coercions between numeric widths.

    Example heuristics:
        · Int8 / Int16 / Int32 / Int64 are mutually compatible.
        · Utf8 is only compatible with Utf8.
        · Boolean is exact.
    """
    # Ética: explicitación > magia.
    numeric_types = {pl.Int8, pl.Int16, pl.Int32, pl.Int64,
                     pl.UInt8, pl.UInt16, pl.UInt32, pl.UInt64,
                     pl.Float32, pl.Float64}

    # Caso exacto
    if actual == expected:
        return True

    # Caso numérico → numérico
    if actual in numeric_types and expected in numeric_types:
        return True

    return False
