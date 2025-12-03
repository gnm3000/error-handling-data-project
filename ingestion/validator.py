from __future__ import annotations

from typing import Mapping

import polars as pl
from polars.datatypes import DataType, DataTypeClass

from .exceptions import InvalidSchemaError

PolarsType = DataType | DataTypeClass


def validate_columns(df: pl.DataFrame, required: Mapping[str, PolarsType]) -> bool:
    """Ensure the DataFrame has required columns with expected dtypes."""
    for col, dtype in required.items():
        if col not in df.columns:
            raise InvalidSchemaError(f"Missing column: {col}")
        if df[col].dtype != dtype:
            raise InvalidSchemaError(f"Invalid dtype for {col}: {df[col].dtype}")
    return True
