import polars as pl
from .exceptions import InvalidSchemaError

def validate_columns(df: pl.DataFrame, required: dict):
    """
    required = {"col1": pl.Int64, "col2": pl.Utf8}
    """
    for col, dtype in required.items():
        if col not in df.columns:
            raise InvalidSchemaError(f"Missing column: {col}")
        if df[col].dtype != dtype:
            raise InvalidSchemaError(f"Invalid dtype for {col}: {df[col].dtype}")
    return True
