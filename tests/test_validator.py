import polars as pl
import pytest
from ingestion.validator import validate_columns
from ingestion.exceptions import InvalidSchemaError

def test_valid_schema():
    df = pl.DataFrame({"id": [1,2], "name": ["a","b"]})
    assert validate_columns(df, {"id": pl.Int64, "name": pl.Utf8})

def test_missing_column():
    df = pl.DataFrame({"id": [1]})
    with pytest.raises(InvalidSchemaError):
        validate_columns(df, {"id": pl.Int64, "name": pl.Utf8})
