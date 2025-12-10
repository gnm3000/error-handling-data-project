import polars as pl
import pytest

from polarspipe.ingestion.exceptions import InvalidSchemaError
from polarspipe.ingestion.validator import validate_columns


def test_valid_schema() -> None:
    df = pl.DataFrame({"id": ["1", "2"], "name": ["a", "b"]})
    assert validate_columns(df, {"id": pl.Utf8, "name": pl.Utf8})


def test_missing_column() -> None:
    df = pl.DataFrame({"id": ["1"]})
    with pytest.raises(InvalidSchemaError):
        validate_columns(df, {"id": pl.Utf8, "name": pl.Utf8})


def test_lazy_schema_validation() -> None:
    df = pl.DataFrame({"id": ["1", "2"], "name": ["a", "b"]}).lazy()
    assert validate_columns(df, {"id": pl.Utf8, "name": pl.Utf8})
