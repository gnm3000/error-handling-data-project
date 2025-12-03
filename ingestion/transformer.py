from __future__ import annotations

import polars as pl


def clean(df: pl.DataFrame) -> pl.DataFrame:
    """Basic cleanup: drop nulls, normalize name, and ensure id is text."""
    return df.drop_nulls().with_columns(
        [
            pl.col("name").str.strip_chars().alias("name"),
            pl.col("id").cast(pl.Utf8),
        ]
    )
