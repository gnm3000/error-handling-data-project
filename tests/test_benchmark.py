from __future__ import annotations

import numpy as np
import pandas as pd
import polars as pl
import pytest


ROWS = 20_000
GROUPS = 200


@pytest.fixture(scope="session")
def sample_data() -> tuple[pd.DataFrame, pl.DataFrame]:
    rng = np.random.default_rng(seed=0)
    groups = rng.integers(0, GROUPS, size=ROWS)
    values = rng.normal(loc=0, scale=1, size=ROWS)
    pandas_df = pd.DataFrame({"group": groups, "value": values})
    polars_df = pl.from_pandas(pandas_df)
    return pandas_df, polars_df


def pandas_groupby_sum(df: pd.DataFrame) -> pd.Series:
    return df.groupby("group")["value"].sum()


def polars_groupby_sum(df: pl.DataFrame) -> pl.DataFrame:
    return (
        df.lazy()
        .group_by("group")
        .agg(pl.col("value").sum())
        .collect(streaming=True)
    )


def test_groupby_equivalence(sample_data) -> None:
    pandas_df, polars_df = sample_data
    pandas_result = pandas_groupby_sum(pandas_df)
    polars_result = polars_groupby_sum(polars_df)
    assert len(pandas_result) == polars_result.height


@pytest.mark.benchmark(group="groupby_sum")
def test_pandas_groupby_benchmark(benchmark, sample_data) -> None:
    pandas_df, _ = sample_data
    result = benchmark(pandas_groupby_sum, pandas_df)
    assert len(result) == GROUPS


@pytest.mark.benchmark(group="groupby_sum")
def test_polars_groupby_benchmark(benchmark, sample_data) -> None:
    _, polars_df = sample_data
    result = benchmark(polars_groupby_sum, polars_df)
    assert result.height == GROUPS
