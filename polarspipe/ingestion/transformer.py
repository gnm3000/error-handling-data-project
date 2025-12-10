from __future__ import annotations

import logging
import time

import polars as pl

logger = logging.getLogger(__name__)
SAMPLE_ROWS = 100_000  # sampling cap to keep metrics cheap

FrameLike = pl.DataFrame | pl.LazyFrame


def clean(df: FrameLike) -> pl.LazyFrame:
    frame = df.lazy() if isinstance(df, pl.DataFrame) else df

    t0 = time.perf_counter()

    # Pre-clean metrics sampled to avoid materializing the full dataset.
    sample_before = frame.limit(SAMPLE_ROWS).collect(engine="streaming")
    null_counts = sample_before.null_count().to_dict(as_series=False)
    rows_sample_before = sample_before.height
    name_len_mean = (
        sample_before.select(pl.col("name").str.len_chars().mean()).to_series()[0]
        if rows_sample_before
        else None
    )

    logger.info(
        {
            "stage": "clean_pre",
            "null_counts": null_counts,
            "rows_sampled": rows_sample_before,
            "sample_cap": SAMPLE_ROWS,
            "name_len_mean_sampled": name_len_mean,
        }
    )

    # Apply lazy cleaning across the full source without eager materialization.
    cleaned = (
        frame.drop_nulls()
        .with_columns(
            [
                pl.col("name")
                .str.strip_chars(characters=" \n\r\t")
                .str.replace_all(r"\s+", " ")
                .alias("name"),
                pl.col("id")
                .cast(pl.Utf8)
                .str.strip_chars(characters=" \n\r\t")
                .alias("id"),
            ]
        )
        .filter(pl.col("id") != "")
    )

    # Post-clean metrics computed only on the already collected sample.
    sample_after = (
        sample_before.lazy()
        .drop_nulls()
        .with_columns(
            [
                pl.col("name")
                .str.strip_chars(characters=" \n\r\t")
                .str.replace_all(r"\s+", " ")
                .alias("name"),
                pl.col("id")
                .cast(pl.Utf8)
                .str.strip_chars(characters=" \n\r\t")
                .alias("id"),
            ]
        )
        .filter(pl.col("id") != "")
        .collect()
    )
    rows_sample_after = sample_after.height
    name_len_mean_after = (
        sample_after.select(pl.col("name").str.len_chars().mean()).to_series()[0]
        if rows_sample_after
        else None
    )
    duration_ms = (time.perf_counter() - t0) * 1000

    logger.info(
        {
            "stage": "clean_post_schema",
            "schema": cleaned.collect_schema(),
            "rows_sampled": rows_sample_after,
            "sample_cap": SAMPLE_ROWS,
            "name_len_mean_sampled": name_len_mean_after,
            "duration_ms": duration_ms,
        }
    )

    return cleaned
