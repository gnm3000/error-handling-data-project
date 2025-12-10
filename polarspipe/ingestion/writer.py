from __future__ import annotations

from pathlib import Path

import polars as pl

FrameLike = pl.DataFrame | pl.LazyFrame


def write_frame(frame: FrameLike, path: str | Path, *, streaming: bool = False) -> Path:
    """
    Persist a Polars frame to disk with minimal branching on extension.

    Parameters:
        frame: DataFrame or LazyFrame to write.
        path: target path; extension chooses the writer.
        streaming: whether to allow Polars streaming execution when supported.
    """
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)

    df = frame
    if isinstance(df, pl.LazyFrame):
        df = df.collect(streaming=streaming)

    suffix = target.suffix.lower()
    if suffix in {".parquet"}:
        df.write_parquet(target)
    elif suffix in {".json", ".ndjson", ".jsonl"}:
        df.write_ndjson(target)
    else:
        df.write_csv(target)

    return target
