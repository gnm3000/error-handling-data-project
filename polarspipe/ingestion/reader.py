from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Callable

import pandas as pd
import polars as pl

from .exceptions import (
    CorruptedFileError,
    IngestionFileNotFound,
    IngestionMemoryError,
    InvalidSchemaError,
)

logger = logging.getLogger(__name__)


def _file_size_mb(path: Path) -> float:
    return os.path.getsize(path) / (1024 * 1024)


def _assert_file_exists(path: Path) -> Path:
    if not path.exists():
        raise IngestionFileNotFound(f"File does not exist: {path}")
    return path


def _read_csv_fallback(path: Path) -> pl.LazyFrame:
    """
    pandas fallback for malformed CSVs.
    WARNING: eager operation -> big memory hit for 1GB+ files.
    """

    size_mb = _file_size_mb(path)
    if size_mb > 500:
        raise IngestionMemoryError(
            f"Refusing pandas fallback for huge CSV ({size_mb:.2f} MB)."
        )

    try:
        df = pd.read_csv(path, engine="python")
        return pl.from_pandas(df).lazy()
    except pd.errors.ParserError as e:
        raise CorruptedFileError(f"CSV corrupted: {path}") from e
    except Exception as e:
        raise InvalidSchemaError(f"Fallback CSV schema invalid: {e}") from e


def read_csv(path: str | Path) -> pl.LazyFrame:
    p = _assert_file_exists(Path(path))

    logger.info(
        {
            "stage": "read_csv",
            "path": str(p),
            "method": "polars.scan_csv",
            "size_mb": _file_size_mb(p),
        }
    )

    try:
        return pl.scan_csv(p)
    except pl.exceptions.ComputeError as e:
        logger.warning(
            {
                "stage": "read_csv_fallback",
                "path": str(p),
                "error": str(e),
                "fallback": "pandas",
            }
        )
        return _read_csv_fallback(p)


ReaderFn = Callable[[Path], pl.LazyFrame]


def scan_file(path: str | Path) -> pl.LazyFrame:
    """
    Router for dataset formats, lazy when possible.
    """
    p = _assert_file_exists(Path(path))
    suffix = p.suffix.lower()

    readers: dict[str, ReaderFn] = {
        ".ndjson": pl.scan_ndjson,
        ".jsonl": pl.scan_ndjson,
        ".csv": read_csv,
        ".parquet": pl.scan_parquet,
    }

    if suffix in readers:
        try:
            logger.info(
                {
                    "stage": "scan_file",
                    "path": str(p),
                    "method": readers[suffix].__name__,
                }
            )
            return readers[suffix](p)
        except Exception as e:
            logger.error({"stage": "reader_error", "error": str(e), "path": str(p)})
            raise

    logger.info(
        {
            "stage": "scan_file_json_fallback",
            "path": str(p),
            "method": "read_json -> .lazy()",
        }
    )

    try:
        df = pl.read_json(p)
        return df.lazy()
    except Exception as e:
        raise InvalidSchemaError(f"Invalid JSON format: {e}") from e
