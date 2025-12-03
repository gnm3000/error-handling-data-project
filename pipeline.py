from __future__ import annotations

import logging
from pathlib import Path

import polars as pl

from ingestion.transformer import clean
from ingestion.validator import validate_columns

logger = logging.getLogger(__name__)

REQUIRED_SCHEMA = {"id": pl.Utf8, "name": pl.Utf8}


def configure_logging() -> None:
    """Set a simple console logger for the pipeline."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )


def load_clean(path: str = "generation-data/data.json") -> pl.DataFrame:
    """Load the generated JSON file, validate schema, and clean the data."""
    path_obj = Path(path)
    logger.info({"stage": "load", "path": str(path_obj)})

    df = pl.read_json(path_obj)
    logger.info(
        {"stage": "loaded", "rows": df.height, "cols": df.columns, "path": str(path_obj)}
    )

    validate_columns(df, REQUIRED_SCHEMA)
    logger.info({"stage": "validated", "path": str(path_obj)})

    cleaned = clean(df)
    logger.info({"stage": "cleaned", "rows": cleaned.height})
    return cleaned


def main() -> None:
    configure_logging()
    df = load_clean()
    logger.info({"stage": "done", "sample": df.head(3).to_dicts()})


if __name__ == "__main__":
    main()
