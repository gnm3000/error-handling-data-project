import logging

import pandas as pd
import polars as pl

from .exceptions import CorruptedFileError, FileNotFoundError, InvalidSchemaError

logger = logging.getLogger(__name__)


def read_csv(path: str) -> pl.DataFrame:
    """Read a CSV file into a Polars DataFrame."""
    logger.info({"stage": "read_csv", "path": path})
    try:
        return pl.read_csv(path)
    except pl.exceptions.ComputeError as e:
        logger.warning({"fallback": "pandas", "reason": str(e)})
    try:
        df = pd.read_csv(path)
        return pl.from_pandas(df)
    except FileNotFoundError:
        raise FileNotFoundError(f"File not found: {path}")
    except pd.errors.ParserError:
        raise CorruptedFileError(f"Corrupted CSV: {path}")
    except Exception as e2:
        raise InvalidSchemaError(f"Invalid schema: {e2}")
