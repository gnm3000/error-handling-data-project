from __future__ import annotations

import cProfile
import pstats
import time
import logging
import os
from pathlib import Path

import polars as pl

from ingestion.reader import scan_file
from ingestion.transformer import clean
from ingestion.validator import validate_columns
from ingestion.exceptions import InvalidSchemaError   # opcional pero recomendado


logger = logging.getLogger(__name__)
_warned_memory = False

try:
    from memory_profiler import memory_usage as _mem_usage
except ImportError:
    _mem_usage = None

REQUIRED_SCHEMA = {
    "id": pl.Utf8,
    "name": pl.Utf8,
}


def _measure_memory_mb() -> float | None:
    """
    Lightweight check of current process memory in MB.
    Uses memory_profiler if available; falls back to psutil.
    """
    global _warned_memory
    if _mem_usage:
        usage = _mem_usage(max_iterations=1, interval=0.05, include_children=True)
        if usage:
            return usage[0]

    try:
        import psutil

        process = psutil.Process(os.getpid())
        return process.memory_info().rss / (1024 * 1024)
    except Exception:
        if not _warned_memory:
            logger.warning(
                {
                    "stage": "profiling",
                    "warning": "memory profiling unavailable (memory_profiler/psutil missing)",
                }
            )
            _warned_memory = True
        return None


def profile_pipeline(fn, *args, **kwargs):
    profiler = cProfile.Profile()
    mem_before = _measure_memory_mb()
    profiler.enable()
    t0 = time.perf_counter()

    result = fn(*args, **kwargs)

    profiler.disable()
    duration_ms = (time.perf_counter() - t0) * 1000
    mem_after = _measure_memory_mb()

    stats = pstats.Stats(profiler).sort_stats("tottime")
    stats.dump_stats("profile.pstats")

    logger.info(
        {
            "stage": "profiling",
            "duration_ms": duration_ms,
            "memory_mb_before": mem_before,
            "memory_mb_after": mem_after,
            "memory_mb_diff": (
                mem_after - mem_before
                if mem_before is not None and mem_after is not None
                else None
            ),
            "profile_path": "profile.pstats",
        }
    )

    return result


def configure_logging(level: int = logging.INFO) -> None:
    """
    Configure console logging for the ETL pipeline.
    Idempotent: calling this multiple times has no effect.
    """
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        force=False,
    )


def load_clean(path: str | Path = "generation-data/large/data_large.ndjson") -> pl.LazyFrame:
    """
    1. Lazily scan the file.
    2. Validate required schema without materializing data.
    3. Apply cleaning transforms.
    4. Return LazyFrame (fully lazy until .collect()).
    """
    p = Path(path)
    t0 = time.perf_counter()
    logger.info({"stage": "load_start", "path": str(p)})

    try:
        lf = scan_file(p)
    except Exception as e:
        logger.error({"stage": "scan_error", "path": str(p), "error": str(e)})
        raise

    # validate_columns does NOT materialize LazyFrame → safe
    try:
        validate_columns(lf, REQUIRED_SCHEMA)
    except InvalidSchemaError as e:
        logger.error({
            "stage": "schema_invalid",
            "path": str(p),
            "error": str(e),
            "expected": REQUIRED_SCHEMA,
            "schema": lf.collect_schema() if isinstance(lf, pl.LazyFrame) else lf.schema,
        })
        raise

    logger.info({
        "stage": "schema_valid",
        "schema": lf.collect_schema(),
    })

    cleaned = clean(lf)
    duration_ms = (time.perf_counter() - t0) * 1000
    logger.info({"stage": "clean_applied", "duration_ms": duration_ms})

    return cleaned


def main() -> None:
    configure_logging()

    lazy_frame = profile_pipeline(load_clean)

    # Actualización: streaming=True funciona solo cuando es soportado por el plan de ejecución.
    sample = lazy_frame.limit(3).collect(engine="streaming")

    logger.info({
        "stage": "done",
        "sample": sample.to_dicts(),
        "rows_returned": sample.height,
    })


if __name__ == "__main__":
    main()
