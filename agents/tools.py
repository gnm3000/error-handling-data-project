from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Iterable

import polars as pl
from langchain_core.tools import tool

from ingestion.reader import scan_file

logger = logging.getLogger(__name__)


def _resolve_output_path(input_path: Path, output_path: Path | None) -> Path:
    if output_path is not None:
        return output_path

    suffix = input_path.suffix or ".parquet"
    default_name = f"{input_path.stem}_agent_output{suffix}"
    return input_path.with_name(default_name)


def _write_frame(frame: pl.DataFrame, path: Path) -> None:
    suffix = path.suffix.lower()
    if suffix in {".parquet", ".pq"}:
        frame.write_parquet(path)
    elif suffix == ".csv":
        frame.write_csv(path)
    elif suffix in {".ndjson", ".jsonl"}:
        frame.write_ndjson(path)
    else:
        fallback = path.with_suffix(".parquet")
        logger.info(
            {
                "stage": "writer_unknown_extension",
                "requested": str(path),
                "fallback": str(fallback),
            }
        )
        frame.write_parquet(fallback)


def run_polars_etl(
    *,
    input_path: str,
    columns: Iterable[str],
    filter_expression: str | None = None,
    output_path: str | None = None,
    limit: int | None = None,
) -> dict[str, str | int | float | list[str] | None]:
    source = Path(input_path)
    destination = _resolve_output_path(source, Path(output_path) if output_path else None)

    lf = scan_file(source)

    if columns:
        lf = lf.select([pl.col(c) for c in columns])

    if filter_expression:
        lf = lf.filter(pl.sql_expr(filter_expression))

    if limit:
        lf = lf.limit(limit)

    result = lf.collect()
    _write_frame(result, destination)

    summary = {
        "rows": result.height,
        "columns": result.columns,
        "output_path": str(destination),
    }
    logger.info({"stage": "agent_etl_complete", **summary})
    return summary


@tool
def execute_polars_etl(
    input_path: str,
    columns: list[str],
    filter_expression: str | None = None,
    output_path: str | None = None,
    limit: int | None = None,
) -> str:
    """
    Execute a concise projection/filter/write flow with Polars.

    Args:
        input_path: input file path (CSV/Parquet/NDJSON/JSON).
        columns: columns to keep in the output.
        filter_expression: Polars SQL-like expression for row filtering (e.g. "X > 0").
        output_path: output path. Defaults to <input>_agent_output.<ext>.
        limit: optional max rows, useful for quick checks.
    """

    result = run_polars_etl(
        input_path=input_path,
        columns=columns,
        filter_expression=filter_expression,
        output_path=output_path,
        limit=limit,
    )
    return json.dumps(result)


@tool
def run_python_in_sandbox(
    code: str,
    requirements: list[str] | None = None,
    template: str = "python3",
) -> str:
    """
    Execute Python code inside an E2B sandbox. Use this tool for auxiliary snippets.
    Requires the E2B_API_KEY environment variable.
    """

    api_key = os.getenv("E2B_API_KEY")
    if not api_key:
        return "E2B_API_KEY is not configured; skipping E2B execution."

    try:
        from e2b import Sandbox
        from e2b.exceptions import SandboxException
    except Exception as exc:  # pragma: no cover - import guard
        return f"Could not initialize E2B: {exc}"

    sandbox = Sandbox.create(template=template, allow_internet_access=True)

    try:
        if requirements:
            sandbox.commands.run(
                f"pip install {' '.join(requirements)}", timeout=120, request_timeout=120
            )

        command = "python - <<'PY'\n" + code + "\nPY"
        result = sandbox.commands.run(command, timeout=180, request_timeout=180)

        payload = {
            "exit_code": result.exit_code,
            "stdout": result.stdout,
            "stderr": result.stderr,
            "error": result.error,
        }
        if result.exit_code != 0:
            raise SandboxException(result.stderr or "Sandbox execution failed")

        return json.dumps(payload)
    except Exception as exc:  # pragma: no cover - runtime guard
        logger.error({"stage": "sandbox_error", "error": str(exc)})
        return f"Error running code in sandbox: {exc}"
    finally:
        try:
            sandbox.kill()
        except Exception:
            logger.debug("Sandbox cleanup failed", exc_info=True)
