from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Dict, List

from e2b import Sandbox

DEFAULT_OUTPUT_PATH = "outputs/output.parquet"


def parse_etl_instruction(instruction: str) -> Dict[str, Any]:
    """
    Lightweight heuristic parser that extracts obvious fields from the prompt.
    A LangGraph node will refine this into a strict JSON spec with the LLM.
    """
    base: Dict[str, Any] = {
        "instruction_raw": instruction,
        "input_path": None,
        "output_path": DEFAULT_OUTPUT_PATH,
        "columns": [],
        "filters": [],
        "filters_raw": None,
    }

    path_match = re.search(r"`([^`]+)`", instruction)
    if not path_match:
        path_match = re.search(
            r"(?:file|from)\s+([\w./\\-]+\.(?:csv|json|ndjson|parquet))",
            instruction,
            flags=re.IGNORECASE,
        )
    if path_match:
        base["input_path"] = path_match.group(1).strip()

    columns_match = re.search(
        r"columns?\s+([\w,\s]+)", instruction, flags=re.IGNORECASE
    )
    if columns_match:
        cols = [c.strip() for c in re.split(r"[,\\s]+", columns_match.group(1)) if c.strip()]
        base["columns"] = [c for c in cols if c]

    filter_match = re.search(r"(?:where|filter)\s+([^;]+)", instruction, flags=re.IGNORECASE)
    if filter_match:
        base["filters_raw"] = filter_match.group(1).strip()

    output_match = re.search(r"\bsave (?:as|to)\s+([^\s]+)", instruction, flags=re.IGNORECASE)
    if output_match:
        base["output_path"] = output_match.group(1).strip()

    return base


def _render_filter(filter_spec: Dict[str, Any]) -> str:
    column = filter_spec.get("column")
    op = filter_spec.get("op", "==")
    value = filter_spec.get("value")

    if column is None:
        return "pl.lit(True)"

    op_normalized = str(op).lower()
    allowed_binary = {"==", "!=", ">", ">=", "<", "<=", "in", "not in"}
    allowed_string = {"contains", "startswith", "endswith"}

    if op_normalized in allowed_binary:
        if op_normalized in {"in", "not in"}:
            if isinstance(value, (list, tuple, set)):
                return f"pl.col({column!r}).is_in({list(value)!r})"
            return f"pl.col({column!r}).is_in([{value!r}])"
        return f"pl.col({column!r}) {op} {value!r}"

    if op_normalized in allowed_string:
        return f"pl.col({column!r}).str.{op_normalized}({value!r})"

    return "pl.lit(True)"


def generate_polars_code(spec: Dict[str, Any]) -> str:
    input_path = spec.get("input_path") or "data.json"
    output_path = spec.get("output_path") or DEFAULT_OUTPUT_PATH
    columns: List[str] = spec.get("columns") or []
    filters: List[Dict[str, Any]] = spec.get("filters") or []
    limit = spec.get("limit")
    fmt = (spec.get("format") or "auto").lower()

    filter_lines = "\n".join(
        [f"    exprs.append({_render_filter(fspec)})" for fspec in filters]
    )

    code = f"""
import json
from pathlib import Path

import polars as pl

INPUT_PATH = {input_path!r}
OUTPUT_PATH = {output_path!r}
COLUMNS = {columns!r}
LIMIT = {limit if limit is not None else 'None'}
FILE_FORMAT = {fmt!r}


def _read_frame(path: str, file_format: str) -> pl.DataFrame:
    target = Path(path)
    fmt = (file_format or target.suffix.lstrip('.')).lower()
    if fmt == 'auto':
        fmt = target.suffix.lstrip('.').lower()
    if fmt in ('csv',):
        return pl.read_csv(target)
    if fmt in ('ndjson', 'jsonl'):
        return pl.read_ndjson(target)
    if fmt in ('json',):
        return pl.read_json(target)
    if fmt in ('parquet',):
        return pl.read_parquet(target)
    return pl.read_csv(target)


def _build_filter_exprs() -> list[pl.Expr]:
    exprs: list[pl.Expr] = []
{filter_lines}
    return exprs


def run() -> None:
    df = _read_frame(INPUT_PATH, FILE_FORMAT)
    if COLUMNS:
        df = df.select([pl.col(name) for name in COLUMNS])

    filter_exprs = [expr for expr in _build_filter_exprs() if expr is not None]
    for expr in filter_exprs:
        df = df.filter(expr)

    if LIMIT:
        df = df.head(int(LIMIT))

    out_target = Path(OUTPUT_PATH)
    out_target.parent.mkdir(parents=True, exist_ok=True)
    if out_target.suffix.lower() in ('.parquet',):
        df.write_parquet(out_target)
    elif out_target.suffix.lower() in ('.json', '.ndjson', '.jsonl'):
        df.write_ndjson(out_target)
    else:
        df.write_csv(out_target)

    print(f"Wrote {{len(df)}} rows to {{out_target}}")


if __name__ == "__main__":
    run()
"""
    return code


def execute_in_e2b(
    code: str,
    *,
    output_path: str | None = None,
    input_path: str | None = None,
) -> Dict[str, Any]:
    trace: list[str] = []
    sandbox = Sandbox.create()
    workdir = "/home/sandbox"
    remote_code_path = f"{workdir}/code.py"
    install_log: Dict[str, Any] = {}
    exec_log: Dict[str, Any] = {"stdout": "", "stderr": "", "exit_code": -1}
    artifact_bytes = None
    remote_output = output_path

    def _run(cmd: str) -> Dict[str, Any]:
        trace.append(f"$ {cmd}")
        try:
            res = sandbox.commands.run(cmd, cwd=workdir)
            return {
                "stdout": getattr(res, "stdout", ""),
                "stderr": getattr(res, "stderr", ""),
                "exit_code": getattr(res, "exit_code", 0),
            }
        except Exception as exc:
            return {"stdout": "", "stderr": str(exc), "exit_code": -1}

    try:
        # Upload input data if available
        remote_input = None
        if input_path:
            local_input = Path(input_path)
            if local_input.exists() and local_input.is_file():
                remote_input = f"{workdir}/{local_input.as_posix()}"
                parent = Path(remote_input).parent.as_posix()
                if parent:
                    _run(f"mkdir -p {parent}")
                sandbox.files.write(remote_input, local_input.read_bytes())
                trace.append(f"Uploaded input -> {remote_input}")

        sandbox.files.write(remote_code_path, code)
        trace.append(f"Wrote code -> {remote_code_path}")

        install_log = _run("pip install --quiet polars pyarrow")
        exec_log = _run(f"python {remote_code_path}")

        if output_path:
            remote_output = output_path
            if not output_path.startswith("/"):
                remote_output = f"{workdir}/{output_path}"
            try:
                artifact_bytes = sandbox.files.read(remote_output, format="bytes")
                trace.append(f"Downloaded artifact <- {remote_output}")
            except Exception as exc:
                trace.append(f"Artifact read failed: {exc}")
                artifact_bytes = None
    finally:
        try:
            sandbox.kill()
        except Exception:
            trace.append("Sandbox cleanup failed")

    return {
        "install": install_log,
        "stdout": exec_log.get("stdout", ""),
        "stderr": exec_log.get("stderr", ""),
        "exit_code": exec_log.get("exit_code", 0),
        "artifact_path": remote_output or output_path,
        "artifact_bytes": artifact_bytes,
        "trace": trace,
    }
