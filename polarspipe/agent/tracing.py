from __future__ import annotations

import os
from typing import Any, Dict

try:
    from langsmith import Client  # type: ignore
except Exception:  # pragma: no cover - optional dependency guard
    Client = None  # type: ignore


def start_run(name: str, inputs: Dict[str, Any]) -> str | None:
    """
    Start a LangSmith run if LANGSMITH_API_KEY is configured.
    Returns run_id or None on failure/disabled.
    """
    if Client is None or not os.getenv("LANGSMITH_API_KEY"):
        return None
    try:
        client = Client()
        project = os.getenv("LANGSMITH_PROJECT", "polarspipe")
        run = client.create_run(
            name=name,
            inputs=inputs,
            project_name=project,
            run_type="chain",
            tags=["polarspipe", "cli"],
            extra={"runtime": "cli"},
        )
        return run.id
    except Exception:
        return None


def finish_run(run_id: str | None, outputs: Dict[str, Any]) -> None:
    """Mark a LangSmith run as finished if enabled."""
    if Client is None or not run_id:
        return
    try:
        client = Client()
        client.update_run(run_id, outputs=outputs)
    except Exception:
        return
