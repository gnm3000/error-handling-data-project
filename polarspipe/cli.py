from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

import click

from .agent.tools import DEFAULT_OUTPUT_PATH
from .agent.tracing import finish_run, start_run


def load_env_files() -> None:
    """Load .env files (repo root or CWD) without overriding existing env vars."""
    candidates = [
        Path.cwd() / ".env",
        Path(__file__).resolve().parent.parent / ".env",
    ]
    loaded = []

    for env_path in candidates:
        if not env_path.exists():
            continue
        for line in env_path.read_text(encoding="utf-8").splitlines():
            stripped = line.strip()
            if not stripped or stripped.startswith("#") or "=" not in stripped:
                continue
            key, value = stripped.split("=", 1)
            os.environ.setdefault(key.strip(), value.strip())
        loaded.append(str(env_path))

    if loaded:
        click.echo(f"[env] Loaded env file(s): {', '.join(loaded)}")


# Load credentials before importing the agent (OpenAI client reads env on import).
load_env_files()
from .agent.graph import graph  # noqa: E402  (import after env load)


@click.group()
def cli() -> None:
    """Agentic CLI for Polars-powered ETL."""


@cli.command()
@click.argument("instruction", nargs=-1, required=True)
@click.option(
    "--output",
    "output_path",
    help="Local path to persist the resulting artifact.",
)
def run(instruction: tuple[str, ...], output_path: str | None) -> None:
    prompt = " ".join(instruction).strip()
    click.echo(f"[cli] Instruction: {prompt}")
    state: dict[str, Any] = {"instruction": prompt}
    if output_path:
        state["preferred_output_path"] = output_path
        click.echo(f"[cli] Preferred output override: {output_path}")

    run_id = start_run("polarspipe-cli", {"instruction": prompt, "output": output_path})
    if run_id:
        click.echo(f"[cli] LangSmith tracing enabled (run_id={run_id})")

    click.echo("[cli] Invoking agent graph...")
    final_state = graph.invoke(state)
    click.echo("[cli] Agent completed.")

    spec = final_state.get("etl_spec", {})
    plan = final_state.get("plan", "")
    code = final_state.get("code", "")
    execution = final_state.get("execution", {})
    install_log = execution.get("install", {})
    trace = execution.get("trace", [])

    click.echo("--- ETL Plan ---")
    click.echo(plan)
    click.echo("\n--- Generated Polars script ---")
    click.echo(code)
    if install_log:
        click.echo("\n--- Sandbox setup ---")
        if install_log.get("stdout"):
            click.echo(install_log["stdout"])
        if install_log.get("stderr"):
            click.echo(install_log["stderr"], err=True)
    if trace:
        click.echo("\n--- Sandbox trace ---")
        for line in trace:
            click.echo(line)
    click.echo("\n--- Sandbox execution ---")
    click.echo(execution.get("stdout", ""))
    if execution.get("stderr"):
        click.echo(execution["stderr"], err=True)

    artifact_bytes = execution.get("artifact_bytes")
    target = Path(output_path or spec.get("output_path", DEFAULT_OUTPUT_PATH))

    if artifact_bytes:
        target.parent.mkdir(parents=True, exist_ok=True)
        if isinstance(artifact_bytes, str):
            target.write_text(artifact_bytes, encoding="utf-8")
        else:
            target.write_bytes(artifact_bytes)
        click.echo(f"Saved artifact to {target}")
    elif execution.get("artifact_path"):
        click.echo(
            "Artifact path in sandbox: "
            f"{execution['artifact_path']} (content not pulled)"
        )

    click.echo("\n--- Final Spec ---")
    click.echo(json.dumps(spec, indent=2))

    finish_run(
        run_id,
        {
            "spec": spec,
            "plan": plan,
            "code": code,
            "execution": execution,
        },
    )


if __name__ == "__main__":
    cli()
