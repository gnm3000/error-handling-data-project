from __future__ import annotations

import logging
from pathlib import Path
from typing import Iterable

from langchain_openai import ChatOpenAI
from langgraph.prebuilt import create_react_agent

from agents.tools import execute_polars_etl, run_python_in_sandbox

logger = logging.getLogger(__name__)

DEFAULT_MODEL = "gpt-4o-mini"


def _build_system_prompt(input_path: Path, output_path: Path | None) -> str:
    destination = output_path or input_path.with_name(
        f"{input_path.stem}_agent_output.parquet"
    )
    return (
        "You are an ETL engineer and best-practices prompter."
        " Always choose Polars for data work."
        " Prefer concise, reproducible instructions and keep explanations in English."
        " Use the provided tools for scanning files, selecting columns, filtering rows,"
        " and writing results."
        f" If the user omits paths, default to input={input_path} and output={destination}."
        " For any scratch work that is not the main ETL, run code in the E2B sandbox via"
        " run_python_in_sandbox."
    )


def build_agent(model: str, *, input_path: Path, output_path: Path | None):
    """
    Build a LangGraph ReAct agent wired with the ETL and sandbox tools.

    The agent receives a best-practice system prompt that biases it toward
    deterministic Polars projections/filters and isolated sandbox execution
    for auxiliary snippets.
    """
    llm = ChatOpenAI(model=model, temperature=0)
    system_prompt = _build_system_prompt(input_path, output_path)
    return create_react_agent(
        llm,
        [execute_polars_etl, run_python_in_sandbox],
        state_modifier=system_prompt,
    )


def run_agent(
    prompt: str,
    *,
    model: str = DEFAULT_MODEL,
    input_path: Path,
    output_path: Path | None,
    extra_context: Iterable[str] | None = None,
) -> str:
    """
    Invoke the natural-language ETL agent and return the final message content.

    Args:
        prompt: user request describing the ETL.
        model: OpenAI model name.
        input_path: default input file for ETL operations.
        output_path: default output file for ETL results.
        extra_context: optional extra lines appended to the prompt.
    """
    agent = build_agent(model, input_path=input_path, output_path=output_path)
    context_blob = "\n".join(extra_context or [])
    full_prompt = (
        f"{prompt}\n\n"
        "Quick guardrails:\n"
        "- Use execute_polars_etl for the primary projection/filter/write.\n"
        "- Use run_python_in_sandbox for auxiliary scratch code.\n"
        f"- Default input path: {input_path}.\n"
        f"- Default output path: {output_path or 'derived from the input name'}.\n"
        f"{context_blob}"
    )

    logger.info("Invoking agent with model %s", model)
    state = agent.invoke({"messages": [("user", full_prompt)]})
    messages = state.get("messages", [])
    if not messages:
        raise RuntimeError("Agent did not return any messages")

    last_message = messages[-1]
    return getattr(last_message, "content", str(last_message))
