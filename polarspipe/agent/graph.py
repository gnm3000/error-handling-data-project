from __future__ import annotations

import json
import os
from typing import Any, TypedDict

from langgraph.graph import END, StateGraph
from openai import OpenAI

from . import prompts
from .tools import (
    DEFAULT_OUTPUT_PATH,
    execute_in_e2b,
    generate_polars_code,
    parse_etl_instruction,
)

client = OpenAI()


class AgentState(TypedDict, total=False):
    instruction: str
    preferred_output_path: str | None
    base_spec: dict[str, Any]
    etl_spec: dict[str, Any]
    plan: str
    code: str
    execution: dict[str, Any]


def _chat(
    messages: list[dict[str, str]], *, response_format: dict | None = None
) -> str:
    resp = client.chat.completions.create(
        model=os.getenv("OPENAI_MODEL", "gpt-4.1"),
        messages=messages,
        temperature=0,
        response_format=response_format,
    )
    return resp.choices[0].message.content or ""


def _safe_parse_json(content: str, fallback: dict[str, Any]) -> dict[str, Any]:
    try:
        return json.loads(content)
    except Exception:
        return fallback


def node_parse(state: AgentState) -> AgentState:
    instruction = state["instruction"]
    base_spec = parse_etl_instruction(instruction)

    messages = [
        {"role": "system", "content": prompts.SYSTEM_PROMPT},
        {"role": "user", "content": prompts.PARSE_PROMPT},
        {
            "role": "user",
            "content": (
                "Seed spec: " + json.dumps(base_spec) + "\nInstruction: " + instruction
            ),
        },
    ]

    parsed_content = _chat(messages, response_format={"type": "json_object"})
    parsed_json = _safe_parse_json(parsed_content, base_spec)
    merged_spec = {**base_spec, **parsed_json}

    preferred_output = state.get("preferred_output_path")
    if preferred_output:
        merged_spec["output_path"] = preferred_output

    return {
        **state,
        "base_spec": base_spec,
        "etl_spec": merged_spec,
    }


def node_plan(state: AgentState) -> AgentState:
    spec = state.get("etl_spec") or {}
    messages = [
        {"role": "system", "content": prompts.SYSTEM_PROMPT},
        {"role": "user", "content": prompts.PLAN_PROMPT},
        {"role": "user", "content": json.dumps(spec, indent=2)},
    ]
    plan_text = _chat(messages)
    return {**state, "plan": plan_text}


def node_code(state: AgentState) -> AgentState:
    spec = state.get("etl_spec") or {"output_path": DEFAULT_OUTPUT_PATH}
    code = generate_polars_code(spec)
    return {**state, "code": code}


def node_execute(state: AgentState) -> AgentState:
    spec = state.get("etl_spec") or {}
    output_path = spec.get("output_path")
    input_path = spec.get("input_path")
    result = execute_in_e2b(
        state.get("code", ""),
        output_path=output_path,
        input_path=input_path,
    )
    return {**state, "execution": result}


def build_graph() -> Any:
    graph = StateGraph(AgentState)
    graph.add_node("parse", node_parse)
    graph.add_node("plan", node_plan)
    graph.add_node("code", node_code)
    graph.add_node("execute", node_execute)

    graph.set_entry_point("parse")
    graph.add_edge("parse", "plan")
    graph.add_edge("plan", "code")
    graph.add_edge("code", "execute")
    graph.add_edge("execute", END)

    return graph.compile()


graph = build_graph()
