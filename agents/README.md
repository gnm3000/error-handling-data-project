# Agent ETL runner

This folder contains a LangGraph + OpenAI agent that turns natural-language ETL
requests into deterministic Polars jobs. The agent favors small, auditable plans
and isolates any auxiliary code in an E2B sandbox.

## Quickstart
1. Export credentials:
   - `OPENAI_API_KEY`
   - Optional: `E2B_API_KEY` (enables isolated code execution)
2. Run the CLI:
   ```bash
   python -m main agent \
     "Load data.json, keep columns X,Y,Z, filter rows where X > 0." \
     --input-path data.json \
     --output-path filtered.parquet
   ```

## Prompting guidelines (English)
- Be explicit about columns to project and the filter expression.
- Mention input/output paths if you need non-default locations.
- Keep prompts concise; the agent adds guardrails so the plan stays deterministic
  and uses Polars end-to-end.

## Tooling reference
- `execute_polars_etl`: scans the dataset with Polars, selects columns, applies a
  filter, optionally limits rows, and writes the result (Parquet/CSV/NDJSON).
- `run_python_in_sandbox`: executes scratch Python in an E2B sandbox when you
  need quick helper code outside the main ETL path.

## Flow
```mermaid
flowchart TD
    U[User prompt] -->|English ETL request| A[System prompt
"ETL engineer & best-practices prompter"]
    A --> B[LangGraph ReAct agent]
    B -->|primary ETL| T[execute_polars_etl
Polars projection/filter/write]
    B -->|auxiliary code| S[run_python_in_sandbox
E2B isolated execution]
    T --> O[Output file
<path>_agent_output.ext or provided]
```

## Files
- `agent_runner.py`: builds the agent with the system prompt and invokes it.
- `tools.py`: Polars ETL helper and E2B sandbox tool definitions.
