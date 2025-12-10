# Polarspipe — Agentic CLI + Polars ETL

Natural-language instruction → plan → Polars code → isolated execution in E2B → final artifact. The ETL core lives in `polarspipe/ingestion` and can be called as an agent tool.

## Architecture
- Reusable Polars ETL: `polarspipe/ingestion/` + `polarspipe/pipeline.py`.
- LangGraph agent + OpenAI SDK + E2B sandbox: `polarspipe/agent/` (see `README_AGENT.md`).
- Agentic CLI: `polarspipe/cli.py` (`polarspipe` entrypoint).
- Sample data in `generation-data/` and quality scripts in `scripts/`.

```
polarspipe/
├── polarspipe/
│   ├── agent/        # LangGraph graph, E2B tools, prompts
│   ├── cli.py        # Agentic CLI
│   ├── ingestion/    # reader/transformer/validator/writer
│   ├── pipeline.py   # reusable ETL base
│   └── main.py
└── tests/...
```

## Quickstart
1) Install deps: `uv sync --extra dev`  
2) Create `.env` (based on `.env.example`) with `OPENAI_API_KEY=...` and `E2B_API_KEY=...` (optional `OPENAI_MODEL=gpt-4.1`). Optional tracing: `LANGSMITH_API_KEY`, `LANGSMITH_PROJECT`, `LANGSMITH_TRACING=true`. CLI loads `.env` automatically without overriding existing env vars.  
3) Run the agent:  
```bash
polarspipe run "My file `data.json`: extract columns id,name, filter where id!='', save to outputs/result.parquet"
```
The CLI prints plan, generated code, sandbox logs, and saves the artifact if available.

### Make helper
- Run via Make with positional instruction:  
  `make etl "My file generation-data/small/data.json columns id,name, filter where id!='', save to outputs/result.parquet"`
- Alternate form: `make etl USER_INPUT="My instruction"`

## Agent flow
1. Parse: `parse` node normalizes instruction into JSON (input, columns, filters, output).  
2. Plan: `plan` node yields concise steps.  
3. Codegen: `generate_polars_code` builds deterministic script.  
4. Exec: `execute_in_e2b` launches E2B sandbox, installs Polars, runs the script, returns stdout/stderr + artifact.  

## Development & tests
- Local pipeline without agent: `make run` (uses `polarspipe/pipeline.py`).  
- Quality: `./scripts/run_quality.sh` (or `./scripts/run_quality.sh check`).  
- Benchmarks and smoke tests in `tests/`.  
