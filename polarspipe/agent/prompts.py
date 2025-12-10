"""Prompts used by the LangGraph agent."""

SYSTEM_PROMPT = (
    "You are an ETL code generator using Python and Polars. "
    "You strictly follow a provided schema, avoid placeholders, "
    "and return deterministic answers."
)

PARSE_PROMPT = (
    "You receive a natural-language ETL instruction. "
    "Normalize it into a JSON object with the keys: \n"
    "- input_path: string (path or URL to the source file).\n"
    "- output_path: string (default to 'outputs/output.parquet' when absent).\n"
    "- columns: array of column names to keep (empty list keeps everything).\n"
    "- filters: array of filters as objects {column, op, value} where op is one of\n"
    "  ['==','!=','>','>=','<','<=','in',\n"
    "   'not in','contains','startswith','endswith'].\n"
    "- limit: optional integer row cap.\n"
    "- format: one of ['auto','csv','json','ndjson','parquet']\n"
    "  (auto infers from extension).\n"
    "Preserve literal column names and numeric thresholds. "
    "Do not add prose. "
    "Return ONLY valid JSON."
)

PLAN_PROMPT = (
    "Draft a concise execution plan (3-6 bullet steps) for the ETL spec below. "
    "Each step should be an imperative action referencing Polars operations "
    "(read, select, filter, write). "
    "Stay terse; no markdown fences or explanations."
)
