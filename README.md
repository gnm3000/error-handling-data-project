## Data Engineer Application

Small ingestion playground that generates fake user data, applies simple transforms, and exercises a validation/writer flow. It ships with a full quality toolchain to keep code style, typing, and security in check.

Key pieces:
- `generation-data/generate.py` creates 10k fake user records.
- `generation-data/generate_large.py` streams 10M+ fake rows to NDJSON without blowing memory and shows progress with tqdm.
- `pipeline.py` lazily scans input (NDJSON/CSV/Parquet/JSON), validates required fields, cleans names/ids, and logs a sample.
- `ingestion/reader.py` and `validator.py` handle input reading and schema checks; `transformer.py` normalizes the data.
- `tests/` holds quick smoke and schema tests to keep things honest.
- `tests/test_benchmark.py` benchmarks Polars vs pandas group-bys with `pytest-benchmark`.
- `scripts/run_quality.sh` runs formatting, lint, types, security, and pytest in one go.
- `evaluate_performance.sh` offers CPU/memory profiling (cProfile, scalene, snakeviz).

## Local Setup

1. Install deps with uv (creates `.venv` automatically): `uv sync --extra dev`
2. Run quality pipeline (uses the venv uv created):
   - Auto-fix and checks: `./scripts/run_quality.sh`
   - Check-only (no edits): `./scripts/run_quality.sh check`

make run result:
<img width="948" height="400" alt="image" src="https://github.com/user-attachments/assets/c44bb8d1-525b-4c6e-8060-fa1c9dec8ba6" />

