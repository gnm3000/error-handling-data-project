## Data Engineer Application

Small ingestion playground that generates fake user data, applies simple transforms, and exercises a validation/writer flow. It ships with a full quality toolchain to keep code style, typing, and security in check.

## Local Setup

1. Create the virtualenv and install deps: `python3 -m venv .venv && .venv/bin/pip install -e ".[dev]"`
2. Run quality pipeline:
   - Auto-fix and checks: `./scripts/run_quality.sh`
   - Check-only (no edits): `./scripts/run_quality.sh check`
