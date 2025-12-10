PY := uv run python
ARGS := $(filter-out etl,$(MAKECMDGOALS))

.PHONY: generate generate-small generate-large run
.PHONY: etl
.PHONY: $(ARGS)

# Generate both small (10k rows) and large datasets
# Small artifacts are checked into git; large artifacts live under generation-data/large (git-ignored).
generate: generate-small generate-large

# Generate 10k sample data into generation-data/small/
generate-small:
	$(PY) generation-data/generate.py

# Generate large NDJSON (+ Parquet) into generation-data/large/
generate-large:
	bash generation-data/generate_large.sh

# Run the pipeline against the large NDJSON by default
run:
	$(PY) -m polarspipe.pipeline

# Run the agentic CLI with a natural-language instruction.
# Usage:
#   make etl "My instruction"
#   make etl USER_INPUT="My instruction"
etl:
	@input="$(USER_INPUT)"; \
	if [ -z "$$input" ] && [ -n "$(ARGS)" ]; then input="$(subst $(space), ,$(ARGS))"; fi; \
	if [ -z "$$input" ]; then echo 'Usage: make etl "My instruction" OR make etl USER_INPUT="My instruction"'; exit 1; fi; \
	echo "[make] Running ETL: $$input"; \
	uv run polarspipe run "$$input"

$(ARGS):
	@:
