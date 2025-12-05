PY := uv run python

.PHONY: generate generate-small generate-large run

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
	$(PY) pipeline.py
