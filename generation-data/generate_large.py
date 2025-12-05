from __future__ import annotations

import argparse
import json
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
from typing import Iterator, Sequence, Tuple

import polars as pl
from faker import Faker
from tqdm import tqdm

LARGE_DIR = Path(__file__).parent / "large"
DEFAULT_ROWS = 10_000_000
DEFAULT_OUTPUT = LARGE_DIR / "data_large.ndjson"
DEFAULT_FORMATS = ("ndjson", "parquet")
DEFAULT_WORKERS = 8
DEFAULT_CHUNK_SIZE = 100_000


def iter_fake_records(total: int, faker: Faker) -> Iterator[dict[str, str]]:
    for _ in range(total):
        yield {
            "id": faker.uuid4(),
            "name": faker.name(),
            "email": faker.email(),
            "phone": faker.phone_number(),
            "address": faker.address(),
            "company": faker.company(),
            "created_at": faker.iso8601(),
        }


def _generate_chunk(args: Tuple[int, int]) -> str:
    """Generate a newline-joined chunk for parallel workers."""
    count, seed = args
    faker = Faker()
    faker.seed_instance(seed)
    lines = (json.dumps(rec) for rec in iter_fake_records(count, faker))
    return "\n".join(lines) + "\n"


def write_ndjson(
    output_path: Path = DEFAULT_OUTPUT,
    rows: int = DEFAULT_ROWS,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
    workers: int = DEFAULT_WORKERS,
) -> None:
    """
    Stream a large synthetic dataset to NDJSON without exhausting memory.

    Uses multiple worker processes to generate chunks in parallel.
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Build chunk sizes with one extra remainder chunk if needed.
    chunks: Sequence[int] = [chunk_size] * (rows // chunk_size)
    remainder = rows % chunk_size
    if remainder:
        chunks = [*chunks, remainder]

    with (
        output_path.open("w", encoding="utf-8") as handle,
        tqdm(
            total=rows, unit="row", unit_scale=True, desc="Generating", smoothing=0.05
        ) as bar,
        ProcessPoolExecutor(max_workers=workers) as pool,
    ):
        # Keep seeds deterministic per chunk.
        tasks = ((count, i) for i, count in enumerate(chunks))
        for chunk_str, count in zip(pool.map(_generate_chunk, tasks), chunks):
            handle.write(chunk_str)
            handle.flush()
            bar.update(count)


def convert_from_ndjson(ndjson_path: Path, formats: Sequence[str]) -> dict[str, Path]:
    """Stream-convert NDJSON into other formats without loading whole dataset."""
    outputs: dict[str, Path] = {"ndjson": ndjson_path}
    lf = pl.scan_ndjson(ndjson_path)

    if "parquet" in formats:
        parquet_path = ndjson_path.with_suffix(".parquet")
        lf.sink_parquet(parquet_path)
        outputs["parquet"] = parquet_path

    if "csv" in formats:
        csv_path = ndjson_path.with_suffix(".csv")
        lf.sink_csv(csv_path)
        outputs["csv"] = csv_path

    return outputs


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate a large NDJSON dataset with Faker."
    )
    parser.add_argument(
        "--rows",
        type=int,
        default=DEFAULT_ROWS,
        help="Number of records to generate (default: 10,000,000)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT,
        help="Output path for NDJSON file",
    )
    parser.add_argument(
        "--formats",
        type=str,
        default=",".join(DEFAULT_FORMATS),
        help=(
            "Comma-separated formats to write (ndjson,parquet,csv). "
            "ndjson is always written."
        ),
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=DEFAULT_WORKERS,
        help="Number of worker processes (default: 8)",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=DEFAULT_CHUNK_SIZE,
        help="Rows per worker chunk (default: 100,000)",
    )
    args = parser.parse_args()

    formats = [fmt.strip() for fmt in args.formats.split(",") if fmt.strip()]
    if "ndjson" not in formats:
        formats = ["ndjson", *formats]

    write_ndjson(
        output_path=args.output,
        rows=args.rows,
        chunk_size=args.chunk_size,
        workers=args.workers,
    )

    outputs = convert_from_ndjson(args.output, formats)
    written = ", ".join(f"{k}={v}" for k, v in outputs.items())
    print(f"Wrote {args.rows:,} records → {written}")


if __name__ == "__main__":
    main()
