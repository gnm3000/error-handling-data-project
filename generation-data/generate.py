from __future__ import annotations

import json
from pathlib import Path
from typing import TypedDict

import polars as pl
from faker import Faker

SMALL_DIR = Path(__file__).parent / "small"
JSON_PATH = SMALL_DIR / "data.json"
CSV_PATH = SMALL_DIR / "data.csv"
PARQUET_PATH = SMALL_DIR / "data.parquet"
PARQUET_PLAIN_PATH = SMALL_DIR / "data_plain.parquet"


class FakeRecord(TypedDict):
    id: str
    name: str
    email: str
    phone: str
    address: str
    company: str
    created_at: str


def generate_fake_data(records: int = 10_000) -> list[FakeRecord]:
    """Generate a collection of fake user records."""
    if records <= 0:
        return []

    faker = Faker()
    return [
        FakeRecord(
            id=faker.uuid4(),
            name=faker.name(),
            email=faker.email(),
            phone=faker.phone_number(),
            address=faker.address(),
            company=faker.company(),
            created_at=faker.iso8601(),
        )
        for _ in range(records)
    ]


def write_json(filepath: Path, payload: list[FakeRecord]) -> None:
    filepath.parent.mkdir(parents=True, exist_ok=True)
    with filepath.open("w", encoding="utf-8") as file:
        json.dump(payload, file, indent=2)


def write_data_to_disk(path: str | Path, payload: list[FakeRecord]) -> None:
    """Helper for tests to persist generated records to disk."""
    write_json(Path(path), payload)


def write_tabular(df: pl.DataFrame) -> None:
    df.write_csv(CSV_PATH)
    df.write_parquet(PARQUET_PATH)
    df.write_parquet(PARQUET_PLAIN_PATH, compression="uncompressed")


def main() -> None:
    data = generate_fake_data()
    write_json(JSON_PATH, data)

    df = pl.DataFrame(data)
    write_tabular(df)

    print(f"Generated {len(data):,} rows in {SMALL_DIR}/")


if __name__ == "__main__":
    main()
