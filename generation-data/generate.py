from __future__ import annotations

import json
from typing import TypedDict

from faker import Faker


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


def write_data_to_disk(filepath: str, payload: list[FakeRecord]) -> None:
    """Persist generated data to a JSON file."""
    with open(filepath, "w", encoding="utf-8") as file:
        json.dump(payload, file, indent=2)


def main() -> None:
    data = generate_fake_data()
    write_data_to_disk("data.json", data)
    print("Generated 10,000 rows in data.json")


if __name__ == "__main__":
    main()
