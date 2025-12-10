from __future__ import annotations

import json
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path
from types import ModuleType
from typing import Any

from main import main


def load_generate_module() -> ModuleType:
    """Load the data generation module despite the hyphenated directory name."""
    module_path = Path("generation-data/generate.py")
    spec = spec_from_file_location("generate_data", module_path)
    if spec is None or spec.loader is None:
        msg = f"Unable to load module from {module_path}"
        raise ImportError(msg)

    module = module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_main_prints_greeting(capsys: Any) -> None:
    main()
    captured = capsys.readouterr()
    assert "Hello from polarspipe!" in captured.out


def test_generate_and_persist(tmp_path: Path) -> None:
    generator = load_generate_module()
    data = generator.generate_fake_data(3)

    assert len(data) == 3
    first_record = data[0]
    expected_keys = {
        "id",
        "name",
        "email",
        "phone",
        "address",
        "company",
        "created_at",
    }
    assert expected_keys.issubset(first_record.keys())

    output_path = tmp_path / "data.json"
    generator.write_data_to_disk(str(output_path), data)

    on_disk = json.loads(output_path.read_text(encoding="utf-8"))
    assert on_disk[0]["id"] == first_record["id"]
