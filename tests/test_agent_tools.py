from pathlib import Path

import polars as pl

from agents.tools import run_polars_etl


def test_run_polars_etl_filters_and_projects(tmp_path: Path) -> None:
    input_path = tmp_path / "data.ndjson"
    df = pl.DataFrame(
        {
            "X": [1, -1, 5],
            "Y": [10, 20, 30],
            "Z": ["a", "b", "c"],
            "other": [0, 1, 2],
        }
    )
    df.write_ndjson(input_path)

    output_path = tmp_path / "result.parquet"
    summary = run_polars_etl(
        input_path=str(input_path),
        columns=["X", "Z"],
        filter_expression="X > 0",
        output_path=str(output_path),
    )

    written = pl.read_parquet(output_path)
    assert written.to_dicts() == [
        {"X": 1, "Z": "a"},
        {"X": 5, "Z": "c"},
    ]
    assert summary["rows"] == 2
    assert Path(summary["output_path"]) == output_path
