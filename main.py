from __future__ import annotations

import argparse
import logging
from pathlib import Path

from agents.agent_runner import DEFAULT_MODEL, run_agent
from pipeline import configure_logging, load_clean, profile_pipeline


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Agent-driven ETL CLI")
    subparsers = parser.add_subparsers(dest="command", required=True)

    agent_parser = subparsers.add_parser(
        "agent", help="Run the natural-language ETL agent"
    )
    agent_parser.add_argument(
        "prompt",
        help=(
            "English prompt describing how to project/filter with Polars. "
            "Example: 'Load data.json, keep columns X,Y,Z, filter rows where X > 0.'"
        ),
    )
    agent_parser.add_argument(
        "--input-path",
        default="generation-data/small/data_small.ndjson",
        type=Path,
        help=(
            "Input file to scan (default: generation-data/small/data_small.ndjson). "
            "Supports NDJSON/JSON/CSV/Parquet."
        ),
    )
    agent_parser.add_argument(
        "--output-path",
        default=None,
        type=Path,
        help=(
            "Optional output path. Defaults to <input>_agent_output.<ext> based on input."
        ),
    )
    agent_parser.add_argument(
        "--model",
        default=DEFAULT_MODEL,
        help="OpenAI chat model to drive the agent (default: gpt-4o-mini)",
    )

    pipeline_parser = subparsers.add_parser(
        "pipeline", help="Run the deterministic pipeline without the agent"
    )
    pipeline_parser.add_argument(
        "--path",
        default="generation-data/large/data_large.ndjson",
        type=Path,
        help="File to process with the traditional pipeline",
    )

    return parser


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()
    configure_logging()

    if args.command == "agent":
        response = run_agent(
            args.prompt,
            model=args.model,
            input_path=args.input_path,
            output_path=args.output_path,
        )
        print(response)
    elif args.command == "pipeline":
        lazy_frame = profile_pipeline(load_clean, args.path)
        sample = lazy_frame.limit(3).collect(engine="streaming")
        logging.getLogger(__name__).info(
            {
                "stage": "done",
                "sample": sample.to_dicts(),
                "rows_returned": sample.height,
            }
        )
    else:  # pragma: no cover - argparse should prevent
        parser.error(f"Unsupported command: {args.command}")


if __name__ == "__main__":
    main()
