from __future__ import annotations

import argparse
import json
from pathlib import Path

from aind_data_schema.core.processing import Processing


def load_processing_metadata(processing_json_path: Path) -> Processing:
    """
    Load and validate processing metadata from a JSON file.

    Parameters
    ----------
    processing_json_path : Path
        Path to a ``processing.json`` file.

    Returns
    -------
    Processing
        Validated processing metadata.
    """
    with open(processing_json_path, "r", encoding="utf-8") as f:
        payload = json.load(f)

    return Processing.model_validate(payload)


def parse_args() -> argparse.Namespace:
    """
    Parse command-line arguments for processing metadata validation.

    Returns
    -------
    argparse.Namespace
        Parsed arguments containing the processing JSON path.
    """
    parser = argparse.ArgumentParser(
        description="Load and validate processing metadata JSON."
    )
    parser.add_argument(
        "processing_json",
        type=Path,
        help="Path to processing.json",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    processing = load_processing_metadata(args.processing_json)
    print(processing)
