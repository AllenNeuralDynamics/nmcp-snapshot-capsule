from __future__ import annotations

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
