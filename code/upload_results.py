from __future__ import annotations

import argparse
import json
import subprocess
from pathlib import Path


def normalize_bucket(bucket: str) -> str:
    normalized = bucket.strip()
    if normalized.startswith("s3://"):
        normalized = normalized[5:]
    normalized = normalized.rstrip("/")
    if not normalized:
        raise ValueError("Destination bucket must not be empty.")
    if "/" in normalized:
        raise ValueError(
            "Destination bucket must be a bucket name only, without path segments."
        )
    return normalized


def load_data_description_name(source_dir: Path) -> str:
    metadata_path = source_dir / "data_description.json"
    if not metadata_path.is_file():
        raise ValueError(
            f"Expected metadata file was not found: {metadata_path}"
        )

    with open(metadata_path, "r", encoding="utf-8") as f:
        payload = json.load(f)

    name = payload.get("name")
    if not isinstance(name, str) or not name.strip():
        raise ValueError(
            f"Metadata file must contain a non-empty 'name': {metadata_path}"
        )

    return name.strip()


def build_destination_uri(
    destination_bucket: str,
    source_dir: Path,
) -> str:
    bucket = normalize_bucket(destination_bucket)
    data_description_name = load_data_description_name(source_dir)
    return f"s3://{bucket}/{data_description_name}"


def sync_results(
    source_dir: Path,
    destination_uri: str,
    runner=subprocess.run,
) -> None:
    if not source_dir.is_dir():
        raise ValueError(f"Source directory does not exist: {source_dir}")

    runner(
        ["aws", "s3", "sync", str(source_dir), destination_uri],
        check=True,
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Build the results destination URI from /results/data_description.json "
            "and upload /results to S3."
        )
    )
    parser.add_argument(
        "raw_data_asset_uri",
        help="Raw data asset S3 URI. Retained for CLI compatibility.",
    )
    parser.add_argument("destination_bucket", help="Destination S3 bucket name.")
    parser.add_argument(
        "--source-dir",
        default="/results",
        help="Local directory to sync to S3. Defaults to /results.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    source_dir = Path(args.source_dir)
    destination_uri = build_destination_uri(
        args.destination_bucket,
        source_dir,
    )
    print(f"Resolved destination URI: {destination_uri}")
    sync_results(source_dir, destination_uri)


if __name__ == "__main__":
    main()
