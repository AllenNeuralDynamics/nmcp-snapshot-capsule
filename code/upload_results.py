from __future__ import annotations

import argparse
import subprocess
from datetime import datetime, timezone
from pathlib import Path

from reconstruction_metadata.utils import parse_subject


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


def build_destination_uri(
    raw_data_asset_uri: str,
    destination_bucket: str,
    now: datetime | None = None,
) -> str:
    try:
        _, dataset_name = parse_subject(raw_data_asset_uri)
    except ValueError as exc:
        raise ValueError(
            "Raw data asset URI must contain an exaSPIM_<subject>_<date>_<time> token."
        ) from exc

    bucket = normalize_bucket(destination_bucket)
    timestamp = now or datetime.now(timezone.utc)
    date_str = timestamp.strftime("%Y-%m-%d")
    time_str = timestamp.strftime("%H-%M-%S")
    return f"s3://{bucket}/{dataset_name}_reconstructions_{date_str}_{time_str}"


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
        description="Build the results destination URI and upload /results to S3."
    )
    parser.add_argument("raw_data_asset_uri", help="Raw data asset S3 URI.")
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
        args.raw_data_asset_uri,
        args.destination_bucket,
    )
    print(f"Resolved destination URI: {destination_uri}")
    sync_results(source_dir, destination_uri)


if __name__ == "__main__":
    main()
