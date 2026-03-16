from __future__ import annotations

import json
import re
from pathlib import Path, PurePosixPath
from typing import Any

import boto3


def parse_s3_path(s3_path: str) -> tuple[str, str]:
    """
    Parse an S3-style path into its bucket and key components.

    Parameters
    ----------
    s3_path : str
        Fully qualified S3 path (for example, ``s3://bucket/prefix/file``).

    Returns
    -------
    tuple[str, str]
        Pair containing the bucket name and key.

    Raises
    ------
    ValueError
        If the path does not contain a bucket/key delimiter.
    """
    if s3_path.startswith("s3://"):
        s3_path = s3_path[5:]
    bucket_name, key = s3_path.rstrip("/").split("/", 1)
    return bucket_name, key


def load_json(s3_path: str) -> Any:
    """
    Fetch and deserialize a JSON payload from Amazon S3.

    Parameters
    ----------
    s3_path : str
        Fully qualified S3 path to the JSON object.

    Returns
    -------
    Any
        Parsed JSON structure (typically a ``dict`` or ``list``).

    Raises
    ------
    FileNotFoundError
        If the S3 object is not found.
    botocore.exceptions.BotoCoreError
        Propagated for other S3 client issues.
    """
    bucket_name, path = parse_s3_path(s3_path)
    s3 = boto3.client("s3")
    file_key = f"{path}"
    try:
        response = s3.get_object(Bucket=bucket_name, Key=file_key)
    except s3.exceptions.NoSuchKey as exc:
        raise FileNotFoundError(f"{s3_path}") from exc

    content = response["Body"].read().decode("utf-8")
    return json.loads(content)


def save_json_file(output_dir: Path, filename: str, payload: Any) -> None:
    """
    Serialize a payload to JSON within the provided directory.

    Parameters
    ----------
    output_dir : Path
        Destination directory that will receive the JSON file.
    filename : str
        Name of the JSON file to create.
    payload : Any
        Serializable payload or Pydantic model instance.

    Returns
    -------
    None
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    if hasattr(payload, "model_dump"):
        data = payload.model_dump(mode="json")
    else:
        data = payload
    with open(output_dir / filename, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4)


def fetch_and_save_json(s3_path: str, output_dir: Path) -> None:
    """
    Download a JSON object from S3 and mirror it locally.

    Parameters
    ----------
    s3_path : str
        Fully qualified S3 path to the source JSON object.
    output_dir : Path
        Directory where the fetched JSON file will be written.

    Returns
    -------
    None
    """
    try:
        payload = load_json(s3_path)
    except FileNotFoundError as exc:
        print(exc)
        return

    _, key = parse_s3_path(s3_path)
    filename = PurePosixPath(key).name
    save_json_file(output_dir, filename, payload)


def parse_subject(data_path: str) -> tuple[str, str]:
    """
    Extract a subject identifier and dataset name from an exaSPIM path.

    Parameters
    ----------
    data_path : str
        S3 or filesystem path that contains a component matching the
        ``exaSPIM_<subject_id>_<date>_<time>`` pattern.

    Returns
    -------
    tuple[str, str]
        Subject identifier and the matched dataset name.

    Raises
    ------
    ValueError
        If the path does not match the expected pattern.
    """
    regex = r"exaSPIM_(\d{6})_(\d{4}-\d{2}-\d{2})_(\d{2}-\d{2}-\d{2})"
    match = re.search(regex, data_path)
    if not match:
        raise ValueError("Subject could not be parsed from s3 path")
    subject_id = match.group(1)
    name = match.group(0)
    return subject_id, name
