from __future__ import annotations

import argparse
from contextlib import contextmanager
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Iterator

from data_description_metadata import create_data_description
from processing_metadata import load_processing_metadata
from quality_control_metadata import generate_qc_json
from smartsheet_utils import fetch_latest_smartsheet_excel
from utils import (fetch_and_save_json, parse_s3_path, parse_subject,
                   save_json_file)


def download_precompiled_metadata(bucket: str, prefix: str, output_dir: Path) -> None:
    """
    Retrieve pre-existing metadata JSON files for a dataset.

    Parameters
    ----------
    bucket : str
        S3 bucket containing the dataset metadata.
    prefix : str
        Key prefix for the dataset within the bucket.
    output_dir : Path
        Local directory where the metadata files will be stored.

    Returns
    -------
    None
    """
    acq_path = f"s3://{bucket}/{prefix}/acquisition.json"
    instrument_path = f"s3://{bucket}/{prefix}/instrument.json"
    subject_path = f"s3://{bucket}/{prefix}/subject.json"
    procedures_path = f"s3://{bucket}/{prefix}/procedures.json"

    for s3_json in (acq_path, instrument_path, subject_path, procedures_path):
        fetch_and_save_json(s3_json, output_dir)


def parse_args() -> argparse.Namespace:
    """
    Parse command-line arguments for metadata generation.

    Returns
    -------
    argparse.Namespace
        Parsed arguments including dataset path and output directory.
    """
    parser = argparse.ArgumentParser(
        description="Generate metadata outputs for a reconstruction dataset."
    )
    parser.add_argument(
        "data_path",
        help="S3 path to the dataset root (e.g., s3://bucket/prefix)",
    )
    parser.add_argument(
        "--output-dir",
        default="/results",
        help="Directory where generated metadata files will be written.",
    )
    parser.add_argument(
        "--reconstruction-json-dir",
        type=Path,
        default=None,
        help=(
            "Directory containing downloaded reconstruction JSON files. "
            "When provided, QC metadata is restricted to reconstructions present here."
        ),
    )
    parser.add_argument(
        "--processing-json",
        type=Path,
        required=True,
        help="Path to processing.json that will be validated and copied to output.",
    )
    return parser.parse_args()


@contextmanager
def download_smartsheet_workbook() -> Iterator[Path]:
    """
    Download the workbook used for reconstruction metadata generation.

    Yields
    ------
    Path
        Filesystem path to an Excel workbook that ``pandas.read_excel`` can load.

    Raises
    ------
    RuntimeError
        If the Smartsheet download fails.
    """

    with TemporaryDirectory(prefix="smartsheet_export_") as temp_dir:
        try:
            downloaded_excel = fetch_latest_smartsheet_excel(temp_dir)
        except RuntimeError as exc:
            raise RuntimeError("Smartsheet export could not be fetched.") from exc
        yield downloaded_excel


def main() -> None:
    """
    Execute the metadata generation workflow for a reconstruction dataset.

    Returns
    -------
    None
    """
    args = parse_args()

    data_path = args.data_path
    output_dir = Path(args.output_dir)
    reconstruction_json_dir = args.reconstruction_json_dir
    processing_json = args.processing_json

    bucket, prefix = parse_s3_path(data_path)
    subject_id, _ = parse_subject(data_path)

    download_precompiled_metadata(bucket, prefix, output_dir)

    with download_smartsheet_workbook() as resolved_excel_file:
        qc = generate_qc_json(
            mouse_id=str(subject_id),
            excel_path=resolved_excel_file,
            output_dir=output_dir,
            reconstruction_json_dir=reconstruction_json_dir,
        )

    save_json_file(output_dir, filename="quality_control.json", payload=qc)

    dd = create_data_description(subject_id)
    save_json_file(output_dir, "data_description.json", dd)

    processing = load_processing_metadata(processing_json)
    save_json_file(output_dir, "processing.json", processing)


if __name__ == "__main__":
    main()
