from __future__ import annotations

import argparse
from datetime import datetime
from pathlib import Path

from data_description_metadata import create_data_description
from processing_metadata import create_processing_metadata
from quality_control_metadata import generate_qc_json
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
        Parsed arguments including dataset path, Excel file, and output directory.
    """
    parser = argparse.ArgumentParser(
        description="Generate metadata outputs for a reconstruction dataset."
    )
    parser.add_argument(
        "data_path",
        help="S3 path to the dataset root (e.g., s3://bucket/prefix)",
    )
    parser.add_argument(
        "--excel-file",
        default="/root/capsule/data/Neuron Reconstructions.xlsx",
        help="Path to the neuron reconstruction Smartsheet export.",
    )
    parser.add_argument(
        "--output-dir",
        default="/results",
        help="Directory where generated metadata files will be written.",
    )
    return parser.parse_args()


def main() -> None:
    """
    Execute the metadata generation workflow for a reconstruction dataset.

    Returns
    -------
    None
    """
    args = parse_args()

    data_path = args.data_path
    excel_file = Path(args.excel_file)
    output_dir = Path(args.output_dir)

    bucket, prefix = parse_s3_path(data_path)
    subject_id, _ = parse_subject(data_path)

    download_precompiled_metadata(bucket, prefix, output_dir)

    qc = generate_qc_json(
        mouse_id=str(subject_id),
        excel_path=excel_file,
        output_dir=output_dir,
    )

    save_json_file(output_dir, filename="quality_control.json", payload=qc)

    dd = create_data_description(subject_id)
    save_json_file(output_dir, "data_description.json", dd)

    processing = create_processing_metadata(datetime.now())
    save_json_file(output_dir, "processing.json", processing)


if __name__ == "__main__":
    main()
