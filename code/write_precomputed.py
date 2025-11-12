
import argparse
import glob
from pathlib import Path

from precomputed import create_from_json_files


def main():
    """Convert JSON files to precomputed format and upload to S3."""
    parser = argparse.ArgumentParser(
        description="Convert JSON files to precomputed format and upload to S3"
    )
    parser.add_argument(
        "--input-dir",
        type=str,
        required=True,
        help="Input directory containing JSON files"
    )
    parser.add_argument(
        "--output",
        type=str,
        required=True,
        help="Output dir or S3 URI (e.g., s3://bucket-name/path)"
    )
    
    args = parser.parse_args()
    
    # Find all JSON files in the input directory
    input_path = Path(args.input_dir)
    json_pattern = str(input_path / "*.json")
    jsons = glob.glob(json_pattern, recursive=True)
    
    print(f"Found {len(jsons)} JSON files in {args.input_dir}")
    print(f"Files: {jsons}")
    
    # Create precomputed format and upload to S3
    create_from_json_files(jsons, args.output)
    print(f"Successfully uploaded to {args.output}")


if __name__ == "__main__":
    main()
    