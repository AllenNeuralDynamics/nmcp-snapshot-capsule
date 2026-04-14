import argparse
import concurrent.futures as futures
import logging
import time
from functools import partial
from pathlib import Path
from typing import Callable, Optional, Sequence

from enums import (
    ExportFormat,
    ReconstructionSpace,
    parse_export_format,
    parse_reconstruction_space,
)
from nmcp_client import (
    QueryError,
    NmcpClientConfig,
    NmcpClient,
    NeuronData,
    allowed_suffix_for,
    DEFAULT_BASE_URL,
)


DEFAULT_EXPORT_FORMAT = ExportFormat.LEGACY_JSON


def _select_download_fn(
    service: NmcpClient, export_format: ExportFormat
) -> Callable[..., object]:
    if export_format in (ExportFormat.LEGACY_JSON, ExportFormat.PORTAL_JSON):
        return partial(service.download_json, export_format=export_format)
    if export_format is ExportFormat.SWC:
        return service.download_swc
    raise ValueError(f"Unsupported export format: {export_format}")


def download_neurons(
    client: NmcpClient,
    export_format: ExportFormat,
    reconstruction_space: ReconstructionSpace,
    output_dir: Path | str,
    subjects: Optional[Sequence[str]] = None,
    *,
    jobs: int = 1,
    retry_attempts: int = 5,
) -> None:
    """
    Download and extract all reconstructions matching the filters.

    Args:
        client: Pre-configured NmcpClient instance.
        export_format: Desired export format for the downloads.
        output_dir: Directory path (str or Path) to write artifacts to.
        subjects: Optional collection of subject identifiers to filter reconstructions.
        jobs: Level of parallelism (>=1). Use >1 for I/O-bound speedup.
        retry_attempts: Number of retry attempts for each archive download.

    """
    output_dir = Path(output_dir)
    try:
        suffix = allowed_suffix_for(export_format)
    except ValueError as exc:
        raise RuntimeError(
            "Selected export format does not have a suffix mapping."
        ) from exc

    download_fn = _select_download_fn(client, export_format)
    # Fetch metadata first (single call)
    metadata_records = client.list_published_neurons(subjects)

    if not metadata_records:
        logging.info("No reconstructions matched the requested filters.")
        return

    def process_one(md: NeuronData) -> None:
        t0 = time.perf_counter()
        target_path = output_dir / f"{md.label}{suffix}"
        try:
            download_fn(
                md,
                reconstruction_space=reconstruction_space,
                output_path=target_path,
                attempts=retry_attempts,
                base_sleep=0.5,
            )
        except Exception as exc:
            raise RuntimeError(f"Failed to download {md.label}: {exc}") from exc

        logging.info(
            "Downloaded %s → %s in %.2fs",
            md.label,
            target_path,
            time.perf_counter() - t0,
        )

    if jobs <= 1:
        for md in metadata_records:
            process_one(md)
    else:
        # I/O bound; threads are sufficient and avoid pickling issues of processes
        with futures.ThreadPoolExecutor(max_workers=jobs) as pool:
            all_futures = [pool.submit(process_one, md) for md in metadata_records]
            try:
                for job in futures.as_completed(all_futures):
                    job.result()
            except Exception:
                for pending in all_futures:
                    pending.cancel()
                raise


def _parse_export_format(value: str) -> ExportFormat:
    """
    Argparse type that accepts integer enum values or stable CLI aliases.
    """
    try:
        return parse_export_format(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(str(exc)) from exc


def _parse_reconstruction_space(value: str) -> ReconstructionSpace:
    try:
        return parse_reconstruction_space(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(str(exc)) from exc


def build_parser() -> argparse.ArgumentParser:
    """Create the CLI argument parser."""
    parser = argparse.ArgumentParser(description=__doc__)

    parser.add_argument(
        "-u",
        "--url",
        help="Base URL for the download service.",
        default=DEFAULT_BASE_URL,
        type=str,
    )
    parser.add_argument(
        "-f",
        "--format",
        help=(
            "Export format (alias or numeric value). "
            "Supported aliases: json, legacy-json, portal-json, swc."
        ),
        default="json",
        type=_parse_export_format,
    )
    parser.add_argument(
        "-r",
        "--reconstruction-space",
        type=_parse_reconstruction_space,
        default=ReconstructionSpace.SPECIMEN,
        help="Reconstruction space to download. Supported aliases: specimen, atlas, ccf.",
    )
    parser.add_argument(
        "-o",
        "--output",
        help="Output directory for reconstruction files (defaults to current working directory).",
        default=Path.cwd(),
        type=Path,
    )
    parser.add_argument(
        "-s",
        "--subject",
        dest="subjects",
        action="append",
        type=str,
        help=(
            "Optional subject identifier to filter reconstructions "
            "(repeat flag to provide multiple subjects)."
        ),
        default=None,
    )
    parser.add_argument(
        "-j",
        "--jobs",
        type=int,
        default=1,
        help="Number of concurrent downloads (I/O bound). Use >1 to speed up.",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="count",
        default=0,
        help="Increase verbosity (-v for INFO, -vv for DEBUG).",
    )
    return parser


def _configure_logging(verbosity: int) -> None:
    if verbosity >= 2:
        level = logging.DEBUG
    elif verbosity == 1:
        level = logging.INFO
    else:
        level = logging.WARNING

    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )


def main(cli_args: Optional[Sequence[str]] = None) -> None:
    """CLI entry point."""
    parser = build_parser()
    args = parser.parse_args(cli_args)

    _configure_logging(args.verbose)

    config = NmcpClientConfig(base_url=args.url)
    client = NmcpClient(config)

    logging.info(
        (
            "Starting download: base_url=%s, format=%s, reconstruction_space=%s, "
            "output=%s, subjects=%s, jobs=%d"
        ),
        config.base_url,
        args.format.name,
        args.reconstruction_space.name.lower(),
        str(args.output),
        args.subjects,
        args.jobs,
    )

    try:
        download_neurons(
            client,
            args.format,
            args.reconstruction_space,
            args.output,
            args.subjects,
            jobs=max(1, args.jobs),
        )
    except QueryError as exc:
        logging.exception("Query failed: %s", exc)
        raise SystemExit(2) from exc
    except Exception as exc:
        logging.exception("Unexpected fatal error: %s", exc)
        raise SystemExit(2) from exc

    logging.info("Finished processing downloads.")


if __name__ == "__main__":
    main()
