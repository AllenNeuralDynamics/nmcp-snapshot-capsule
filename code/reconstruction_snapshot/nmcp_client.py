import json
import requests
import base64
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

from nmcpscripting.download_reconstruction import ExportFormat
from query_published import query_published
from utils import with_retries, write_bytes_file, write_text_file
from zip_utils import ZipExtractor, ZipExtractError


FORMAT_SUFFIXES: Dict[ExportFormat, str] = {
    ExportFormat.JSON: ".json",
    ExportFormat.SWC: ".swc",
}

DEFAULT_BASE_URL = "https://morphology.allenneuraldynamics.org"


def allowed_suffix_for(export_format: ExportFormat) -> str:
    """Return the suffix that should be extracted for the given format."""
    suffix = FORMAT_SUFFIXES.get(export_format)
    if suffix is None:
        raise ValueError(
            f"No suffix mapping defined for export format {export_format}."
        )
    return suffix


@dataclass(frozen=True)
class NmcpClientConfig:
    """Immutable configuration for the portal API."""

    base_url: str

    def __post_init__(self) -> None:
        normalized = self.base_url.rstrip("/")
        object.__setattr__(self, "base_url", normalized)

    @property
    def graphql_url(self) -> str:
        return f"{self.base_url}/graphql"

    @property
    def export_url(self) -> str:
        return f"{self.base_url}/export"


@dataclass(frozen=True)
class NeuronData:
    """Small wrapper class for GraphQL query result."""
    uuid: str  # e.g., "550e8400-e29b-41d4-a716-446655440000"
    label: str  # e.g., N001-685221
    subject: str  # e.g., 685221
    data: dict

    @staticmethod
    def from_api(payload: dict) -> "NeuronData":
        sample_id = payload["neuron"]["specimen"]["label"]
        neuron_label = payload["neuron"]["label"]
        return NeuronData(
            uuid=payload["id"],
            label=f"{neuron_label}-{sample_id}",
            subject=str(sample_id),
            data=payload
        )


@dataclass
class ZipDownloadResult:
    """Structured outcome for each attempted download/extract."""

    neuron: NeuronData
    elapsed_s: float
    error: Optional[str] = None
    zip_content_bytes: Optional[bytes] = None


class QueryError(RuntimeError):
    """Raised when fetching the published neurons fails."""


class NmcpClient:
    """Facade around the published reconstruction service endpoints."""

    def __init__(self, config: NmcpClientConfig) -> None:
        self._config = config

    @property
    def config(self) -> NmcpClientConfig:
        """Expose the immutable configuration."""
        return self._config

    def list_published_neurons(
        self, subjects: Optional[Sequence[str]] = None
    ) -> List[NeuronData]:
        """Fetch reconstruction metadata from the publishing service."""
        try:
            records = list(query_published(host=self._config.graphql_url))
        except Exception as exc:  # Surface a domain error
            raise QueryError(f"Failed to query published neurons: {exc}") from exc

        # TODO: bake this into GraphQL query for performance
        if subjects:
            subject_set = {str(subject) for subject in subjects if subject is not None}
            if subject_set:
                records = [
                    r
                    for r in records
                    if r["neuron"]["specimen"]["label"] in subject_set
                ]

        return [NeuronData.from_api(record) for record in records]

    def download_archive(
        self,
        neuron: NeuronData,
        export_format: ExportFormat,
        reconstruction_space: int,
        *,
        output_path: Optional[Path | str] = None,
        attempts: int = 1,
        base_sleep: float = 0.5,
    ) -> ZipDownloadResult:
        """
        Download a reconstruction archive for the neuron.

        Args:
            neuron: Target neuron.
            export_format: Desired export format for the archive.
            output_path: Optional path to persist the raw archive bytes.
            attempts: Number of retry attempts.
            base_sleep: Base backoff duration between retries.

        Returns:
            ZipDownloadResult: Outcome describing the download attempt.
        """
        def _attempt() -> Tuple[Optional[Tuple[bytes, str]], float]:
            t0 = time.perf_counter()
            payload = self._download_archive_bytes(
                neuron.uuid,
                export_format,
                reconstruction_space=reconstruction_space,
            )
            return payload, time.perf_counter() - t0

        try:
            payload, elapsed = with_retries(
                _attempt,
                attempts=attempts,
                base_sleep=base_sleep,
            )
        except Exception as exc:
            return ZipDownloadResult(
                neuron=neuron,
                elapsed_s=0.0,
                error=f"download failed: {exc}",
                zip_content_bytes=None,
            )

        if payload is None:
            return ZipDownloadResult(
                neuron=neuron,
                elapsed_s=elapsed,
                error="archive not available",
                zip_content_bytes=None,
            )

        archive_bytes, _ = payload
        if output_path is not None:
            write_bytes_file(output_path, archive_bytes)

        return ZipDownloadResult(
            neuron=neuron,
            elapsed_s=elapsed,
            error=None,
            zip_content_bytes=archive_bytes,
        )

    def download_json(
        self,
        neuron: NeuronData,
        *,
        reconstruction_space: int,
        output_path: Optional[Path | str] = None,
        attempts: int = 1,
        base_sleep: float = 0.5,
    ) -> Dict:
        """
        Convenience helper that downloads and parses JSON reconstruction content.

        Args:
            neuron: Target neuron metadata.
            output_path: Optional path to write the raw JSON text.
            attempts: Retry attempts for the archive download.
            base_sleep: Backoff base sleep (seconds) between retries.

        Returns:
            dict: Parsed JSON payload.
        """
        json_text = self._download_text_payload(
            neuron,
            ExportFormat.JSON,
            reconstruction_space,
            output_path,
            attempts,
            base_sleep,
        )

        try:
            return json.loads(json_text)
        except json.JSONDecodeError as exc:
            raise RuntimeError(
                f"Downloaded archive for {neuron.label} did not contain valid JSON."
            ) from exc

    def download_swc(
        self,
        neuron: NeuronData,
        *,
        reconstruction_space: int,
        output_path: Optional[Path | str] = None,
        attempts: int = 1,
        base_sleep: float = 0.5,
    ) -> str:
        """
        Convenience helper that downloads and returns SWC reconstruction text.

        Args:
            neuron: Target neuron metadata.
            output_path: Optional path to write the SWC text.
            attempts: Retry attempts for the archive download.
            base_sleep: Backoff base sleep (seconds) between retries.

        Returns:
            str: SWC file contents.
        """
        return self._download_text_payload(
            neuron,
            ExportFormat.SWC,
            reconstruction_space,
            output_path,
            attempts,
            base_sleep,
        )

    def _require_archive_bytes(
        self,
        neuron: NeuronData,
        export_format: ExportFormat,
        reconstruction_space: int,
        attempts: int,
        base_sleep: float,
    ) -> bytes:
        result = self.download_archive(
            neuron,
            export_format,
            reconstruction_space=reconstruction_space,
            attempts=attempts,
            base_sleep=base_sleep,
        )
        if result.error:
            raise RuntimeError(
                f"Failed to download archive for {neuron.label}: {result.error}"
            )
        if result.zip_content_bytes is None:
            raise RuntimeError(
                f"Archive for {neuron.label} did not include file contents."
            )
        return result.zip_content_bytes

    def _download_text_payload(
        self,
        neuron: NeuronData,
        export_format: ExportFormat,
        reconstruction_space: int,
        output_path: Optional[Path | str],
        attempts: int,
        base_sleep: float,
    ) -> str:
        archive_bytes = self._require_archive_bytes(
            neuron, export_format, reconstruction_space, attempts, base_sleep
        )
        suffix = allowed_suffix_for(export_format)
        try:
            payload_bytes = ZipExtractor.extract_member_bytes(
                archive_bytes, [suffix]
            )
        except ZipExtractError as exc:
            raise RuntimeError(
                f"Archive for {neuron.label} missing '{suffix}' content: {exc}"
            ) from exc

        text = payload_bytes.decode("utf-8")

        if output_path is not None:
            write_text_file(output_path, text)

        return text

    def _download_archive_bytes(self, reconstruction_id: str, export_format: ExportFormat, reconstruction_space: int = 0):
        payload = {"ids": [reconstruction_id], "format": export_format, "reconstructionSpace": reconstruction_space}
        response = requests.post(self._config.export_url, json=payload, headers={"Content-Type": "application/json"})
        if response.status_code == 200:
            content = response.json()
            base64_bytes = content["contents"].encode("utf-8")
            decoded_bytes = base64.b64decode(base64_bytes)
            return decoded_bytes, content["filename"]
        else:
            raise RuntimeError(f"Failed to download archive for reconstruction {reconstruction_id}\n status code {response.status_code}\n message: {response.text}")