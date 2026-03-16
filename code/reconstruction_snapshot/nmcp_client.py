import json
import requests
import base64
import binascii
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

from enums import ExportFormat, ReconstructionSpace
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
    """Structured success outcome for a completed archive download."""

    neuron: NeuronData
    elapsed_s: float
    zip_content_bytes: bytes


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
                    if str(r["neuron"]["specimen"]["label"]) in subject_set
                ]

        return [NeuronData.from_api(record) for record in records]

    def download_archive(
        self,
        neuron: NeuronData,
        export_format: ExportFormat,
        reconstruction_space: ReconstructionSpace,
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
            ZipDownloadResult: Completed download result.

        Raises:
            Exception: Propagates any request, validation, retry-exhaustion,
                or output write failure.
        """
        def _attempt() -> Tuple[Tuple[bytes, str], float]:
            t0 = time.perf_counter()
            payload = self._download_archive_bytes(
                neuron.uuid,
                export_format,
                reconstruction_space=reconstruction_space,
            )
            return payload, time.perf_counter() - t0

        payload, elapsed = with_retries(
            _attempt,
            attempts=attempts,
            base_sleep=base_sleep,
        )

        archive_bytes, _ = payload
        if output_path is not None:
            write_bytes_file(output_path, archive_bytes)

        return ZipDownloadResult(
            neuron=neuron,
            elapsed_s=elapsed,
            zip_content_bytes=archive_bytes,
        )

    def download_json(
        self,
        neuron: NeuronData,
        *,
        reconstruction_space: ReconstructionSpace,
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
        reconstruction_space: ReconstructionSpace,
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

    def _download_text_payload(
        self,
        neuron: NeuronData,
        export_format: ExportFormat,
        reconstruction_space: ReconstructionSpace,
        output_path: Optional[Path | str],
        attempts: int,
        base_sleep: float,
    ) -> str:
        result = self.download_archive(
            neuron,
            export_format,
            reconstruction_space=reconstruction_space,
            attempts=attempts,
            base_sleep=base_sleep,
        )
        archive_bytes = result.zip_content_bytes
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

    def _download_archive_bytes(
        self,
        reconstruction_id: str,
        export_format: ExportFormat,
        reconstruction_space: ReconstructionSpace = ReconstructionSpace.SPECIMEN,
    ) -> Tuple[bytes, str]:
        payload = {
            "ids": [reconstruction_id],
            "format": export_format.value,
            "reconstructionSpace": reconstruction_space.value,
        }
        response = requests.post(
            self._config.export_url,
            json=payload,
            headers={"Content-Type": "application/json"},
        )
        if response.status_code != 200:
            raise RuntimeError(
                f"Failed to download archive for reconstruction {reconstruction_id}\n"
                f" status code {response.status_code}\n"
                f" message: {response.text}"
            )

        try:
            content = response.json()
        except ValueError as exc:
            raise RuntimeError(
                f"Export endpoint returned non-JSON content for reconstruction {reconstruction_id}."
            ) from exc

        if not isinstance(content, dict):
            raise RuntimeError(
                f"Export endpoint returned invalid JSON payload type for reconstruction {reconstruction_id}: "
                f"{type(content).__name__}"
            )

        missing_keys = [key for key in ("contents", "filename") if key not in content]
        if missing_keys:
            missing = ", ".join(missing_keys)
            raise RuntimeError(
                f"Export response for reconstruction {reconstruction_id} missing keys: {missing}"
            )

        encoded_contents = content["contents"]
        filename = content["filename"]

        if not isinstance(encoded_contents, str) or not encoded_contents:
            raise RuntimeError(
                f"Export response for reconstruction {reconstruction_id} has invalid 'contents'; "
                "expected a non-empty base64 string."
            )
        if not isinstance(filename, str) or not filename:
            raise RuntimeError(
                f"Export response for reconstruction {reconstruction_id} has invalid 'filename'; "
                "expected a non-empty string."
            )

        try:
            decoded_bytes = base64.b64decode(
                encoded_contents.encode("utf-8"), validate=True
            )
        except (binascii.Error, ValueError) as exc:
            raise RuntimeError(
                f"Export response for reconstruction {reconstruction_id} contains invalid base64 data."
            ) from exc

        if not decoded_bytes:
            raise RuntimeError(
                f"Export response for reconstruction {reconstruction_id} contained empty archive bytes."
            )

        return decoded_bytes, filename
