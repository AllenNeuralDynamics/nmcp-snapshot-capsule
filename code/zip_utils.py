import io
import zipfile
from pathlib import Path
from typing import Optional, Sequence


class ZipExtractor:
    """Utility helpers for working with in-memory ZIP archives."""

    @staticmethod
    def extract_from_bytes(
        archive_bytes: bytes,
        output_dir: Path | str,
        allowed_suffixes: Optional[Sequence[str]] = None,
    ) -> int:
        """Extract matching files from the archive, returning how many were saved."""
        try:
            with zipfile.ZipFile(io.BytesIO(archive_bytes)) as archive:
                return ZipExtractor.extract(
                    archive, output_dir, allowed_suffixes
                )
        except zipfile.BadZipFile as exc:
            raise ZipExtractError("Archive is not a valid zip file.") from exc

    @staticmethod
    def extract(
        archive: zipfile.ZipFile,
        output_dir: Path,
        allowed_suffixes: Optional[Sequence[str]],
    ) -> int:
        """Extract archive members while preserving their directory structure."""
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        extracted_count = 0
        suffix_filter = (
            {suffix.lower() for suffix in allowed_suffixes}
            if allowed_suffixes is not None
            else None
        )

        for member in archive.infolist():
            if member.is_dir():
                continue

            suffix = Path(member.filename).suffix.lower()
            if suffix_filter is not None and suffix not in suffix_filter:
                continue

            archive.extract(member, path=output_dir)
            extracted_count += 1

        return extracted_count

    @staticmethod
    def extract_member_bytes(
        archive_bytes: bytes, allowed_suffixes: Sequence[str]
    ) -> bytes:
        """Return the bytes of the first archive member matching the suffix filter."""
        try:
            with zipfile.ZipFile(io.BytesIO(archive_bytes)) as archive:
                for member in archive.infolist():
                    if member.is_dir():
                        continue
                    suffix = Path(member.filename).suffix.lower()
                    if suffix in allowed_suffixes:
                        with archive.open(member) as src:
                            return src.read()
        except zipfile.BadZipFile as exc:
            raise ZipExtractError("Archive is not a valid zip file.") from exc

        suffix_list = ", ".join(allowed_suffixes)
        raise ZipExtractError(
            f"Archive did not contain files with suffixes: {suffix_list}"
        )


class ZipExtractError(RuntimeError):
    """Raised when a downloaded archive cannot be unpacked."""
