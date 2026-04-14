from __future__ import annotations

from nmcp import ExportFormat, ReconstructionSpace


EXPORT_FORMAT_ALIASES: dict[str, ExportFormat] = {
    "json": ExportFormat.LEGACY_JSON,
    "legacy-json": ExportFormat.LEGACY_JSON,
    "portal-json": ExportFormat.PORTAL_JSON,
    "swc": ExportFormat.SWC,
}

RECONSTRUCTION_SPACE_ALIASES: dict[str, ReconstructionSpace] = {
    "specimen": ReconstructionSpace.SPECIMEN,
    "atlas": ReconstructionSpace.ATLAS,
    "ccf": ReconstructionSpace.ATLAS,
}


def parse_export_format(value: str) -> ExportFormat:
    """Parse CLI export format values, including stable aliases."""
    try:
        return ExportFormat(int(value))
    except (TypeError, ValueError):
        pass

    normalized = value.strip().lower().replace("_", "-")

    try:
        return ExportFormat[normalized.upper().replace("-", "_")]
    except KeyError:
        pass

    try:
        return EXPORT_FORMAT_ALIASES[normalized]
    except KeyError as exc:
        names = ", ".join(sorted(EXPORT_FORMAT_ALIASES))
        values = ", ".join(str(item.value) for item in ExportFormat)
        raise ValueError(
            f"Invalid format '{value}'. Use one of names {{{names}}} or values {{{values}}}."
        ) from exc


def parse_reconstruction_space(value: str) -> ReconstructionSpace:
    """Parse CLI reconstruction-space values, including legacy aliases."""
    try:
        return ReconstructionSpace(int(value))
    except (TypeError, ValueError):
        pass

    normalized = value.strip().lower().replace("_", "-")

    try:
        return ReconstructionSpace[normalized.upper().replace("-", "_")]
    except KeyError:
        pass

    try:
        return RECONSTRUCTION_SPACE_ALIASES[normalized]
    except KeyError as exc:
        names = ", ".join(sorted(RECONSTRUCTION_SPACE_ALIASES))
        values = ", ".join(str(item.value) for item in ReconstructionSpace)
        raise ValueError(
            f"Invalid reconstruction space '{value}'. Use one of {{{names}}} or values {{{values}}}."
        ) from exc
