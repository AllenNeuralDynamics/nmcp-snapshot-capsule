from __future__ import annotations

import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import pandas as pd
from aind_data_schema.core.quality_control import (CurationMetric, QCStatus,
                                                   QualityControl, Stage,
                                                   Status)
from aind_data_schema_models.modalities import Modality

from smartsheet_utils import (SmartsheetField, SmartsheetStatus,
                              ensure_cell_id, normalize_mouse_id, parse_coord,
                              safe_datetime, safe_float, safe_string)

logger = logging.getLogger(__name__)

REQUIRED_FIELDS = (
    SmartsheetField.HORTA_COORDINATES,
    SmartsheetField.ANNOTATOR_1,
)
OPTIONAL_FIELDS = (
    SmartsheetField.CCF_COORDINATES,
    SmartsheetField.CCF_SOMA_COMPARTMENT,
    SmartsheetField.SOMA_COMPARTMENT_MANUAL,
    SmartsheetField.DATE_STARTED,
    SmartsheetField.DATE_COMPLETED,
    SmartsheetField.NEURON_LENGTH_MM,
    SmartsheetField.TIME_TO_TRACE_HRS,
    SmartsheetField.ASSIGNED_TYPE,
    SmartsheetField.SEGMENTATION_VERSION,
    SmartsheetField.ANNOTATOR_2,
)
STATUS_COLUMN = SmartsheetField.STATUS_1
DEFAULT_STATUS_FILTER = SmartsheetStatus.COMPLETED.value


def _load_downloaded_cell_ids(directory: str | Path) -> set[str]:
    """
    Collect cell identifiers from downloaded reconstruction JSON filenames.

    Parameters
    ----------
    directory : str | Path
        Directory containing reconstruction JSON files.

    Returns
    -------
    set[str]
        File stems derived from ``*.json`` files in the directory.

    Raises
    ------
    FileNotFoundError
        If the directory does not exist.
    NotADirectoryError
        If the provided path is not a directory.
    ValueError
        If no JSON files are found within the directory.
    """
    json_dir = Path(directory)
    if not json_dir.exists():
        raise FileNotFoundError(
            f"Reconstruction JSON directory {json_dir} does not exist."
        )
    if not json_dir.is_dir():
        raise NotADirectoryError(
            f"Reconstruction JSON path {json_dir} is not a directory."
        )

    cell_ids = {
        path.stem
        for path in json_dir.iterdir()
        if path.is_file() and path.suffix.lower() == ".json"
    }

    if not cell_ids:
        raise ValueError(
            f"No reconstruction JSON files (*.json) found in {json_dir}."
        )
    return cell_ids


def _pending_status(evaluator: str = "Automated Init") -> list[QCStatus]:
    """
    Create an initial pending status entry with a UTC timestamp.

    Parameters
    ----------
    evaluator : str, optional
        Free-text evaluator name stored alongside the status, by default ``"Automated Init"``.

    Returns
    -------
    list[QCStatus]
        Single-element status history marked as pending.
    """
    return [
        QCStatus(
            evaluator=evaluator,
            status=Status.PENDING,
            timestamp=datetime.now(tz=timezone.utc),
        )
    ]


def build_quality_control(
    *,
    curation_values: dict[str, dict[str, Any]],
    annotators: Iterable[str],
) -> QualityControl:
    """
    Assemble a ``QualityControl`` record for a cohort of reconstructions.

    Parameters
    ----------
    curation_values : dict[str, dict[str, Any]]
        Mapping from neuron identifiers to their extracted Smartsheet fields.
    annotators : Iterable[str]
        Collection of annotator names contributing to the dataset.

    Returns
    -------
    QualityControl
        Fully populated QC metadata object with curated metrics.
    """

    curation_metric = CurationMetric.model_construct(
        name="Neuron reconstructions",
        modality=Modality.SPIM,
        stage=Stage.ANALYSIS,
        value=[curation_values],  # this must be a list
        type="reconstruction",
        status_history=_pending_status(),
        description=None,
        reference=None,
        tags={},
        evaluated_assets=None,
        curation_history=[],
    )

    unique_annotators = sorted(
        {
            str(name).strip()
            for name in annotators
            if isinstance(name, str) and str(name).strip()
        }
    )

    qc = QualityControl(
        metrics=[curation_metric],
        key_experimenters=unique_annotators or None,
        default_grouping=["core"],  # minimal non-empty grouping
        allow_tag_failures=[],  # user can override later
    )
    return qc


def generate_qc_json(
    *,
    mouse_id: int | str,
    excel_path: str | Path,
    output_dir: str | Path,
    status_filter: str | None = DEFAULT_STATUS_FILTER,
    reconstruction_json_dir: str | Path | None = None,
) -> QualityControl:
    """
    Generate a ``QualityControl`` object for all reconstructions of a mouse.

    Parameters
    ----------
    mouse_id : int | str
        Mouse identifier used to filter the Smartsheet rows.
    excel_path : str | Path
        Path to the Smartsheet export containing reconstruction metadata. This
        may be a local workbook or a runtime-downloaded Smartsheet export.
    output_dir : str | Path
        Output directory (unused placeholder for compatibility).
    status_filter : str | None, optional
        Status value required in ``SmartsheetField.STATUS_1`` (default is ``"Completed"``).
    reconstruction_json_dir : str | Path | None, optional
        Directory containing downloaded reconstruction JSON files. When provided,
        QC entries are limited to rows whose ``CELL_ID`` has a matching JSON file stem.

    Returns
    -------
    QualityControl
        Compiled QC metadata object containing curated reconstruction entries.

    Raises
    ------
    ValueError
        If no rows match the requested mouse or status, or required fields are missing.
    KeyError
        If the status column is absent from the Smartsheet export.
    """
    df = pd.read_excel(excel_path)

    normalized_mouse_id = str(mouse_id).strip()

    downloaded_cell_ids: set[str] | None = None
    json_dir: Path | None = None
    if reconstruction_json_dir is not None:
        json_dir = Path(reconstruction_json_dir)
        downloaded_cell_ids = _load_downloaded_cell_ids(json_dir)

    rows = df[
        df[SmartsheetField.MOUSE_ID.value].apply(normalize_mouse_id)
        == normalized_mouse_id
    ]
    if rows.empty:
        raise ValueError(f"No reconstructions found for Mouse ID {mouse_id}")

    if status_filter is not None:
        normalized_status = safe_string(status_filter)
        if normalized_status is None:
            raise ValueError("status_filter must be a non-empty string when provided")
        if STATUS_COLUMN.value not in df.columns:
            raise KeyError(
                f'Required status column "{STATUS_COLUMN.value}" not found in data'
            )
        rows = rows[rows[STATUS_COLUMN.value].apply(safe_string) == normalized_status]
        if rows.empty:
            raise ValueError(
                f'No reconstructions found for Mouse ID {mouse_id} with status "{normalized_status}"'
            )

    curation_values: dict[str, dict[str, Any]] = {}
    annotators: set[str] = set()

    for idx, row in rows.iterrows():
        cell_id = ensure_cell_id(
            row.get(SmartsheetField.CELL_ID.value),
            idx,
            curation_values,
            normalized_mouse_id,
        )

        if downloaded_cell_ids is not None and cell_id not in downloaded_cell_ids:
            logger.info(
                "Skipping neuron %s because JSON file was not downloaded into %s",
                cell_id,
                json_dir,
            )
            continue

        horta_xyz = parse_coord(row.get(SmartsheetField.HORTA_COORDINATES.value))
        ccf_xyz = parse_coord(row.get(SmartsheetField.CCF_COORDINATES.value))

        neuron_length_mm = safe_float(row.get(SmartsheetField.NEURON_LENGTH_MM.value))

        time_to_trace_hrs = safe_float(row.get(SmartsheetField.TIME_TO_TRACE_HRS.value))

        ccf_soma_compartment = safe_string(
            row.get(SmartsheetField.CCF_SOMA_COMPARTMENT.value)
        )
        soma_compartment_manual = safe_string(
            row.get(SmartsheetField.SOMA_COMPARTMENT_MANUAL.value)
        )
        assigned_type = safe_string(row.get(SmartsheetField.ASSIGNED_TYPE.value))
        segmentation_version = row.get(SmartsheetField.SEGMENTATION_VERSION.value)
        if isinstance(segmentation_version, str):
            segmentation_version = segmentation_version.strip() or None
        elif pd.isna(segmentation_version):
            segmentation_version = None
        annotator1 = safe_string(row.get(SmartsheetField.ANNOTATOR_1.value))
        annotator2 = safe_string(row.get(SmartsheetField.ANNOTATOR_2.value))

        for name in (annotator1, annotator2):
            if name:
                annotators.add(name)

        date_started = safe_datetime(row.get(SmartsheetField.DATE_STARTED.value))
        date_completed = safe_datetime(row.get(SmartsheetField.DATE_COMPLETED.value))

        entry: dict[str, Any] = {
            SmartsheetField.HORTA_COORDINATES.value: (
                list(horta_xyz) if horta_xyz is not None else None
            ),
            SmartsheetField.CCF_COORDINATES.value: (
                list(ccf_xyz) if ccf_xyz is not None else None
            ),
            SmartsheetField.CCF_SOMA_COMPARTMENT.value: ccf_soma_compartment,
            SmartsheetField.SOMA_COMPARTMENT_MANUAL.value: soma_compartment_manual,
            SmartsheetField.DATE_STARTED.value: date_started,
            SmartsheetField.DATE_COMPLETED.value: date_completed,
            SmartsheetField.NEURON_LENGTH_MM.value: neuron_length_mm,
            SmartsheetField.TIME_TO_TRACE_HRS.value: time_to_trace_hrs,
            SmartsheetField.ASSIGNED_TYPE.value: assigned_type,
            SmartsheetField.SEGMENTATION_VERSION.value: segmentation_version,
            SmartsheetField.ANNOTATOR_1.value: annotator1,
            SmartsheetField.ANNOTATOR_2.value: annotator2,
        }

        missing_required = [
            field.value for field in REQUIRED_FIELDS if entry.get(field.value) is None
        ]
        if missing_required:
            formatted = ", ".join(sorted(missing_required))
            raise ValueError(
                f"Missing required field(s) {formatted} for neuron ID {cell_id}"
            )

        missing_optional = sorted(
            field.value for field in OPTIONAL_FIELDS if entry.get(field.value) is None
        )
        if missing_optional:
            logger.warning(
                "Neuron %s missing optional field(s): %s",
                cell_id,
                ", ".join(missing_optional),
            )

        sanitized_entry = {k: v for k, v in entry.items() if v is not None}
        curation_values[cell_id] = sanitized_entry

    if not curation_values:
        if downloaded_cell_ids is not None and json_dir is not None:
            raise ValueError(
                f"No usable reconstruction records found for Mouse ID {mouse_id} "
                f"after filtering to downloaded JSON files in {json_dir}"
            )
        raise ValueError(
            f"No usable reconstruction records found for Mouse ID {mouse_id}"
        )

    qc = build_quality_control(
        curation_values=curation_values,
        annotators=annotators,
    )

    return qc
