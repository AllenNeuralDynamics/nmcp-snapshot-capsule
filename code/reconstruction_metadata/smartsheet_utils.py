from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from numbers import Integral
from typing import Any, Optional, Tuple

import pandas as pd


class SmartsheetField(str, Enum):
    """Column names used within the neuron reconstruction Smartsheet."""

    MOUSE_ID = "Mouse ID"
    CELL_ID = "ID"
    HORTA_COORDINATES = "Horta Coordinates"
    CCF_COORDINATES = "CCF Coordinates"
    NEURON_LENGTH_MM = "Neuron Length (mm)"
    TIME_TO_TRACE_HRS = "Time to Trace (hrs)"
    CCF_SOMA_COMPARTMENT = "CCF Soma Compartment"
    SOMA_COMPARTMENT_MANUAL = "Manual Estimated Soma Compartment"
    ASSIGNED_TYPE = "Assigned Type"
    SEGMENTATION_VERSION = "Segmentation Version"
    NOTES = "Notes"
    ANNOTATOR_1 = "Annotator 1"
    ANNOTATOR_2 = "Annotator 2"
    DATE_STARTED = "Date Started"
    DATE_COMPLETED = "Date Completed"
    STATUS_1 = "Status 1"


class SmartsheetStatus(str, Enum):
    """Well-known status values for reconstruction rows."""

    COMPLETED = "Completed"


def parse_coord(
    val: str | float | Tuple[float, float, float],
) -> Optional[Tuple[float, float, float]]:
    """
    Normalize a Smartsheet coordinate entry into a numeric XYZ tuple.

    Parameters
    ----------
    val : str | float | tuple[float, float, float]
        Coordinate value read from the sheet. Strings may be formatted like
        ``"[x, y, z]"`` or ``"(x, y, z)"``.

    Returns
    -------
    tuple[float, float, float] | None
        Parsed coordinate triple, or ``None`` when the value is blank.

    Raises
    ------
    ValueError
        If the value cannot be parsed into three numeric components.
    """
    if val is None:
        return None
    if isinstance(val, str):
        stripped = val.strip()
        if not stripped:
            return None
        return tuple(float(x) for x in stripped.strip("[]()").split(","))  # type: ignore[return-value]
    if pd.isna(val):
        return None
    if isinstance(val, (tuple, list)):
        return tuple(float(x) for x in val)  # type: ignore[return-value]
    raise ValueError(f"Cannot parse coordinate value: {val!r}")


def safe_datetime(series_value: Any) -> Optional[datetime]:
    """
    Convert spreadsheet timestamp fields into timezone-aware datetimes.

    Parameters
    ----------
    series_value : Any
        Raw timestamp value, typically parsed by Pandas from the Smartsheet.

    Returns
    -------
    datetime | None
        Timezone-aware UTC datetime when conversion succeeds, otherwise ``None``.
    """
    if (
        series_value is None
        or (isinstance(series_value, str) and not series_value.strip())
        or pd.isna(series_value)
    ):
        return None

    ts = pd.to_datetime(series_value, utc=True, errors="coerce")
    if pd.isna(ts):
        return None
    if getattr(ts, "tzinfo", None) is None:
        ts = ts.tz_localize(timezone.utc)
    return ts.to_pydatetime()


def safe_string(value: Any) -> Optional[str]:
    """
    Normalize a scalar value into a stripped string.

    Parameters
    ----------
    value : Any
        Cell value to normalize.

    Returns
    -------
    str | None
        Trimmed string value, or ``None`` when the input is blank.
    """
    if value is None:
        return None
    if isinstance(value, str):
        stripped = value.strip()
        return stripped or None
    if pd.isna(value):
        return None
    stripped = str(value).strip()
    return stripped or None


def normalize_mouse_id(value: Any) -> Optional[str]:
    """
    Convert Smartsheet entries into canonical mouse identifiers.

    Parameters
    ----------
    value : Any
        Raw mouse identifier from the sheet.

    Returns
    -------
    str | None
        String representation of the mouse identifier, or ``None`` if missing.
    """
    if pd.isna(value):
        return None
    if isinstance(value, Integral):
        return str(value)
    if isinstance(value, float):
        if float(value).is_integer():
            return str(int(value))
        return str(value).strip()
    value_str = str(value).strip()
    if not value_str:
        return None
    try:
        return str(int(float(value_str)))
    except ValueError:
        return value_str


def ensure_cell_id(
    raw_id: Any,
    fallback_suffix: int,
    curation_values: dict[str, dict[str, Any]],
    normalized_mouse_id: str,
) -> str:
    """
    Construct a unique cell identifier for a reconstruction row.

    Parameters
    ----------
    raw_id : Any
        Cell identifier provided in the sheet.
    fallback_suffix : int
        Row index used to synthesize an identifier when ``raw_id`` is missing.
    curation_values : dict[str, dict[str, Any]]
        Map of known cell identifiers to their QC entries.
    normalized_mouse_id : str
        Mouse identifier used as a prefix for synthesized IDs.

    Returns
    -------
    str
        Identifier that is unique within ``curation_values``.
    """
    if pd.isna(raw_id):
        base = f"{normalized_mouse_id}_{fallback_suffix}"
    else:
        base = str(raw_id).strip()
    if not base:
        base = f"{normalized_mouse_id}_{fallback_suffix}"

    candidate = base
    counter = 1
    while candidate in curation_values:
        candidate = f"{base}_{counter}"
        counter += 1
    return candidate


def safe_float(value: Any) -> Optional[float]:
    """
    Convert numeric-like spreadsheet values to ``float``.

    Parameters
    ----------
    value : Any
        Spreadsheet value to convert.

    Returns
    -------
    float | None
        Floating-point representation, or ``None`` when conversion fails.
    """
    if (
        value is None
        or (isinstance(value, str) and not value.strip())
        or pd.isna(value)
    ):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None
