from __future__ import annotations

from enum import IntEnum


class ExportFormat(IntEnum):
    SWC = 0
    JSON = 1


class ReconstructionSpace(IntEnum):
    SPECIMEN = 0
    CCF = 1

    @classmethod
    def parse_name(cls, value: str) -> "ReconstructionSpace":
        normalized = value.strip().upper()
        try:
            return cls[normalized]
        except KeyError as exc:
            names = ", ".join(space.name.lower() for space in cls)
            raise ValueError(
                f"Invalid reconstruction space '{value}'. Use one of {{{names}}}."
            ) from exc
