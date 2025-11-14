from __future__ import annotations

from datetime import datetime
from typing import Union

from aind_data_schema.components.identifiers import Person
from aind_data_schema.core.data_description import DataDescription, Funding
from aind_data_schema_models.data_name_patterns import DataLevel
from aind_data_schema_models.modalities import Modality
from aind_data_schema_models.organizations import Organization


def create_data_description(subject: Union[str, int]) -> DataDescription:
    """
    Construct a ``DataDescription`` record for a reconstruction dataset.

    Parameters
    ----------
    subject : str | int
        Subject identifier associated with the dataset.

    Returns
    -------
    DataDescription
        Metadata describing the dataset, including investigators, project name,
        and acquisition parameters.
    """
    return DataDescription(
        modalities=[Modality.SPIM],
        subject_id=str(subject),
        creation_time=datetime.now(),
        institution=Organization.AIND,
        investigators=[
            Person(name="Jayaram Chandrashekar"),
            Person(name="Karel Svoboda"),
        ],
        funding_source=[Funding(funder=Organization.AI)],
        project_name="MSMA",
        data_level=DataLevel.RAW,
    )
