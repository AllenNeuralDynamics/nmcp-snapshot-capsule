from __future__ import annotations

from datetime import datetime
from typing import Union

from aind_data_schema.components.identifiers import Person
from aind_data_schema.core.data_description import DataDescription, Funding
from aind_data_schema_models.data_name_patterns import DataLevel
from aind_data_schema_models.modalities import Modality
from aind_data_schema_models.organizations import Organization

from utils import parse_s3_path


def get_fused_asset_uri(fused_zarr_path: str) -> str:
    """
    Resolve the top-level fused asset URI from a fused zarr path.

    Parameters
    ----------
    fused_zarr_path : str
        S3 path to the fused zarr group.

    Returns
    -------
    str
        Top-level fused asset URI.
    """
    try:
        bucket, key = parse_s3_path(fused_zarr_path)
    except ValueError as exc:
        raise ValueError(
            "Fused zarr path must include a bucket and asset prefix."
        ) from exc

    asset_root = key.split("/", 1)[0]
    if not asset_root:
        raise ValueError(
            "Fused zarr path must include a top-level asset prefix after the bucket."
        )

    return f"s3://{bucket}/{asset_root}"


def create_data_description(
    subject: Union[str, int],
    dataset_name: str,
    fused_zarr_path: str,
) -> DataDescription:
    """
    Construct a ``DataDescription`` record for a reconstruction dataset.

    Parameters
    ----------
    subject : str | int
        Subject identifier associated with the dataset.
    dataset_name : str
        Dataset name used as the base for the reconstruction asset name.
    fused_zarr_path : str
        S3 path to the fused zarr group for the source fused asset.

    Returns
    -------
    DataDescription
        Metadata describing the dataset, including investigators, project name,
        and acquisition parameters.
    """
    creation_time = datetime.now()
    fused_asset_uri = get_fused_asset_uri(fused_zarr_path)
    derived_name = (
        f"{dataset_name}_reconstructions_"
        f"{creation_time.strftime('%Y-%m-%d_%H-%M-%S')}"
    )

    return DataDescription(
        modalities=[Modality.SPIM],
        subject_id=str(subject),
        creation_time=creation_time,
        name=derived_name,
        institution=Organization.AIND,
        investigators=[
            Person(name="Jayaram Chandrashekar"),
            Person(name="Karel Svoboda"),
        ],
        funding_source=[Funding(funder=Organization.AI)],
        project_name="MSMA",
        data_level=DataLevel.DERIVED,
        source_data=[fused_asset_uri],
    )
