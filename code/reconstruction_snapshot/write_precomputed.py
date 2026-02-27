import argparse
import glob
import json
from copy import deepcopy
from pathlib import Path

from cloudvolume import CloudVolume
from precomputed import create_from_dict, create_from_json_files


def _load_dataset_zero_metadata(zarr_group_path: str) -> tuple[tuple[int, ...], list[float]]:
    try:
        import fsspec
        import zarr
    except ImportError as ex:
        raise RuntimeError(
            "Missing optional dependencies for --zarr-group. Install `zarr` and `fsspec`."
        ) from ex

    try:
        group_path = zarr_group_path.rstrip("/")
        group_store = fsspec.get_mapper(group_path)
        group = zarr.open_group(store=group_store, mode="r")

        multiscales = group.attrs.get("multiscales")
        if not isinstance(multiscales, list) or len(multiscales) == 0:
            raise ValueError("OME-Zarr metadata is missing `multiscales`.")

        datasets = multiscales[0].get("datasets")
        if not isinstance(datasets, list):
            raise ValueError("OME-Zarr metadata is missing `multiscales[0].datasets`.")

        dataset_zero = None
        for dataset in datasets:
            if isinstance(dataset, dict) and str(dataset.get("path")) == "0":
                dataset_zero = dataset
                break

        if dataset_zero is None:
            raise ValueError("OME-Zarr metadata is missing dataset path `0`.")

        transform = None
        for item in dataset_zero.get("coordinateTransformations", []):
            if isinstance(item, dict) and item.get("type") == "scale" and "scale" in item:
                transform = item
                break

        if transform is None:
            raise ValueError("Dataset `0` is missing a `scale` coordinate transformation.")

        scale = transform.get("scale")
        if not isinstance(scale, list) or len(scale) < 3:
            raise ValueError("Dataset `0` scale must include at least 3 values.")

        scale_zyx = [float(scale[-3]), float(scale[-2]), float(scale[-1])]
        for value in scale_zyx:
            if value <= 0:
                raise ValueError("Scale values must be greater than zero.")

        # OME scale is z,y,x for spatial axes; convert to x,y,z for downstream usage.
        scale_um_xyz = [scale_zyx[2], scale_zyx[1], scale_zyx[0]]

        array_store = fsspec.get_mapper(f"{group_path}/0")
        zarr_array = zarr.open_array(store=array_store, mode="r")
        shape = tuple(int(dim) for dim in zarr_array.shape)

        return shape, scale_um_xyz
    except Exception as ex:
        raise RuntimeError(
            f"Unable to read OME-Zarr metadata from group '{zarr_group_path}'."
        ) from ex


def _shape_to_volume_size(shape: tuple[int, ...]) -> list[int]:
    if len(shape) < 3:
        raise ValueError(
            f"Zarr array shape must have at least 3 dimensions, received {shape}."
        )

    return [int(shape[-1]), int(shape[-2]), int(shape[-3])]


def _build_precomputed_info(volume_size_xyz: list[int], resolution_nm_xyz: list[float]) -> dict:
    info = CloudVolume.create_new_info(
        num_channels=1,
        layer_type="segmentation",
        data_type="uint64",
        encoding="raw",
        resolution=resolution_nm_xyz,
        voxel_offset=[0, 0, 0],
        skeletons="skeleton",
        chunk_size=[128, 128, 128],
        volume_size=volume_size_xyz,
    )

    info["segment_properties"] = "segment_properties"
    return info


def _scale_point_to_voxel_space(point: dict, scale_um_xyz: list[float]):
    for axis, divisor in (("x", scale_um_xyz[0]), ("y", scale_um_xyz[1]), ("z", scale_um_xyz[2])):
        if axis in point and point[axis] is not None:
            point[axis] = float(point[axis]) / divisor


def _scale_neuron_to_voxel_space(neuron: dict, scale_um_xyz: list[float]) -> dict:
    scaled = deepcopy(neuron)

    soma = scaled.get("soma")
    if isinstance(soma, dict):
        _scale_point_to_voxel_space(soma, scale_um_xyz)

    for key in ("axon", "dendrite"):
        points = scaled.get(key)
        if isinstance(points, list):
            for point in points:
                if isinstance(point, dict):
                    _scale_point_to_voxel_space(point, scale_um_xyz)

    return scaled


def main():
    """Convert JSON files to precomputed format and upload to S3."""
    parser = argparse.ArgumentParser(
        description="Convert JSON files to precomputed format and upload to S3"
    )
    parser.add_argument(
        "--input-dir",
        type=str,
        required=True,
        help="Input directory containing JSON files"
    )
    parser.add_argument(
        "--output",
        type=str,
        required=True,
        help="Output dir or S3 URI (e.g., s3://bucket-name/path)"
    )
    parser.add_argument(
        "--zarr-group",
        type=str,
        required=False,
        help=(
            "Optional path to an OME-Zarr group root (e.g., s3://bucket/path/to/group). "
            "When provided, dataset `0` metadata drives info resolution/volume size and "
            "neuron coordinates are scaled to voxel space."
        ),
    )
    
    args = parser.parse_args()
    
    # Find all JSON files in the input directory
    input_path = Path(args.input_dir)
    json_pattern = str(input_path / "*.json")
    jsons = glob.glob(json_pattern, recursive=True)
    
    print(f"Found {len(jsons)} JSON files in {args.input_dir}")
    print(f"Files: {jsons}")
    
    # Create precomputed format and upload to S3
    info = None
    if args.zarr_group:
        try:
            shape, scale_um_xyz = _load_dataset_zero_metadata(args.zarr_group)
            volume_size_xyz = _shape_to_volume_size(shape)
            resolution_nm_xyz = [scale_um_xyz[0] * 1000.0, scale_um_xyz[1] * 1000.0, scale_um_xyz[2] * 1000.0]
            info = _build_precomputed_info(volume_size_xyz, resolution_nm_xyz)
        except Exception as ex:
            raise SystemExit(
                f"Error preparing info from zarr group '{args.zarr_group}': {ex}"
            ) from ex

        print(f"Zarr dataset `0` shape: {shape}")
        print(f"Dataset `0` scale (um, x/y/z): {scale_um_xyz}")
        print(f"Derived precomputed resolution (nm, x/y/z): {resolution_nm_xyz}")
        print(f"Derived precomputed volume_size (x,y,z): {volume_size_xyz}")

    else:
        # Assumes 10um CCFv3 space defaults
        volume_size_xyz = [1320, 800, 1140]
        resolution_nm_xyz = [10000, 10000, 10000]
        scale_um_xyz = [10, 10, 10]
        info = _build_precomputed_info(volume_size_xyz, resolution_nm_xyz)
    
    for json_file in jsons:
        with open(json_file) as f:
            data = json.load(f)
        neuron = data["neurons"][0]
        scaled_neuron = _scale_neuron_to_voxel_space(neuron, scale_um_xyz)
        create_from_dict(scaled_neuron, args.output, info)

    print(f"Successfully uploaded to {args.output}")


if __name__ == "__main__":
    main()
