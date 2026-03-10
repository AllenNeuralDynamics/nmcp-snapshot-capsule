# nmcp snapshot capsule

This capsule builds a reconstruction snapshot for one exaSPIM subject and publishes the results to S3.

It performs the following workflow:
- Parses the subject ID from a raw data asset URI.
- Downloads published neuron reconstructions from the NMCP service in both spaces:
  - CCF space (`reconstruction-space=1`): JSON and SWC
  - Specimen space (`reconstruction-space=0`): JSON and SWC
- Generates Neuroglancer precomputed skeleton outputs from downloaded JSON reconstructions.
- Generates reconstruction metadata JSON files.
- Uploads everything under `/results` to an S3 destination generated from the raw asset name and a destination bucket.

## Run script

From the capsule code directory:

```bash
cd /root/capsule/code
./run <raw-data-asset-uri> <s3-destination-bucket> <reconstruction-excel-file> <fused-zarr-path> <processing.json path>
```

Arguments:
- `<raw-data-asset-uri>`: Source dataset URI. Must contain `exaSPIM_<subject>_...` so the script can extract the subject ID and download required metadata files.
- `<s3-destination-bucket>`: Destination bucket for final results (for example, `aind-open-data` or `s3://aind-open-data`).
- `<reconstruction-excel-file>`: Path to the reconstruction spreadsheet (for example, `/root/capsule/data/Neuron Reconstructions.xlsx`).
- `<fused-zarr-path>`: OME-Zarr group path used to derive specimen-space precomputed resolution and volume size.
- `<processing.json path>: Path to the processing.json file within the final processed reconstruction asset in CodeOcean, mounted to the capsule.

Example:

```bash
cd /root/capsule/code
./run \
  "s3://aind-open-data/exaSPIM_685221_2024-04-12_11-46-38" \
  "aind-open-data" \
  "/root/capsule/data/Neuron Reconstructions.xlsx" \
  "s3://aind-open-data/exaSPIM_685221_2024-04-12_11-46-38_fusion_2024-07-22_21-00-15/fused.zarr" \
  "/root/capsule/data/swc_processing_pipeline_685221_2026_03_04/processing.json"
```

For the example above, the upload destination is generated automatically in this format:

```text
s3://aind-open-data/exaSPIM_685221_2024-04-12_11-46-38_reconstructions_<current_date>_<current_time>
```

## Outputs

The script writes to `/results` and then syncs that directory to the generated S3 destination.

Main outputs:
- `/results/ccf_space_reconstructions/json`
- `/results/ccf_space_reconstructions/swc`
- `/results/ccf_space_reconstructions/reconstructions.precomputed`
- `/results/specimen_space_reconstructions/json`
- `/results/specimen_space_reconstructions/swc`
- `/results/specimen_space_reconstructions/reconstructions.precomputed`
- `/results/acquisition.json`
- `/results/instrument.json`
- `/results/subject.json`
- `/results/procedures.json`
- `/results/quality_control.json`
- `/results/data_description.json`
- `/results/processing.json`

## Prerequisites

- AWS CLI configured with credentials/permissions to read required inputs and write to the generated destination under `<s3-destination-bucket>`.
- Network access to:
  - `https://morphology.allenneuraldynamics.org`
  - Referenced S3 paths
- Capsule dependencies installed (handled by this capsule environment setup).
