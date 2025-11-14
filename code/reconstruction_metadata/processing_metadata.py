from __future__ import annotations

from datetime import datetime

from aind_data_schema.core.processing import (DataProcess, Processing,
                                              ProcessStage)
from aind_data_schema_models.process_names import ProcessName


def create_processing_metadata(start_date_time: datetime) -> Processing:
    """
    Create processing metadata for the neuron reconstruction pipeline.

    Parameters
    ----------
    start_date_time : datetime
        Timestamp marking when the processing step began.

    Returns
    -------
    Processing
        Processing metadata populated with a single ``DataProcess`` entry.
    """
    processing_info = {
        "stage": ProcessStage.ANALYSIS,
        "process_type": ProcessName.NEURON_SKELETON_PROCESSING,
        "code": {"url": "https://github.com/AllenNeuralDynamics/neuron-tracing-utils"},
        "name": "Neuron Reconstruction Processing Pipeline",
        "notes": "Neuron Reconstruction Processing Pipeline",
        "experimenters": ["MSMA Team"],
        "start_date_time": start_date_time,
        "end_date_time": datetime.now(),
    }
    process = DataProcess(**processing_info)
    return Processing(data_processes=[process], dependency_graph=None)


if __name__ == "__main__":
    processing = create_processing_metadata(datetime.now())
    print(processing)
