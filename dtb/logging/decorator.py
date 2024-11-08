import functools
from typing import Dict, Any, Callable
from pyspark.sql import SparkSession
from .output_handler import OutputHandler
from ..tracking.delta_version_log_entry import DeltaVersionLogEntry
from ..model.output import Output
from ..tracking.delta_version_tracker import DeltaVersionTracker


def log_delta_versions(
    spark: SparkSession, 
    output_handler: OutputHandler, 
    common_metadata: Dict[str, Any]
) -> Callable[..., Any]:
    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            datasets = getattr(func, "datasets", {})
            trackers: Dict[str, DeltaVersionTracker] = {}
            log_entries: Dict[str, DeltaVersionLogEntry] = {}
            for ds_name, ds in datasets.items():
                if isinstance(ds, Output) and ds.metadata.get("format") == "delta":
                    metadata = { **ds.metadata, **common_metadata }
                    delta_table = metadata.get("save")
                    if '/' in delta_table:
                        metadata["table_name"] = None
                        metadata["table_path"] = delta_table
                    else:
                        metadata["table_name"] = delta_table
                        metadata["table_path"] = None
                    trackers[ds_name] = DeltaVersionTracker(spark, metadata)
                    log_entries[ds_name] = DeltaVersionLogEntry(
                        TableName=metadata["table_name"],
                        TablePath=metadata["table_path"],
                    )
                    trackers[ds_name].start(log_entries[ds_name])

            result = func(*args, **kwargs)
            # End tracking for each output
            for ds_name, tracker in trackers.items():
                tracker.end(spark, log_entries[ds_name])
                output_handler.write(log_entries[ds_name])
            return result

        return wrapper

    return decorator
