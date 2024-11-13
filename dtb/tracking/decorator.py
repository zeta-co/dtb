import functools
from typing import Any, Callable, Dict
from pyspark.sql import SparkSession
from ..logging.log_entry import DeltaVersionLogEntry
from ..logging.log_service import LogService
from ..io.output import Output
from .tracker_delta_version import DeltaVersionTracker


def log_delta_versions(
    spark: SparkSession, log_service: LogService, common_metadata: Dict[str, Any]
) -> Callable[..., Any]:
    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            datasets = getattr(func, "datasets", {})
            trackers: Dict[str, DeltaVersionTracker] = {}
            log_entries: Dict[str, Dict[str, Any]] = {}

            for ds_name, ds in datasets.items():
                if isinstance(ds, Output) and ds.metadata.type == "delta":
                    metadata = {**common_metadata}
                    if ds.metadata.is_table:
                        metadata["table_name"] = ds.metadata.path
                        metadata["table_path"] = None
                    else:
                        metadata["table_name"] = None
                        metadata["table_path"] = ds.metadata.path
                    trackers[ds_name] = DeltaVersionTracker(spark, metadata)
                    # start tracking
                    trackers[ds_name].start(log_entries[ds_name])

            result = func(*args, **kwargs)

            # End tracking for each output
            for ds_name, tracker in trackers.items():
                tracker.end(spark, log_entries[ds_name])
                log_service.add_log_entry(
                    DeltaVersionLogEntry(log_entry_dict=tracker.get_log_entry_dict())
                )
            log_service.flush()
            
            return result

        return wrapper

    return decorator
