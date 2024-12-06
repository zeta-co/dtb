from ..model.delta_table_config import DeltaTableConfig


class LogDeltaTableConfig(DeltaTableConfig):

    def __post_init__(self):
        if self.properties is None:
            self.properties = {
                "delta.autoOptimize.optimizeWrite": "true",
                "delta.autoOptimize.autoCompact": "true",
                "delta.logRetentionDuration": "interval 90 days",
                "delta.appendOnly": "true",
                "delta.enableParallelFileListings": "false",
                "delta.deletedFileRetentionDuration": "interval 7 days",
            }
