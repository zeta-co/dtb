from dataclasses import dataclass
from typing import Optional, Dict
from .table import Table


@dataclass
class DeltaTableConfig:
    """Configuration for Delta table properties and partitioning."""

    table: Table
    partition_columns: Optional[list[str]] = None
    properties: Dict[str, str] = None

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

    @property
    def full_table_name(self) -> str:
        return self.table.full_name
