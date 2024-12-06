from dataclasses import dataclass
from typing import Optional, Dict
from .table import Table


@dataclass(frozen=False)
class DeltaTableConfig:
    """Configuration for Delta table properties and partitioning."""

    table: Table
    partition_columns: Optional[list[str]] = None
    properties: Optional[Dict[str, str]] = None

    @property
    def full_table_name(self) -> str:
        return self.table.full_name
