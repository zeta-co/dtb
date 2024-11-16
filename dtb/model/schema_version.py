from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Optional


@dataclass
class SchemaVersion:
    """Represents a schema version with its effective time period."""

    start_date: datetime
    end_date: Optional[datetime]  # None means "currently active"
    columns: Dict[str, str]  # column_name -> data_type
    version: int  # Sequential version number

    def __str__(self):
        end_str = (
            "PRESENT" if self.end_date is None else self.end_date.strftime("%Y-%m-%d")
        )
        return (
            f"Schema V{self.version}: "
            f"{self.start_date.strftime('%Y-%m-%d')} to {end_str}"
        )
