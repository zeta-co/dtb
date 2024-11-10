from typing import Dict, Any, Optional
from dataclasses import dataclass


@dataclass
class Metadata:
    """Wrapper class for metadata dictionary with helper methods"""

    _metadata: Dict[str, Any]

    def __post_init__(self):
        # TODO validate metadata
        pass

    @property
    def type(self) -> str:
        return self._metadata["type"].lower()

    @property
    def path(self) -> str:
        return self._metadata.get("path", None)

    @property
    def table(self) -> str:
        return self._metadata.get("table", None)

    @property
    def format_options(self) -> Dict[str, Any]:
        return self._metadata.get("format_options", {})

    @property
    def schema(self) -> Dict[str, Any]:
        return self._metadata["schema"]

    def get_column_names(self) -> list:
        return [col["name"] for col in self.schema["columns"]]

    def get_partition_columns(self) -> list:
        return [
            col["name"]
            for col in self.schema["columns"]
            if col.get("partition_key", False)
        ]

    def to_dict(self) -> Dict[str, Any]:
        return self._metadata
