from typing import Any, Dict, List
from dataclasses import dataclass


@dataclass
class Metadata:
    """A wrapper class for handling ETL metadata with helper methods.

    This class provides a structured interface for accessing and validating metadata
    properties commonly used in ETL operations, including table information, stream
    configurations, and schema details.

    Attributes:
        _metadata (Dict[str, Any]): Raw metadata dictionary containing configuration
            for data source properties.
    """

    _metadata: Dict[str, Any]

    def __post_init__(self):
        """Validates metadata after initialisation."""
        # TODO validate metadata
        pass

    @property
    def type(self) -> str:
        """Gets the lowercase type of the data source.

        Returns:
            str: The data source type (e.g., 'delta', 'parquet', 'csv').
        """
        return self._metadata["type"].lower()

    @property
    def path(self) -> str:
        return self._metadata["path"]

    @property
    def is_table(self) -> bool:
        if "/" in self.path:
            return False
        return True

    @property
    def table_details(self) -> Dict[str, str]:
        if self.is_table:
            components = self.path.split(".")
            if len(components) == 3:
                catalog, schema, table = components
            elif len(components) == 2:
                catalog = "hive_metastore"
                schema, table = components
            return {"catalog": catalog, "schema": schema, "table": table}
        return {}

    @property
    def table_catalog(self) -> str:
        return self.table_details.get("catalog", None)

    @property
    def table_schema(self) -> str:
        return self.table_details.get("schema", None)

    @property
    def table_name(self) -> str:
        return self.table_details.get("table", None)

    @property
    def is_stream(self) -> bool:
        if self.type == "cloudfiles":
            return True
        is_stream = self._metadata.get("is_stream")
        if str(is_stream).lower() in ("t", "true", "1"):
            return True
        else:
            return False

    @property
    def format_options(self) -> Dict[str, Any]:
        return self._metadata.get("format_options", {})

    @property
    def schema(self) -> Dict[str, Any]:
        return self._metadata["schema"]

    # TODO - handle date variant
    @property
    def schema_string(self) -> str:
        return ",".join(
            [f'{c["name"]} {c["type"]}' for c in self._metadata["schema"]["columns"]]
        )

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
    
    @property
    def mode(self) -> str:
        return self._metadata.get("mode", '').lower()

    @property
    def output_mode(self) -> str:
        return self._metadata.get("output_mode", '').lower()
    
    @property
    def partition_by(self) -> List[str]:
        return self._metadata.get("partition_by", [])

    @property
    def sort_by(self) -> List[str]:
        return self._metadata.get("sort_by", [])
