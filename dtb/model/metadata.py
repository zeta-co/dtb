from typing import Any, Dict, List
from dataclasses import dataclass


@dataclass
class Metadata:
    """A wrapper class for managing and validating ETL (Extract, Transform, Load) metadata.

    This class provides a structured interface for accessing and validating metadata
    configurations used in ETL operations, including source/target information, streaming
    configurations, and schema details. It handles various data source types and provides
    convenient properties for accessing common metadata attributes.

    Attributes:
        _metadata (Dict[str, Any]): Raw metadata dictionary containing all configuration
            properties for the data source/target.

    Properties:
        type (str):
            The lowercase type of the data source (e.g., 'delta', 'parquet', 'csv').

        path (str):
            The location path for the data source/target.

        is_table (bool):
            Whether the path represents a table (True) or file path (False).

        table_details (Dict[str, str]):
            Dictionary containing catalog, schema, and table names if is_table is True.
            Returns empty dict for file paths.

        table_catalog (str):
            The catalog name for table paths. None for file paths.

        table_schema (str):
            The schema name for table paths. None for file paths.

        table_name (str):
            The table name for table paths. None for file paths.

        is_stream (bool):
            Whether the source should be processed as a stream. True for cloudfiles type
            or when explicitly set.

        format_options (Dict[str, Any]):
            Additional format-specific options for reading/writing data.

        schemas (Dict[str, Any]):
            Schema definitions for the data source/target.

        mode (str):
            Specifies the behavior when data or table already exists.
            Options include:
            - append: Append contents of this DataFrame to existing data.
            - overwrite: Overwrite existing data.
            - error or errorifexists: Throw an exception if data already exists.
            - ignore: Silently ignore this operation if data already exists.

        output_mode (str):
            Specifies how data of a streaming DataFrame/Dataset is written to a streaming sink.
            Options include:
            - append: Only the new rows in the streaming DataFrame/Dataset will be written to the sink
            - complete: All the rows in the streaming DataFrame/Dataset will be written to the sink every time these are some updates
            - update: Only the rows that were updated in the streaming DataFrame/Dataset will be written to the sink every time there are some updates. If the query doesn’t contain aggregations, it will be equivalent to append mode.

        partition_by (List[str]):
            List of columns to partition the data by.

        sort_by (List[str]):
            List of columns to sort the data by.

        table_properties (Dict[str, Any]):
            Table property dictionary.

    Examples:
        >>> # Create metadata for a Delta table
        >>> table_metadata = Metadata({
        ...     "type": "delta",
        ...     "path": "my_catalog.my_schema.my_table",
        ...     "mode": "append",
        ...     "partition_by": ["date"],
        ...     "schemas": {
        ...         "columns": {
        ...             "id": "string",
        ...             "date": "date",
        ...             "value": "double"
        ...         }
        ...     }
        ... })
        >>>
        >>> print(table_metadata.is_table)  # True
        >>> print(table_metadata.table_details)
        # {'catalog': 'my_catalog', 'schema': 'my_schema', 'table': 'my_table'}

        >>> # Create metadata for a streaming source
        >>> stream_metadata = Metadata({
        ...     "type": "cloudfiles",
        ...     "path": "s3://bucket/path",
        ...     "format_options": {
        ...         "cloudFiles.format": "json",
        ...         "cloudFiles.schemaLocation": "s3://bucket/checkpoint"
        ...     },
        ...     "schemas": {...}
        ... })
        >>>
        >>> print(stream_metadata.is_stream)  # True
        >>> print(stream_metadata.is_table)   # False

    Notes:
        - Table paths can be specified in either 2-part (schema.table) or
          3-part (catalog.schema.table) format
        - For 2-part table paths, 'hive_metastore' is used as the default catalog
        - The is_stream property returns True for 'cloudfiles' type or when 'is_stream'
          is explicitly set to True/1/T
        - All dictionary keys are case-sensitive
        - Mode and output_mode values are converted to lowercase
        - Default empty values: format_options={}, partition_by=[], sort_by=[],
          mode='', output_mode=''
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
    def schemas(self) -> List[Dict[str, Any]]:
        return self._metadata.get("schemas", [])

    def to_dict(self) -> Dict[str, Any]:
        return self._metadata

    @property
    def mode(self) -> str:
        return self._metadata.get("mode", "").lower()

    @property
    def output_mode(self) -> str:
        return self._metadata.get("output_mode", "").lower()

    @property
    def partition_by(self) -> List[str]:
        return self._metadata.get("partition_by", [])

    @property
    def sort_by(self) -> List[str]:
        return self._metadata.get("sort_by", [])

    @property
    def table_properties(self) -> Dict[str, Any]:
        return self._metadata.get("table_properties", {})
