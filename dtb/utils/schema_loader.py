from typing import Dict, Optional, Union
from ..model.schema_registry import SchemaRegistry
from .date_extractor import DateExtractor


class SchemaLoader:
    """Loads schema for files."""

    def __init__(
        self, schema_registry: SchemaRegistry, datetime_pattern: Union[str, Dict]
    ):
        self.schema_registry = schema_registry
        self.datetime_pattern = datetime_pattern

    def get_schema_for_file(self, filename: str) -> Optional[Dict[str, str]]:
        """Get the appropriate schema for a file."""
        if isinstance(self.datetime_pattern, dict):
            file_date = DateExtractor.extract_custom(
                filename,
                self.datetime_pattern["regex"],
                self.datetime_pattern["datetime_format"],
            )
        else:
            file_date = DateExtractor.extract_from(filename, self.datetime_pattern)
        if file_date is None:
            raise ValueError(f"Could not extract date from filename: {filename}")

        schema = self.schema_registry.get_schema_for_date(file_date)
        if schema is None:
            raise ValueError(f"No schema found for file date: {file_date}")

        return schema
