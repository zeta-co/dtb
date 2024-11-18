from typing import Dict, Optional
from ..model.schema_registry import SchemaRegistry
from .date_extractor import DateExtractor


class SchemaLoader:
    """Loads schema for files."""

    def __init__(self, schema_registry: SchemaRegistry, date_extractor: DateExtractor):
        self.schema_registry = schema_registry
        self.date_extractor = date_extractor

    def get_schema_for_file(self, filename: str) -> Optional[Dict[str, str]]:
        """Get the appropriate schema for a file."""
        file_date = self.date_extractor.extract_date(filename)
        if file_date is None:
            raise ValueError(f"Could not extract date from filename: {filename}")
            
        schema = self.schema_registry.get_schema_for_date(file_date)
        if schema is None:
            raise ValueError(f"No schema found for file date: {file_date}")
            
        return schema