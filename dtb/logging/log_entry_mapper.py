from typing import Any, Dict
from pyspark.sql.types import StructType


class LogEntryMapper:
    """Helper class to map and transform log entries to specific schemas"""

    @staticmethod
    def snake_to_pascal(snake_str: str) -> str:
        """Convert snake_case to PascalCase"""
        return "".join(word.title() for word in snake_str.split("_"))

    @staticmethod
    def get_schema_fields(schema: StructType) -> Set[str]:
        """Get set of field names from schema"""
        return {field.name for field in schema.fields}

    @staticmethod
    def transform_key(key: str, target_naming: str = "pascal") -> str:
        """Transform key to target naming convention"""
        if target_naming == "pascal":
            return LogEntryMapper.snake_to_pascal(key)
        # Add more naming conventions as needed
        return key

    @classmethod
    def map_to_schema(
        cls, data: Dict[str, Any], schema: StructType, target_naming: str = "pascal"
    ) -> Dict[str, Any]:
        """
        Map dictionary to schema fields with naming convention transformation.
        Only includes fields present in the schema.
        """
        schema_fields = cls.get_schema_fields(schema)
        result = {}

        for key, value in data.items():
            transformed_key = cls.transform_key(key, target_naming)
            if transformed_key in schema_fields:
                result[transformed_key] = value

        return result
