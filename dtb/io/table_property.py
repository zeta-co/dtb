from dataclasses import dataclass
from enum import Enum
from typing import Any, Optional, Set


class TablePropertyType(Enum):
    """Enumeration of supported table property types."""

    DELTA = "delta"
    HIVE = "hive"
    CUSTOM = "custom"


class TablePropertyValueType(Enum):
    """Enumeration of supported property value types."""

    BOOLEAN = "boolean"
    INTEGER = "integer"
    STRING = "string"
    CALENDAR_INTERVAL = "calendar_interval"
    SIZE_IN_BYTES = "size_in_bytes"
    ENUM = "enum"


@dataclass
class TablePropertyDefinition:
    """Defines the validation rules for a table property."""

    value_type: TablePropertyValueType
    allowed_values: Optional[Set[str]] = None
    min_value: Optional[int] = None
    max_value: Optional[int] = None
    default_value: Optional[Any] = None
    description: Optional[str] = None


@dataclass
class TableProperty:
    """Represents a single table property with its key, value, and type."""

    key: str
    value: Any

    def __str__(self) -> str:
        """String representation of the property for SQL statements."""
        return f"'{self.key}' = '{self.value}'"

    @property
    def property_type(self) -> TablePropertyType:
        if self.key.startswith("delta."):
            return TablePropertyType.DELTA
        if self.key.startswith("hive."):
            return TablePropertyType.HIVE
        return TablePropertyType.CUSTOM
