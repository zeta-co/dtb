from abc import ABC, abstractmethod
from typing import Any, Dict, Optional
from .byte_size import ByteSize
from .calendar_interval import CalendarInterval
from .table_property import TablePropertyDefinition, TablePropertyValueType


class TablePropertyValidator(ABC):
    """Validator for table properties."""

    _PROPERTY_DEFINITIONS: Dict[str, TablePropertyDefinition] = {
        "delta.appendOnly": TablePropertyDefinition(
            value_type=TablePropertyValueType.BOOLEAN,
            default_value=False,
            description="Prevents users from modifying existing data in the table.",
        ),
        "delta.autoOptimize.autoCompact": TablePropertyDefinition(
            value_type=TablePropertyValueType.BOOLEAN,
            default_value=False,
            description="Controls whether auto compaction is enabled.",
        ),
        "delta.autoOptimize.optimizeWrite": TablePropertyDefinition(
            value_type=TablePropertyValueType.BOOLEAN,
            default_value=False,
            description="Controls whether optimize write is enabled.",
        ),
        "delta.checkpointInterval": TablePropertyDefinition(
            value_type=TablePropertyValueType.INTEGER,
            min_value=1,
            default_value=10,
            description="Number of commits after which to checkpoint the DeltaLog.",
        ),
        "delta.checkpointPolicy": TablePropertyDefinition(
            value_type=TablePropertyValueType.ENUM,
            allowed_values={"classic", "v2"},
            default_value="classic",
            description="Policy to use for checkpoint cleanup.",
        ),
        "delta.columnMapping.mode": TablePropertyDefinition(
            value_type=TablePropertyValueType.ENUM,
            allowed_values={"name", "id"},
            default_value="name",
            description="Column mapping mode for schema evolution.",
        ),
        "delta.compatibility.symlinkFormatManifest.enabled": TablePropertyDefinition(
            value_type=TablePropertyValueType.BOOLEAN,
            default_value=False,
            description="Controls whether to generate symlink format manifest.",
        ),
        "delta.dataSkippingNumIndexedCols": TablePropertyDefinition(
            value_type=TablePropertyValueType.INTEGER,
            min_value=0,
            max_value=32,
            default_value=32,
            description="Number of columns to collect statistics for.",
        ),
        "delta.deletedFileRetentionDuration": TablePropertyDefinition(
            value_type=TablePropertyValueType.CALENDAR_INTERVAL,
            default_value="interval 1 week",
            description="How long to keep deleted data files.",
        ),
        "delta.enableChangeDataFeed": TablePropertyDefinition(
            value_type=TablePropertyValueType.BOOLEAN,
            default_value=False,
            description="Controls whether to enable Change Data Feed.",
        ),
        "delta.isolationLevel": TablePropertyDefinition(
            value_type=TablePropertyValueType.ENUM,
            allowed_values={"Serializable", "WriteSerializable"},
            default_value="WriteSerializable",
            description="Transaction isolation level.",
        ),
        "delta.logRetentionDuration": TablePropertyDefinition(
            value_type=TablePropertyValueType.CALENDAR_INTERVAL,
            default_value="interval 30 days",
            description="How long to keep transaction log files.",
        ),
        "delta.minReaderVersion": TablePropertyDefinition(
            value_type=TablePropertyValueType.INTEGER,
            min_value=1,
            max_value=3,
            description="Minimum required reader version.",
        ),
        "delta.minWriterVersion": TablePropertyDefinition(
            value_type=TablePropertyValueType.INTEGER,
            min_value=1,
            max_value=7,
            description="Minimum required writer version.",
        ),
        "delta.randomizeFilePrefixes": TablePropertyDefinition(
            value_type=TablePropertyValueType.BOOLEAN,
            default_value=False,
            description="Controls whether to randomize file prefixes.",
        ),
        "delta.randomPrefixLength": TablePropertyDefinition(
            value_type=TablePropertyValueType.INTEGER,
            min_value=1,
            max_value=10,
            default_value=2,
            description="Length of random prefix if randomizeFilePrefixes is true.",
        ),
        "delta.targetFileSize": TablePropertyDefinition(
            value_type=TablePropertyValueType.SIZE_IN_BYTES,
            default_value="134217728b",  # 128MB
            description="Target file size for compaction.",
        ),
    }

    @classmethod
    def validate(cls, key: str, value: Any) -> tuple[bool, Optional[str]]:
        """Validates a Delta table property value.

        Args:
            key (str): Property key.
            value (Any): Property value to validate.

        Returns:
            tuple[bool, Optional[str]]: (is_valid, error_message)
        """
        if key not in cls._PROPERTY_DEFINITIONS:
            return False, f"Unknown Delta table property: {key}"

        definition = cls._PROPERTY_DEFINITIONS[key]

        try:
            # Convert value to appropriate type and validate
            if definition.value_type == TablePropertyValueType.BOOLEAN:
                if isinstance(value, bool):
                    return True, None
                if isinstance(value, str):
                    if value.lower() not in ("true", "false"):
                        return False, f"Invalid boolean value for {key}: {value}"
                    return True, None
                return False, f"Invalid type for {key}: {type(value)}"

            elif definition.value_type == TablePropertyValueType.INTEGER:
                if isinstance(value, str):
                    value = int(value)
                if not isinstance(value, int):
                    return False, f"Invalid integer value for {key}: {value}"
                if definition.min_value is not None and value < definition.min_value:
                    return False, f"Value for {key} must be >= {definition.min_value}"
                if definition.max_value is not None and value > definition.max_value:
                    return False, f"Value for {key} must be <= {definition.max_value}"
                return True, None

            elif definition.value_type == TablePropertyValueType.ENUM:
                if not isinstance(value, str):
                    return False, f"Invalid type for {key}: {type(value)}"
                if value not in definition.allowed_values:
                    return False, (
                        f"Invalid value for {key}. "
                        f"Allowed values: {', '.join(definition.allowed_values)}"
                    )
                return True, None

            elif definition.value_type == TablePropertyValueType.CALENDAR_INTERVAL:
                if not isinstance(value, str):
                    return False, f"Invalid type for {key}: {type(value)}"
                try:
                    CalendarInterval(value)
                    return True, None
                except ValueError as e:
                    return False, str(e)

            elif definition.value_type == TablePropertyValueType.SIZE_IN_BYTES:
                if not isinstance(value, str):
                    return False, f"Invalid type for {key}: {type(value)}"
                try:
                    ByteSize(value)
                    return True, None
                except ValueError as e:
                    return False, str(e)

            else:
                return True, None

        except Exception as e:
            return False, f"Validation error for {key}: {str(e)}"

    @classmethod
    def get_property_info(cls, key: str) -> Optional[Dict[str, Any]]:
        """Gets detailed information about a Delta table property.

        Args:
            key (str): Property key.

        Returns:
            Optional[Dict[str, Any]]: Property information if found.
        """
        if key not in cls._PROPERTY_DEFINITIONS:
            return None

        definition = cls._PROPERTY_DEFINITIONS[key]
        info = {
            "type": definition.value_type.value,
            "description": definition.description,
        }

        if definition.default_value is not None:
            info["default_value"] = definition.default_value

        if definition.allowed_values is not None:
            info["allowed_values"] = list(definition.allowed_values)

        if definition.min_value is not None:
            info["min_value"] = definition.min_value

        if definition.max_value is not None:
            info["max_value"] = definition.max_value

        return info
