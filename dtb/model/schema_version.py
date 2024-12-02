import datetime
from dataclasses import dataclass
from typing import Any, Dict, Optional, Union
from pyspark.sql.types import (
    StructField,
    StructType,
)
from .column import Column


@dataclass
class SchemaVersion:
    """Represents a versioned data schema with temporal validity and column definitions.

    This class manages schema versions for data structures, tracking when each version
    is active and maintaining column definitions. It supports conversion between
    different formats and provides compatibility with PySpark's StructType.

    Attributes:
        start_date (datetime.datetime): When this schema version becomes effective.
        end_date (Optional[datetime.datetime]): When this schema version expires.
            None indicates the schema is currently active.
        columns (Dict[str, Union[str, Dict[str, Any], Column]]): Column definitions,
            which can be specified in three formats:
            - str: Simple data type specification (e.g., "string", "integer")
            - Dict: Dictionary of column attributes
            - Column: Pre-configured Column object
        version (int): Sequential version number for this schema

    Methods:
        to_struct_type(): Converts the schema version to a PySpark StructType.
        to_dict(): Converts the schema version to a dictionary format.
        __str__(): Returns a human-readable string representation.

    Raises:
        ValueError: If column information is provided in an invalid format.

    Examples:
        >>> # Create a schema version with different column specification formats
        >>> schema_v1 = SchemaVersion(
        ...     start_date=datetime.datetime(2024, 1, 1),
        ...     end_date=None,  # Currently active
        ...     version=1,
        ...     columns={
        ...         # Simple string format
        ...         "user_id": "string",
        ...
        ...         # Dictionary format
        ...         "amount": {
        ...             "data_type": "decimal",
        ...             "precision": 10,
        ...             "scale": 2,
        ...             "nullable": False
        ...         },
        ...
        ...         # Column object format
        ...         "transaction_date": Column(
        ...             name="transaction_date",
        ...             data_type="date",
        ...             datetime_format="yyyy-MM-dd"
        ...         )
        ...     }
        ... )

        >>> # Convert to PySpark StructType
        >>> spark_schema = schema_v1.to_struct_type()

        >>> # Export schema to dictionary
        >>> schema_dict = schema_v1.to_dict()

        >>> # Print human-readable representation
        >>> print(schema_v1)  # "Schema V1: 2024-01-01 to PRESENT"

    Notes:
        - During initialisation, all column specifications are converted to Column
          objects internally, regardless of input format.
        - The end_date should be None only for the currently active schema version.
        - When converted to a dictionary format using to_dict(), dates are serialised
          to ISO format strings.
        - The struct_type property creates a new PySpark StructType instance each
          time it's accessed, useful for defining DataFrame schemas.
    """

    start_date: datetime.datetime
    end_date: Optional[datetime.datetime]  # None means "currently active"
    columns: Dict[str, Union[str, Dict[str, Any], Column]]
    version: int  # Sequential version number

    def __post_init__(self):
        # Convert all columns to Column objects
        processed_columns = {}
        for col_name, col_info in self.columns.items():
            if isinstance(col_info, str):
                processed_columns[col_name] = Column(name=col_name, data_type=col_info)
            elif isinstance(col_info, dict):
                processed_columns[col_name] = Column(name=col_name, **col_info)
            elif isinstance(col_info, Column):
                processed_columns[col_name] = col_info
            else:
                raise ValueError(
                    f"Invalid column info type for {col_name}: {type(col_info)}"
                )

        self.columns = processed_columns

    def __str__(self):
        end_str = (
            "PRESENT" if self.end_date is None else self.end_date.strftime("%Y-%m-%d")
        )
        return (
            f"Schema V{self.version}: "
            f"{self.start_date.strftime('%Y-%m-%d')} to {end_str}"
        )

    def to_struct_type(self) -> StructType:
        fields = []
        for name, col in self.columns.items():
            fields.append(StructField(name, col.to_spark_type(), col.nullable))

        return StructType(fields)

    def to_dict(self) -> Dict[str, Any]:
        """Convert schema version to dictionary format."""
        return {
            "version": self.version,
            "start_date": self.start_date.isoformat(),
            "end_date": self.end_date.isoformat() if self.end_date else None,
            "columns": {name: col.to_dict() for name, col in self.columns.items()},
        }
