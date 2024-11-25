import datetime
from dataclasses import dataclass
from typing import Dict, Optional
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    IntegerType,
    LongType,
    StructField,
    StructType,
    StringType,
    TimestampType,
)


@dataclass
class SchemaVersion:
    """Represents a schema version with its effective time period."""

    start_date: datetime.datetime
    end_date: Optional[datetime.datetime]  # None means "currently active"
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

    @property
    def struct_type(self) -> StructType:
        """Convert metadata schema to PySpark StructType"""
        type_mapping = {
            "string": StringType(),
            "integer": IntegerType(),
            "long": LongType(),
            "double": DoubleType(),
            "decimal": DecimalType(),
            "date": DateType(),
            "datetime": TimestampType(),
            "boolean": BooleanType(),
        }

        fields = []
        for col_name, col_info in self.columns.items():
            col_type = col_info["type"] if isinstance(col_info, dict) else col_info
            if col_type.lower() not in type_mapping:
                raise ValueError(f"Unsupported data type: {col_type}")
            nullable = (
                col_info.get("nullable", True) if isinstance(col_info, dict) else True
            )
            fields.append(
                StructField(col_name, type_mapping[col_type.lower()], nullable)
            )

        return StructType(fields)
