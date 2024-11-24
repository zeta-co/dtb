from typing import Dict
from pyspark.sql import DataFrame, SparkSession
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
from .validation_logger import ValidationLogger
from .validator_dataframe import DataFrameValidator


class SchemaValidator(DataFrameValidator):

    def __init__(self, spark: SparkSession, logger: ValidationLogger, df: DataFrame, schema_dict: Dict):
        super().__init__(spark, logger, df)
        self._schema_dict = schema_dict

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
        for col_name, col_info in self._schema_dict["columns"].items():
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
