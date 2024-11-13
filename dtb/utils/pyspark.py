import datetime
from typing import get_type_hints
from delta import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    TimestampType,
)


def class_to_struct_type(cls):
    fields = []
    type_hints = get_type_hints(cls)
    type_mapping = {
        str: StringType(),
        int: IntegerType(),
        datetime.datetime: TimestampType(),
    }
    for attr_name, attr_type in type_hints.items():
        if attr_type in type_mapping:
            fields.append(StructField(attr_name, type_mapping[attr_type], True))
        else:
            raise ValueError(f"Unsupported type for attribute {attr_name}: {attr_type}")
    return StructType(fields)

def create_delta_table_if_not_exists(
    spark: SparkSession, table_name: str, schema: StructType
) -> None:
    try:
        DeltaTable.forName(spark, table_name)
    except Exception:
        empty_df = spark.createDataFrame([], schema)
        empty_df.write.format("delta").saveAsTable(table_name)
