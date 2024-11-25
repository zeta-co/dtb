from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    IntegerType,
    StructType,
    StructField,
    StringType,
    TimestampType,
)


class CheckLogEntrySchema:
    """Container class for different log schemas"""

    @staticmethod
    def get_schema_check_schema() -> StructType:
        return StructType(
            [
                StructField("JobId", StringType()),
                StructField("JobName", StringType()),
                StructField("RunId", StringType()),
                StructField("Datetime", TimestampType()),
                StructField("TableName", StringType()),
                StructField("TablePath", StringType()),
                StructField("Passed", BooleanType()),
                StructField("MissingColumns", ArrayType(StringType())),
                StructField("ExtraColumns", ArrayType(StringType())),
                StructField("ExpectedSchema", StringType()),
                StructField("ActualSchema", StringType()),
                StructField("AdditionalInfo", StringType()),
            ]
        )

    @staticmethod
    def get_record_check_schema() -> StructType:
        return StructType(
            [
                StructField("JobId", StringType(), False),
                StructField("JobName", StringType(), False),
                StructField("RunId", StringType(), False),
                StructField("SourceFileName", StringType(), False),
                StructField("EventTimestamp", TimestampType(), False),
                StructField("CorruptRecord", StringType(), False),
                StructField("LineNumber", LongType(), True),
                StructField("ErrorMessage", StringType(), True),
                StructField("CreatedAt", TimestampType(), False),
                StructField("CreatedBy", StringType(), False),
            ]
        )
