from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from delta.tables import DeltaTable
from .delta_table_config import DeltaTableConfig


class DeltaTableManager:
    """
    A utility class for managing Delta table operations in a Spark environment.

    This class provides static methods to create and configure Delta tables with specified
    schemas, partitioning, and properties. It handles the complexities of Delta table
    creation and ensures idempotent operations.

    Example:
        >>> spark = SparkSession.builder.getOrCreate()
        >>> schema = StructType([...])  # Define your schema
        >>> config = DeltaTableConfig(
        ...     table=Table("my_catalog.my_schema.my_table"),
        ...     partition_columns=["date"],
        ...     properties={"delta.autoOptimize.optimizeWrite": "true"}
        ... )
        >>> DeltaTableManager.create_if_not_exists(spark, schema, config)
    """

    @staticmethod
    def create_if_not_exists(
        spark: SparkSession,
        schema: StructType,
        config: DeltaTableConfig,
    ) -> None:
        """
        Create a Delta table if it doesn't exist with the specified schema and configuration.

        This method performs an idempotent creation of a Delta table, meaning it will
        only create the table if it doesn't already exist. The table is created with
        the specified schema, partitioning, and properties as defined in the config.

        Args:
            spark (SparkSession): The active Spark session to use for Delta operations.
                                This session should have Delta Lake support enabled.
            schema (StructType): The Spark SQL schema defining the structure of the table.
                               This should be a complete schema with all columns and their types.
            config (DeltaTableConfig): Configuration object containing table specifications including:
                                     - Table object
                                     - partition columns (optional)
                                     - table properties (optional)

        Raises:
            pyspark.sql.utils.AnalysisException: If there are syntax errors in the schema
                                               or if the table location is invalid.
            pyspark.sql.utils.ParseException: If there are errors in the SQL statement generation.

        Example:
            >>> schema = StructType([
            ...     StructField("id", LongType(), False),
            ...     StructField("date", DateType(), False),
            ...     StructField("value", StringType(), True)
            ... ])
            >>> config = DeltaTableConfig(
            ...     table=Table("my_catalog.my_schema.my_table"),
            ...     partition_columns=["date"]
            ... )
            >>> DeltaTableManager.create_if_not_exists(spark, schema, config)
        """
        if not DeltaTable.isDeltaTable(spark, config.full_table_name):
            # Construct the complete CREATE TABLE statement
            create_sql = f"""
                CREATE TABLE IF NOT EXISTS {config.full_table_name} (
                    {DeltaTableManager._schema_to_sql(schema)}
                )
                USING DELTA"""

            # Add partition columns if specified
            if config.partition_columns:
                partition_cols = ", ".join(config.partition_columns)
                create_sql += f"\nPARTITIONED BY ({partition_cols})"

            # Add table properties if specified
            if config.properties:
                properties_sql = ", ".join(
                    f"'{k}' = '{v}'" for k, v in config.properties.items()
                )
                create_sql += f"\nTBLPROPERTIES ({properties_sql})"

            # Execute the complete SQL statement
            spark.sql(create_sql)

    @staticmethod
    def _schema_to_sql(schema: StructType) -> str:
        """
        Convert a Spark StructType schema to its SQL DDL representation.

        This internal method handles the conversion of Spark SQL data types to their
        corresponding SQL DDL syntax. It supports common Spark SQL data types and
        ensures proper SQL syntax generation for table creation.

        Args:
            schema (StructType): The Spark SQL schema to convert.

        Returns:
            str: A SQL DDL string representing the schema, suitable for CREATE TABLE statements.

        Note:
            Currently supported data types:
            - long -> BIGINT
            - boolean -> BOOLEAN
            - date -> DATE
            - double -> DOUBLE
            - float -> FLOAT
            - integer -> INT
            - string -> STRING
            - timestamp -> TIMESTAMP
        """
        type_mapping = {
            "bigint": "BIGINT",
            "boolean": "BOOLEAN",
            "date": "DATE",
            "double": "DOUBLE",
            "float": "FLOAT",
            "int": "INT",
            "string": "STRING",
            "timestamp": "TIMESTAMP",
        }

        fields = []
        for field in schema.fields:
            sql_type = type_mapping.get(field.dataType.simpleString().lower())
            if sql_type:
                fields.append(f"{field.name} {sql_type}")
            else:
                raise ValueError(f"Unsupported type: [{field.dataType.simpleString()}]")

        return ", ".join(fields)
