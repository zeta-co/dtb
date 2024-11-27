from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from delta.tables import DeltaTable
from .delta_table_config import DeltaTableConfig


class DeltaTableManager:
    """Manages Delta table creation and configuration."""
    
    def __init__(self, spark: SparkSession):
        self._spark = spark

    def create_if_not_exists(
        self, 
        schema: StructType,
        config: DeltaTableConfig,
    ) -> None:
        """
        Create a Delta table if it doesn't exist with the specified schema and properties.
        
        Args:
            schema: The StructType schema for the table
            config: DeltaTableConfig instance with table configuration
        """
        if not DeltaTable.isDeltaTable(self._spark, config.full_table_name):
            builder = self._spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {config.full_table_name} (
                    {self._schema_to_sql(schema)}
                )
                USING DELTA
            """)
            
            if config.partition_columns:
                partition_cols = ", ".join(config.partition_columns)
                builder = builder.sql(f"PARTITIONED BY ({partition_cols})")
                
            # Set table properties
            properties_sql = ", ".join(
                f"'{k}' = '{v}'" for k, v in config.properties.items()
            )
            builder.sql(f"TBLPROPERTIES ({properties_sql})")

    def _schema_to_sql(self, schema: StructType) -> str:
        """Convert StructType schema to SQL DDL string."""
        type_mapping = {
            'string': 'STRING',
            'long': 'BIGINT',
            'integer': 'INT',
            'boolean': 'BOOLEAN',
            'timestamp': 'TIMESTAMP',
            'date': 'DATE'
        }
        
        fields = []
        for field in schema.fields:
            sql_type = type_mapping.get(field.dataType.simpleString().lower())
            if sql_type:
                fields.append(f"{field.name} {sql_type}")
            
        return ", ".join(fields)
