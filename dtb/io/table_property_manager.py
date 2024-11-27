from typing import Any, Dict, List
from pyspark.sql import SparkSession
from ..model.metadata import Metadata
from .table_property import TableProperty, TablePropertyType
from .table_property_validator import TablePropertyValidator


class TablePropertyManager:
    """Enhanced table property manager with detailed validation."""

    def __init__(self, spark: SparkSession, metadata: Metadata):
        self.spark = spark
        self.metadata = metadata
        self.validator = TablePropertyValidator()

    def _validate_properties(self, properties: Dict[str, Any]) -> List[tuple[str, str]]:
        """Validates table properties and returns validation errors.

        Args:
            properties (Dict[str, Any]): Properties to validate.

        Returns:
            List[tuple[str, str]]: List of (property_key, error_message).
        """
        errors = []
        for key, value in properties.items():
            is_valid, error = self.validator.validate(key, value)
            if not is_valid:
                errors.append((key, error))
        return errors

    def _get_target_properties(self) -> Dict[str, str]:
        """Retrieves current properties from target table.

        Returns:
            Dict[str, str]: Current table properties.
        """
        if not self.metadata.is_table:
            return {}

        table_path = (
            f"{self.metadata.table_catalog}."
            f"{self.metadata.table_schema}."
            f"{self.metadata.table_name}"
        )

        properties = {}
        try:
            describe_sql = f"DESCRIBE TABLE EXTENDED {table_path}"
            result = self.spark.sql(describe_sql)

            # Extract properties from description
            for row in result.collect():
                if row["col_name"] == "Table Properties":
                    # Parse properties string into dictionary
                    props_str = row["data_type"]
                    if props_str:
                        for prop in props_str.strip("[]").split(","):
                            if "=" in prop:
                                key, value = prop.split("=", 1)
                                properties[key.strip()] = value.strip()
        except Exception as e:
            # Log error and return empty dict if table doesn't exist
            print(f"Error getting table properties: {e}")
            return {}

        return properties

    def _generate_alter_statements(
        self,
        current_properties: Dict[str, str],
        desired_properties: List[TableProperty],
    ) -> List[str]:
        """Generates ALTER TABLE statements for property updates.

        Args:
            current_properties (Dict[str, str]): Current table properties.
            desired_properties (List[TableProperty]): Desired properties from metadata.

        Returns:
            List[str]: List of ALTER TABLE statements.
        """
        table_path = (
            f"{self.metadata.table_catalog}."
            f"{self.metadata.table_schema}."
            f"{self.metadata.table_name}"
        )

        statements = []
        for prop in desired_properties:
            current_value = current_properties.get(prop.key)
            if current_value is None or str(current_value) != str(prop.value):
                statements.append(
                    f"ALTER TABLE {table_path} " f"SET TBLPROPERTIES ({str(prop)})"
                )

        return statements

    def _parse_metadata_properties(self) -> List[TableProperty]:
        """Parses and validates properties from metadata.

        Returns:
            List[TableProperty]: List of validated table properties.
        """
        properties = []

        # Get properties from metadata
        meta_properties = self.metadata._metadata.get("properties", {})

        for key, value in meta_properties.items():
            if self.validator.validate(key, value):
                properties.append(TableProperty(key, value))

        # Process other property types as needed
        return properties

    def sync_properties(self) -> None:
        """Synchronises table properties with validation."""
        if not self.metadata.is_table:
            return

        # Get properties from metadata
        properties = self.metadata._metadata.get("properties", {})

        # Validate properties
        validation_errors = self._validate_properties(properties)
        if validation_errors:
            error_msg = "\n".join(
                f"- {key}: {error}" for key, error in validation_errors
            )
            raise ValueError(f"Invalid Delta table properties:\n{error_msg}")

        # Proceed with property synchronisation
        current_properties = self._get_target_properties()
        statements = self._generate_alter_statements(
            current_properties, self._parse_metadata_properties()
        )

        for statement in statements:
            try:
                self.spark.sql(statement)
            except Exception as e:
                print(f"Error executing {statement}: {e}")
