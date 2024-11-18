import pytest
from unittest.mock import Mock, patch
from dtb.io.table_property_manager import TablePropertyManager


class TestTablePropertyManager:
    @pytest.fixture
    def mock_spark(self):
        spark = Mock()
        spark.sql = Mock()
        return spark

    @pytest.fixture
    def mock_metadata(self):
        metadata = Mock()
        metadata.is_table = True
        metadata.table_catalog = "catalog"
        metadata.table_schema = "schema"
        metadata.table_name = "table"
        metadata.type = "delta"
        metadata._metadata = {
            "properties": {
                "delta.appendOnly": "true",
                "delta.autoOptimize.optimizeWrite": "true"
            }
        }
        return metadata

    @pytest.fixture
    def property_manager(self, mock_spark, mock_metadata):
        return TablePropertyManager(mock_spark, mock_metadata)

    def test_validate_properties(self, property_manager):
        valid_properties = {
            "delta.appendOnly": "true",
            "delta.autoOptimize.optimizeWrite": "true"
        }
        assert not property_manager._validate_properties(valid_properties)

        invalid_properties = {
            "delta.appendOnly": "invalid",
            "delta.unknownProperty": "value"
        }
        errors = property_manager._validate_properties(invalid_properties)
        assert len(errors) == 2

    def test_sync_properties_valid(self, property_manager, mock_spark):
        # Mock current properties
        with patch.object(
            property_manager,
            '_get_target_properties',
            return_value={
                "delta.appendOnly": "false",
                "delta.autoOptimize.optimizeWrite": "true"
            }
        ):
            property_manager.sync_properties()

        # Verify SQL execution
        mock_spark.sql.assert_called_with(
            "ALTER TABLE catalog.schema.table "
            "SET TBLPROPERTIES ('delta.appendOnly' = 'true')"
        )

    def test_sync_properties_invalid(self, property_manager):
        # Set invalid properties
        property_manager.metadata._metadata["properties"] = {
            "delta.appendOnly": "invalid"
        }

        with pytest.raises(ValueError) as exc_info:
            property_manager.sync_properties()

        assert "Invalid Delta table properties" in str(exc_info.value)

    def test_sync_properties_non_table(self, property_manager):
        property_manager.metadata.is_table = False
        property_manager.sync_properties()  # Should do nothing
        property_manager.spark.sql.assert_not_called()
