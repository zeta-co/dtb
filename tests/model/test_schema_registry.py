import pytest
from datetime import datetime
from dtb.model.schema_registry import SchemaRegistry


class TestSchemaRegistry:
    @pytest.fixture
    def registry(self):
        return SchemaRegistry()
    
    def test_add_schema_version(self, registry):
        registry.add_schema_version(
            start_date=datetime(2024, 1, 1),
            columns={"id": "integer", "name": "string"},
            end_date=datetime(2024, 2, 1)
        )
        
        schema = registry.get_schema_for_date(datetime(2024, 1, 15))
        assert schema == {"id": "integer", "name": "string"}
    
    def test_schema_version_no_end_date(self, registry):
        registry.add_schema_version(
            start_date=datetime(2024, 1, 1),
            columns={"id": "integer", "name": "string"}
        )
        
        # Should work for any date after start_date
        schema = registry.get_schema_for_date(datetime(2025, 1, 1))
        assert schema == {"id": "integer", "name": "string"}
    
    def test_overlapping_schema_versions(self, registry):
        registry.add_schema_version(
            start_date=datetime(2024, 1, 1),
            columns={"id": "integer"},
            end_date=datetime(2024, 3, 1)
        )
        
        with pytest.raises(ValueError, match="Schema version .* overlaps with version .*"):
            registry.add_schema_version(
                start_date=datetime(2024, 2, 1),  # Overlaps with previous
                columns={"id": "integer", "name": "string"}
            )
    
    def test_multiple_schema_versions(self, registry):
        # Add multiple non-overlapping schemas
        registry.add_schema_version(
            start_date=datetime(2024, 1, 1),
            columns={"id": "integer"},
            end_date=datetime(2024, 2, 1)
        )
        
        registry.add_schema_version(
            start_date=datetime(2024, 2, 1),
            columns={"id": "integer", "name": "string"},
            end_date=datetime(2024, 3, 1)
        )
        
        registry.add_schema_version(
            start_date=datetime(2024, 3, 1),
            columns={"id": "integer", "name": "string", "value": "double"}
        )
        
        # Test different dates
        schema1 = registry.get_schema_for_date(datetime(2024, 1, 15))
        assert schema1 == {"id": "integer"}
        
        schema2 = registry.get_schema_for_date(datetime(2024, 2, 15))
        assert schema2 == {"id": "integer", "name": "string"}
        
        schema3 = registry.get_schema_for_date(datetime(2024, 3, 15))
        assert schema3 == {"id": "integer", "name": "string", "value": "double"}
    
    def test_no_schema_for_date(self, registry):
        registry.add_schema_version(
            start_date=datetime(2024, 2, 1),
            columns={"id": "integer"}
        )
        
        schema = registry.get_schema_for_date(datetime(2024, 1, 1))
        assert schema is None
