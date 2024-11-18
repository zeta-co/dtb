from datetime import datetime
from dtb.model.schema_registry import SchemaRegistry
from dtb.utils.date_extractor import DateExtractors
from dtb.utils.schema_loader import SchemaLoader


def test_schema_integration():
    """Integration test for the entire system."""
    # Set up registry with schemas
    registry = SchemaRegistry()
    registry.add_schema_version(
        start_date=datetime(2024, 1, 1),
        columns={"id": "integer"},
        end_date=datetime(2024, 2, 1),
    )
    registry.add_schema_version(
        start_date=datetime(2024, 2, 1), columns={"id": "integer", "name": "string"}
    )

    # Test with YYYY-MM-DD format dataset
    yyyy_mm_dd_loader = SchemaLoader(registry, DateExtractors.yyyy_mm_dd())
    schema1 = yyyy_mm_dd_loader.get_schema_for_file("data_2024-01-15_daily.csv")
    assert schema1 == {"id": "integer"}

    schema2 = yyyy_mm_dd_loader.get_schema_for_file("data_2024-02-15_daily.csv")
    assert schema2 == {"id": "integer", "name": "string"}

    # Test with YYYYMMDD format dataset (different dataset, same registry)
    yyyymmdd_loader = SchemaLoader(registry, DateExtractors.yyyymmdd())
    schema3 = yyyymmdd_loader.get_schema_for_file("20240115_data.csv")
    assert schema3 == {"id": "integer"}

    schema4 = yyyymmdd_loader.get_schema_for_file("20240215_data.csv")
    assert schema4 == {"id": "integer", "name": "string"}
