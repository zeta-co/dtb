from datetime import datetime
from dtb.model.schema_registry import SchemaRegistry
from dtb.utils.date_extractor import DateExtractors
from dtb.utils.filename_date_resolver import FilenameDateResolver
from dtb.utils.schema_loader import SchemaLoader


def test_schema_integration():
    """Integration test for the entire system."""
    # Set up the system
    registry = SchemaRegistry()
    resolver = FilenameDateResolver()
    resolver.add_extractor(DateExtractors.yyyy_mm_dd())
    resolver.add_extractor(DateExtractors.yyyymmdd())

    # Add schema versions
    registry.add_schema_version(
        start_date=datetime(2024, 1, 1),
        columns={"id": "integer"},
        end_date=datetime(2024, 2, 1),
    )

    registry.add_schema_version(
        start_date=datetime(2024, 2, 1),
        columns={"id": "integer", "name": "string"},
        end_date=datetime(2024, 3, 1),
    )

    registry.add_schema_version(
        start_date=datetime(2024, 3, 1),
        columns={"id": "integer", "name": "string", "value": "double"},
    )

    loader = SchemaLoader(registry, resolver)

    # Test with different file formats and dates
    test_cases = [
        ("data_2024-01-15_daily.csv", {"id": "integer"}),
        ("20240215_data.csv", {"id": "integer", "name": "string"}),
        (
            "data_2024-03-20_daily.csv",
            {"id": "integer", "name": "string", "value": "double"},
        ),
    ]

    for filename, expected_schema in test_cases:
        schema = loader.get_schema_for_file(filename)
        assert schema == expected_schema
