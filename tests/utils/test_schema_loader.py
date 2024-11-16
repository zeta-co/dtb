import pytest
from datetime import datetime
from dtb.model.schema_registry import SchemaRegistry
from dtb.utils.date_extractor import DateExtractors
from dtb.utils.filename_date_resolver import FilenameDateResolver
from dtb.utils.schema_loader import SchemaLoader


class TestSchemaLoader:
    @pytest.fixture
    def loader(self):
        registry = SchemaRegistry()
        resolver = FilenameDateResolver()
        resolver.add_extractor(DateExtractors.yyyy_mm_dd())

        # Add schema versions
        registry.add_schema_version(
            start_date=datetime(2024, 1, 1),
            columns={"id": "integer"},
            end_date=datetime(2024, 2, 1),
        )

        registry.add_schema_version(
            start_date=datetime(2024, 2, 1), columns={"id": "integer", "name": "string"}
        )

        return SchemaLoader(registry, resolver)

    def test_get_schema_for_valid_file(self, loader):
        schema = loader.get_schema_for_file("data_2024-01-15_daily.csv")
        assert schema == {"id": "integer"}

        schema = loader.get_schema_for_file("data_2024-02-15_daily.csv")
        assert schema == {"id": "integer", "name": "string"}

    def test_invalid_filename_format(self, loader):
        with pytest.raises(ValueError, match="Could not extract date from filename"):
            loader.get_schema_for_file("invalid_filename.csv")

    def test_no_schema_for_date(self, loader):
        with pytest.raises(ValueError, match="No schema found for file date"):
            loader.get_schema_for_file("data_2023-12-31_daily.csv")
