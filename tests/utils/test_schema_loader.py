import pytest
from datetime import datetime
from dtb.model.schema_registry import SchemaRegistry
from dtb.utils.date_extractor import DateExtractors
from dtb.utils.schema_loader import SchemaLoader


class TestSchemaLoader:

    def test_loader_with_yyyy_mm_dd(self):
        registry = SchemaRegistry()
        extractor = DateExtractors.yyyy_mm_dd()

        registry.add_schema_version(
            start_date=datetime(2024, 1, 1),
            columns={"id": "integer", "name": "string"},
            end_date=datetime(2024, 2, 1),
        )

        loader = SchemaLoader(registry, extractor)
        schema = loader.get_schema_for_file("data_2024-01-15_daily.csv")
        assert schema == {"id": "integer", "name": "string"}

    def test_loader_with_yyyymmdd(self):
        registry = SchemaRegistry()
        extractor = DateExtractors.yyyymmdd()

        registry.add_schema_version(
            start_date=datetime(2024, 1, 1), columns={"id": "integer", "name": "string"}
        )

        loader = SchemaLoader(registry, extractor)
        schema = loader.get_schema_for_file("20240115_data.csv")
        assert schema == {"id": "integer", "name": "string"}

    def test_loader_with_custom_format(self):
        registry = SchemaRegistry()
        extractor = DateExtractors.custom_format(
            pattern=r"data_(\d{4}-\d{2}-\d{2}_\d{2}-\d{2})",
            date_format="%Y-%m-%d_%H-%M",
            description="YYYY-MM-DD_HH-MM",
        )

        registry.add_schema_version(
            start_date=datetime(2024, 1, 1), columns={"id": "integer", "name": "string"}
        )

        loader = SchemaLoader(registry, extractor)
        schema = loader.get_schema_for_file("data_2024-01-15_14-30_daily.csv")
        assert schema == {"id": "integer", "name": "string"}
