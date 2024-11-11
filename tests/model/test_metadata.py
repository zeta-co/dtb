import pytest
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
)
from dtb.model.metadata import Metadata


@pytest.fixture
def sample_schema():
    """Create a sample schema for testing."""
    return StructType(
        [
            StructField("id", IntegerType(), True),
            StructField("date", StringType(), True),
            StructField("product", StringType(), True),
            StructField("quantity", IntegerType(), True),
            StructField("price", DoubleType(), True),
        ]
    )


@pytest.fixture
def file_metadata():
    """Create sample metadata for file-based input."""
    return {
        "type": "csv",
        "path": "tests/data/raw/sales/*.csv",
        "format_options": {"header": "true", "inferSchema": "false"},
        "schema": {
            "columns": [
                {"name": "id", "type": "integer"},
                {"name": "date", "type": "string"},
                {"name": "product", "type": "string"},
                {"name": "quantity", "type": "integer"},
                {"name": "price", "type": "double"},
            ]
        },
    }


@pytest.fixture
def table_metadata():
    """Create sample metadata for table-based input."""
    return {
        "type": "delta",
        "path": "default.sales",
        "schema": {
            "columns": [
                {"name": "id", "type": "integer"},
                {"name": "date", "type": "string"},
                {"name": "product", "type": "string"},
                {"name": "quantity", "type": "integer"},
                {"name": "price", "type": "double"},
            ]
        },
    }


class TestMetadata:
    def test_metadata_initialization(self, file_metadata):
        metadata = Metadata(file_metadata)
        assert metadata.type == "csv"
        assert metadata.path == "tests/data/raw/sales/*.csv"
        assert metadata.is_table == False
        assert metadata.format_options == {"header": "true", "inferSchema": "false"}

    def test_table_metadata(self, table_metadata):
        metadata = Metadata(table_metadata)
        assert metadata.is_table == True
        assert metadata.table_schema == "default"
        assert metadata.table_name == "sales"

    def test_get_column_names(self, file_metadata):
        metadata = Metadata(file_metadata)
        expected_columns = ["id", "date", "product", "quantity", "price"]
        assert metadata.get_column_names() == expected_columns
