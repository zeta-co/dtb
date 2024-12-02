import pytest
from dtb.model.metadata import Metadata


@pytest.fixture
def delta_table_metadata():
    """Fixture for a Delta table metadata."""
    return Metadata(
        {
            "type": "DELTA",
            "path": "my_catalog.my_schema.my_table",
            "mode": "APPEND",
            "partition_by": ["date"],
            "sort_by": ["id"],
            "schemas": {"columns": {"id": "string", "date": "date", "value": "double"}},
            "table_properties": {"delta.autoOptimize.optimizeWrite": "true"},
        }
    )


@pytest.fixture
def file_metadata():
    """Fixture for a file-based metadata."""
    return Metadata(
        {
            "type": "parquet",
            "path": "s3://bucket/path/to/files",
            "format_options": {"compression": "snappy"},
            "schemas": {"columns": {"id": "string", "value": "double"}},
        }
    )


@pytest.fixture
def stream_metadata():
    """Fixture for a streaming source metadata."""
    return Metadata(
        {
            "type": "cloudfiles",
            "path": "s3://bucket/path",
            "format_options": {
                "cloudFiles.format": "json",
                "cloudFiles.schemaLocation": "s3://bucket/checkpoint",
            },
            "schemas": {"columns": {"id": "string"}},
            "output_mode": "Append",
        }
    )


def test_type_property():
    """Test type property with different cases."""
    assert Metadata({"type": "DELTA", "path": "", "schemas": {}}).type == "delta"
    assert Metadata({"type": "Parquet", "path": "", "schemas": {}}).type == "parquet"
    assert Metadata({"type": "csv", "path": "", "schemas": {}}).type == "csv"


def test_path_property(delta_table_metadata, file_metadata):
    """Test path property."""
    assert delta_table_metadata.path == "my_catalog.my_schema.my_table"
    assert file_metadata.path == "s3://bucket/path/to/files"


def test_is_table_property(delta_table_metadata, file_metadata):
    """Test is_table property."""
    assert delta_table_metadata.is_table is True
    assert file_metadata.is_table is False

    # Test 2-part table name
    two_part = Metadata({"type": "delta", "path": "my_schema.my_table", "schemas": {}})
    assert two_part.is_table is True


def test_table_details_property():
    """Test table_details property with different table path formats."""
    # 3-part table name
    three_part = Metadata(
        {"type": "delta", "path": "my_catalog.my_schema.my_table", "schemas": {}}
    )
    assert three_part.table_details == {
        "catalog": "my_catalog",
        "schema": "my_schema",
        "table": "my_table",
    }

    # 2-part table name
    two_part = Metadata({"type": "delta", "path": "my_schema.my_table", "schemas": {}})
    assert two_part.table_details == {
        "catalog": "hive_metastore",
        "schema": "my_schema",
        "table": "my_table",
    }

    # File path
    file_path = Metadata({"type": "parquet", "path": "s3://bucket/path", "schemas": {}})
    assert file_path.table_details == {}


def test_table_component_properties(delta_table_metadata, file_metadata):
    """Test table_catalog, table_schema, and table_name properties."""
    # Table path
    assert delta_table_metadata.table_catalog == "my_catalog"
    assert delta_table_metadata.table_schema == "my_schema"
    assert delta_table_metadata.table_name == "my_table"

    # File path
    assert file_metadata.table_catalog is None
    assert file_metadata.table_schema is None
    assert file_metadata.table_name is None


def test_is_stream_property(stream_metadata):
    """Test is_stream property."""
    # Cloudfiles type
    assert stream_metadata.is_stream is True

    # Explicit is_stream flag
    explicit_stream = Metadata(
        {"type": "delta", "path": "", "schemas": {}, "is_stream": "true"}
    )
    assert explicit_stream.is_stream is True

    # Different variations of True
    for true_val in ["T", "TRUE", "1", "True"]:
        metadata = Metadata(
            {"type": "delta", "path": "", "schemas": {}, "is_stream": true_val}
        )
        assert metadata.is_stream is True

    # Non-stream
    non_stream = Metadata({"type": "delta", "path": "", "schemas": {}})
    assert non_stream.is_stream is False


def test_format_options_property(file_metadata):
    """Test format_options property."""
    assert file_metadata.format_options == {"compression": "snappy"}

    # Empty format options
    empty = Metadata({"type": "delta", "path": "", "schemas": {}})
    assert empty.format_options == {}


def test_schemas_property(delta_table_metadata):
    """Test schemas property."""
    expected_schemas = {"columns": {"id": "string", "date": "date", "value": "double"}}
    assert delta_table_metadata.schemas == expected_schemas


def test_mode_properties(delta_table_metadata, stream_metadata):
    """Test mode and output_mode properties."""
    assert delta_table_metadata.mode == "append"
    assert stream_metadata.output_mode == "append"

    # Empty modes
    empty = Metadata({"type": "delta", "path": "", "schemas": {}})
    assert empty.mode == ""
    assert empty.output_mode == ""


def test_partition_and_sort_properties(delta_table_metadata):
    """Test partition_by and sort_by properties."""
    assert delta_table_metadata.partition_by == ["date"]
    assert delta_table_metadata.sort_by == ["id"]

    # Empty lists
    empty = Metadata({"type": "delta", "path": "", "schemas": {}})
    assert empty.partition_by == []
    assert empty.sort_by == []


def test_table_properties_property(delta_table_metadata):
    """Test table_properties property."""
    expected_properties = {"delta.autoOptimize.optimizeWrite": "true"}
    assert delta_table_metadata.table_properties == expected_properties

    # Empty properties
    empty = Metadata({"type": "delta", "path": "", "schemas": {}})
    assert empty.table_properties == {}


def test_to_dict_method(delta_table_metadata):
    """Test to_dict method."""
    original_dict = {
        "type": "DELTA",
        "path": "my_catalog.my_schema.my_table",
        "mode": "APPEND",
        "partition_by": ["date"],
        "sort_by": ["id"],
        "schemas": {"columns": {"id": "string", "date": "date", "value": "double"}},
        "table_properties": {"delta.autoOptimize.optimizeWrite": "true"},
    }
    assert delta_table_metadata.to_dict() == original_dict


# def test_required_fields():
#     """Test that required fields raise appropriate errors if missing."""
#     with pytest.raises(KeyError):
#         Metadata({})  # Missing all required fields

#     with pytest.raises(KeyError):
#         Metadata({"type": "delta", "schemas": {}})  # Missing path

#     with pytest.raises(KeyError):
#         Metadata({"type": "delta", "path": ""})  # Missing schemas
