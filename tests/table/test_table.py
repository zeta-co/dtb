import pytest
from dtb.table.table import Table


def test_table_init_with_schema_and_name():
    table = Table("my_schema", "my_table")
    assert table.catalog == "hive_metastore"
    assert table.schema == "my_schema"
    assert table.name == "my_table"

def test_table_init_with_schema_dot_name():
    table = Table("my_schema.my_table")
    assert table.catalog == "hive_metastore"
    assert table.schema == "my_schema"
    assert table.name == "my_table"

def test_table_init_with_custom_catalog():
    table = Table("my_schema", "my_table", catalog="custom_catalog")
    assert table.catalog == "custom_catalog"
    assert table.schema == "my_schema"
    assert table.name == "my_table"

def test_table_init_with_schema_dot_name_and_custom_catalog():
    table = Table("my_schema.my_table", catalog="custom_catalog")
    assert table.catalog == "custom_catalog"
    assert table.schema == "my_schema"
    assert table.name == "my_table"

def test_table_init_invalid_single_argument():
    with pytest.raises(ValueError, match="Invalid name pattern. Expected .+"):
        Table("invalid_name")

def test_table_init_invalid_argument_count():
    with pytest.raises(ValueError, match="Invalid number of arguments. Expected 1 or 2!"):
        Table("schema", "name", "extra")

def test_table_full_name_property():
    table = Table("my_schema", "my_table")
    assert table.full_name == "hive_metastore.my_schema.my_table"

def test_table_schema_table_property():
    table = Table("my_schema", "my_table")
    assert table.schema_table == "my_schema.my_table"

def test_table_full_name_property_with_custom_catalog():
    table = Table("my_schema", "my_table", catalog="custom_catalog")
    assert table.full_name == "custom_catalog.my_schema.my_table"

def test_table_schema_table_property_with_custom_catalog():
    table = Table("my_schema", "my_table", catalog="custom_catalog")
    assert table.schema_table == "my_schema.my_table"
