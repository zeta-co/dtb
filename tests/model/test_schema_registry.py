import pytest
from datetime import datetime
from dtb.model.column import Column
from dtb.model.schema_registry import SchemaRegistry


@pytest.fixture
def empty_registry():
    return SchemaRegistry()

@pytest.fixture
def sample_registry():
    registry = SchemaRegistry()
    registry.add_schema_version(
        start_date=datetime(2024, 1, 1),
        columns={"id": "string", "name": "string"},
        end_date=datetime(2024, 6, 30)
    )
    registry.add_schema_version(
        start_date=datetime(2024, 7, 1),
        columns={"id": "string", "name": "string", "email": "string"}
    )
    return registry

def test_add_schema_version(empty_registry):
    # Test adding a single schema version
    empty_registry.add_schema_version(
        start_date=datetime(2024, 1, 1),
        columns={"id": "string"}
    )
    assert len(empty_registry._versions) == 1
    assert empty_registry._versions[0].version == 1
    assert empty_registry._versions[0].columns == {"id": Column("id", "string")}

def test_add_multiple_schema_versions(empty_registry):
    # Test adding multiple non-overlapping schema versions
    empty_registry.add_schema_version(
        start_date=datetime(2024, 1, 1),
        columns={"id": "string"},
        end_date=datetime(2024, 6, 30)
    )
    empty_registry.add_schema_version(
        start_date=datetime(2024, 7, 1),
        columns={"id": "string", "name": "string"}
    )
    
    assert len(empty_registry._versions) == 2
    assert empty_registry._versions[0].version == 1
    assert empty_registry._versions[1].version == 2

def test_chronological_ordering(empty_registry):
    # Test that versions are ordered by start date regardless of insertion order
    empty_registry.add_schema_version(
        start_date=datetime(2024, 7, 1),
        columns={"id": "string", "name": "string"}
    )
    empty_registry.add_schema_version(
        start_date=datetime(2024, 1, 1),
        columns={"id": "string"},
        end_date=datetime(2024, 6, 30)
    )
    
    assert empty_registry._versions[0].start_date == datetime(2024, 1, 1)
    assert empty_registry._versions[1].start_date == datetime(2024, 7, 1)

def test_overlapping_dates(empty_registry):
    # Test that overlapping dates raise ValueError
    empty_registry.add_schema_version(
        start_date=datetime(2024, 1, 1),
        columns={"id": "string"},
        end_date=datetime(2024, 7, 15)
    )
    
    with pytest.raises(ValueError):
        empty_registry.add_schema_version(
            start_date=datetime(2024, 7, 1),
            columns={"id": "string", "name": "string"}
        )

def test_get_schema_for_date(sample_registry):
    # Test getting schema for different dates
    schema_early = sample_registry.get_schema_for_date(datetime(2024, 3, 15))
    schema_late = sample_registry.get_schema_for_date(datetime(2024, 8, 1))
    schema_none = sample_registry.get_schema_for_date(datetime(2023, 12, 31))
    
    assert schema_early == {"id": Column("id", "string"), "name": Column("name", "string")}
    assert schema_late == {"id": Column("id", "string"), "name": Column("name", "string"), "email": Column("email", "string")}
    assert schema_none is None

def test_load_from_list(empty_registry):
    # Test loading multiple schema versions from a list
    schemas = [
        {
            "start_date": datetime(2024, 1, 1),
            "end_date": datetime(2024, 6, 30),
            "columns": {"id": "string"}
        },
        {
            "start_date": datetime(2024, 7, 1),
            "columns": {"id": "string", "name": "string"}
        }
    ]
    
    empty_registry.load_from_list(schemas)
    assert len(empty_registry._versions) == 2
    assert empty_registry._versions[0].start_date == datetime(2024, 1, 1)
    assert empty_registry._versions[1].start_date == datetime(2024, 7, 1)

def test_load_from_list_unsorted(empty_registry):
    # Test loading schemas in non-chronological order
    schemas = [
        {
            "start_date": datetime(2024, 7, 1),
            "columns": {"id": "string", "name": "string"}
        },
        {
            "start_date": datetime(2024, 1, 1),
            "end_date": datetime(2024, 6, 30),
            "columns": {"id": "string"}
        }
    ]
    
    empty_registry.load_from_list(schemas)
    assert empty_registry._versions[0].start_date == datetime(2024, 1, 1)
    assert empty_registry._versions[1].start_date == datetime(2024, 7, 1)

def test_complex_column_definitions(empty_registry):
    # Test adding schema with complex column definitions
    complex_columns = {
        "id": "string",
        "name": {
            "data_type": "string",
            "nullable": False
        },
        "age": {
            "data_type": "integer",
            "nullable": True
        }
    }
    
    empty_registry.add_schema_version(
        start_date=datetime(2024, 1, 1),
        columns=complex_columns
    )
    
    retrieved_schema = empty_registry.get_schema_for_date(datetime(2024, 1, 1))
    assert retrieved_schema == {
        "id": Column("id", "string"),
        "name": Column("name", "string", False),
        "age": Column("age", "integer", True),
    }

def test_boundary_dates(sample_registry):
    # Test schema retrieval at boundary dates
    end_date = datetime(2024, 6, 30)
    start_date = datetime(2024, 7, 1)
    
    schema_at_end = sample_registry.get_schema_for_date(end_date)
    schema_at_start = sample_registry.get_schema_for_date(start_date)
    
    assert schema_at_end == {"id": Column("id", "string"), "name": Column("name", "string")}
    assert schema_at_start == {"id": Column("id", "string"), "name": Column("name", "string"), "email": Column("email", "string")}
