import pytest
from datetime import datetime
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DecimalType,
)
from dtb.model.column import Column
from dtb.model.schema_version import SchemaVersion


@pytest.fixture
def sample_datetime():
    return datetime(2024, 1, 1)


def test_basic_initialization(sample_datetime):
    """Test basic initialization with string column types."""
    schema = SchemaVersion(
        start_date=sample_datetime,
        end_date=None,
        version=1,
        columns={
            "id": "integer",
            "name": "string"
        }
    )
    
    assert schema.version == 1
    assert schema.start_date == sample_datetime
    assert schema.end_date is None
    assert len(schema.columns) == 2
    assert all(isinstance(col, Column) for col in schema.columns.values())
    assert schema.columns["id"].data_type == "integer"
    assert schema.columns["name"].data_type == "string"


def test_initialization_with_dict_columns(sample_datetime):
    """Test initialization with dictionary column definitions."""
    schema = SchemaVersion(
        start_date=sample_datetime,
        end_date=None,
        version=1,
        columns={
            "id": {
                "data_type": "integer",
                "nullable": False,
                "is_primary_key": True
            },
            "amount": {
                "data_type": "decimal",
                "precision": 10,
                "scale": 2,
                "nullable": False
            }
        }
    )
    
    assert len(schema.columns) == 2
    assert schema.columns["id"].is_primary_key
    assert not schema.columns["id"].nullable
    assert schema.columns["amount"].precision == 10
    assert schema.columns["amount"].scale == 2


def test_initialization_with_column_objects(sample_datetime):
    """Test initialization with Column objects."""
    id_column = Column(name="id", data_type="integer", nullable=False)
    name_column = Column(name="name", data_type="string")
    
    schema = SchemaVersion(
        start_date=sample_datetime,
        end_date=None,
        version=1,
        columns={
            "id": id_column,
            "name": name_column
        }
    )
    
    assert len(schema.columns) == 2
    assert schema.columns["id"] is id_column
    assert schema.columns["name"] is name_column


def test_invalid_column_type(sample_datetime):
    """Test that invalid column types raise ValueError."""
    with pytest.raises(ValueError, match="Invalid column info type"):
        SchemaVersion(
            start_date=sample_datetime,
            end_date=None,
            version=1,
            columns={
                "id": 123  # Invalid type
            }
        )


def test_str_representation(sample_datetime):
    """Test string representation of SchemaVersion."""
    # With end_date
    end_date = datetime(2024, 12, 31)
    schema = SchemaVersion(
        start_date=sample_datetime,
        end_date=end_date,
        version=1,
        columns={"id": "integer"}
    )
    assert str(schema) == "Schema V1: 2024-01-01 to 2024-12-31"
    
    # Without end_date
    schema = SchemaVersion(
        start_date=sample_datetime,
        end_date=None,
        version=1,
        columns={"id": "integer"}
    )
    assert str(schema) == "Schema V1: 2024-01-01 to PRESENT"


def test_to_struct_type():
    """Test conversion to PySpark StructType."""
    schema = SchemaVersion(
        start_date=datetime(2024, 1, 1),
        end_date=None,
        version=1,
        columns={
            "id": {
                "data_type": "integer",
                "nullable": False
            },
            "name": "string",
            "amount": {
                "data_type": "decimal",
                "precision": 10,
                "scale": 2
            }
        }
    )
    
    spark_schema = schema.to_struct_type()
    assert isinstance(spark_schema, StructType)
    assert len(spark_schema.fields) == 3
    
    # Check individual fields
    id_field = spark_schema.fields[0]
    assert isinstance(id_field, StructField)
    assert id_field.name == "id"
    assert isinstance(id_field.dataType, IntegerType)
    assert not id_field.nullable
    
    name_field = spark_schema.fields[1]
    assert name_field.name == "name"
    assert isinstance(name_field.dataType, StringType)
    assert name_field.nullable
    
    amount_field = spark_schema.fields[2]
    assert amount_field.name == "amount"
    assert isinstance(amount_field.dataType, DecimalType)
    assert amount_field.dataType.precision == 10
    assert amount_field.dataType.scale == 2


def test_to_dict(sample_datetime):
    """Test conversion to dictionary format."""
    end_date = datetime(2024, 12, 31)
    schema = SchemaVersion(
        start_date=sample_datetime,
        end_date=end_date,
        version=1,
        columns={
            "id": {
                "data_type": "integer",
                "nullable": False,
                "is_primary_key": True
            },
            "name": "string"
        }
    )
    
    result = schema.to_dict()
    assert result["version"] == 1
    assert result["start_date"] == sample_datetime.isoformat()
    assert result["end_date"] == end_date.isoformat()
    assert "id" in result["columns"]
    assert "name" in result["columns"]
    assert result["columns"]["id"]["type"] == "integer"
    assert not result["columns"]["id"]["nullable"]
    assert result["columns"]["id"]["is_primary_key"]


def test_column_order_preservation(sample_datetime):
    """Test that column order is preserved."""
    columns = {
        "third": "string",
        "first": "integer",
        "second": "string"
    }
    
    schema = SchemaVersion(
        start_date=sample_datetime,
        end_date=None,
        version=1,
        columns=columns
    )
    
    # Check that the order matches the input
    assert list(schema.columns.keys()) == list(columns.keys())
