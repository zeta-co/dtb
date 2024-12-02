import pytest
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    TimestampType,
)
from dtb.model.column import Column


def test_basic_initialization():
    """Test basic column initialization with minimal parameters."""
    col = Column(name="test", data_type="string")
    assert col.name == "test"
    assert col.data_type == "string"
    assert col.nullable is True
    assert col.metadata == {}

def test_full_initialization():
    """Test column initialization with all parameters."""
    col = Column(
        name="amount",
        data_type="decimal",
        nullable=False,
        description="Transaction amount",
        precision=10,
        scale=2,
        min_value=0,
        max_value=1000000,
        is_primary_key=False,
        is_unique=True,
        metadata={"source": "transactions"}
    )
    assert col.name == "amount"
    assert col.data_type == "decimal"
    assert col.nullable is False
    assert col.precision == 10
    assert col.scale == 2
    assert col.min_value == 0
    assert col.max_value == 1000000
    assert col.metadata == {"source": "transactions"}

def test_invalid_data_type():
    """Test that invalid data types raise ValueError."""
    with pytest.raises(ValueError, match="Unsupported data type"):
        Column(name="test", data_type="invalid_type")

def test_non_string_data_type():
    """Test that non-string data type raises ValueError."""
    with pytest.raises(ValueError, match="data_type must be in string"):
        Column(name="test", data_type=123)

@pytest.mark.parametrize("data_type,spark_type", [
    ("boolean", BooleanType()),
    ("date", DateType()),
    ("datetime", TimestampType()),
    ("double", DoubleType()),
    ("integer", IntegerType()),
    ("long", LongType()),
    ("string", StringType()),
])
def test_spark_type_conversion(data_type, spark_type):
    """Test conversion to Spark types."""
    col = Column(name="test", data_type=data_type)
    assert isinstance(col.to_spark_type(), type(spark_type))

def test_decimal_spark_type():
    """Test decimal type conversion with precision and scale."""
    col = Column(
        name="amount",
        data_type="decimal",
        precision=10,
        scale=2
    )
    spark_type = col.to_spark_type()
    assert isinstance(spark_type, DecimalType)
    assert spark_type.precision == 10
    assert spark_type.scale == 2

@pytest.mark.parametrize("test_case", [
    {"precision": None, "error": "Decimal type requires precision specification"},
    {"precision": 0, "error": "Decimal precision must be positive"},
    {"precision": 5, "scale": 6, "error": "Decimal scale must be between 0 and precision"},
    {"precision": 5, "scale": -1, "error": "Decimal scale must be between 0 and precision"},
])
def test_decimal_validation(test_case):
    """Test decimal type validation rules."""
    with pytest.raises(ValueError, match=test_case["error"]):
        Column(
            name="amount",
            data_type="decimal",
            precision=test_case.get("precision"),
            scale=test_case.get("scale")
        )

def test_datetime_format_validation():
    """Test datetime format validation."""
    # Valid cases
    Column(name="date", data_type="date", datetime_format="yyyy-MM-dd")
    Column(name="timestamp", data_type="datetime", datetime_format="yyyy-MM-dd HH:mm:ss")
    
    # Invalid case
    with pytest.raises(ValueError, match="datetime_format not applicable for type"):
        Column(name="string", data_type="string", datetime_format="yyyy-MM-dd")

@pytest.mark.parametrize("data_type,valid_value,invalid_value", [
    ("boolean", [True, False], [1, "true"]),
    ("integer", [1, 2, 3], [1.5, "1"]),
    ("decimal", [1, 1.5, 2], ["1", "1.5"]),
])
def test_valid_values_validation(data_type, valid_value, invalid_value):
    """Test valid_values validation for different types."""
    # Valid case
    if data_type == "decimal":
        col = Column(name="test", data_type=data_type, valid_values=valid_value, precision=10, scale=2)
    else:
        col = Column(name="test", data_type=data_type, valid_values=valid_value)
    
    # Invalid case
    with pytest.raises(ValueError):
        if data_type == "decimal":
            Column(name="test", data_type=data_type, valid_values=invalid_value, precision=10, scale=2)
        else:
            Column(name="test", data_type=data_type, valid_values=invalid_value)

def test_numeric_bounds_validation():
    """Test numeric min/max value validation."""
    # Valid case
    Column(name="test", data_type="integer", min_value=0, max_value=100)
    
    # Invalid cases
    with pytest.raises(ValueError, match="max_value cannot be less than min_value"):
        Column(name="test", data_type="integer", min_value=100, max_value=0)
    
    with pytest.raises(ValueError, match="min_value for integer must be numeric"):
        Column(name="test", data_type="integer", min_value="0")

def test_regex_pattern_validation():
    """Test regex pattern validation."""
    # Valid case
    Column(name="email", data_type="string", regex_pattern=r"^[\w\.-]+@[\w\.-]+\.\w+$")
    
    # Invalid case
    with pytest.raises(ValueError, match="regex_pattern only applicable for string type"):
        Column(name="number", data_type="integer", regex_pattern=r"\d+")
