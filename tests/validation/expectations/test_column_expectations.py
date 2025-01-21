import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from dtb.validation.expectations.column import (
    ColumnExpectation,
    ColumnGreaterThanExpectation,
    ColumnLessThanExpectation,
    ColumnEqualsValueExpectation,
    ColumnInExpectation,
    ColumnIsNullExpectation,
    ColumnIsNotNullExpectation
)

@pytest.fixture
def spark():
    return SparkSession.builder.getOrCreate()

@pytest.fixture
def sample_df(spark):
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True)
    ])
    data = [
        (1, "Alice"),
        (2, "Bob"),
        (3, None),
        (None, "Dave")
    ]
    return spark.createDataFrame(data, schema)

def test_column_greater_than(sample_df):
    # Test greater than expectation
    expectation = ColumnGreaterThanExpectation("id", 1)
    result = expectation.validate(sample_df)
    
    # Check results
    validation_df = result.df
    assert validation_df.filter(f"{expectation.flag_column} == True").count() == 2
    assert validation_df.filter(f"{expectation.flag_column} == False").count() == 1
    assert validation_df.filter(f"{expectation.flag_column} IS NULL").count() == 1

def test_column_less_than(sample_df):
    expectation = ColumnLessThanExpectation("id", 2)
    result = expectation.validate(sample_df)
    
    validation_df = result.df
    assert validation_df.filter(f"{expectation.flag_column} == True").count() == 1
    assert validation_df.filter(f"{expectation.flag_column} == False").count() == 2
    assert validation_df.filter(f"{expectation.flag_column} IS NULL").count() == 1

def test_column_equals(sample_df):
    expectation = ColumnEqualsValueExpectation("name", "Alice")
    result = expectation.validate(sample_df)
    
    validation_df = result.df
    assert validation_df.filter(f"{expectation.flag_column} == True").count() == 1
    assert validation_df.filter(f"{expectation.flag_column} == False").count() == 2
    assert validation_df.filter(f"{expectation.flag_column} IS NULL").count() == 1

def test_column_in(sample_df):
    expectation = ColumnInExpectation("name", ["Alice", "Bob"])
    result = expectation.validate(sample_df)
    
    validation_df = result.df
    assert validation_df.filter(f"{expectation.flag_column} == True").count() == 2
    assert validation_df.filter(f"{expectation.flag_column} == False").count() == 1
    assert validation_df.filter(f"{expectation.flag_column} IS NULL").count() == 1

def test_column_is_null(sample_df):
    expectation = ColumnIsNullExpectation("name")
    result = expectation.validate(sample_df)
    
    validation_df = result.df
    assert validation_df.filter(f"{expectation.flag_column} == True").count() == 1
    assert validation_df.filter(f"{expectation.flag_column} == False").count() == 3

def test_column_is_not_null(sample_df):
    expectation = ColumnIsNotNullExpectation("name")
    result = expectation.validate(sample_df)
    
    validation_df = result.df
    assert validation_df.filter(f"{expectation.flag_column} == True").count() == 3
    assert validation_df.filter(f"{expectation.flag_column} == False").count() == 1

def test_column_expectation_builder_methods():
    # Test the builder methods on ColumnExpectation base class
    col_exp = ColumnExpectation("test_column")
    
    assert isinstance(col_exp.gt(5), ColumnGreaterThanExpectation)
    assert isinstance(col_exp.lt(5), ColumnLessThanExpectation)
    assert isinstance(col_exp.eq("value"), ColumnEqualsValueExpectation)
    assert isinstance(col_exp.is_in([1, 2, 3]), ColumnInExpectation)
    assert isinstance(col_exp.is_null(), ColumnIsNullExpectation)
    assert isinstance(col_exp.is_not_null(), ColumnIsNotNullExpectation)

def test_column_expectation_abstract():
    # Test that base ColumnExpectation cannot be used directly
    col_exp = ColumnExpectation("test_column")
    with pytest.raises(NotImplementedError):
        col_exp.validate(None)
