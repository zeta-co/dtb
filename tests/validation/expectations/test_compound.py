import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from dtb.validation.expectations.compound import AllExpectation, AnyExpectation, NegateExpectation
from dtb.validation.expectations.column import ColumnIsNotNullExpectation, ColumnInExpectation

@pytest.fixture
def spark():
    return SparkSession.builder.getOrCreate()

@pytest.fixture
def sample_df(spark):
    data = [
        (1, "A", None),
        (2, "B", "X"),
        (3, None, "Y"),
        (4, "D", "Z")
    ]
    return spark.createDataFrame(data, ["id", "col1", "col2"])

def test_all_expectation(sample_df):
    # Create two expectations
    not_null_exp = ColumnIsNotNullExpectation("col1")
    in_list_exp = ColumnInExpectation("col1", ["A", "B", "C"])
    
    # Combine them with AllExpectation
    all_exp = AllExpectation(not_null_exp, in_list_exp)
    
    # Validate
    result = all_exp.validate(sample_df)
    
    # Check results
    flags = result.df.select(all_exp.flag_column).collect()
    expected = [True, True, False, False]  # Only rows where col1 is both not null AND in list
    assert [row[0] for row in flags] == expected

def test_any_expectation(sample_df):
    # Create two expectations
    in_list_exp1 = ColumnInExpectation("col1", ["A", "B"])
    in_list_exp2 = ColumnInExpectation("col2", ["X", "Y"])
    
    # Combine them with AnyExpectation
    any_exp = AnyExpectation(in_list_exp1, in_list_exp2)
    
    # Validate
    result = any_exp.validate(sample_df)
    
    # Check results
    flags = result.df.select(any_exp.flag_column).collect()
    expected = [True, True, True, False]  # Rows where either col1 in [A,B] OR col2 in [X,Y]
    assert [row[0] for row in flags] == expected

def test_negate_expectation(sample_df):
    # Create base expectation
    not_null_exp = ColumnIsNotNullExpectation("col1")
    
    # Negate it
    negate_exp = NegateExpectation(not_null_exp)
    
    # Validate
    result = negate_exp.validate(sample_df)
    
    # Check results
    flags = result.df.select(negate_exp.flag_column).collect()
    expected = [False, False, True, False]  # Only row where col1 IS null
    assert [row[0] for row in flags] == expected

def test_nested_compound_expectations(sample_df):
    # Create base expectations
    not_null_col1 = ColumnIsNotNullExpectation("col1")
    not_null_col2 = ColumnIsNotNullExpectation("col2")
    in_list_col1 = ColumnInExpectation("col1", ["A", "B"])
    
    # Create nested structure: (NOT NULL col1 AND in_list_col1) OR (NOT NULL col2)
    inner_all = AllExpectation(not_null_col1, in_list_col1)
    nested_exp = AnyExpectation(inner_all, not_null_col2)
    
    # Validate
    result = nested_exp.validate(sample_df)
    
    # Check results
    flags = result.df.select(nested_exp.flag_column).collect()
    expected = [True, True, True, True]  # All rows satisfy the compound condition
    assert [row[0] for row in flags] == expected

# def test_empty_all_expectation(sample_df):
#     # Test with no expectations
#     all_exp = AllExpectation()
    
#     with pytest.raises(IndexError):
#         all_exp.value_column

# def test_empty_any_expectation(sample_df):
#     # Test with no expectations
#     any_exp = AnyExpectation()
    
#     with pytest.raises(IndexError):
#         any_exp.value_column
