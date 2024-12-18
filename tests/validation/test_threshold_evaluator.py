import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, BooleanType, IntegerType
from dtb.validation.threshold_evaluator import ThresholdEvaluator


@pytest.fixture(scope="module")
def spark():
    """Create a Spark session for testing."""
    return (
        SparkSession.builder.master("local[1]")
        .appName("threshold_evaluator_tests")
        .getOrCreate()
    )


@pytest.fixture(scope="module")
def schema():
    """Define the schema for test DataFrames."""
    return StructType(
        [
            StructField("id", IntegerType(), False),
            StructField("_dtb_all_checks_passed", BooleanType(), False),
        ]
    )


@pytest.fixture
def create_test_df(spark, schema):
    """Factory fixture to create test DataFrames with specified pass/fail ratios."""

    def _create_df(pass_count: int, fail_count: int):
        total_rows = pass_count + fail_count
        data = [(i, True) for i in range(pass_count)] + [
            (i + pass_count, False) for i in range(fail_count)
        ]
        return spark.createDataFrame(data, schema)

    return _create_df


def test_threshold_evaluator_initialization():
    """Test ThresholdEvaluator initialization with various threshold types."""
    # Test with integer
    evaluator = ThresholdEvaluator(5)
    assert evaluator.threshold == 5

    # Test with float
    evaluator = ThresholdEvaluator(0.1)
    assert evaluator.threshold == 0.1

    # Test with zero
    evaluator = ThresholdEvaluator(0)
    assert evaluator.threshold == 0


def test_threshold_evaluator_with_integer_threshold(create_test_df):
    """Test ThresholdEvaluator with integer threshold."""
    evaluator = ThresholdEvaluator(2)

    # Test when failures are within threshold
    df = create_test_df(8, 2)  # 2 failures
    assert evaluator.apply(df) == (True, 2)

    # Test when failures exceed threshold
    df = create_test_df(7, 3)  # 3 failures
    assert evaluator.apply(df) == (False, 3)


def test_threshold_evaluator_with_float_threshold(create_test_df):
    """Test ThresholdEvaluator with float threshold (percentage)."""
    evaluator = ThresholdEvaluator(0.2)  # 20% threshold

    # Test when failures are within threshold (20%)
    df = create_test_df(80, 20)  # 20% failures
    assert evaluator.apply(df) == (True, 20)

    # Test when failures exceed threshold
    df = create_test_df(70, 30)  # 30% failures
    assert evaluator.apply(df) == (False, 30)


def test_threshold_evaluator_with_zero_threshold(create_test_df):
    """Test ThresholdEvaluator with zero threshold."""
    evaluator = ThresholdEvaluator(0)

    # Test with no failures
    df = create_test_df(10, 0)
    assert evaluator.apply(df) == (True, 0)

    # Test with any failures
    df = create_test_df(9, 1)
    assert evaluator.apply(df) == (False, 1)


def test_threshold_evaluator_with_custom_column_name(spark):
    """Test ThresholdEvaluator with custom pass column name."""

    # Create DataFrame with different column name
    def create_custom_df(spark):
        custom_schema = StructType(
            [
                StructField("id", IntegerType(), False),
                StructField("custom_pass_column", BooleanType(), False),
            ]
        )
        data = [(1, True), (2, True), (3, False)]
        return spark.createDataFrame(data, custom_schema)

    evaluator = ThresholdEvaluator(1)
    df = create_custom_df(spark)

    # Test with custom column name
    assert evaluator.apply(df, "custom_pass_column") == (True, 1)


def test_threshold_evaluator_with_empty_dataframe(spark, schema):
    """Test ThresholdEvaluator with empty DataFrame."""
    evaluator = ThresholdEvaluator(0.1)
    empty_df = spark.createDataFrame([], schema)

    # Empty DataFrame should pass validation
    assert evaluator.apply(empty_df) == (True, 0)


def test_threshold_evaluator_with_all_passes(create_test_df):
    """Test ThresholdEvaluator with all passing rows."""
    evaluator = ThresholdEvaluator(0.1)
    df = create_test_df(10, 0)  # All passes
    assert evaluator.apply(df) == (True, 0)


def test_threshold_evaluator_with_all_failures(create_test_df):
    """Test ThresholdEvaluator with all failing rows."""
    evaluator = ThresholdEvaluator(0.1)
    df = create_test_df(0, 10)  # All failures
    assert evaluator.apply(df) == (False, 10)


@pytest.mark.parametrize(
    "threshold,pass_count,fail_count,expected",
    [
        (0.1, 90, 10, (True, 10)),  # 10% failures - within threshold
        (0.1, 89, 11, (False, 11)),  # 11% failures - exceeds threshold
        (5, 95, 5, (True, 5)),  # 5 failures - within threshold
        (5, 94, 6, (False, 6)),  # 6 failures - exceeds threshold
        (0, 100, 0, (True, 0)),  # No failures - passes zero threshold
        (0, 99, 1, (False, 1)),  # One failure - fails zero threshold
    ],
)
def test_threshold_evaluator_parametrized(
    create_test_df, threshold, pass_count, fail_count, expected
):
    """Parametrized tests for various threshold scenarios."""
    evaluator = ThresholdEvaluator(threshold)
    df = create_test_df(pass_count, fail_count)
    assert evaluator.apply(df) == expected
