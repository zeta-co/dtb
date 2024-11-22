from unittest import TestCase
from unittest.mock import Mock
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StringType, StructField, StructType
from dtb.io.input_reader import InputReaderFactory, FileInputReader, TableInputReader


class TestInputReader(TestCase):
    """Test suite for InputReader implementations."""

    def setUp(self):
        """Set up test fixtures."""
        self.mock_spark = Mock(spec=SparkSession)
        self.mock_reader = Mock()
        self.mock_df = Mock(spec=DataFrame)
        self.mock_spark.read = self.mock_reader
        self.mock_spark.readStream = self.mock_reader
        self.mock_reader.format.return_value = self.mock_reader
        self.mock_reader.options.return_value = self.mock_reader
        self.mock_reader.schema.return_value = self.mock_reader
        self.mock_reader.load.return_value = self.mock_df
        self.mock_reader.table.return_value = self.mock_df

        self.test_schema = StructType([StructField("col1", StringType(), True)])

    def test_file_input_reader_batch(self):
        """Test FileInputReader with batch processing."""
        metadata = Mock(
            path="/test/path",
            type="csv",
            is_stream=False,
            format_options={"header": "true"},
            is_table=False,
        )

        reader = FileInputReader(metadata)
        result = reader.read(
            self.mock_spark,
            schema=self.test_schema,
            filter=["2023"],
            format_options={"delimiter": ","},
        )

        self.mock_reader.format.assert_called_with("csv")
        self.mock_reader.options.assert_called_once()
        self.mock_reader.schema.assert_called_with(self.test_schema)
        self.assertEqual(result, self.mock_df)

    def test_file_input_reader_streaming(self):
        """Test FileInputReader with streaming."""
        metadata = Mock(
            path="/test/path",
            type="csv",
            is_stream=True,
            format_options={},
            is_table=False,
        )

        reader = FileInputReader(metadata)
        result = reader.read(self.mock_spark, schema=self.test_schema)

        self.assertEqual(result, self.mock_df)
        self.mock_reader.schema.assert_called_with(self.test_schema)

    def test_table_input_reader(self):
        """Test TableInputReader with SQL filtering."""
        metadata = Mock(
            path="default.test_table", type="hive", format_options={}, is_table=True
        )

        reader = TableInputReader(metadata)
        filter_conditions = "year = 2023 and category = 'sales'"
        result = reader.read(self.mock_spark, filter=filter_conditions)

        self.mock_reader.format.assert_called_with("hive")
        self.mock_reader.table.assert_called_with("default.test_table")
        self.mock_df.where.assert_called_with("year = 2023 and category = 'sales'")

    def test_input_reader_factory(self):
        """Test InputReaderFactory creates correct reader types."""
        table_metadata = Mock(is_table=True)
        file_metadata = Mock(is_table=False)

        table_reader = InputReaderFactory.create_input_reader(table_metadata)
        file_reader = InputReaderFactory.create_input_reader(file_metadata)

        self.assertIsInstance(table_reader, TableInputReader)
        self.assertIsInstance(file_reader, FileInputReader)

    def test_file_input_reader_with_format_options(self):
        """Test FileInputReader with custom format options."""
        metadata = Mock(
            path="/test/path",
            type="csv",
            is_stream=False,
            format_options={"header": "true"},
            is_table=False,
        )

        reader = FileInputReader(metadata)
        custom_options = {"delimiter": "|", "quote": '"'}
        result = reader.read(self.mock_spark, format_options=custom_options)

        # Verify that both metadata options and custom options were used
        expected_options = {"header": "true", "delimiter": "|", "quote": '"'}
        self.mock_reader.options.assert_called_with(**expected_options)
        self.assertEqual(result, self.mock_df)
