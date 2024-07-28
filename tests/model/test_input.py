import unittest
from unittest.mock import patch, MagicMock
from pyspark.sql import SparkSession
from dtb.model.input import Input


class TestInput(unittest.TestCase):

    def setUp(self):
        self.spark = SparkSession.builder.appName("TestTransformAPI").getOrCreate()

    @patch("pyspark.sql.SparkSession.read")
    def test_csv_batch(self, mock_read):
        mock_df = MagicMock()
        mock_read.load.return_value = mock_df
        mock_read.format.return_value = mock_read
        mock_read.options.return_value = mock_read
        metadata = {
            "format": "csv",
            "options": {"header": "true", "inferSchema": "true"},
            "load": "/path/to/data.csv",
        }
        input = Input(metadata)
        df = input.df(self.spark)
        mock_read.format.assert_called_once_with("csv")
        mock_read.options.assert_any_call({"header": "true", "inferSchema": "true"})
        mock_read.load.assert_called_once_with("/path/to/data.csv")
        self.assertEqual(df, mock_df)

    @patch("pyspark.sql.SparkSession.read")
    def test_table_batch(self, mock_read):
        mock_df = MagicMock()
        mock_read.table.return_value = mock_df
        metadata = {
            "format": "table",
            "load": "table1"
        }
        input = Input(metadata)
        df = input.df(self.spark)
        mock_read.table.assert_called_once_with("table1")
        self.assertEqual(df, mock_df)

    def test_missing_metadata(self):
        metadata = {}
        input = Input(metadata)
        df = input.df(self.spark)
        self.assertIsNone(df)

    def tearDown(self):
        self.spark.stop()


if __name__ == "__main__":
    unittest.main()
