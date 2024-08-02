import unittest
from unittest.mock import patch, MagicMock
from dtb.model.input import Input


class TestInput(unittest.TestCase):

    def test_csv_batch(self):
        mock_spark = MagicMock()
        mock_read = MagicMock()
        mock_df = MagicMock()
        mock_spark.read = mock_read
        mock_read.format.return_value = mock_read
        mock_read.options.return_value = mock_read
        mock_read.load.return_value = mock_df
        metadata = {
            "table": False,
            "format": "csv",
            "options": {"header": "true", "inferSchema": "true"},
            "load": "/path/to/data.csv",
        }
        input = Input(metadata)
        df = input.df(mock_spark)
        mock_read.format.assert_called_once_with("csv")
        mock_read.options.assert_any_call(**{"header": "true", "inferSchema": "true"})
        mock_read.load.assert_called_once_with("/path/to/data.csv")
        self.assertEqual(df, mock_df)

    def test_table_batch(self):
        mock_spark = MagicMock()
        mock_read = MagicMock()
        mock_df = MagicMock()
        mock_spark.read = mock_read
        mock_read.table.return_value = mock_df
        metadata = {
            "table": True,
            "format": "delta",
            "load": "table1"
        }
        input = Input(metadata)
        df = input.df(mock_spark)
        mock_read.table.assert_called_once_with("table1")
        self.assertEqual(df, mock_df)

    def test_missing_metadata(self):
        mock_spark = MagicMock()
        metadata = {}
        input = Input(metadata)
        df = input.df(mock_spark)
        self.assertIsNone(df)

if __name__ == "__main__":
    unittest.main()
