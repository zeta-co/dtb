import unittest
from unittest.mock import patch, MagicMock
from pyspark.sql import SparkSession
from dtb.utils.delta_table import table_is_delta


class TestUtils(unittest.TestCase):

    def setUp(self):
        # Create a mock SparkSession
        self.mock_spark = MagicMock(spec=SparkSession)

    @patch('dtb.utils.delta_table.DeltaTable')
    def test_table_is_delta_true(self, mock_delta_table):
        # Set up the mock to not raise an exception
        mock_delta_table.forName.return_value = MagicMock()

        # Test the method
        result = table_is_delta(self.mock_spark, "valid_delta_table")

        # Assert that the result is True
        self.assertTrue(result)

        # Assert that DeltaTable.forName was called with the correct arguments
        mock_delta_table.forName.assert_called_once_with(self.mock_spark, "valid_delta_table")

    @patch('dtb.utils.delta_table.DeltaTable')
    def test_table_is_delta_false(self, mock_delta_table):
        # Set up the mock to raise an exception
        mock_delta_table.forName.side_effect = Exception("Not a Delta table")

        # Test the method
        result = table_is_delta(self.mock_spark, "invalid_table")

        # Assert that the result is False
        self.assertFalse(result)

        # Assert that DeltaTable.forName was called with the correct arguments
        mock_delta_table.forName.assert_called_once_with(self.mock_spark, "invalid_table")

if __name__ == '__main__':
    unittest.main()
