import unittest
from unittest.mock import patch, MagicMock
from pyspark.sql import SparkSession
from dtb.delta.delta_table_checker import DeltaTableChecker


class TestDeltaTableChecker(unittest.TestCase):

    def setUp(self):
        # Create a mock SparkSession
        self.mock_spark = MagicMock(spec=SparkSession)

    @patch('dtb.delta.delta_table_checker.DeltaTable')
    def test_is_delta_table_true(self, mock_delta_table):
        # Set up the mock to not raise an exception
        mock_delta_table.forName.return_value = MagicMock()

        # Test the method
        result = DeltaTableChecker.is_delta_table(self.mock_spark, "valid_delta_table")

        # Assert that the result is True
        self.assertTrue(result)

        # Assert that DeltaTable.forName was called with the correct arguments
        mock_delta_table.forName.assert_called_once_with(self.mock_spark, "valid_delta_table")

    @patch('dtb.delta.delta_table_checker.DeltaTable')
    def test_is_delta_table_false(self, mock_delta_table):
        # Set up the mock to raise an exception
        mock_delta_table.forName.side_effect = Exception("Not a Delta table")

        # Test the method
        result = DeltaTableChecker.is_delta_table(self.mock_spark, "invalid_table")

        # Assert that the result is False
        self.assertFalse(result)

        # Assert that DeltaTable.forName was called with the correct arguments
        mock_delta_table.forName.assert_called_once_with(self.mock_spark, "invalid_table")

if __name__ == '__main__':
    unittest.main()
