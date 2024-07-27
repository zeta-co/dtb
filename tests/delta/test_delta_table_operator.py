import unittest
from unittest.mock import Mock, patch
from datetime import datetime
from pyspark.sql import SparkSession
from dtb.delta.delta_table_operator import DeltaTableOperator
from dtb.logging.version import Version
from dtb.table.table import Table


class TestDeltaTableOperator(unittest.TestCase):

    def setUp(self):
        self.mock_spark = Mock(spec=SparkSession)
        self.operator = DeltaTableOperator(self.mock_spark)

    @patch('dtb.delta.delta_table_operator.DeltaTableChecker.is_delta_table')
    @patch('dtb.delta.delta_table_operator.DeltaTable.forName')
    def test_rollback_table_not_delta(self, mock_for_name, mock_is_delta_table):
        mock_is_delta_table.return_value = False
        table = Table("test_schema", "test_table")
        version = Version(1)

        result = self.operator.rollback_table(table, version)

        self.assertFalse(result["success"])
        self.assertIn("is not a Delta table", result["error"])

    @patch('dtb.delta.delta_table_operator.DeltaTableChecker.is_delta_table')
    @patch('dtb.delta.delta_table_operator.DeltaTable.forName')
    def test_rollback_table_to_version(self, mock_for_name, mock_is_delta_table):
        mock_is_delta_table.return_value = True
        mock_delta_table = Mock()
        mock_for_name.return_value = mock_delta_table
        mock_delta_table.history.return_value.select.return_value.orderBy.return_value.first.return_value = [5]
        mock_delta_table.detail.return_value.select.return_value.first.return_value = [datetime(2023, 1, 1)]

        table = Table("test_schema", "test_table")
        version = Version(3)

        result = self.operator.rollback_table(table, version)

        self.assertTrue(result["success"])
        self.assertEqual(result["from_version"], "5")
        self.assertEqual(result["to_version"], "3")
        self.assertEqual(result["action"], "rollback")
        mock_delta_table.restoreToVersion.assert_called_once_with(3)

    @patch('dtb.delta.delta_table_operator.DeltaTableChecker.is_delta_table')
    @patch('dtb.delta.delta_table_operator.DeltaTable.forName')
    def test_rollback_table_to_timestamp(self, mock_for_name, mock_is_delta_table):
        mock_is_delta_table.return_value = True
        mock_delta_table = Mock()
        mock_for_name.return_value = mock_delta_table
        mock_delta_table.history.return_value.select.return_value.orderBy.return_value.first.return_value = [5]
        mock_delta_table.detail.return_value.select.return_value.first.return_value = [datetime(2023, 1, 1)]
        mock_delta_table.history.return_value.filter.return_value.select.return_value.orderBy.return_value.first.return_value = [3]

        table = Table("test_schema", "test_table")
        version = Version(datetime(2023, 6, 1))

        result = self.operator.rollback_table(table, version)

        self.assertTrue(result["success"])
        self.assertEqual(result["from_version"], "5")
        self.assertEqual(result["to_version"], "3")
        self.assertEqual(result["action"], "rollback")
        mock_delta_table.restoreToTimestamp.assert_called_once_with("2023-06-01 00:00:00")

    @patch('dtb.delta.delta_table_operator.DeltaTableChecker.is_delta_table')
    @patch('dtb.delta.delta_table_operator.DeltaTable.forName')
    def test_rollback_table_truncate(self, mock_for_name, mock_is_delta_table):
        mock_is_delta_table.return_value = True
        mock_delta_table = Mock()
        mock_for_name.return_value = mock_delta_table
        mock_delta_table.history.return_value.select.return_value.orderBy.return_value.first.return_value = [5]
        mock_delta_table.detail.return_value.select.return_value.first.return_value = [datetime(2023, 6, 1)]

        table = Table("test_schema", "test_table")
        version = Version(datetime(2023, 1, 1))

        result = self.operator.rollback_table(table, version)

        self.assertTrue(result["success"])
        self.assertEqual(result["from_version"], "5")
        self.assertIsNone(result["to_version"])
        self.assertEqual(result["action"], "truncate")
        mock_delta_table.delete.assert_called_once()

    @patch('dtb.delta.delta_table_operator.DeltaTableChecker.is_delta_table')
    @patch('dtb.delta.delta_table_operator.DeltaTable.forName')
    def test_rollback_table_dry_run(self, mock_for_name, mock_is_delta_table):
        mock_is_delta_table.return_value = True
        mock_delta_table = Mock()
        mock_for_name.return_value = mock_delta_table

        # Mock the history() call
        mock_history = Mock()
        mock_history.select.return_value.orderBy.return_value.first.return_value = [5]
        mock_delta_table.history.return_value = mock_history

        # Mock the detail() call
        mock_detail = Mock()
        mock_detail.select.return_value.first.return_value = [datetime(2023, 1, 1)]
        mock_delta_table.detail.return_value = mock_detail

        table = Table("test_schema", "test_table")
        version = Version(3)

        result = self.operator.rollback_table(table, version, dry_run=True)

        self.assertTrue(result["success"], f"Expected success to be True, but got {result['success']}")
        self.assertEqual(result["from_version"], "5", f"Expected from_version to be '5', but got {result['from_version']}")
        self.assertEqual(result["to_version"], "3", f"Expected to_version to be '3', but got {result['to_version']}")
        self.assertEqual(result["action"], "rollback", f"Expected action to be 'rollback', but got {result['action']}")
        self.assertTrue(result["dry_run"], f"Expected dry_run to be True, but got {result['dry_run']}")
        mock_delta_table.restoreToVersion.assert_not_called()

if __name__ == '__main__':
    unittest.main()
