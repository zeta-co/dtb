import unittest
from unittest.mock import Mock, patch
from datetime import datetime
from dtb.delta.delta_table_operator import DeltaTableOperator
from dtb.logging.log_service import LogService
from dtb.rollback.rollback_criteria import RollbackCriteria
from dtb.rollback.rollback_manager import RollbackManager
from dtb.rollback.rollback_plan import RollbackPlan
from dtb.rollback.rollback_request import RollbackRequest
from dtb.rollback.rollback_result import RollbackResult
from dtb.table.table import Table
from dtb.table.table_selector import TableSelector


class TestRollbackManager(unittest.TestCase):

    def setUp(self):
        self.mock_spark = Mock()
        self.mock_table_selector = Mock(spec=TableSelector)
        self.mock_delta_table_operator = Mock(spec=DeltaTableOperator)
        self.mock_log_service = Mock(spec=LogService)

        self.rollback_manager = RollbackManager(self.mock_spark)
        self.rollback_manager.table_selector = self.mock_table_selector
        self.rollback_manager.delta_table_operator = self.mock_delta_table_operator

    def test_execute_rollback_workflow(self):
        request = RollbackRequest(
            criteria=RollbackCriteria.WORKFLOW,
            user="test_user",
            log_table_name="log_table",
            job_id="job1",
            run_id="run1"
        )

        mock_tables = [
            (Table("schema1", "table1"), "1"),
            (Table("schema2", "table2"), "2")
        ]
        self.mock_table_selector.select_tables_from_log.return_value = mock_tables

        self.mock_delta_table_operator.rollback_table.side_effect = [
            {"success": True, "from_version": "2", "to_version": "1", "rollback_timestamp": datetime.now(), "action": "rollback"},
            {"success": True, "from_version": "3", "to_version": "2", "rollback_timestamp": datetime.now(), "action": "rollback"}
        ]

        result = self.rollback_manager.execute_rollback(request, self.mock_log_service)

        self.assertIsInstance(result, RollbackResult)
        self.assertEqual(len(result.details), 2)
        self.mock_log_service.log_rollback.assert_called_once_with(result)

    def test_execute_rollback_schema(self):
        request = RollbackRequest(
            criteria=RollbackCriteria.SCHEMA,
            user="test_user",
            schema="test_schema",
            timestamp=datetime(2023, 1, 1)
        )

        mock_tables = [Table("test_schema", "table1"), Table("test_schema", "table2")]
        self.mock_table_selector.select_tables_by_schema.return_value = mock_tables

        self.mock_delta_table_operator.rollback_table.side_effect = [
            {"success": True, "from_version": "2", "to_version": "1", "rollback_timestamp": datetime.now(), "action": "rollback"},
            {"success": True, "from_version": "3", "to_version": "2", "rollback_timestamp": datetime.now(), "action": "rollback"}
        ]

        result = self.rollback_manager.execute_rollback(request, self.mock_log_service)

        self.assertIsInstance(result, RollbackResult)
        self.assertEqual(len(result.details), 2)
        self.mock_log_service.log_rollback.assert_called_once_with(result)

    def test_execute_rollback_table_list(self):
        request = RollbackRequest(
            criteria=RollbackCriteria.TABLE_LIST,
            user="test_user",
            tables=["schema1.table1", "schema2.table2"],
            timestamp=datetime(2023, 1, 1)
        )

        mock_tables = [Table("schema1", "table1"), Table("schema2", "table2")]
        self.mock_table_selector.select_tables_by_list.return_value = mock_tables

        self.mock_delta_table_operator.rollback_table.side_effect = [
            {"success": True, "from_version": "2", "to_version": "1", "rollback_timestamp": datetime.now(), "action": "rollback"},
            {"success": True, "from_version": "3", "to_version": "2", "rollback_timestamp": datetime.now(), "action": "rollback"}
        ]

        result = self.rollback_manager.execute_rollback(request, self.mock_log_service)

        self.assertIsInstance(result, RollbackResult)
        self.assertEqual(len(result.details), 2)
        self.mock_log_service.log_rollback.assert_called_once_with(result)

    def test_execute_rollback_dry_run(self):
        request = RollbackRequest(
            criteria=RollbackCriteria.SCHEMA,
            user="test_user",
            dry_run=True,
            schema="test_schema",
            timestamp=datetime(2023, 1, 1)
        )

        mock_tables = [Table("test_schema", "table1"), Table("test_schema", "table2")]
        self.mock_table_selector.select_tables_by_schema.return_value = mock_tables

        self.mock_delta_table_operator.rollback_table.side_effect = [
            {"success": True, "from_version": "2", "to_version": "1", "rollback_timestamp": None, "action": "rollback", "dry_run": True},
            {"success": True, "from_version": "3", "to_version": "2", "rollback_timestamp": None, "action": "rollback", "dry_run": True}
        ]

        result = self.rollback_manager.execute_rollback(request, self.mock_log_service)

        self.assertIsInstance(result, RollbackPlan)
        self.assertEqual(len(result.details), 2)
        self.mock_log_service.log_rollback.assert_not_called()

if __name__ == '__main__':
    unittest.main()
