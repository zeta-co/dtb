import unittest
from datetime import datetime
from dtb.rollback.rollback_criteria import RollbackCriteria
from dtb.rollback.rollback_request import RollbackRequest


class TestRollbackRequest(unittest.TestCase):

    def test_init_with_minimal_args(self):
        request = RollbackRequest(RollbackCriteria.WORKFLOW, "test_user")
        self.assertEqual(request.criteria, RollbackCriteria.WORKFLOW)
        self.assertEqual(request.user, "test_user")
        self.assertFalse(request.dry_run)
        self.assertEqual(request.kwargs, {})

    def test_init_with_dry_run(self):
        request = RollbackRequest(RollbackCriteria.TABLE_LIST, "test_user", dry_run=True)
        self.assertEqual(request.criteria, RollbackCriteria.TABLE_LIST)
        self.assertEqual(request.user, "test_user")
        self.assertTrue(request.dry_run)
        self.assertEqual(request.kwargs, {})

    def test_init_with_kwargs(self):
        kwargs = {
            "schema": "test_schema",
            "timestamp": datetime(2023, 1, 1)
        }
        request = RollbackRequest(RollbackCriteria.SCHEMA, "test_user", **kwargs)
        self.assertEqual(request.criteria, RollbackCriteria.SCHEMA)
        self.assertEqual(request.user, "test_user")
        self.assertFalse(request.dry_run)
        self.assertEqual(request.kwargs, kwargs)

    def test_init_with_all_args(self):
        kwargs = {
            "tables": ["table1", "table2"],
            "timestamp": datetime(2023, 1, 1)
        }
        request = RollbackRequest(RollbackCriteria.TABLE_LIST, "test_user", True, **kwargs)
        self.assertEqual(request.criteria, RollbackCriteria.TABLE_LIST)
        self.assertEqual(request.user, "test_user")
        self.assertTrue(request.dry_run)
        self.assertEqual(request.kwargs, kwargs)

    def test_invalid_criteria_type(self):
        with self.assertRaises(TypeError):
            RollbackRequest("INVALID_CRITERIA", "test_user")

    def test_invalid_user_type(self):
        with self.assertRaises(TypeError):
            RollbackRequest(RollbackCriteria.WORKFLOW, 123)

    def test_invalid_dry_run_type(self):
        with self.assertRaises(TypeError):
            RollbackRequest(RollbackCriteria.WORKFLOW, "test_user", dry_run="True")

if __name__ == '__main__':
    unittest.main()
