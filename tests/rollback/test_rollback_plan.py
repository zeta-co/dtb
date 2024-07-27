import unittest
from dtb.rollback.rollback_plan import RollbackPlan


class TestRollbackPlan(unittest.TestCase):

    def test_init(self):
        details = [
            {
                "catalog": "test_catalog",
                "schema": "test_schema",
                "table": "test_table",
                "from_version": "1",
                "to_version": "0"
            }
        ]
        plan = RollbackPlan(details)
        self.assertEqual(plan.details, details)

    def test_str_single_detail(self):
        details = [
            {
                "catalog": "test_catalog",
                "schema": "test_schema",
                "table": "test_table",
                "from_version": "1",
                "to_version": "0"
            }
        ]
        plan = RollbackPlan(details)
        expected_str = (
            "Restore Plan:\n"
            "  - test_catalog.test_schema.test_table: From version 1 to 0"
        )
        self.assertEqual(str(plan), expected_str)

    def test_str_multiple_details(self):
        details = [
            {
                "catalog": "catalog1",
                "schema": "schema1",
                "table": "table1",
                "from_version": "2",
                "to_version": "1"
            },
            {
                "catalog": "catalog2",
                "schema": "schema2",
                "table": "table2",
                "from_version": "3",
                "to_version": "2"
            }
        ]
        plan = RollbackPlan(details)
        expected_str = (
            "Restore Plan:\n"
            "  - catalog1.schema1.table1: From version 2 to 1\n"
            "  - catalog2.schema2.table2: From version 3 to 2"
        )
        self.assertEqual(str(plan), expected_str)

    def test_empty_details(self):
        plan = RollbackPlan([])
        self.assertEqual(str(plan), "Restore Plan:\n")

    def test_details_with_different_types(self):
        details = [
            {
                "catalog": "test_catalog",
                "schema": "test_schema",
                "table": "test_table",
                "from_version": 1,
                "to_version": 0
            }
        ]
        plan = RollbackPlan(details)
        expected_str = (
            "Restore Plan:\n"
            "  - test_catalog.test_schema.test_table: From version 1 to 0"
        )
        self.assertEqual(str(plan), expected_str)

if __name__ == '__main__':
    unittest.main()
