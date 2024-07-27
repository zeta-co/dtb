import unittest
from unittest.mock import MagicMock
from dtb.utils.databricks_context import DatabricksContext


class TestDatabricksContext(unittest.TestCase):
    def setUp(self):
        # Create a mock for dbutils
        self.mock_dbutils = MagicMock()
        
        # Set up the mock chain
        self.mock_context = MagicMock()
        self.mock_dbutils.notebook.entry_point.getDbutils.return_value.notebook.return_value.getContext.return_value = self.mock_context

    def test_get_current_user_success(self):
        # Set up the mock to return a specific username
        self.mock_context.userName.return_value.get.return_value = "test_user"
        
        # Create an instance of DatabricksContext with our mock
        db_context = DatabricksContext(self.mock_dbutils)
        
        # Test the get_current_user method
        self.assertEqual(db_context.get_current_user(), "test_user")

    def test_get_current_user_exception(self):
        # Set up the mock to raise an exception
        self.mock_context.userName.return_value.get.side_effect = Exception("Test exception")
        
        # Create an instance of DatabricksContext with our mock
        db_context = DatabricksContext(self.mock_dbutils)
        
        # Test the get_current_user method when an exception occurs
        self.assertEqual(db_context.get_current_user(), "unknown_user")

if __name__ == '__main__':
    unittest.main()
