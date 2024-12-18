import unittest
from datetime import datetime
from dtb.utils.date_extractor import DateExtractor, DatePattern


class TestDateExtractor(unittest.TestCase):
    """Unit tests for DateExtractor class."""

    def test_extract_from_basic_date(self):
        """Test basic date extraction with yyyy-mm-dd format."""
        text = "file_2024-01-15_data.csv"
        result = DateExtractor.extract_from(text, "yyyy-mm-dd")
        expected = datetime(2024, 1, 15)
        self.assertEqual(result, expected)

    def test_extract_from_timestamp(self):
        """Test timestamp extraction with yyyy-mm-dd_hh:mm:ss format."""
        text = "log_2024-01-15 14:30:00_data.txt"
        result = DateExtractor.extract_from(text, "yyyy-mm-dd_hh:mm:ss")
        expected = datetime(2024, 1, 15, 14, 30, 0)
        self.assertEqual(result, expected)

    def test_extract_from_compact_format(self):
        """Test compact date format extraction with yyyymmdd format."""
        text = "data_20240115.csv"
        result = DateExtractor.extract_from(text, "yyyymmdd")
        expected = datetime(2024, 1, 15)
        self.assertEqual(result, expected)

    def test_extract_from_invalid_pattern(self):
        """Test extraction with invalid pattern key."""
        text = "file_2024-01-15_data.csv"
        with self.assertRaises(KeyError):
            DateExtractor.extract_from(text, "invalid_pattern")

    def test_extract_from_no_match(self):
        """Test extraction when no date is found in text."""
        text = "no_date_here.csv"
        result = DateExtractor.extract_from(text, "yyyy-mm-dd")
        self.assertIsNone(result)

    def test_extract_from_invalid_date(self):
        """Test extraction with invalid date values."""
        text = "file_2024-13-45_data.csv"  # Invalid month and day
        result = DateExtractor.extract_from(text, "yyyy-mm-dd")
        self.assertIsNone(result)

    def test_extract_custom_basic(self):
        """Test basic custom pattern extraction."""
        text = "data_@2024-01-15@_file.csv"
        result = DateExtractor.extract_custom(
            text, r"@(\d{4}-\d{2}-\d{2})@", "%Y-%m-%d"
        )
        expected = datetime(2024, 1, 15)
        self.assertEqual(result, expected)

    def test_extract_custom_no_match(self):
        """Test custom pattern with no match."""
        text = "no_date_here.csv"
        result = DateExtractor.extract_custom(
            text, r"@(\d{4}-\d{2}-\d{2})@", "%Y-%m-%d"
        )
        self.assertIsNone(result)

    def test_extract_custom_invalid_format(self):
        """Test custom pattern with invalid datetime format."""
        text = "data_@2024-01-15@_file.csv"
        result = DateExtractor.extract_custom(
            text,
            r"@(\d{4}-\d{2}-\d{2})@",
            "%d-%m-%Y",  # Wrong format for the matched date
        )
        self.assertIsNone(result)

    def test_list_patterns(self):
        """Test listing available patterns."""
        patterns = DateExtractor.list_patterns()
        self.assertIsInstance(patterns, dict)
        self.assertIn("yyyy-mm-dd", patterns)
        self.assertIsInstance(patterns["yyyy-mm-dd"], str)
        self.assertTrue(patterns["yyyy-mm-dd"].startswith("YYYY-MM-DD format"))

    def test_multiple_dates_first_match(self):
        """Test that only the first matching date is extracted."""
        text = "log_2024-01-15_backup_2024-02-01.csv"
        result = DateExtractor.extract_from(text, "yyyy-mm-dd")
        expected = datetime(2024, 1, 15)
        self.assertEqual(result, expected)

    def test_t_separator_in_timestamp(self):
        """Test timestamp extraction with T separator."""
        text = "log_2024-01-15T14:30:00_data.txt"
        result = DateExtractor.extract_from(text, "yyyy-mm-ddThh:mm:ss")
        expected = datetime(2024, 1, 15, 14, 30, 0)
        self.assertEqual(result, expected)


if __name__ == "__main__":
    unittest.main()
