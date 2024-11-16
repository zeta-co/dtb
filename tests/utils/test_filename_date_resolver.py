import pytest
from datetime import datetime
from dtb.utils.date_extractor import DateExtractors
from dtb.utils.filename_date_resolver import FilenameDateResolver


class TestFilenameDateResolver:
    @pytest.fixture
    def resolver(self):
        resolver = FilenameDateResolver()
        resolver.add_extractor(DateExtractors.yyyy_mm_dd())
        resolver.add_extractor(DateExtractors.yyyymmdd())
        return resolver

    def test_multiple_extractors(self, resolver):
        # Test YYYY-MM-DD format
        date1 = resolver.get_date("data_2024-01-15_daily.csv")
        assert date1 == datetime(2024, 1, 15)

        # Test YYYYMMDD format
        date2 = resolver.get_date("20240115_data.csv")
        assert date2 == datetime(2024, 1, 15)

    def test_no_matching_extractor(self, resolver):
        date = resolver.get_date("invalid_filename.csv")
        assert date is None

    def test_adding_custom_extractor(self, resolver):
        resolver.add_extractor(
            DateExtractors.custom_format(
                pattern=r"(\d{4}-\d{2}-\d{2}_\d{2}-\d{2})",
                date_format="%Y-%m-%d_%H-%M",
                description="YYYY-MM-DD_HH-MM",
            )
        )

        date = resolver.get_date("data_2024-01-15_14-30_daily.csv")
        expected = datetime(2024, 1, 15, 14, 30)
        assert date == expected
