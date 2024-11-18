from datetime import datetime
from dtb.utils.date_extractor import DateExtractors, RegexDateExtractor


class TestRegexDateExtractor:
    def test_successful_date_extraction(self):
        extractor = RegexDateExtractor(
            pattern=r"(\d{4}-\d{2}-\d{2})",
            parse_func=lambda x: datetime.strptime(x, "%Y-%m-%d"),
            description="YYYY-MM-DD",
        )

        result = extractor.extract_date("data_2024-01-15_daily.csv")
        assert result == datetime(2024, 1, 15)

    def test_no_date_match(self):
        extractor = RegexDateExtractor(
            pattern=r"(\d{4}-\d{2}-\d{2})",
            parse_func=lambda x: datetime.strptime(x, "%Y-%m-%d"),
            description="YYYY-MM-DD",
        )

        result = extractor.extract_date("invalid_filename.csv")
        assert result is None

    def test_invalid_date_format(self):
        extractor = RegexDateExtractor(
            pattern=r"(\d{4}-\d{2}-\d{2})",
            parse_func=lambda x: datetime.strptime(x, "%Y-%m-%d"),
            description="YYYY-MM-DD",
        )

        result = extractor.extract_date("data_2024-13-45_daily.csv")  # Invalid date
        assert result is None


class TestDateExtractors:
    def test_yyyy_mm_dd_extractor(self):
        extractor = DateExtractors.yyyy_mm_dd()
        result = extractor.extract_date("data_2024-01-15_daily.csv")
        assert result == datetime(2024, 1, 15)

    def test_yyyymmdd_extractor(self):
        extractor = DateExtractors.yyyymmdd()
        result = extractor.extract_date("20240115_data.csv")
        assert result == datetime(2024, 1, 15)

    def test_custom_format_extractor(self):
        extractor = DateExtractors.custom_format(
            pattern=r"data_(\d{4}-\d{2}-\d{2}_\d{2}-\d{2})",
            date_format="%Y-%m-%d_%H-%M",
            description="YYYY-MM-DD_HH-MM",
        )
        result = extractor.extract_date("data_2024-01-15_14-30_daily.csv")
        assert result == datetime(2024, 1, 15, 14, 30)
