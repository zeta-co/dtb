from dtb.io.filter_strategy import FileListFilterStrategy, SqlFilterStrategy


class TestFileListFilterStrategy:
    def test_apply_filter(self):
        strategy = FileListFilterStrategy()
        file_list = ["tests/data/raw/sales/202402.csv", "tests/data/raw/sales/202403.csv"]
        result = strategy.apply_filter(file_list)
        assert result == "tests/data/raw/sales/202402.csv,tests/data/raw/sales/202403.csv"

class TestSqlFilterStrategy:
    def test_apply_filter(self):
        strategy = SqlFilterStrategy()
        condition = "date >= '2024-01-01' AND date <= '2024-01-31'"
        result = strategy.apply_filter(condition)
        assert result == condition
