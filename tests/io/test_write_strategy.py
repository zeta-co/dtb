import pytest
from unittest.mock import Mock
from pyspark.sql import DataFrame
from dtb.model.metadata import Metadata
from dtb.io.write_strategy import WriteStrategy, AppendStrategy, OverwriteStrategy


class TestWriteStrategy:
    @pytest.fixture
    def mock_df(self):
        """Fixture providing a mock DataFrame"""
        return Mock(spec=DataFrame)

    @pytest.fixture
    def mock_writer(self):
        """Fixture providing a mock writer with chainable methods"""
        writer = Mock()
        writer.mode.return_value = writer
        writer.outputMode.return_value = writer
        writer.option.return_value = writer
        return writer

    @pytest.fixture
    def table_metadata(self):
        """Fixture providing metadata for table writes"""
        metadata = Mock(spec=Metadata)
        metadata.is_table = True
        metadata.table_catalog = "catalog"
        metadata.table_schema = "schema"
        metadata.table_name = "table"
        metadata.type = "delta"
        return metadata

    @pytest.fixture
    def path_metadata(self):
        """Fixture providing metadata for path writes"""
        metadata = Mock(spec=Metadata)
        metadata.is_table = False
        metadata.path = "/test/path"
        metadata.type = "delta"
        return metadata

    class TestAppendStrategy:
        def test_write_batch_table(self, mock_df, mock_writer, table_metadata):
            """Test append batch write to table"""
            strategy = AppendStrategy()

            strategy.write_batch(mock_df, mock_writer, table_metadata)

            mock_writer.mode.assert_called_once_with("append")
            mock_writer.saveAsTable.assert_called_once_with("catalog.schema.table")

        def test_write_batch_path(self, mock_df, mock_writer, path_metadata):
            """Test append batch write to path"""
            strategy = AppendStrategy()

            strategy.write_batch(mock_df, mock_writer, path_metadata)

            mock_writer.mode.assert_called_once_with("append")
            mock_writer.save.assert_called_once_with("/test/path")

        def test_write_stream_table(self, mock_df, mock_writer, table_metadata):
            """Test append stream write to table"""
            strategy = AppendStrategy()

            strategy.write_stream(mock_df, mock_writer, table_metadata)

            mock_writer.outputMode.assert_called_once_with("append")
            mock_writer.toTable.assert_called_once_with("catalog.schema.table")

        def test_write_stream_path(self, mock_df, mock_writer, path_metadata):
            """Test append stream write to path"""
            strategy = AppendStrategy()

            strategy.write_stream(mock_df, mock_writer, path_metadata)

            mock_writer.outputMode.assert_called_once_with("append")
            mock_writer.start.assert_called_once_with("/test/path")

    class TestOverwriteStrategy:
        def test_write_batch_table_delta(self, mock_df, mock_writer, table_metadata):
            """Test overwrite batch write to Delta table"""
            strategy = OverwriteStrategy()

            strategy.write_batch(mock_df, mock_writer, table_metadata)

            mock_writer.mode.assert_called_once_with("overwrite")
            mock_writer.option.assert_called_once_with("overwriteSchema", "true")
            mock_writer.saveAsTable.assert_called_once_with("catalog.schema.table")

        def test_write_batch_table_non_delta(
            self, mock_df, mock_writer, table_metadata
        ):
            """Test overwrite batch write to non-Delta table"""
            strategy = OverwriteStrategy()
            table_metadata.type = "parquet"

            strategy.write_batch(mock_df, mock_writer, table_metadata)

            mock_writer.mode.assert_called_once_with("overwrite")
            mock_writer.option.assert_not_called()
            mock_writer.saveAsTable.assert_called_once_with("catalog.schema.table")

        def test_write_batch_path(self, mock_df, mock_writer, path_metadata):
            """Test overwrite batch write to path"""
            strategy = OverwriteStrategy()

            strategy.write_batch(mock_df, mock_writer, path_metadata)

            mock_writer.mode.assert_called_once_with("overwrite")
            mock_writer.save.assert_called_once_with("/test/path")

        def test_write_stream_raises_error(self, mock_df, mock_writer, table_metadata):
            """Test that streaming write raises ValueError"""
            strategy = OverwriteStrategy()

            with pytest.raises(ValueError) as exc_info:
                strategy.write_stream(mock_df, mock_writer, table_metadata)

            assert "Streaming doesn't support overwrite!" in str(exc_info.value)

        def test_should_evolve_schema(self):
            """Test schema evolution check"""
            strategy = OverwriteStrategy()
            new_schema = {"field1": "string", "field2": "int"}
            existing_schema = {"field1": "string"}

            # Currently always returns True as per TODO comment
            assert strategy._should_evolve_schema(new_schema, existing_schema) is True

    def test_write_strategy_is_abstract(self):
        """Test that WriteStrategy cannot be instantiated"""
        with pytest.raises(TypeError):
            WriteStrategy()

    def test_write_strategy_requires_implementation(self):
        """Test that subclasses must implement abstract methods"""

        class IncompleteStrategy(WriteStrategy):
            pass

        with pytest.raises(TypeError):
            IncompleteStrategy()

    def test_method_call_order(self, mock_df, mock_writer, table_metadata):
        """Test that writer method calls are made in correct order"""
        strategy = OverwriteStrategy()

        strategy.write_batch(mock_df, mock_writer, table_metadata)

        # Verify order of calls
        call_order = mock_writer.method_calls
        assert call_order[0][0] == "mode"
        assert call_order[1][0] == "option"
        assert call_order[2][0] == "saveAsTable"

    def test_writer_chain_independence(self, mock_df, mock_writer, table_metadata):
        """Test that each write operation starts with a fresh writer"""
        strategy = AppendStrategy()

        strategy.write_batch(mock_df, mock_writer, table_metadata)
        strategy.write_batch(mock_df, mock_writer, table_metadata)

        assert mock_writer.mode.call_count == 2
        assert mock_writer.saveAsTable.call_count == 2
