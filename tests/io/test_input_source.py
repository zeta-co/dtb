import pytest
from unittest.mock import Mock, patch
from pyspark.sql import DataFrame, DataFrameReader, DataFrameWriter
from dtb.io.input_source import InputSourceFactory, FileInputSource, TableInputSource
from dtb.model.metadata import Metadata


@pytest.fixture
def mock_spark():
    """Create a mock Spark session with necessary methods"""
    spark = Mock()
    
    # Mock DataFrame reader
    reader = Mock(spec=DataFrameReader)
    reader.format.return_value = reader
    reader.options.return_value = reader
    reader.schema.return_value = reader
    reader.load.return_value = Mock(spec=DataFrame)
    reader.table.return_value = Mock(spec=DataFrame)
    
    # Mock DataFrame writer
    writer = Mock(spec=DataFrameWriter)
    writer.format.return_value = writer
    writer.options.return_value = writer
    writer.save.return_value = None
    
    # Set up spark.read
    spark.read = reader
    
    # Set up spark.readStream
    spark.readStream = reader
    
    return spark


@pytest.fixture
def file_metadata():
    """Create sample metadata for file-based input"""
    return {
        "type": "csv",
        "path": "tests/data/raw/sales/*.csv",
        "is_table": False,
        "is_stream": False,
        "format_options": {
            "header": "true",
            "inferSchema": "false"
        },
        "schema": {
            "type": "struct",
            "columns": [
                {"name": "id", "type": "integer", "nullable": True},
                {"name": "date", "type": "string", "nullable": True},
                {"name": "product", "type": "string", "nullable": True},
                {"name": "quantity", "type": "integer", "nullable": True},
                {"name": "price", "type": "double", "nullable": True}
            ]
        }
    }


@pytest.fixture
def table_metadata():
    """Create sample metadata for table-based input"""
    return {
        "type": "delta",
        "path": "default.sales",
        "is_table": True,
        "is_stream": False,
        "schema": {
            "type": "struct",
            "columns": [
                {"name": "id", "type": "integer", "nullable": True},
                {"name": "date", "type": "string", "nullable": True},
                {"name": "product", "type": "string", "nullable": True},
                {"name": "quantity", "type": "integer", "nullable": True},
                {"name": "price", "type": "double", "nullable": True}
            ]
        }
    }


class TestFileInputSource:
    def test_create_file_input_source(self, file_metadata):
        """Test creation of file input source"""
        metadata = Metadata(file_metadata)
        input_source = InputSourceFactory.create_input_source(metadata)
        assert isinstance(input_source, FileInputSource)

    def test_file_read_without_filter(self, mock_spark, file_metadata):
        """Test reading file without filter"""
        metadata = Metadata(file_metadata)
        input_source = FileInputSource(metadata)
        
        df = input_source.df(mock_spark)
        
        # Verify the correct methods were called
        mock_spark.read.format.assert_called_with("csv")
        mock_spark.read.options.assert_called_with(**metadata.format_options)
        mock_spark.read.load.assert_called_with(metadata.path)
        assert isinstance(df, Mock)
        assert df == mock_spark.read.load.return_value

    def test_file_read_with_filter(self, mock_spark, file_metadata):
        """Test reading file with filter"""
        metadata = Metadata(file_metadata)
        input_source = FileInputSource(metadata)
        
        filter_files = ["path/to/file1.csv", "path/to/file2.csv"]
        df = input_source.df(mock_spark, filter=filter_files)
        
        # Verify filter was applied
        mock_spark.read.load.assert_called_with(','.join(filter_files))

    # def test_file_stream_read(self, mock_spark, file_metadata):
    #     """Test reading streaming data"""
    #     metadata = dict(file_metadata)
    #     metadata["is_stream"] = True
    #     metadata = Metadata(metadata)
    #     input_source = FileInputSource(metadata)
        
    #     df = input_source.df(mock_spark)
        
    #     # Verify streaming reader was used
    #     assert mock_spark.readStream.format.called
    #     assert not mock_spark.read.format.called


class TestTableInputSource:
    def test_create_table_input_source(self, table_metadata):
        """Test creation of table input source"""
        metadata = Metadata(table_metadata)
        input_source = InputSourceFactory.create_input_source(metadata)
        assert isinstance(input_source, TableInputSource)

    def test_table_read_without_filter(self, mock_spark, table_metadata):
        """Test reading table without filter"""
        metadata = Metadata(table_metadata)
        input_source = TableInputSource(metadata)
        
        df = input_source.df(mock_spark)
        
        # Verify the correct methods were called
        mock_spark.read.format.assert_called_with("delta")
        mock_spark.read.table.assert_called_with(metadata.path)
        assert isinstance(df, Mock)
        assert df == mock_spark.read.table.return_value

    def test_table_read_with_filter(self, mock_spark, table_metadata):
        """Test reading table with filter"""
        metadata = Metadata(table_metadata)
        input_source = TableInputSource(metadata)
        
        filter_condition = "date = '2024-02-01'"
        mock_filtered_df = Mock(spec=DataFrame)
        mock_spark.read.table.return_value.where.return_value = mock_filtered_df
        
        df = input_source.df(mock_spark, filter=filter_condition)
        
        # Verify filter was applied
        mock_spark.read.table.return_value.where.assert_called_with(filter_condition)
        assert df == mock_filtered_df


class TestInputSourceFactory:
    def test_create_file_input_source(self, file_metadata):
        """Test factory creates file input source"""
        metadata = Metadata(file_metadata)
        input_source = InputSourceFactory.create_input_source(metadata)
        assert isinstance(input_source, FileInputSource)

    def test_create_table_input_source(self, table_metadata):
        """Test factory creates table input source"""
        metadata = Metadata(table_metadata)
        input_source = InputSourceFactory.create_input_source(metadata)
        assert isinstance(input_source, TableInputSource)
