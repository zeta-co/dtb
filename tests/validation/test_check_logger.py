import pytest
from unittest.mock import Mock, patch, call
from pyspark.sql import SparkSession
from typing import List
from dtb.validation.check_logger import CheckLogger
from dtb.validation.check_log_entry import CheckLogEntry
from dtb.logging.log_service import LogService
from dtb.logging.log_writer import LogWriter
from dtb.logging.log_delta_table_config import LogDeltaTableConfig
from dtb.logging.log_writer_delta_table import DeltaTableLogWriter
from dtb.model.table import Table

@pytest.fixture
def mock_spark():
    spark = Mock(spec=SparkSession)
    # Mock the internal SparkContext
    sc = Mock()
    spark._sc = sc
    spark.sparkContext = sc
    
    # Mock Java-related attributes
    java_spark = Mock()
    spark._jsparkSession = java_spark
    spark._jvm = Mock()
    spark._jconf = Mock()
    
    # Mock common SparkSession methods
    spark.version = "3.1.2"
    return spark

@pytest.fixture
def mock_writer():
    return Mock(spec=LogWriter)

@pytest.fixture
def mock_log_service():
    return Mock(spec=LogService)

@pytest.fixture
def sample_check_entry():
    return CheckLogEntry(log_entry_dict={
        "job_name":"test_job",
        "check_id":"check1",
        "check_name":"test_check",
        "status":"SUCCESS"
    })

class TestCheckLogger:
    def test_init_with_custom_writer(self, mock_spark, mock_writer):
        """Test initialization with a custom writer"""
        logger = CheckLogger(mock_spark, writer=mock_writer)
        
        assert logger.spark == mock_spark
        assert logger.writer == mock_writer
        assert logger._log_service is not None
        
    def test_init_with_default_writer(self, mock_spark):
        """Test initialization with default writer configuration"""
        with patch('dtb.validation.check_logger.DeltaTableLogWriter') as mock_delta_writer:
            logger = CheckLogger(mock_spark)
            
            # Verify DeltaTableLogWriter was created with correct config
            mock_delta_writer.assert_called_once()
            call_args = mock_delta_writer.call_args[1]
            assert call_args['spark'] == mock_spark
            assert isinstance(call_args['config'], LogDeltaTableConfig)
            assert call_args['config'].table.full_name == "hive_metastore.lg.dtb_checks"
            assert call_args['config'].partition_columns == ["JobName", "Date", "CheckId"]
            
    def test_log_entries_success(self, mock_spark, mock_writer, mock_log_service, sample_check_entry):
        """Test successful logging of multiple entries"""
        with patch('dtb.validation.check_logger.LogService', return_value=mock_log_service):
            logger = CheckLogger(mock_spark, writer=mock_writer)
            entries = [sample_check_entry, sample_check_entry]
            
            logger.log_entries(entries)
            
            # Verify entries were added and flushed
            assert mock_log_service.add_log_entry.call_count == 2
            mock_log_service.flush.assert_called_once()
            
    def test_log_entries_empty_list(self, mock_spark, mock_writer):
        """Test logging with empty entries list raises ValueError"""
        logger = CheckLogger(mock_spark, writer=mock_writer)
        
        with pytest.raises(ValueError) as exc_info:
            logger.log_entries([])
        assert "entries cannot be None or empty" in str(exc_info.value)
            
    def test_log_entries_none(self, mock_spark, mock_writer):
        """Test logging with None entries raises ValueError"""
        logger = CheckLogger(mock_spark, writer=mock_writer)
        
        with pytest.raises(ValueError) as exc_info:
            logger.log_entries(None)
        assert "entries cannot be None or empty" in str(exc_info.value)
            
    def test_log_entries_failure(self, mock_spark, mock_writer, mock_log_service, sample_check_entry):
        """Test handling of logging failure"""
        mock_log_service.add_log_entry.side_effect = Exception("Simulated failure")
        
        with patch('dtb.validation.check_logger.LogService', return_value=mock_log_service):
            logger = CheckLogger(mock_spark, writer=mock_writer)
            
            with pytest.raises(RuntimeError) as exc_info:
                logger.log_entries([sample_check_entry])
            assert "Failed to log entries" in str(exc_info.value)
            
    def test_log_entry_success(self, mock_spark, mock_writer, mock_log_service, sample_check_entry):
        """Test successful logging of single entry"""
        with patch('dtb.validation.check_logger.LogService', return_value=mock_log_service):
            logger = CheckLogger(mock_spark, writer=mock_writer)
            
            logger.log_entry(sample_check_entry)
            
            # Verify entry was added and flushed
            mock_log_service.add_log_entry.assert_called_once_with(sample_check_entry)
            mock_log_service.flush.assert_called_once()
            
    def test_log_entry_none(self, mock_spark, mock_writer):
        """Test logging with None entry raises ValueError"""
        logger = CheckLogger(mock_spark, writer=mock_writer)
        
        with pytest.raises(ValueError) as exc_info:
            logger.log_entry(None)
        assert "entry cannot be None" in str(exc_info.value)
            
    def test_log_entry_failure(self, mock_spark, mock_writer, mock_log_service, sample_check_entry):
        """Test handling of single entry logging failure"""
        mock_log_service.add_log_entry.side_effect = Exception("Simulated failure")
        
        with patch('dtb.validation.check_logger.LogService', return_value=mock_log_service):
            logger = CheckLogger(mock_spark, writer=mock_writer)
            
            with pytest.raises(RuntimeError) as exc_info:
                logger.log_entry(sample_check_entry)
            assert "Failed to log entries" in str(exc_info.value)

    def test_initialise_logging_custom_writer(self, mock_spark, mock_writer):
        """Test _initialise_logging with custom writer"""
        logger = CheckLogger(mock_spark, writer=mock_writer)
        
        # Reset the log service to test re-initialization
        logger._log_service = None
        logger._initialise_logging(mock_writer)
        
        assert logger._log_service is not None
        assert isinstance(logger._log_service, LogService)
        
    def test_initialise_logging_default_writer(self, mock_spark):
        """Test _initialise_logging with default writer creation"""
        with patch('dtb.validation.check_logger.DeltaTableLogWriter') as mock_delta_writer:
            logger = CheckLogger(mock_spark)
            
            # Reset and reinitialize
            logger._log_service = None
            logger._initialise_logging()
            
            # Verify default writer configuration
            mock_delta_writer.assert_called()
            assert logger._log_service is not None
