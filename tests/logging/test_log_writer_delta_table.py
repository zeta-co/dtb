# import pytest
# from unittest.mock import Mock, patch
# from pyspark.sql import SparkSession, DataFrame
# from pyspark.sql.types import StructType, StructField, StringType

# from dtb.logging.log_entry import LogEntry
# from dtb.logging.log_writer_delta_table import DeltaTableLogWriter
# from dtb.model.delta_table_config import DeltaTableConfig
# from dtb.model.delta_table_manager import DeltaTableManager


# class TestDeltaTableLogWriter:
#     @pytest.fixture
#     def spark(self):
#         """Create mock SparkSession with necessary attributes"""
#         spark = Mock(spec=SparkSession)
#         sc = Mock(name="sc")
#         spark._sc = sc
#         return spark

#     @pytest.fixture
#     def schema(self):
#         """Create a sample schema for testing"""
#         return StructType(
#             [
#                 StructField("timestamp", StringType(), True),
#                 StructField("level", StringType(), True),
#                 StructField("message", StringType(), True),
#             ]
#         )

#     @pytest.fixture
#     def config(self):
#         """Create mock DeltaTableConfig"""
#         config = Mock(spec=DeltaTableConfig)
#         config.database = "test_db"
#         config.table_name = "test_logs"
#         config.full_table_name = "test_db.test_logs"
#         return config

#     @pytest.fixture
#     def table_manager(self):
#         """Create mock DeltaTableManager"""
#         return Mock(spec=DeltaTableManager)

#     @pytest.fixture
#     def log_entry(self):
#         """Create a mock LogEntry"""
#         entry = Mock(spec=LogEntry)
#         entry._log_entry_dict = {
#             "timestamp": "2024-01-01 00:00:00",
#             "level": "INFO",
#             "message": "Test message",
#         }
#         entry.output_df.return_value = entry._log_entry_dict
#         return entry

#     @pytest.fixture
#     def writer(self, spark, config, schema, table_manager):
#         """Create DeltaTableLogWriter instance"""
#         with patch(
#             "dtb.model.delta_table_manager.DeltaTableManager",
#             return_value=table_manager,
#         ):
#             return DeltaTableLogWriter(spark, config, schema)

#     def test_initialization(self, spark, config, schema, table_manager):
#         """Test successful initialization of DeltaTableLogWriter"""
#         with patch(
#             "dtb.model.delta_table_manager.DeltaTableManager",
#             return_value=table_manager,
#         ):
#             writer = DeltaTableLogWriter(spark, config, schema)

#             assert writer._spark == spark
#             assert writer._config == config
#             assert writer._schema == schema
#             assert writer._table_manager == table_manager
#             table_manager.create_if_not_exists.assert_called_once_with(schema, config)

#     def test_initialization_creates_table(self, spark, config, schema, table_manager):
#         """Test that initialization creates table if it doesn't exist"""
#         with patch(
#             "dtb.model.delta_table_manager.DeltaTableManager",
#             return_value=table_manager,
#         ):
#             DeltaTableLogWriter(spark, config, schema)
#             table_manager.create_if_not_exists.assert_called_once_with(schema, config)

#     def test_write_empty_entries(self, writer):
#         """Test that writing empty log entries raises ValueError"""
#         with pytest.raises(ValueError, match="log_entries must not be empty"):
#             writer.write([])

#     def test_write_invalid_entries(self, writer):
#         """Test that writing invalid log entries raises ValueError"""
#         invalid_entries = [{"not": "a log entry"}]
#         with pytest.raises(ValueError, match="All entries must be LogEntry instances"):
#             writer.write(invalid_entries)

#     def test_write_successful(self, writer, spark, log_entry):
#         """Test successful writing of log entries"""
#         # Mock DataFrame and write operations
#         mock_df = Mock(spec=DataFrame)
#         mock_df.write.format.return_value.mode.return_value.saveAsTable.return_value = (
#             None
#         )
#         spark.createDataFrame.return_value = mock_df

#         # Execute write operation
#         writer.write([log_entry])

#         # Verify DataFrame creation and write operations
#         spark.createDataFrame.assert_called_once_with(
#             [log_entry._log_entry_dict], writer._schema
#         )
#         mock_df.write.format.assert_called_once_with("delta")
#         mock_df.write.format.return_value.mode.assert_called_once_with("append")
#         mock_df.write.format.return_value.mode.return_value.saveAsTable.assert_called_once_with(
#             writer._config.full_table_name
#         )

#     def test_write_multiple_entries(self, writer, spark):
#         """Test writing multiple log entries"""
#         # Create multiple mock log entries
#         mock_entries = []
#         for i in range(3):
#             entry = Mock(spec=LogEntry)
#             entry._log_entry_dict = {
#                 "timestamp": f"2024-01-01 00:00:0{i}",
#                 "level": "INFO",
#                 "message": f"Test message {i}",
#             }
#             entry.output_df.return_value = entry._log_entry_dict
#             mock_entries.append(entry)

#         # Mock DataFrame and write operations
#         mock_df = Mock(spec=DataFrame)
#         mock_df.write.format.return_value.mode.return_value.saveAsTable.return_value = (
#             None
#         )
#         spark.createDataFrame.return_value = mock_df

#         # Execute write operation
#         writer.write(mock_entries)

#         # Verify correct handling of multiple entries
#         spark.createDataFrame.assert_called_once_with(
#             [entry._log_entry_dict for entry in mock_entries], writer._schema
#         )
#         mock_df.write.format.assert_called_once_with("delta")
#         mock_df.write.format.return_value.mode.assert_called_once_with("append")

#     def test_write_runtime_error(self, writer, spark, log_entry):
#         """Test RuntimeError is raised when write operation fails"""
#         spark.createDataFrame.side_effect = Exception("Database error")

#         with pytest.raises(
#             RuntimeError, match="Failed to write to Delta table: Database error"
#         ):
#             writer.write([log_entry])

#     @pytest.mark.parametrize(
#         "error,expected_message",
#         [
#             (
#                 ValueError("Schema mismatch"),
#                 "Failed to write to Delta table: Schema mismatch",
#             ),
#             (
#                 RuntimeError("Connection error"),
#                 "Failed to write to Delta table: Connection error",
#             ),
#             (
#                 Exception("Unknown error"),
#                 "Failed to write to Delta table: Unknown error",
#             ),
#         ],
#     )
#     def test_write_various_errors(
#         self, writer, spark, log_entry, error, expected_message
#     ):
#         """Test different error scenarios during write operation"""
#         spark.createDataFrame.side_effect = error

#         with pytest.raises(RuntimeError, match=expected_message):
#             writer.write([log_entry])

#     def test_write_none_log_entry_dict(self, writer, log_entry):
#         """Test handling of log entry with None _log_entry_dict"""
#         log_entry._log_entry_dict = None
#         with pytest.raises(RuntimeError):
#             writer.write([log_entry])

#     def test_schema_validation(self, spark, config, schema):
#         """Test schema validation during initialization"""
#         invalid_schema = "not a schema"
#         with pytest.raises(TypeError):
#             DeltaTableLogWriter(spark, config, invalid_schema)
