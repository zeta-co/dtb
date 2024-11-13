import unittest
from unittest.mock import MagicMock, patch
from pyspark.sql import SparkSession, DataFrame
from dtb.io.output import Output


class TestOutput(unittest.TestCase):

    def setUp(self):
        self.metadata = {
            'is_stream': False,
            'mode': 'overwrite',
            'output_mode': 'append',
            'format_options': {'path': '/path/to/save'},
            'partition_by': ['col1'],
            'sort_by': ['col2'],
            'trigger': {'processingTime': '10 seconds'},
            'type': 'parquet',
            'path': '/path/to/save'
        }
        self.output = Output(self.metadata)
        self.spark = MagicMock(spec=SparkSession)
        self.df = MagicMock(spec=DataFrame)
        self.mock_writer = MagicMock()
        self.df.writeStream = self.mock_writer
        self.df.write = self.mock_writer
        self.mock_writer.format.return_value = self.mock_writer
        self.mock_writer.mode.return_value = self.mock_writer
        self.mock_writer.outputMode.return_value = self.mock_writer
        self.mock_writer.option.return_value = self.mock_writer
        self.mock_writer.options.return_value = self.mock_writer
        self.mock_writer.partitionBy.return_value = self.mock_writer
        self.mock_writer.sortBy.return_value = self.mock_writer
        self.mock_writer.trigger.return_value = self.mock_writer

    def test_write_batch_mode(self):
        self.output.write(self.df, self.spark)
        self.mock_writer.format.assert_called_with('parquet')
        self.mock_writer.save.assert_called_with('/path/to/save')

if __name__ == '__main__':
    unittest.main()
