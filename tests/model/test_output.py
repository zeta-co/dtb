import unittest
from unittest.mock import MagicMock, patch
from pyspark.sql import SparkSession, DataFrame
from dtb.model.output import Output


class TestOutput(unittest.TestCase):

    def setUp(self):
        self.metadata = {
            'stream': False,
            'mode': 'overwrite',
            'outputMode': 'append',
            'options': {'path': '/path/to/save'},
            'partitionBy': ['col1'],
            'sortBy': ['col2'],
            'trigger': {'processingTime': '10 seconds'},
            'format': 'parquet',
            'save': '/path/to/save'
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
        self.mock_writer.options.return_value = self.mock_writer
        self.mock_writer.partitionBy.return_value = self.mock_writer
        self.mock_writer.sortBy.return_value = self.mock_writer
        self.mock_writer.trigger.return_value = self.mock_writer

    def test_writer_batch_mode(self):
        self.output.writer(self.df)
        self.mock_writer.mode.assert_called_with('overwrite')
        self.mock_writer.outputMode.assert_called_with('append')
        self.mock_writer.options.assert_called_with({'path': '/path/to/save'})
        self.mock_writer.partitionBy.assert_called_with(['col1'])
        self.mock_writer.sortBy.assert_called_with(['col2'])

    def test_write_batch_mode(self):
        self.output.write(self.df)
        self.mock_writer.format.assert_called_with('parquet')
        self.mock_writer.save.assert_called_with('/path/to/save')

    def test_writer_stream_mode(self):
        self.output.metadata['stream'] = True
        self.output.writer(self.df)
        self.mock_writer.mode.assert_called_with('overwrite')
        self.mock_writer.outputMode.assert_called_with('append')
        self.mock_writer.options.assert_called_with({'path': '/path/to/save'})
        self.mock_writer.partitionBy.assert_called_with(['col1'])
        self.mock_writer.sortBy.assert_called_with(['col2'])
        self.mock_writer.trigger.assert_called_with({'processingTime': '10 seconds'})

    def test_write_stream_mode(self):
        self.output.metadata['stream'] = True
        self.output.write(self.df)
        self.mock_writer.format.assert_called_with('parquet')
        self.mock_writer.start.assert_called_with('/path/to/save')

if __name__ == '__main__':
    unittest.main()
