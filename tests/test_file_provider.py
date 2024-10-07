import unittest
from unittest.mock import patch, mock_open, MagicMock
from botocore.exceptions import NoCredentialsError
import pandas as pd
import io
import json
from file_provider import FileProvider, FileFormat

class TestFileProvider(unittest.TestCase):

    def setUp(self):
        self.local_provider = FileProvider(local_directory='test_data')
        self.s3_config = {
            'bucket': 'test-bucket',
            'key': 'test-key',
            'region': 'us-west-2'
        }
        with patch('boto3.client') as mock_boto3:
            self.mock_s3 = MagicMock()
            mock_boto3.return_value = self.mock_s3
            self.s3_provider = FileProvider(s3_config=self.s3_config)
            self.s3_provider.s3 = self.mock_s3  # Ensure the mocked S3 client is set

    @patch('os.makedirs')
    def test_init(self, mock_makedirs):
        FileProvider()
        mock_makedirs.assert_called_once_with('data', exist_ok=True)

    @patch('boto3.client')
    def test_init_s3(self, mock_boto3_client):
        FileProvider(s3_config=self.s3_config)
        mock_boto3_client.assert_called_once_with('s3', region_name='us-west-2')

    @patch('pandas.DataFrame.to_csv')
    @patch('builtins.open', new_callable=mock_open)
    def test_save_file_local_csv(self, mock_file, mock_to_csv):
        data = [{'name': 'Alice', 'age': 30}, {'name': 'Bob', 'age': 25}]
        self.local_provider.save_file('test', data, FileFormat.CSV)
        mock_file.assert_called_once_with('test_data/test.csv', 'w')
        mock_to_csv.assert_called_once()

    @patch('pandas.DataFrame.to_parquet')
    def test_save_file_local_parquet(self, mock_to_parquet):
        data = [{'name': 'Alice', 'age': 30}, {'name': 'Bob', 'age': 25}]
        self.local_provider.save_file('test', data, FileFormat.PARQUET)
        mock_to_parquet.assert_called_once_with('test_data/test.parquet', index=False)

    @patch('builtins.open', new_callable=mock_open)
    def test_save_file_local_json(self, mock_file):
        data = [{'name': 'Alice', 'age': 30}, {'name': 'Bob', 'age': 25}]
        self.local_provider.save_file('test', data, FileFormat.JSON)
        mock_file.assert_called_once_with('test_data/test.json', 'w')
        mock_file().write.assert_called_once_with(json.dumps(data))

    @patch('boto3.client')
    def test_save_file_s3_csv(self, mock_boto3):
        mock_s3 = MagicMock()
        mock_boto3.return_value = mock_s3
        data = [{'name': 'Alice', 'age': 30}, {'name': 'Bob', 'age': 25}]
        self.s3_provider.s3 = mock_s3  # Set the mocked S3 client
        self.s3_provider.save_file('test', data, FileFormat.CSV)
        mock_s3.put_object.assert_called_once()

    @patch('boto3.client')
    def test_save_file_s3_parquet(self, mock_boto3):
        mock_s3 = MagicMock()
        mock_boto3.return_value = mock_s3
        mock_to_parquet = MagicMock()
        mock_to_parquet.return_value = b'mocked parquet data'
        data = [{'name': 'Alice', 'age': 30}, {'name': 'Bob', 'age': 25}]
        self.s3_provider.s3 = mock_s3  # Set the mocked S3 client
        self.s3_provider.save_file('test', data, FileFormat.PARQUET)
        mock_s3.put_object.assert_called_once()

    @patch('boto3.client')
    def test_save_file_s3_json(self, mock_boto3):
        mock_s3 = MagicMock()
        mock_boto3.return_value = mock_s3
        data = [{'name': 'Alice', 'age': 30}, {'name': 'Bob', 'age': 25}]
        self.s3_provider.s3 = mock_s3  # Set the mocked S3 client
        self.s3_provider.save_file('test', data, FileFormat.JSON)
        mock_s3.put_object.assert_called_once()

    def test_save_file_invalid_format(self):
        with self.assertRaises(ValueError):
            self.local_provider.save_file('test', [], 'invalid')

    @patch('pandas.read_csv')
    @patch('builtins.open', new_callable=mock_open)
    def test_retrieve_file_local_csv(self, mock_file, mock_read_csv):
        mock_read_csv.return_value = pd.DataFrame([{'name': 'Alice', 'age': 30}])
        result = self.local_provider.retrieve_file('test', FileFormat.CSV)
        mock_file.assert_called_once_with('test_data/test.csv', 'r')
        self.assertEqual(result, [{'name': 'Alice', 'age': 30}])

    @patch('pandas.read_parquet')
    def test_retrieve_file_local_parquet(self, mock_read_parquet):
        mock_read_parquet.return_value = pd.DataFrame([{'name': 'Alice', 'age': 30}])
        result = self.local_provider.retrieve_file('test', FileFormat.PARQUET)
        mock_read_parquet.assert_called_once_with('test_data/test.parquet')
        self.assertEqual(result, [{'name': 'Alice', 'age': 30}])

    @patch('builtins.open', new_callable=mock_open, read_data='[{"name": "Alice", "age": 30}]')
    def test_retrieve_file_local_json(self, mock_file):
        result = self.local_provider.retrieve_file('test', FileFormat.JSON)
        mock_file.assert_called_once_with('test_data/test.json', 'r')
        self.assertEqual(result, [{'name': 'Alice', 'age': 30}])

    @patch('boto3.client')
    def test_retrieve_file_s3_csv(self, mock_boto3):
        mock_s3 = MagicMock()
        mock_boto3.return_value = mock_s3
        mock_body = MagicMock()
        mock_body.read.return_value = b'name,age\nAlice,30'
        mock_s3.get_object.return_value = {'Body': mock_body}
        self.s3_provider.s3 = mock_s3  # Set the mocked S3 client
        result = self.s3_provider.retrieve_file('test', FileFormat.CSV)
        self.assertEqual(result, [{'name': 'Alice', 'age': 30}])

    @patch('boto3.client')
    @patch('pandas.read_parquet')
    def test_retrieve_file_s3_parquet(self, mock_read_parquet, mock_boto3):
        mock_s3 = MagicMock()
        mock_boto3.return_value = mock_s3
        mock_read_parquet.return_value = pd.DataFrame([{'name': 'Alice', 'age': 30}])
        mock_s3.get_object.return_value = {'Body': io.BytesIO()}
        result = self.s3_provider.retrieve_file('test', FileFormat.PARQUET)
        self.assertEqual(result, [{'name': 'Alice', 'age': 30}])

    @patch('boto3.client')
    def test_retrieve_file_s3_json(self, mock_boto3):
        mock_s3 = MagicMock()
        mock_boto3.return_value = mock_s3
        mock_body = MagicMock()
        mock_body.read.return_value = b'[{"name": "Alice", "age": 30}]'
        mock_s3.get_object.return_value = {'Body': mock_body}
        self.s3_provider.s3 = mock_s3  # Set the mocked S3 client
        result = self.s3_provider.retrieve_file('test', FileFormat.JSON)
        self.assertEqual(result, [{'name': 'Alice', 'age': 30}])

    def test_retrieve_file_invalid_format(self):
        with self.assertRaises(ValueError):
            self.local_provider.retrieve_file('test', 'INVALID_FORMAT')

    @patch('boto3.client')
    def test_s3_credentials_error(self, mock_boto3):
        mock_boto3.side_effect = NoCredentialsError()
        with self.assertRaises(Exception) as context:
            FileProvider(s3_config=self.s3_config)
        self.assertIn('Unable to locate credentials', str(context.exception))

if __name__ == '__main__':
    unittest.main()