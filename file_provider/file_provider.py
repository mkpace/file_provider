import os
import csv
import json
import pandas as pd
import boto3
from io import StringIO
from botocore.exceptions import NoCredentialsError
from enum import Enum, auto

class FileFormat(Enum):
    """Enumeration of supported file formats."""
    CSV = auto()
    PARQUET = auto()
    JSON = auto()

class FileProvider:
    """
    A class for handling file operations (save, update, retrieve) with support for local storage and Amazon S3.
    Supports CSV, Parquet, and JSON file formats.
    """

    def __init__(self, s3_config=None, local_directory='data'):
        """
        Initialize the FileProvider.
        
        :param s3_config: Dictionary containing S3 configuration (bucket, key, region)
        :param local_directory: Local directory for file storage if not using S3
        """
        self.s3_config = s3_config
        self.local_directory = local_directory

        if self.s3_config:
            self.s3 = boto3.client('s3', region_name=self.s3_config.get('region'))
        else:
            os.makedirs(self.local_directory, exist_ok=True)

    def save_file(self, file_name, data, file_format: FileFormat):
        """
        Save data to a file in the specified format.
        
        :param file_name: Name of the file to save
        :param data: Data to be saved
        :param file_format: Format of the file (FileFormat enum)
        """
        self._validate_file_format(file_format)

        if file_format == FileFormat.CSV:
            content = self._data_to_csv(data)
        elif file_format == FileFormat.PARQUET:
            content = self._data_to_parquet(data)
        elif file_format == FileFormat.JSON:
            content = self._data_to_json(data)

        if self.s3_config:
            self._save_to_s3(file_name, content, file_format)
        else:
            self._save_to_local(file_name, content, file_format)

    def update_file(self, file_name, data, file_format: FileFormat):
        """
        Update an existing file with new data. Currently, this method simply overwrites the file.
        
        :param file_name: Name of the file to update
        :param data: New data to be saved
        :param file_format: Format of the file (FileFormat enum)
        """
        self._validate_file_format(file_format)

        # For simplicity, update is the same as save
        self.save_file(file_name, data, file_format)

    def retrieve_file(self, file_name, file_format: FileFormat):
        """
        Retrieve data from a file in the specified format.
        
        :param file_name: Name of the file to retrieve
        :param file_format: Format of the file (FileFormat enum)
        :return: Data from the file
        """
        self._validate_file_format(file_format)

        if self.s3_config:
            content = self._retrieve_from_s3(file_name, file_format)
        else:
            content = self._retrieve_from_local(file_name, file_format)

        if file_format == FileFormat.CSV:
            return self._csv_to_data(content)
        elif file_format == FileFormat.PARQUET:
            return self._parquet_to_data(content)
        elif file_format == FileFormat.JSON:
            return self._json_to_data(content)

    def _save_to_local(self, file_name, content, file_format: FileFormat):
        """
        Save content to a local file.
        
        :param file_name: Name of the file to save
        :param content: Content to be saved
        :param file_format: Format of the file (FileFormat enum)
        """
        self._validate_file_format(file_format)

        file_path = os.path.join(self.local_directory, f"{file_name}.{file_format.name.lower()}")
        if file_format == FileFormat.PARQUET:
            content.to_parquet(file_path, index=False)
        else:
            with open(file_path, 'w') as f:
                f.write(content)

    def _retrieve_from_local(self, file_name, file_format: FileFormat):
        """
        Retrieve content from a local file.
        
        :param file_name: Name of the file to retrieve
        :param file_format: Format of the file (FileFormat enum)
        :return: Content of the file
        """
        self._validate_file_format(file_format)

        file_path = os.path.join(self.local_directory, f"{file_name}.{file_format.name.lower()}")
        if file_format == FileFormat.PARQUET:
            return pd.read_parquet(file_path)
        else:
            with open(file_path, 'r') as f:
                return f.read()

    def _save_to_s3(self, file_name, content, file_format: FileFormat):
        """
        Save content to an S3 file.
        
        :param file_name: Name of the file to save
        :param content: Content to be saved
        :param file_format: Format of the file (FileFormat enum)
        """
        self._validate_file_format(file_format)

        try:
            key = f"{self.s3_config['key']}/{file_name}.{file_format.name.lower()}"
            if file_format == FileFormat.PARQUET:
                buffer = content.to_parquet()
                self.s3.put_object(Bucket=self.s3_config['bucket'], Key=key, Body=buffer)
            else:
                self.s3.put_object(Bucket=self.s3_config['bucket'], Key=key, Body=content)
        except NoCredentialsError:
            raise Exception("S3 credentials not available")

    def _retrieve_from_s3(self, file_name, file_format: FileFormat):
        """
        Retrieve content from an S3 file.
        
        :param file_name: Name of the file to retrieve
        :param file_format: Format of the file (FileFormat enum)
        :return: Content of the file
        """
        self._validate_file_format(file_format)

        try:
            key = f"{self.s3_config['key']}/{file_name}.{file_format.name.lower()}"
            response = self.s3.get_object(Bucket=self.s3_config['bucket'], Key=key)
            if file_format == FileFormat.PARQUET:
                return pd.read_parquet(response['Body'])
            else:
                return response['Body'].read().decode('utf-8')
        except NoCredentialsError:
            raise Exception("S3 credentials not available")

    def _validate_file_format(self, file_format: FileFormat):
        """
        Validate the file format.
        
        :param file_format: Format of the file (FileFormat enum)
        """
        if not isinstance(file_format, FileFormat):
            raise ValueError(f"Invalid file format: {file_format}")

    def _data_to_csv(self, data):
        """Convert data to CSV format."""
        return pd.DataFrame(data).to_csv(index=False)

    def _csv_to_data(self, content):
        """Convert CSV content to data."""
        return pd.read_csv(content if isinstance(content, pd.DataFrame) else StringIO(content)).to_dict('records')

    def _data_to_parquet(self, data):
        """Convert data to Parquet format."""
        return pd.DataFrame(data)

    def _parquet_to_data(self, content):
        """Convert Parquet content to data."""
        return content.to_dict('records')

    def _data_to_json(self, data):
        """Convert data to JSON format."""
        return json.dumps(data)

    def _json_to_data(self, content):
        """Convert JSON content to data."""
        return json.loads(content)