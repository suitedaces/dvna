```
import unittest
from unittest.mock import patch, MagicMock
import json
import boto3
from botocore.exceptions import ClientError

from your_module import (
    publish_multi_file_handler,
    find_files_and_build_payloads,
    process_payloads,
    check_file_exists,
    LANDING_ZONE_BUCKET,
    MAX_WORKERS
)

class TestDataProcessingPipeline(unittest.TestCase):

    def setUp(self):
        self.s3_client = MagicMock(spec=boto3.client('s3'))
        self.lambda_client = MagicMock(spec=boto3.client('lambda'))

    @patch('your_module.logger')
    def test_publish_multi_file_handler_success(self, mock_logger):
        event = {
            "pipelineId": "test_pipeline",
            "s3Prefix": "test_prefix",
            "datasetGuid": "test_guid",
            "filename": "test.csv",
            "containerImageName": "test_image",
            "threads": 4,
            "startDate": "2024-01-01",
            "endDate": "2024-01-31"
        }

        with patch('your_module.find_files_and_build_payloads') as mock_find:
            mock_find.return_value = ([{'key': 'value'}], [])

            with patch('your_module.process_payloads') as mock_process:
                mock_process.return_value = ([{'success': True}], [])

                result = publish_multi_file_handler(self.s3_client, self.lambda_client, event)

        self.assertEqual(result['statusCode'], 200)
        self.assertIn('successes', json.loads(result['body']))

    @patch('your_module.logger')
    def test_publish_multi_file_handler_no_files(self, mock_logger):
        event = {
            "pipelineId": "test_pipeline",
            "s3Prefix": "test_prefix",
            "datasetGuid": "test_guid",
            "filename": "test.csv",
            "containerImageName": "test_image",
            "threads": 4,
        }

        with patch('your_module.find_files_and_build_payloads') as mock_find:
            mock_find.return_value = ([], [])

            result = publish_multi_file_handler(self.s3_client, self.lambda_client, event)

        self.assertEqual(result['statusCode'], 204)

    @patch('your_module.logger')
    def test_publish_multi_file_handler_missing_params(self, mock_logger):
        event = {
            "pipelineId": "test_pipeline",
        }

        result = publish_multi_file_handler(self.s3_client, self.lambda_client, event)

        self.assertEqual(result['statusCode'], 400)

    @patch('your_module.logger')
    @patch('your_module.concurrent.futures.ThreadPoolExecutor')
    def test_find_files_and_build_payloads(self, mock_executor, mock_logger):
        self.s3_client.get_paginator.return_value.paginate.return_value = [
            {'Contents': [{'Key': 'test/file1.csv'}, {'Key': 'test/file2.csv'}]},
            {'Contents': [{'Key': 'test/file3.csv'}]}
        ]

        mock_executor.return_value.__enter__.return_value.submit.side_effect = [
            MagicMock(result=lambda: ([{'key': 'value1'}, {'key': 'value2'}], [])),
            MagicMock(result=lambda: ([{'key': 'value3'}], []))
        ]

        payloads, failures = find_files_and_build_payloads(
            self.s3_client, 'test/', 'guid', 'name', 'version', 'pipeline', 'file.csv', 'image', 4
        )

        self.assertEqual(len(payloads), 3)
        self.assertEqual(len(failures), 0)

    @patch('your_module.logger')
    @patch('your_module.concurrent.futures.ThreadPoolExecutor')
    def test_process_payloads(self, mock_executor, mock_logger):
        payloads = [{'key1': 'value1'}, {'key2': 'value2'}]

        mock_executor.return_value.__enter__.return_value.submit.side_effect = [
            MagicMock(result=lambda: {'success': True}),
            MagicMock(result=lambda: {'error_message': 'Failed'})
        ]

        successes, failures = process_payloads(self.lambda_client, payloads)

        self.assertEqual(len(successes), 1)
        self.assertEqual(len(failures), 1)

    def test_check_file_exists_true(self):
        self.s3_client.head_object.return_value = {}
        result = check_file_exists(self.s3_client, 'test-bucket', 'test-key')
        self.assertTrue(result)

    def test_check_file_exists_false(self):
        self.s3_client.head_object.side_effect = ClientError(
            {'Error': {'Code': '404', 'Message': 'Not Found'}},
            'HeadObject'
        )
        result = check_file_exists(self.s3_client, 'test-bucket', 'test-key')
        self.assertFalse(result)

if __name__ == '__main__':
    unittest.main()
```
