```

import unittest
from unittest.mock import patch, MagicMock
from start_jobs import (
    publish_multi_file_handler,
    find_files_and_build_payloads,
    process_payloads,
    MAX_RETRIES,
)

class TestStartJobs(unittest.TestCase):

    @patch('start_jobs.boto3.client')
    @patch('start_jobs.parse_publish_dataset_request')
    def test_publish_multi_file_handler_success(self, mock_parse_request, mock_boto_client):
        mock_s3_client = MagicMock()
        mock_lambda_client = MagicMock()
        mock_boto_client.side_effect = [mock_s3_client, mock_lambda_client]

        mock_event = {"key": "value"}
        mock_request = {
            "datasetName": "test-dataset",
            "datasetVersion": "v1",
            "pipelineId": "pipeline123",
            "s3Prefix": "prefix/",
            "datasetGuid": "guid123",
            "fileName": "file.txt",
            "containerImageName": "container-image",
            "threads": 4,
            "startDate": "2024-01-01",
            "endDate": "2024-01-31",
        }
        mock_parse_request.return_value.model_dump.return_value = mock_request

        with patch('start_jobs.find_files_and_build_payloads', return_value=(["payload1"], [])):
            with patch('start_jobs.process_payloads', return_value=(["success"], [])):
                response = publish_multi_file_handler(mock_s3_client, mock_lambda_client, mock_event)
                self.assertEqual(response['statusCode'], 200)
                self.assertIn("Found 1 + 0 files", response['body'])

    @patch('start_jobs.boto3.client')
    @patch('start_jobs.find_files_and_build_payloads')
    def test_find_files_and_build_payloads_no_files(self, mock_find_files, mock_boto_client):
        mock_s3_client = MagicMock()
        mock_find_files.return_value = ([], [])
        payloads, failures = find_files_and_build_payloads(
            s3=mock_s3_client,
            prefix="prefix/",
            dataset_guid="guid123",
            dataset_name="test-dataset",
            dataset_version="v1",
            pipeline_id="pipeline123",
            file_name="file.txt",
            container_image="container-image",
            threads=4,
        )
        self.assertEqual(payloads, [])
        self.assertEqual(failures, [])

    @patch('start_jobs.boto3.client')
    def test_process_payloads_success(self, mock_boto_client):
        mock_lambda_client = MagicMock()
        payload = {"datasetName": "test-dataset", "businessDate": "2024-01-01", "baseDataFilePath": "path/"}
        mock_lambda_client.invoke.return_value = {"StatusCode": 202, "ResponseMetadata": {"RequestId": "req-123"}}

        successes, failures = process_payloads(mock_lambda_client, [payload])
        self.assertEqual(len(successes), 1)
        self.assertEqual(len(failures), 0)

    @patch('start_jobs.boto3.client')
    def test_process_payloads_failure(self, mock_boto_client):
        mock_lambda_client = MagicMock()
        payload = {"datasetName": "test-dataset", "businessDate": "2024-01-01", "baseDataFilePath": "path/"}
        mock_lambda_client.invoke.return_value = {"StatusCode": 500}

        successes, failures = process_payloads(mock_lambda_client, [payload])
        self.assertEqual(len(successes), 0)
        self.assertEqual(len(failures), 1)

    @patch('start_jobs.boto3.client')
    def test_process_payloads_exception(self, mock_boto_client):
        mock_lambda_client = MagicMock()
        payload = {"datasetName": "test-dataset", "businessDate": "2024-01-01", "baseDataFilePath": "path/"}
        mock_lambda_client.invoke.side_effect = Exception("Lambda error")

        successes, failures = process_payloads(mock_lambda_client, [payload])
        self.assertEqual(len(successes), 0)
        self.assertEqual(len(failures), 1)
        self.assertIn("Unhandled Error while invoking lambda", failures[0]['reason'])

    @patch('start_jobs.boto3.client')
    def test_process_payloads_max_retries(self, mock_boto_client):
        mock_lambda_client = MagicMock()
        payload = {"datasetName": "test-dataset", "businessDate": "2024-01-01", "baseDataFilePath": "path/"}
        mock_lambda_client.invoke.side_effect = Exception("Lambda error")

        with patch('start_jobs.MAX_RETRIES', 2):
            successes, failures = process_payloads(mock_lambda_client, [payload])
            self.assertEqual(len(successes), 0)
            self.assertEqual(len(failures), 1)
            self.assertIn("Unhandled Error while invoking lambda", failures[0]['reason'])

if __name__ == '__main__':
    unittest.main()

```
