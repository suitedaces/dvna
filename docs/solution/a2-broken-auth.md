```
import unittest
from unittest.mock import patch, MagicMock
from typing import Dict, Any, List
import json

# Import the functions you want to test
from your_module import (
    publish_multi_file_handler,
    find_files_and_build_payloads,
    process_page,
    process_payloads,
    check_file_exists,
    invoke_lambda
)

class TestDataProcessingPipeline(unittest.TestCase):

    @patch('your_module.boto3.client')
    @patch('your_module.logger')
    def test_publish_multi_file_handler(self, mock_logger, mock_boto3_client):
        mock_event = {
            "pipelineId": "test_pipeline",
            "s3Prefix": "test_prefix",
            "datasetGuid": "test_guid",
            "filename": "test_file.csv",
            "containerImageName": "test_image",
            "threads": 4,
            "startDate": "2024-01-01",
            "endDate": "2024-01-31"
        }
        mock_context = MagicMock()

        mock_boto3_client.return_value.invoke.return_value = {
            'StatusCode': 200,
            'Payload': MagicMock()
        }

        result = publish_multi_file_handler(mock_event, mock_context)

        self.assertEqual(result['statusCode'], 200)
        mock_logger.info.assert_called_with(f"Received event: {json.dumps(mock_event)}")
        mock_boto3_client.assert_called_with('lambda')

    @patch('your_module.boto3.client')
    @patch('your_module.logger')
    def test_find_files_and_build_payloads(self, mock_logger, mock_boto3_client):
        s3 = mock_boto3_client.return_value
        prefix = "test_prefix"
        dataset_guid = "test_guid"
        dataset_name = "test_dataset"
        dataset_version = "1.0"
        pipeline_id = "test_pipeline"
        file_name = "test_file.csv"
        container_image = "test_image"
        threads = 4
        start_date = "2024-01-01"
        end_date = "2024-01-31"

        s3.list_objects_v2.return_value = {
            'Contents': [{'Key': 'test_prefix/test_file.csv'}]
        }

        payloads, failures = find_files_and_build_payloads(
            s3, prefix, dataset_guid, dataset_name, dataset_version,
            pipeline_id, file_name, container_image, threads,
            start_date, end_date
        )

        self.assertEqual(len(payloads), 1)
        self.assertEqual(len(failures), 0)
        self.assertEqual(payloads[0]['filename'], 'test_file.csv')
        mock_logger.info.assert_called_with("Building payloads for request: %s", str({
            'prefix': prefix,
            'dataset_guid': dataset_guid,
            'dataset_name': dataset_name,
            'dataset_version': dataset_version,
            'pipeline_id': pipeline_id,
            'file_name': file_name,
            'container_image': container_image,
            'threads': threads,
            'start_date': start_date,
            'end_date': end_date
        }))

    @patch('your_module.process_object')
    def test_process_page(self, mock_process_object):
        batch = [{'Key': 'test_key1'}, {'Key': 'test_key2'}]
        mock_process_object.side_effect = [{'result': 'success'}, None]

        result = process_page(batch)

        self.assertEqual(len(result), 2)
        self.assertEqual(result[0], {'Key': 'test_key1', 'result': 'success'})
        self.assertIsNone(result[1])
        mock_process_object.assert_called_with({'Key': 'test_key2'})

    @patch('your_module.boto3.client')
    def test_check_file_exists(self, mock_boto3_client):
        s3 = mock_boto3_client.return_value
        bucket = "test-bucket"
        key = "test-key"

        s3.head_object.return_value = {}
        self.assertTrue(check_file_exists(s3, bucket, key))

        s3.head_object.side_effect = Exception("Not Found")
        self.assertFalse(check_file_exists(s3, bucket, key))

    @patch('your_module.concurrent.futures.ThreadPoolExecutor')
    @patch('your_module.logger')
    def test_process_payloads(self, mock_logger, mock_executor):
        mock_lambda_client = MagicMock()
        payloads = [{'key': 'value1'}, {'key': 'value2'}]

        mock_executor.return_value.__enter__.return_value.submit.side_effect = [
            MagicMock(result=lambda: {'success': True}),
            MagicMock(result=lambda: {'success': False, 'error': 'Some error'})
        ]

        successes, failures = process_payloads(mock_lambda_client, payloads)

        self.assertEqual(len(successes), 1)
        self.assertEqual(len(failures), 1)
        mock_logger.info.assert_called_with("Invoking lambda with payloads now...")

    @patch('your_module.boto3.client')
    @patch('your_module.logger')
    def test_publish_multi_file_handler_error_handling(self, mock_logger, mock_boto3_client):
        mock_event = {
            "pipelineId": "test_pipeline",
            # Missing required fields
        }
        mock_context = MagicMock()

        with self.assertRaises(ValueError):
            publish_multi_file_handler(mock_event, mock_context)

        mock_logger.info.assert_called_with("Building payloads for request: %s", str(mock_event))

    @patch('your_module.boto3.client')
    def test_find_files_and_build_payloads_no_files(self, mock_boto3_client):
        s3 = mock_boto3_client.return_value
        s3.list_objects_v2.return_value = {'Contents': []}

        payloads, failures = find_files_and_build_payloads(
            s3, "prefix", "guid", "name", "version",
            "pipeline", "file.csv", "image", 4,
            "2024-01-01", "2024-01-31"
        )

        self.assertEqual(len(payloads), 0)
        self.assertEqual(len(failures), 0)

    @patch('your_module.process_object')
    def test_process_page_all_failures(self, mock_process_object):
        mock_process_object.return_value = None
        batch = [{'Key': 'test_key1'}, {'Key': 'test_key2'}]

        result = process_page(batch)

        self.assertEqual(len(result), 2)
        self.assertIsNone(result[0])
        self.assertIsNone(result[1])

    @patch('your_module.boto3.client')
    @patch('your_module.logger')
    def test_invoke_lambda(self, mock_logger, mock_boto3_client):
        mock_lambda_client = mock_boto3_client.return_value
        payload: Dict[str, Any] = {"key": "value"}

        # Test successful invocation
        mock_lambda_client.invoke.return_value = {
            'StatusCode': 200,
            'Payload': MagicMock(read=lambda: json.dumps({"status": "success"}).encode())
        }

        result = invoke_lambda(mock_lambda_client, payload)
        self.assertTrue(result['success'])

        # Test failed invocation
        mock_lambda_client.invoke.return_value = {
            'StatusCode': 400,
            'Payload': MagicMock(read=lambda: json.dumps({"error": "Some error"}).encode())
        }

        result = invoke_lambda(mock_lambda_client, payload)
        self.assertFalse(result['success'])
        self.assertEqual(result['error'], "Some error")

        # Test exception handling
        mock_lambda_client.invoke.side_effect = Exception("Lambda invocation failed")

        result = invoke_lambda(mock_lambda_client, payload)
        self.assertFalse(result['success'])
        self.assertIn("Lambda invocation failed", result['error'])

if __name__ == '__main__':
    unittest.main()
```
