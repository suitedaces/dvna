```

import json
from unittest.mock import Mock, patch

class TestDatasetVersionsHandler(unittest.TestCase):
    def setUp(self):
        self.mock_s3_client = Mock()

    def test_get_dataset_versions_handler_success(self):
        event = {"queryStringParameters": {"datasetName": "test_dataset"}}
        
        with patch.dict(DATASET_TO_FILENAME_MAP, {
            "test_dataset": {"fileName": "test.csv", "s3Prefix": "test_prefix"}
        }), \
        patch("src.ccbedac_api_orchestrator.handlers.publishing.dataset_versions.group_files_by_columns", return_value={
            frozenset(["col1", "col2"]): [("20230101", "path/to/file1.csv")]
        }):
            result = get_dataset_versions_handler(self.mock_s3_client, event)
        
        self.assertIn('statusCode', result)
        self.assertIn('body', result)
        self.assertEqual(result['statusCode'], 200)
        
        body = result['body']
        self.assertIn('message', body)
        self.assertIn('body', body)
        self.assertIn("Successfully retrieved 1 version(s)", body['message'])
        
        versions = body['body']
        self.assertIn("version1", versions)
        version1 = versions["version1"]
        self.assertIn("columnNames", version1)
        self.assertIn("businessDates", version1)
        self.assertIn("filePaths", version1)
        
        self.assertEqual(set(version1["columnNames"]), {"col1", "col2"})
        self.assertEqual(version1["businessDates"], ["20230101"])
        self.assertEqual(version1["filePaths"], ["path/to/file1.csv"])

    def test_get_dataset_versions_handler_invalid_dataset(self):
        event = {"queryStringParameters": {"datasetName": "invalid_dataset"}}
        
        result = get_dataset_versions_handler(self.mock_s3_client, event)
        
        self.assertEqual(result['statusCode'], 400)
        self.assertIn("Invalid Dataset name", result['body']['message'])

    def test_get_dataset_versions_handler_no_files_found(self):
        event = {"queryStringParameters": {"datasetName": "test_dataset"}}
        
        with patch.dict(DATASET_TO_FILENAME_MAP, {
            "test_dataset": {"fileName": "test.csv", "s3Prefix": "test_prefix"}
        }), \
        patch("src.ccbedac_api_orchestrator.handlers.publishing.dataset_versions.group_files_by_columns", return_value={}):
            result = get_dataset_versions_handler(self.mock_s3_client, event)
        
        self.assertEqual(result['statusCode'], 404)
        self.assertIn("No files found", result['body']['message'])

    def test_get_dataset_versions_handler_unhandled_exception(self):
        event = {"queryStringParameters": {"datasetName": "test_dataset"}}
        
        with patch.dict(DATASET_TO_FILENAME_MAP, {
            "test_dataset": {"fileName": "test.csv", "s3Prefix": "test_prefix"}
        }), \
        patch("src.ccbedac_api_orchestrator.handlers.publishing.dataset_versions.group_files_by_columns", side_effect=Exception("Unhandled error")):
            result = get_dataset_versions_handler(self.mock_s3_client, event)
        
        self.assertEqual(result['statusCode'], 500)
        self.assertEqual(result['body']['message'], "Internal server error")
        self.assertIn("error", result['body'])
        self.assertIn("Exception: Unhandled error", result['body']['error_data'])

    def test_create_return_response(self):
        result = create_return_response(200, "Success", {"data": "test"}, "job123", "No error")
        
        self.assertIn('statusCode', result)
        self.assertIn('body', result)
        self.assertEqual(result['statusCode'], 200)
        self.assertEqual(result['body']['message'], "Success")
        self.assertEqual(result['body']['body'], {"data": "test"})
        self.assertEqual(result['body']['job_id'], "job123")
        self.assertEqual(result['body']['error_data'], "No error")

if __name__ == '__main__':
    unittest.main()

```
