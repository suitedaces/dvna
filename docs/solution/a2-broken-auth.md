```
import unittest
from unittest.mock import Mock, patch
import json
from your_module import (
    get_dataset_versions_handler, group_files_by_columns, 
    get_file_columns_s3_select, parse_group_map, is_valid_date_format,
    DATASET_TO_FILENAME_MAP, LANDING_ZONE_BUCKET
)

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
        
        self.assertEqual(result['statusCode'], 200)
        self.assertEqual(result['headers'], {'Content-Type': 'application/json'})
        body = json.loads(result['body'])
        self.assertEqual(body['statusCode'], 200)
        self.assertIn("Successfully retrieved 1 version(s)", body['message'])
        self.assertIsNotNone(body['body'])
        self.assertIsNone(body['jobId'])
        self.assertIsNone(body['errorData'])

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
        self.assertEqual(result['headers'], {'Content-Type': 'application/json'})
        body = json.loads(result['body'])
        self.assertEqual(body['statusCode'], 400)
        self.assertIn("Invalid Dataset name", body['message'])
        self.assertIsNone(body['body'])
        self.assertIsNone(body['jobId'])
        self.assertIsNotNone(body['errorData'])

    def test_get_dataset_versions_handler_no_files_found(self):
        event = {"queryStringParameters": {"datasetName": "test_dataset"}}
        
        with patch.dict(DATASET_TO_FILENAME_MAP, {
            "test_dataset": {"fileName": "test.csv", "s3Prefix": "test_prefix"}
        }), \
        patch("src.ccbedac_api_orchestrator.handlers.publishing.dataset_versions.group_files_by_columns", return_value={}):
            result = get_dataset_versions_handler(self.mock_s3_client, event)
        
        self.assertEqual(result['statusCode'], 404)
        self.assertEqual(result['headers'], {'Content-Type': 'application/json'})
        body = json.loads(result['body'])
        self.assertEqual(body['statusCode'], 404)
        self.assertEqual(body['message'], "No files found in test_prefix for test_dataset, please check logs for details")
        self.assertIsNone(body['body'])
        self.assertIsNone(body['jobId'])
        self.assertIsNone(body['errorData'])

    def test_get_dataset_versions_handler_unhandled_exception(self):
        event = {"queryStringParameters": {"datasetName": "test_dataset"}}
        
        with patch.dict(DATASET_TO_FILENAME_MAP, {
            "test_dataset": {"fileName": "test.csv", "s3Prefix": "test_prefix"}
        }), \
        patch("src.ccbedac_api_orchestrator.handlers.publishing.dataset_versions.group_files_by_columns", side_effect=Exception("Unhandled error")):
            result = get_dataset_versions_handler(self.mock_s3_client, event)
        
        self.assertEqual(result['statusCode'], 500)
        self.assertEqual(result['headers'], {'Content-Type': 'application/json'})
        body = json.loads(result['body'])
        self.assertEqual(body['statusCode'], 500)
        self.assertEqual(body['message'], "Internal server error")
        self.assertIsNone(body['body'])
        self.assertIsNone(body['jobId'])
        self.assertIsNotNone(body['errorData'])

    def test_group_files_by_columns(self):
        self.mock_s3_client.get_paginator.return_value.paginate.return_value = [
            {
                "CommonPrefixes": [
                    {"Prefix": "test_prefix/20230101/"},
                    {"Prefix": "test_prefix/20230102/"}
                ]
            }
        ]
        self.mock_s3_client.list_objects_v2.side_effect = [
            {
                "Contents": [
                    {"Key": "test_prefix/20230101/test.csv"},
                ]
            },
            {
                "Contents": [
                    {"Key": "test_prefix/20230102/test.csv"}
                ]
            }
        ]
        
        with patch("your_module.get_file_columns_s3_select") as mock_get_columns:
            mock_get_columns.side_effect = [
                frozenset(["col1", "col2"]),
                frozenset(["col1", "col2", "col3"])
            ]
            
            result = group_files_by_columns(self.mock_s3_client, LANDING_ZONE_BUCKET, "test_prefix", "test.csv")
        
        self.assertEqual(len(result), 2)
        self.assertIn(frozenset(["col1", "col2"]), result)
        self.assertIn(frozenset(["col1", "col2", "col3"]), result)

    def test_get_file_columns_s3_select_csv(self):
        self.mock_s3_client.select_object_content.return_value = {
            "Payload": [
                {
                    "Records": {
                        "Payload": b"col1,col2\n"
                    }
                }
            ]
        }
        
        result = get_file_columns_s3_select(self.mock_s3_client, "test-bucket", "test.csv")
        
        self.assertEqual(result, frozenset(["col1", "col2"]))

    def test_get_file_columns_s3_select_gzip(self):
        self.mock_s3_client.select_object_content.return_value = {
            "Payload": [
                {
                    "Records": {
                        "Payload": b"col1,col2,col3\n"
                    }
                }
            ]
        }
        
        result = get_file_columns_s3_select(self.mock_s3_client, "test-bucket", "test.csv.gz")
        
        self.assertEqual(result, frozenset(["col1", "col2", "col3"]))
        self.mock_s3_client.select_object_content.assert_called_with(
            Bucket="test-bucket",
            Key="test.csv.gz",
            ExpressionType='SQL',
            Expression="SELECT * FROM s3object LIMIT 1",
            InputSerialization={"CSV": {"FileHeaderInfo": "NONE"}, "CompressionType": "GZIP"},
            OutputSerialization={"CSV": {}}
        )

    def test_parse_group_map(self):
        input_map = {
            frozenset(["col1", "col2"]): [("20230101", "path/to/file1.csv"), ("20230102", "path/to/file2.csv")],
            frozenset(["col1", "col2", "col3"]): [("20230103", "path/to/file3.csv")]
        }
        
        result = parse_group_map(input_map)
        
        self.assertEqual(len(result), 2)
        self.assertIn("version1", result)
        self.assertIn("version2", result)
        self.assertEqual(set(result["version1"]["columnNames"]), {"col1", "col2"})
        self.assertEqual(result["version1"]["businessDates"], ["20230101", "20230102"])
        self.assertEqual(result["version1"]["filePaths"], ["path/to/file1.csv", "path/to/file2.csv"])
        self.assertEqual(set(result["version2"]["columnNames"]), {"col1", "col2", "col3"})
        self.assertEqual(result["version2"]["businessDates"], ["20230103"])
        self.assertEqual(result["version2"]["filePaths"], ["path/to/file3.csv"])

    def test_is_valid_date_format(self):
        self.assertTrue(is_valid_date_format("20230101"))
        self.assertTrue(is_valid_date_format("20231231"))
        self.assertFalse(is_valid_date_format("20231301"))  # Invalid month
        self.assertFalse(is_valid_date_format("20230132"))  # Invalid day
        self.assertFalse(is_valid_date_format("not-a-date"))

if __name__ == '__main__':
    unittest.main()


```
