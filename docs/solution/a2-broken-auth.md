```
import unittest
from unittest.mock import Mock, patch, call
from io import StringIO
from your_module import (
    get_dataset_versions_handler, group_files_by_columns, 
    get_file_columns_s3_select, parse_group_map, is_valid_date_format,
    DATASET_TO_FILENAME_MAP, LANDING_ZONE_BUCKET, create_return_response
)

class TestDatasetVersionsHandler(unittest.TestCase):
    def setUp(self):
        self.mock_s3_client = Mock()

    def test_get_dataset_versions_handler_success(self):
        event = {"queryStringParameters": {"datasetName": "test_dataset"}}
        
        with patch.dict(DATASET_TO_FILENAME_MAP, {
            "test_dataset": {"fileName": "test.csv", "s3Prefix": "test_prefix"}
        }), \
        patch("your_module.group_files_by_columns", return_value={
            frozenset(["col1", "col2"]): [("2023-01-01", "path/to/file1.csv")]
        }):
            result = get_dataset_versions_handler(self.mock_s3_client, event)
        
        self.assertEqual(result["statusCode"], 200)
        self.assertIn("Successfully retrieved 1 version(s)", result["body"]["message"])
        self.assertIn("version1", result["body"]["body"])

    def test_get_dataset_versions_handler_invalid_dataset(self):
        event = {"queryStringParameters": {"datasetName": "invalid_dataset"}}
        
        result = get_dataset_versions_handler(self.mock_s3_client, event)
        
        self.assertEqual(result["statusCode"], 400)
        self.assertIn("Invalid Dataset name", result["body"]["message"])

    def test_get_dataset_versions_handler_no_files_found(self):
        event = {"queryStringParameters": {"datasetName": "test_dataset"}}
        
        with patch.dict(DATASET_TO_FILENAME_MAP, {
            "test_dataset": {"fileName": "test.csv", "s3Prefix": "test_prefix"}
        }), \
        patch("your_module.group_files_by_columns", return_value={}):
            result = get_dataset_versions_handler(self.mock_s3_client, event)
        
        self.assertEqual(result["statusCode"], 404)
        self.assertIn("No files found", result["body"]["message"])

    def test_get_dataset_versions_handler_unhandled_exception(self):
        event = {"queryStringParameters": {"datasetName": "test_dataset"}}
        
        with patch.dict(DATASET_TO_FILENAME_MAP, {
            "test_dataset": {"fileName": "test.csv", "s3Prefix": "test_prefix"}
        }), \
        patch("your_module.group_files_by_columns", side_effect=Exception("Unhandled error")):
            result = get_dataset_versions_handler(self.mock_s3_client, event)
        
        self.assertEqual(result["statusCode"], 500)
        self.assertEqual(result["body"]["message"], "Internal server error")

    def test_group_files_by_columns(self):
        self.mock_s3_client.get_paginator.return_value.paginate.return_value = [
            {
                "CommonPrefixes": [
                    {"Prefix": "test_prefix/2023-01-01/"},
                    {"Prefix": "test_prefix/invalid_date/"},
                    {"Prefix": "test_prefix/2023-01-02/"}
                ]
            }
        ]
        self.mock_s3_client.list_objects_v2.side_effect = [
            {
                "Contents": [
                    {"Key": "test_prefix/2023-01-01/test.csv"},
                    {"Key": "test_prefix/2023-01-01/other.csv"}
                ]
            },
            {},  # No contents for invalid_date
            {
                "Contents": [
                    {"Key": "test_prefix/2023-01-02/test.csv"}
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

    def test_group_files_by_columns_error_handling(self):
        self.mock_s3_client.get_paginator.return_value.paginate.return_value = [
            {
                "CommonPrefixes": [
                    {"Prefix": "test_prefix/2023-01-01/"}
                ]
            }
        ]
        self.mock_s3_client.list_objects_v2.return_value = {
            "Contents": [
                {"Key": "test_prefix/2023-01-01/test.csv"}
            ]
        }
        
        with patch("your_module.get_file_columns_s3_select", side_effect=Exception("Error processing file")):
            result = group_files_by_columns(self.mock_s3_client, LANDING_ZONE_BUCKET, "test_prefix", "test.csv")
        
        self.assertEqual(len(result), 0)

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
            Expression="SELECT * FROM s3object LIMIT 1",
            InputSerialization={"CSV": {"FileHeaderInfo": "NONE"}, "CompressionType": "GZIP"},
            OutputSerialization={"CSV": {}}
        )

    def test_get_file_columns_s3_select_no_records(self):
        self.mock_s3_client.select_object_content.return_value = {
            "Payload": []
        }
        
        result = get_file_columns_s3_select(self.mock_s3_client, "test-bucket", "test.csv")
        
        self.assertEqual(result, frozenset())

    def test_get_file_columns_s3_select_error(self):
        self.mock_s3_client.select_object_content.side_effect = Exception("S3 Select error")
        
        result = get_file_columns_s3_select(self.mock_s3_client, "test-bucket", "test.csv")
        
        self.assertEqual(result, frozenset())

    def test_parse_group_map(self):
        input_map = {
            frozenset(["col1", "col2"]): [("2023-01-01", "path/to/file1.csv"), ("2023-01-02", "path/to/file2.csv")],
            frozenset(["col1", "col2", "col3"]): [("2023-01-03", "path/to/file3.csv")]
        }
        
        result = parse_group_map(input_map)
        
        self.assertEqual(len(result), 2)
        self.assertIn("version1", result)
        self.assertIn("version2", result)
        self.assertEqual(result["version1"]["columnNames"], ["col1", "col2"])
        self.assertEqual(result["version1"]["businessDates"], ["2023-01-01", "2023-01-02"])
        self.assertEqual(result["version1"]["filePaths"], ["path/to/file1.csv", "path/to/file2.csv"])
        self.assertEqual(result["version2"]["columnNames"], ["col1", "col2", "col3"])
        self.assertEqual(result["version2"]["businessDates"], ["2023-01-03"])
        self.assertEqual(result["version2"]["filePaths"], ["path/to/file3.csv"])

    def test_is_valid_date_format(self):
        self.assertTrue(is_valid_date_format("2023-01-01"))
        self.assertTrue(is_valid_date_format("2023-12-31"))
        self.assertFalse(is_valid_date_format("2023-13-01"))
        self.assertFalse(is_valid_date_format("2023-01-32"))
        self.assertFalse(is_valid_date_format("not-a-date"))

    def test_create_return_response(self):
        response = create_return_response(200, "Success", {"key": "value"})
        self.assertEqual(response["statusCode"], 200)
        self.assertEqual(response["body"]["message"], "Success")
        self.assertEqual(response["body"]["body"], {"key": "value"})

        error_response = create_return_response(400, "Bad Request", error_data="Invalid input")
        self.assertEqual(error_response["statusCode"], 400)
        self.assertEqual(error_response["body"]["message"], "Bad Request")
        self.assertEqual(error_response["body"]["error_data"], "Invalid input")

if __name__ == '__main__':
    unittest.main()

```
