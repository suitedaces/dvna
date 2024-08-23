```
import json

def test_get_dataset_versions_handler_success(self):
    event = {"queryStringParameters": {"datasetName": "test_dataset"}}
    
    with patch.dict(DATASET_TO_FILENAME_MAP, {
        "test_dataset": {"fileName": "test.csv", "s3Prefix": "test_prefix"}
    }), \
    patch("src.ccbedac_api_orchestrator.handlers.publishing.dataset_versions.group_files_by_columns", return_value={
        frozenset(["col1", "col2"]): [("20230101", "path/to/file1.csv")]
    }):
        result = get_dataset_versions_handler(self.mock_s3_client, event)
    
    self.assertEqual(result["statusCode"], 200)
    
    # Parse the JSON-encoded body
    body = json.loads(result["body"])
    
    self.assertIn("Successfully retrieved 1 version(s)", body["message"])
    self.assertIn("version1", body["body"])
    
    # Additional checks on the response structure
    self.assertIsInstance(body["body"], dict)
    self.assertIn("version1", body["body"])
    self.assertIn("columnNames", body["body"]["version1"])
    self.assertIn("businessDates", body["body"]["version1"])
    self.assertIn("filePaths", body["body"]["version1"])
    
    # Check the content of version1
    version1 = body["body"]["version1"]
    self.assertEqual(set(version1["columnNames"]), {"col1", "col2"})
    self.assertEqual(version1["businessDates"], ["20230101"])
    self.assertEqual(version1["filePaths"], ["path/to/file1.csv"])
```
