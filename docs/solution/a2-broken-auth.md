```
def test_get_file_columns_s3_select_gzip(self):
    mock_response = {
        "Payload": [
            {
                "Records": {
                    "Payload": b"col1,col2,col3\n"
                }
            }
        ]
    }
    self.mock_s3_client.select_object_content.return_value = mock_response
    
    result = get_file_columns_s3_select(self.mock_s3_client, "test-bucket", "test.csv.gz")
    
    self.assertEqual(result, frozenset(["col1", "col2", "col3"]))
    self.mock_s3_client.select_object_content.assert_called_once_with(
        Bucket="test-bucket",
        Key='test.csv.gz',
        ExpressionType='SQL',
        Expression='SELECT * FROM s3object LIMIT 1',
        InputSerialization={'CSV': {'FileHeaderInfo': 'NONE'}, 'CompressionType': 'GZIP'},
        OutputSerialization={'CSV': {}}
    )

```
