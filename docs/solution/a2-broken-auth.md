```
    @patch('your_module.concurrent.futures.ThreadPoolExecutor')
    @patch('your_module.logger')
    def test_process_payloads(self, mock_logger, mock_executor):
        # Prepare test payloads
        payloads = [
            {"datasetName": "dataset1", "businessDate": "2024-01-01", "baseDataFilePath": "path1"},
            {"datasetName": "dataset2", "businessDate": "2024-01-02", "baseDataFilePath": "path2"},
            {"datasetName": "dataset3", "businessDate": "2024-01-03", "baseDataFilePath": "path3"}
        ]

        # Mock Lambda invoke responses
        def mock_lambda_invoke(FunctionName, InvocationType, Payload):
            payload_dict = json.loads(Payload)
            if payload_dict['datasetName'] == 'dataset2':
                return {'StatusCode': 400}  # Simulate a failed invocation
            return {'StatusCode': 202}  # Successful invocation

        self.lambda_client.invoke.side_effect = mock_lambda_invoke

        # Mock ThreadPoolExecutor
        mock_executor_instance = MagicMock()
        mock_executor.return_value.__enter__.return_value = mock_executor_instance

        # Mock the submit method to directly call our mocked invoke_lambda function
        def mock_submit(fn, payload):
            future = Future()
            if payload['datasetName'] == 'dataset2':
                future.set_result(PublishDatasetFailure(
                    dataset_name=payload['datasetName'],
                    business_date=payload['businessDate'],
                    data_filepath=payload['baseDataFilePath'],
                    reason="Lambda invocation failed"
                ).model_dump(by_alias=True))
            else:
                future.set_result(PublishDatasetSuccess(
                    dataset_name=payload['datasetName'],
                    business_date=payload['businessDate'],
                    data_filepath=payload['baseDataFilePath']
                ).model_dump(by_alias=True))
            return future

        mock_executor_instance.submit.side_effect = mock_submit

        # Call the function
        successes, failures = process_payloads(self.lambda_client, payloads)

        # Assertions
        self.assertEqual(len(successes), 2)
        self.assertEqual(len(failures), 1)

        # Check if ThreadPoolExecutor was used correctly
        mock_executor.assert_called_once_with(max_workers=MAX_WORKERS)
        self.assertEqual(mock_executor_instance.submit.call_count, 3)  # Called once for each payload

        # Check the content of successes and failures
        self.assertEqual(successes[0]['dataset_name'], 'dataset1')
        self.assertEqual(successes[1]['dataset_name'], 'dataset3')
        self.assertEqual(failures[0]['dataset_name'], 'dataset2')
        self.assertIn('reason', failures[0])

        # Check if Lambda client's invoke method was called for each payload
        self.assertEqual(self.lambda_client.invoke.call_count, 3)

        # Check logger calls
        mock_logger.info.assert_called()
        mock_logger.warning.assert_called()  # For the failed invocation

    @patch('your_module.concurrent.futures.ThreadPoolExecutor')
    @patch('your_module.logger')
    def test_process_payloads_with_retries(self, mock_logger, mock_executor):
        payloads = [
            {"datasetName": "dataset1", "businessDate": "2024-01-01", "baseDataFilePath": "path1"}
        ]

        # Mock Lambda invoke to fail MAX_RETRIES times
        self.lambda_client.invoke.side_effect = [
            {'StatusCode': 400}
        ] * MAX_RETRIES

        # Mock ThreadPoolExecutor
        mock_executor_instance = MagicMock()
        mock_executor.return_value.__enter__.return_value = mock_executor_instance

        # Mock the submit method to directly call our mocked invoke_lambda function
        def mock_submit(fn, payload):
            future = Future()
            future.set_result(PublishDatasetFailure(
                dataset_name=payload['datasetName'],
                business_date=payload['businessDate'],
                data_filepath=payload['baseDataFilePath'],
                reason="Unhandled Error while invoking lambda"
            ).model_dump(by_alias=True))
            return future

        mock_executor_instance.submit.side_effect = mock_submit

        # Call the function
        successes, failures = process_payloads(self.lambda_client, payloads)

        # Assertions
        self.assertEqual(len(successes), 0)
        self.assertEqual(len(failures), 1)

        # Check if Lambda client's invoke method was called MAX_RETRIES times
        self.assertEqual(self.lambda_client.invoke.call_count, MAX_RETRIES)

        # Check logger calls
        mock_logger.warning.assert_called()
        mock_logger.error.assert_called()
```
