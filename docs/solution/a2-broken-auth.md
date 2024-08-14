```
    @patch('your_module.check_file_exists')
    @patch('your_module.PublishSdkDatasetInput')
    @patch('your_module.concurrent.futures.ThreadPoolExecutor')
    @patch('your_module.logger')
    def test_find_files_and_build_payloads(self, mock_logger, mock_executor, mock_publish_input, mock_check_file):
        # Mock S3 paginator
        mock_paginator = MagicMock(spec=Paginator)
        self.s3_client.get_paginator.return_value = mock_paginator

        # Set up paginator to return two pages of results
        mock_paginator.paginate.return_value = [
            {'Contents': [
                {'Key': 'prefix/file1.csv'},
                {'Key': 'prefix/file2.csv'}
            ]},
            {'Contents': [
                {'Key': 'prefix/file3.csv'},
                {'Key': 'prefix/ignored_file.txt'}
            ]}
        ]

        # Mock check_file_exists to return True for all files
        mock_check_file.return_value = True

        # Mock PublishSdkDatasetInput to return a dict for valid files
        mock_publish_input.return_value.model_dump.return_value = {'mocked': 'payload'}

        # Set up the ThreadPoolExecutor mock
        mock_executor_instance = MagicMock()
        mock_executor.return_value.__enter__.return_value = mock_executor_instance

        # Mock the submit method to directly call our mocked process_page function
        def mock_submit(fn, batch):
            return concurrent.futures.Future()

        mock_executor_instance.submit.side_effect = mock_submit

        # Call the function
        payloads, failures = find_files_and_build_payloads(
            s3=self.s3_client,
            prefix='prefix',
            dataset_guid='guid',
            dataset_name='name',
            dataset_version='version',
            pipeline_id='pipeline',
            file_name='file.csv',
            container_image='image',
            threads=4,
            start_date='2024-01-01',
            end_date='2024-01-31'
        )

        # Assertions
        self.assertEqual(len(payloads), 3)  # We expect 3 valid payloads
        self.assertEqual(len(failures), 0)  # We expect no failures

        # Check if the paginator was called correctly
        self.s3_client.get_paginator.assert_called_once_with('list_objects_v2')
        mock_paginator.paginate.assert_called_once_with(Bucket=LANDING_ZONE_BUCKET, Prefix='prefix')

        # Check if ThreadPoolExecutor was used correctly
        mock_executor.assert_called_once_with(max_workers=MAX_WORKERS)
        self.assertEqual(mock_executor_instance.submit.call_count, 2)  # Called once for each page

        # Check if check_file_exists was called for each valid file
        self.assertEqual(mock_check_file.call_count, 3)

        # Check if PublishSdkDatasetInput was called for each valid file
        self.assertEqual(mock_publish_input.call_count, 3)

        # Check logger calls
        mock_logger.info.assert_called()

```
