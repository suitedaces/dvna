```
import os
import boto3
import aiohttp
import asyncio
import logging
import json
import uuid
from datetime import datetime

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Configuration
API_ENDPOINT = os.environ['API_ENDPOINT']
ADDITIONAL_PARAMS = {
    'param1': 'value1',
    'param2': 'value2'
}
AWS_REGION = os.environ['AWS_REGION']

# Initialize S3 client
s3 = boto3.client('s3', region_name=AWS_REGION)

def is_valid_date_format(date_string):
    try:
        datetime.strptime(date_string, '%Y%m%d')
        return True
    except ValueError:
        return False

def is_date_in_range(date_string, start_date, end_date):
    if not start_date or not end_date:
        return True
    return start_date <= date_string <= end_date

def find_files_in_date_folders(bucket, base_path, file_names, start_date=None, end_date=None):
    matching_files = []
    paginator = s3.get_paginator('list_objects_v2')
    
    for page in paginator.paginate(Bucket=bucket, Prefix=base_path):
        for obj in page.get('Contents', []):
            key = obj['Key']
            parts = key.split('/')
            if len(parts) >= 3:
                potential_date = parts[-2]
                file_name = parts[-1]
                if is_valid_date_format(potential_date) and file_name in file_names:
                    if is_date_in_range(potential_date, start_date, end_date):
                        matching_files.append((key, potential_date))
            
            # Check if we've found all the files we're looking for
            if len(matching_files) == len(file_names):
                return matching_files
    
    return matching_files

async def process_files(session, bucket, files_info, dataset_name, pipeline_id):
    tasks = []
    for file_info in files_info:
        task = asyncio.create_task(call_api(session, bucket, file_info, dataset_name, pipeline_id))
        tasks.append(task)
    
    results = await asyncio.gather(*tasks, return_exceptions=True)
    return results

async def call_api(session, bucket, file_info, dataset_name, pipeline_id):
    key, business_date = file_info
    try:
        # Prepare payload
        payload = {
            'date': business_date,
            'file_path': f"s3://{bucket}/{key}",
            'dataset_name': dataset_name,
            'pipeline_id': pipeline_id,
            **ADDITIONAL_PARAMS
        }

        # Make API call
        async with session.post(API_ENDPOINT, json=payload) as response:
            if response.status == 200:
                response_json = await response.json()
                provenance_guid = response_json.get('provenance_guid')
                
                if provenance_guid:
                    # Write successful response to S3
                    s3_key = f"jobs/placement/{dataset_name}/{business_date}/{provenance_guid}.json"
                    s3.put_object(
                        Bucket=bucket,
                        Key=s3_key,
                        Body=json.dumps(response_json),
                        ContentType='application/json'
                    )
                    logger.info(f"Successfully processed file: {key}. Response written to {s3_key}")
                else:
                    raise ValueError("Response does not contain provenance_guid")
            else:
                raise Exception(f"API call failed with status: {response.status}")

    except Exception as e:
        error_id = str(uuid.uuid4())
        error_key = f"jobs/placement/{dataset_name}/{business_date}/error/{error_id}.json"
        error_content = {
            'file_path': key,
            'error_message': str(e),
            'timestamp': datetime.utcnow().isoformat()
        }
        s3.put_object(
            Bucket=bucket,
            Key=error_key,
            Body=json.dumps(error_content),
            ContentType='application/json'
        )
        logger.error(f"Error processing file {key}: {str(e)}. Error details written to {error_key}")
        return {'error': str(e), 'file': key}

def lambda_handler(event, context):
    try:
        # Extract input parameters
        bucket = event['bucket']
        base_path = event['base_path']  # This should be 'vendor/<dataset_name>'
        dataset_name = event['dataset_name']
        file_names = event['file_names'].split(';')
        pipeline_id = event['pipeline_id']
        
        # Extract optional date range
        start_date = event.get('start_date')
        end_date = event.get('end_date')
        
        if start_date and end_date:
            if not is_valid_date_format(start_date) or not is_valid_date_format(end_date):
                return {'statusCode': 400, 'body': 'Invalid date format. Use YYYYMMDD.'}
            if start_date > end_date:
                return {'statusCode': 400, 'body': 'Start date must be before or equal to end date.'}

        matching_files = find_files_in_date_folders(bucket, base_path, file_names, start_date, end_date)

        if not matching_files:
            logger.info("No matching files found.")
            return {'statusCode': 200, 'body': 'No matching files found.'}

        # If we have more files than we can process in one go, we might want to split the work
        if len(matching_files) > 100:  # Arbitrary threshold, adjust as needed
            # Process first 100 files and trigger another Lambda for the rest
            files_to_process = matching_files[:100]
            remaining_files = matching_files[100:]
            
            # Trigger another Lambda invocation for remaining files
            lambda_client = boto3.client('lambda')
            lambda_client.invoke(
                FunctionName=context.function_name,
                InvocationType='Event',
                Payload=json.dumps({
                    **event,
                    'remaining_files': remaining_files
                })
            )
        else:
            files_to_process = matching_files

        # Process files asynchronously
        async def main():
            async with aiohttp.ClientSession() as session:
                results = await process_files(session, bucket, files_to_process, dataset_name, pipeline_id)
            return results

        results = asyncio.run(main())

        # Summarize results
        successful = sum(1 for r in results if not isinstance(r, dict) or 'error' not in r)
        failed = len(results) - successful

        return {
            'statusCode': 200,
            'body': f'Processed {len(files_to_process)} files. Successful: {successful}, Failed: {failed}'
        }

    except Exception as e:
        logger.error(f"Error in lambda_handler: {str(e)}")
        return {
            'statusCode': 500,
            'body': f'Error: {str(e)}'
        }
```


