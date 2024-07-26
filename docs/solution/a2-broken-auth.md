```

import os
import boto3
import aiohttp
import asyncio
import logging
import json
import uuid
from datetime import datetime
from botocore.exceptions import ClientError

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Configuration
S3_BUCKET_NAME = os.environ['S3_BUCKET_NAME']
API_ENDPOINT = os.environ['API_ENDPOINT']
AWS_REGION = os.environ['AWS_REGION']
ASSUME_ROLE = os.environ['ASSUME_ROLE']
KMS_KEY_ARN = os.environ['KMS_KEY_ARN']

# Initialize S3 client
s3 = boto3.client('s3', region_name=AWS_REGION)

# Predefined list of allowed dataset names
ALLOWED_DATASET_NAMES = [
    "customer_data",
    "sales_transactions",
    "inventory_levels",
    "marketing_campaigns",
    "product_catalog"
]

def create_return_response(status_code, message, error_data=None):
    response = {
        'statusCode': status_code,
        'message': message
    }
    if error_data:
        response['error_data'] = error_data
    return response

def is_valid_date_format(date_string):
    try:
        datetime.strptime(date_string, '%Y%m%d')
        return True
    except ValueError:
        return False

async def find_files_and_build_payloads(base_path, dataset_name, dataset_version, pipeline_id):
    payloads = []
    paginator = s3.get_paginator('list_objects_v2')
    
    try:
        for page in paginator.paginate(Bucket=S3_BUCKET_NAME, Prefix=base_path):
            for obj in page.get('Contents', []):
                key = obj['Key']
                if key.endswith('.csv'):
                    parts = key.split('/')
                    if len(parts) < 2:
                        continue
                    
                    business_date = parts[-2]
                    if not is_valid_date_format(business_date):
                        logger.warning(f"Invalid business date format in folder: {business_date}")
                        continue

                    csv_s3_path = f"s3://{S3_BUCKET_NAME}/{key}"
                    tok_key = key.rsplit('.', 1)[0] + '.tok'
                    tok_s3_path = f"s3://{S3_BUCKET_NAME}/{tok_key}"

                    payload = {
                        'businessdate': business_date,
                        'orderdate': business_date,
                        'datasetversion': dataset_version,
                        'pipelineid': pipeline_id,
                        'datafilename': csv_s3_path,
                        'region': AWS_REGION,
                        'assumerole': ASSUME_ROLE,
                        'kmskeyarn': KMS_KEY_ARN
                    }

                    if await check_file_exists(S3_BUCKET_NAME, tok_key):
                        payload['tokenfilename'] = tok_s3_path
                    else:
                        error_message = f"Token file not found for {csv_s3_path}"
                        await write_error_to_s3(dataset_name, error_message, csv_s3_path)
                        logger.warning(error_message)

                    payloads.append(payload)
                    logger.info(f"Found file: {csv_s3_path}")

        logger.info(f"Total payloads built: {len(payloads)}")
        return payloads
    except ClientError as e:
        logger.error(f"Error accessing S3: {str(e)}")
        raise

async def check_file_exists(bucket, key):
    try:
        await asyncio.get_event_loop().run_in_executor(
            None, lambda: s3.head_object(Bucket=bucket, Key=key)
        )
        return True
    except ClientError:
        return False

async def process_payloads(payloads):
    async with aiohttp.ClientSession() as session:
        tasks = [call_api(session, payload) for payload in payloads]
        results = await asyncio.gather(*tasks, return_exceptions=True)
    return results

async def call_api(session, payload):
    try:
        async with session.post(API_ENDPOINT, params=payload) as response:
            if response.status == 200:
                response_json = await response.json()
                provenance_guid = response_json.get('provenance_guid')
                
                if provenance_guid:
                    s3_key = f"jobs/placement/{payload['datasetversion']}/{payload['businessdate']}/{provenance_guid}.json"
                    await write_to_s3(S3_BUCKET_NAME, s3_key, json.dumps(response_json))
                    logger.info(f"Successfully processed file: {payload['datafilename']}. Response written to {s3_key}")
                    return {'status': 'success', 'file_path': payload['datafilename']}
                else:
                    raise ValueError("Response does not contain provenance_guid")
            else:
                raise Exception(f"API call failed with status: {response.status}")

    except Exception as e:
        error_message = f"Error processing file {payload['datafilename']}: {str(e)}"
        await write_error_to_s3(payload['datasetversion'], error_message, payload['datafilename'])
        return {'status': 'error', 'file_path': payload['datafilename'], 'error': str(e)}

async def write_to_s3(bucket, key, content):
    try:
        await asyncio.get_event_loop().run_in_executor(
            None, lambda: s3.put_object(Bucket=bucket, Key=key, Body=content, ContentType='application/json')
        )
    except ClientError as e:
        logger.error(f"Error writing to S3: {str(e)}")
        raise

async def write_error_to_s3(dataset_name, error_message, file_path):
    error_id = str(uuid.uuid4())
    error_key = f"jobs/placement/{dataset_name}/error/{error_id}.json"
    error_content = {
        'file_path': file_path,
        'error_message': error_message,
        'timestamp': datetime.utcnow().isoformat()
    }
    await write_to_s3(S3_BUCKET_NAME, error_key, json.dumps(error_content))
    logger.error(f"{error_message} Error details written to {error_key}")

def lambda_handler(event, context):
    try:
        logger.info(f"Received event: {json.dumps(event)}")

        base_path = event['base_path']
        dataset_name = event['dataset_name']
        dataset_version = event['dataset_version']
        pipeline_id = event['pipeline_id']

        if dataset_name not in ALLOWED_DATASET_NAMES:
            return create_return_response(400, f"Invalid dataset name. Allowed values are: {', '.join(ALLOWED_DATASET_NAMES)}")

        async def main():
            payloads = await find_files_and_build_payloads(base_path, dataset_name, dataset_version, pipeline_id)
            
            if not payloads:
                logger.info("No matching files found.")
                return create_return_response(200, 'No matching files found.')

            results = await process_payloads(payloads)
            return results

        results = asyncio.run(main())

        successful = sum(1 for r in results if r['status'] == 'success')
        failed = len(results) - successful

        logger.info(f"Processing complete. Successful: {successful}, Failed: {failed}")

        return create_return_response(
            200, 
            f'Processed {len(results)} files. Successful: {successful}, Failed: {failed}',
            {'results': results}
        )

    except Exception as e:
        logger.error(f"Unhandled error in lambda_handler: {str(e)}", exc_info=True)
        return create_return_response(500, 'Internal server error', {'error': str(e)})

```
