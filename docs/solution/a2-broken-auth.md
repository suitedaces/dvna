```

import os
import boto3
import aiohttp
import asyncio
import logging
import json
import uuid
from datetime import datetime
from typing import List, Dict, Any, Optional, Tuple
from botocore.exceptions import ClientError

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Configuration
S3_BUCKET_NAME: str = os.environ['S3_BUCKET_NAME']
API_ENDPOINT: str = os.environ['API_ENDPOINT']
AWS_REGION: str = os.environ['AWS_REGION']
ASSUME_ROLE: str = os.environ['ASSUME_ROLE']
KMS_KEY_ARN: str = os.environ['KMS_KEY_ARN']

# Initialize S3 client
s3 = boto3.client('s3', region_name=AWS_REGION)

# Predefined list of allowed dataset names
ALLOWED_DATASET_NAMES: List[str] = [
    "customer_data",
    "sales_transactions",
    "inventory_levels",
    "marketing_campaigns",
    "product_catalog"
]

def create_return_response(status_code: int, message: str, error_data: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    response: Dict[str, Any] = {
        'statusCode': status_code,
        'message': message
    }
    if error_data:
        response['error_data'] = error_data
    return response

def is_valid_date_format(date_string: str) -> bool:
    try:
        datetime.strptime(date_string, '%Y%m%d')
        return True
    except ValueError:
        return False

async def find_files_and_build_payloads(
    base_path: str,
    dataset_name: str,
    dataset_version: str,
    pipeline_id: str
) -> List[Dict[str, str]]:
    payloads: List[Dict[str, str]] = []
    paginator = s3.get_paginator('list_objects_v2')
    
    try:
        for page in paginator.paginate(Bucket=S3_BUCKET_NAME, Prefix=base_path):
            for obj in page.get('Contents', []):
                key: str = obj['Key']
                if key.endswith('.csv'):
                    parts: List[str] = key.split('/')
                    if len(parts) < 2:
                        continue
                    
                    business_date: str = parts[-2]
                    if not is_valid_date_format(business_date):
                        logger.warning(f"Invalid business date format in folder: {business_date}")
                        continue

                    csv_s3_path: str = f"s3://{S3_BUCKET_NAME}/{key}"
                    tok_key: str = key.rsplit('.', 1)[0] + '.tok'
                    tok_s3_path: str = f"s3://{S3_BUCKET_NAME}/{tok_key}"

                    payload: Dict[str, str] = {
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
                        error_message: str = f"Token file not found for {csv_s3_path}"
                        await write_error_to_s3(dataset_name, error_message, csv_s3_path)
                        logger.warning(error_message)

                    payloads.append(payload)
                    logger.info(f"Found file: {csv_s3_path}")

        logger.info(f"Total payloads built: {len(payloads)}")
        return payloads
    except ClientError as e:
        logger.error(f"Error accessing S3: {str(e)}")
        raise

async def check_file_exists(bucket: str, key: str) -> bool:
    try:
        await asyncio.get_event_loop().run_in_executor(
            None, lambda: s3.head_object(Bucket=bucket, Key=key)
        )
        return True
    except ClientError:
        return False

async def process_payloads(payloads: List[Dict[str, str]]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    async with aiohttp.ClientSession() as session:
        tasks: List[asyncio.Task] = [call_api(session, payload) for payload in payloads]
        results: List[Dict[str, Any]] = await asyncio.gather(*tasks, return_exceptions=True)
    
    successes: List[Dict[str, Any]] = []
    failures: List[Dict[str, Any]] = []
    
    for result in results:
        if isinstance(result, Exception):
            failures.append({'status': 'error', 'error': str(result)})
        elif result['status'] == 'success':
            successes.append(result)
        else:
            failures.append(result)
    
    return successes, failures

async def call_api(session: aiohttp.ClientSession, payload: Dict[str, str]) -> Dict[str, Any]:
    try:
        async with session.post(API_ENDPOINT, params=payload) as response:
            if response.status == 200:
                response_json: Dict[str, Any] = await response.json()
                provenance_guid: Optional[str] = response_json.get('provenance_guid')
                
                if provenance_guid:
                    s3_key: str = f"jobs/placement/{payload['datasetversion']}/{payload['businessdate']}/{provenance_guid}.json"
                    await write_to_s3(S3_BUCKET_NAME, s3_key, json.dumps(response_json))
                    logger.info(f"Successfully processed file: {payload['datafilename']}. Response written to {s3_key}")
                    return {'status': 'success', 'file_path': payload['datafilename']}
                else:
                    raise ValueError("Response does not contain provenance_guid")
            else:
                raise Exception(f"API call failed with status: {response.status}")

    except Exception as e:
        error_message: str = f"Error processing file {payload['datafilename']}: {str(e)}"
        await write_error_to_s3(payload['datasetversion'], error_message, payload['datafilename'])
        return {'status': 'error', 'file_path': payload['datafilename'], 'error': str(e)}

async def write_to_s3(bucket: str, key: str, content: str) -> None:
    try:
        await asyncio.get_event_loop().run_in_executor(
            None, lambda: s3.put_object(Bucket=bucket, Key=key, Body=content, ContentType='application/json')
        )
    except ClientError as e:
        logger.error(f"Error writing to S3: {str(e)}")
        raise

async def write_error_to_s3(dataset_name: str, error_message: str, data_filename: str) -> None:
    error_id: str = str(uuid.uuid4())
    error_key: str = f"jobs/placement/{dataset_name}/error/{error_id}.json"
    error_content: Dict[str, Any] = {
        'datafilename': data_filename,
        'error_message': error_message,
        'timestamp': datetime.utcnow().isoformat()
    }
    await write_to_s3(S3_BUCKET_NAME, error_key, json.dumps(error_content))
    logger.error(f"{error_message} Error details written to {error_key}")

def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    try:
        logger.info(f"Received event: {json.dumps(event)}")

        base_path: str = event['base_path']
        dataset_name: str = event['dataset_name']
        dataset_version: str = event['dataset_version']
        pipeline_id: str = event['pipeline_id']

        if dataset_name not in ALLOWED_DATASET_NAMES:
            return create_return_response(400, f"Invalid dataset name. Allowed values are: {', '.join(ALLOWED_DATASET_NAMES)}")

        async def main() -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
            payloads: List[Dict[str, str]] = await find_files_and_build_payloads(base_path, dataset_name, dataset_version, pipeline_id)
            
            if not payloads:
                logger.info("No matching files found.")
                return [], []

            return await process_payloads(payloads)

        successes, failures = asyncio.run(main())

        logger.info(f"Processing complete. Successful: {len(successes)}, Failed: {len(failures)}")

        return create_return_response(
            200, 
            f'Processed {len(successes) + len(failures)} files. Successful: {len(successes)}, Failed: {len(failures)}',
            {'successes': successes, 'failures': failures}
        )

    except Exception as e:
        logger.error(f"Unhandled error in lambda_handler: {str(e)}", exc_info=True)
        return create_return_response(500, 'Internal server error', {'error': str(e)})

```
