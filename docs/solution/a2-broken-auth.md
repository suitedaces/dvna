```
import json
import os
from typing import Any, Dict, List, Optional, Tuple
import concurrent.futures
import boto3
from botocore.exceptions import ClientError
from pydantic import ValidationError

from ...config import (
    LANDING_ZONE_BUCKET,
    PUBLISH_SDK_LAMBDA,
    logger,
)

from ...models import (
    PublishDatasetFailure,
    PublishDatasetSuccess,
    PublishSdkLambdaEvent,
    create_return_response,
    parse_publish_dataset_request,
)

from ...utils import (
    check_file_exists,
    is_date_in_range,
    is_valid_date_format
)

# Configuration constants
MAX_RETRIES = 3
MAX_WORKERS = int(os.environ.get('MAX_WORKERS', '10'))
LAMBDA_BATCH_SIZE = int(os.environ.get('LAMBDA_BATCH_SIZE', '10'))

def list_s3_folders(s3: boto3.client, bucket: str, prefix: str) -> List[str]:
    """
    List S3 folders efficiently using delimiter.
    
    Args:
        s3 (boto3.client): S3 client
        bucket (str): S3 bucket name
        prefix (str): S3 prefix to list folders from
    
    Returns:
        List[str]: List of folder names
    """
    paginator = s3.get_paginator('list_objects_v2')
    folders = []
    
    try:
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter='/'):
            for prefix_obj in page.get('CommonPrefixes', []):
                folder = prefix_obj.get('Prefix')
                if folder:
                    folders.append(folder)
    except ClientError as e:
        logger.error(f"Error listing S3 folders: {str(e)}")
        raise

    return folders

def publish_multi_file_handler(
    event: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Handle multi-file publishing process.
    
    Args:
        event (Dict[str, Any]): Lambda event
    
    Returns:
        Dict[str, Any]: Response with status and results
    """
    try:
        logger.info(f"Received event: {json.dumps(event)}")

        request: Dict[str, Any] = parse_publish_dataset_request(event).model_dump(
            by_alias=True
        )

        dataset_name = request.get("datasetName")
        dataset_version = request.get("datasetVersion")
        pipeline_id = request.get("pipelineId")
        s3_prefix = request.get("s3Prefix")
        dataset_guid = request.get("datasetGuid")
        file_name = request.get("fileName")
        container_image = request.get("containerImageName")
        threads = request.get("threads")

        start_date: Optional[str] = request.get("startDate")
        end_date: Optional[str] = request.get("endDate")

        if not all([dataset_name, dataset_version, pipeline_id, s3_prefix, dataset_guid, file_name, container_image, threads]):
            raise ValueError("Missing required parameters in the request")

        logger.info("Building payloads for request: %s", str(request))
        
        s3 = boto3.client('s3')
        lambda_client = boto3.client('lambda')
        
        payloads, failures = find_files_and_build_payloads(
            s3=s3,
            lambda_client=lambda_client,
            prefix=s3_prefix,
            dataset_guid=dataset_guid,
            dataset_name=dataset_name,
            dataset_version=dataset_version,
            file_name=file_name,
            pipeline_id=pipeline_id,
            container_image=container_image,
            threads=threads,
            start_date=start_date,
            end_date=end_date,
        )

        if not payloads:
            logger.warning(f"No matching files found for dataset {dataset_name}.")
            return create_return_response(
                status_code=204,
                message="No matching files found for given dates.",
                body={"successes": "0", "failures": "0"},
            )

        logger.info("Invoking Lambda with payloads now...")
        successes, new_failures = process_payloads(
            lambda_client=lambda_client, payloads=payloads
        )
        failures.extend(new_failures)

        logger.info(
            f"Successfully Triggered: {len(successes)}, Failed: {len(failures)}"
        )

        return create_return_response(
            status_code=200,
            message=f"Found {len(successes) + len(failures)} files for {start_date} - {end_date}. Successfully started Jobs for: {len(successes)}, And Failed: {len(failures)}",
            body={"successes": successes, "failures": failures},
        )

    except ValidationError as e:
        logger.error(f"Invalid request body: {str(e)}")
        return create_return_response(
            status_code=400,
            message=f"[CCB:PUBLISH:001] - Invalid request body: {str(e)}",
            error_data=str(e),
        )

    except ValueError as e:
        logger.error(f"Invalid input: {str(e)}")
        return create_return_response(
            status_code=400,
            message=f"[CCB:PUBLISH:002] - Invalid input: {str(e)}",
            error_data=str(e),
        )

    except Exception as e:
        logger.error(f"Unhandled error in lambda handler: {str(e)}", exc_info=True)
        return create_return_response(
            status_code=500,
            message="Internal server error",
            error_data=str({"error": f"{type(e)}: {str(e)}"}),
        )

def find_files_and_build_payloads(
    s3: boto3.client,
    lambda_client: boto3.client,
    prefix: str,
    dataset_guid: str,
    dataset_name: str,
    dataset_version: str,
    pipeline_id: str,
    file_name: str,
    container_image: str,
    threads: int,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> Tuple[List[Dict[str, str]], List[Dict[str, str]]]:
    """
    Find files in S3 and build payloads for processing.
    
    Args:
        s3 (boto3.client): S3 client
        lambda_client (boto3.client): Lambda client
        prefix (str): S3 prefix to search
        dataset_guid (str): Dataset GUID
        dataset_name (str): Dataset name
        dataset_version (str): Dataset version
        pipeline_id (str): Pipeline ID
        file_name (str): File name to search for
        container_image (str): Container image name
        threads (int): Number of threads
        start_date (Optional[str]): Start date for filtering
        end_date (Optional[str]): End date for filtering
    
    Returns:
        Tuple[List[Dict[str, str]], List[Dict[str, str]]]: Payloads and failures
    """
    payloads = []
    failures = []
    
    folders = list_s3_folders(s3, LANDING_ZONE_BUCKET, prefix)
    valid_folders = [
        folder for folder in folders
        if is_valid_date_format(folder.rstrip('/').split('/')[-1]) and 
        (not start_date or not end_date or is_date_in_range(folder.rstrip('/').split('/')[-1], start_date, end_date))
    ]
    
    logger.info(f"Found {len(valid_folders)} valid date folders")

    def process_object(obj):
        key = obj["Key"]
        if not key.endswith(file_name):
            return None

        parts = key.split("/")
        if len(parts) <= 2:
            return None

        business_date = str(parts[-2])
        if not is_valid_date_format(date_string=business_date):
            return None

        if start_date and end_date and not is_date_in_range(business_date, start_date, end_date):
            return None
        
        tok_key = key.rsplit("/", 1)[0] + ".tok"
        csv_s3_path = f"s3://{LANDING_ZONE_BUCKET}/{key}"
        
        if check_file_exists(s3=s3, bucket=LANDING_ZONE_BUCKET, key=tok_key):
            base_data_filepath = key.rsplit("/", 1)[0] + ".csv"
            token_file_name = key.rsplit("/", 1)[1].rsplit(".", 1)[0] + ".tok"
            return PublishSdkLambdaEvent(
                dataset_name=dataset_name,
                dataset_version=dataset_version,
                business_date=business_date,
                order_date=business_date,
                pipeline_id=pipeline_id,
                dataset_guid=dataset_guid,
                data_file_name=file_name,
                token_file_name=token_file_name,
                container_image=container_image,
                threads=threads,
                glue_table_name=dataset_name.lower(),
                base_data_filepath=base_data_filepath,
                base_token_filepath=token_file_name,
            ).model_dump(by_alias=True)
        else:
            return PublishDatasetFailure(
                dataset_name=dataset_name,
                error_message=f"Token file not found for {csv_s3_path}",
                data_filepath=csv_s3_path,
                business_date=business_date,
                reason="Token file not found",
            ).model_dump(by_alias=True)

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        for folder in valid_folders:
            paginator = s3.get_paginator("list_objects_v2")
            try:
                for page in paginator.paginate(Bucket=LANDING_ZONE_BUCKET, Prefix=folder):
                    futures = [executor.submit(process_object, obj) for obj in page.get("Contents", [])]
                    for future in concurrent.futures.as_completed(futures):
                        result = future.result()
                        if result:
                            if "error_message" in result:
                                failures.append(result)
                            else:
                                payloads.append(result)
            except ClientError as e:
                logger.error(f"Error processing folder {folder}: {str(e)}")
                failures.append(PublishDatasetFailure(
                    dataset_name=dataset_name,
                    error_message=f"Error processing folder: {str(e)}",
                    data_filepath=folder,
                    business_date="Unknown",
                    reason="S3 ClientError",
                ).model_dump(by_alias=True))

    logger.info(f"Total Publish Lambda events built: {len(payloads)}")
    logger.info(f"Total Failures: {len(failures)}")
    return payloads, failures

def process_payloads(
    lambda_client: boto3.client,
    payloads: List[Dict[str, Any]],
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Process payloads by invoking Lambda functions.
    
    Args:
        lambda_client (boto3.client): Lambda client
        payloads (List[Dict[str, Any]]): List of payloads to process
    
    Returns:
        Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]: Successes and failures
    """
    successes = []
    failures = []

    def invoke_lambda(payload):
        for attempt in range(MAX_RETRIES):
            try:
                response = lambda_client.invoke(
                    FunctionName="app-ccbedac-publish-sdk-lambda",
                    InvocationType="Event",
                    Payload=json.dumps(payload),
                )
                if response["StatusCode"] == 202:
                    return PublishDatasetSuccess(
                        dataset_name=payload["datasetName"],
                        business_date=payload["businessDate"],
                        data_filepath=payload["baseDataFilePath"],
                        response=f"Status: {response['StatusCode']}, RequestId: {response['ResponseMetadata']['RequestId']}",
                    ).model_dump(by_alias=True)
                else:
                    logger.warning(f"Lambda invocation failed (attempt {attempt + 1}): {response}")
            except Exception as e:
                logger.error(f"Error invoking lambda (attempt {attempt + 1}): {str(e)}")
                if attempt == MAX_RETRIES - 1:
                    return PublishDatasetFailure(
                        dataset_name=payload["datasetName"],
                        error_message=str({"error": f"{type(e)}: {str(e)}"}),
                        data_filepath=payload["baseDataFilePath"],
                        business_date=payload["businessDate"],
                        reason="Unhandled Error while invoking lambda",
                    ).model_dump(by_alias=True)

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        for i in range(0, len(payloads), LAMBDA_BATCH_SIZE):
            batch = payloads[i:i+LAMBDA_BATCH_SIZE]
            future_to_payload = {executor.submit(invoke_lambda, payload): payload for payload in batch}
            for future in concurrent.futures.as_completed(future_to_payload):
                result = future.result()
                if result:
                    if "error_message" in result:
                        failures.append(result)
                    else:
                        successes.append(result)

    return successes, failures
```
