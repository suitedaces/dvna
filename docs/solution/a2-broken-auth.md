```
import concurrent.futures
from functools import partial
import boto3
import csv
from io import StringIO
from typing import Dict, List, Tuple, Set

def group_files_by_columns(s3: boto3.client, bucket_name: str, prefix: str) -> Dict[Set[str], List[Tuple[str, str]]]:
    file_groups = defaultdict(list)
    logger.info(f"Looking into Prefix {prefix}")
    paginator = s3.get_paginator("list_objects_v2")

    def process_page(page):
        local_file_groups = defaultdict(list)
        for prefix in page.get("CommonPrefixes", []):
            folder_name = prefix.get("Prefix").split("/")[-2]
            if is_valid_date_format(folder_name):
                date_prefix = prefix.get("Prefix")
                response = s3.list_objects_v2(Bucket=bucket_name, Prefix=date_prefix)
                if "Contents" in response:
                    for obj in response["Contents"]:
                        key = obj["Key"]
                        try:
                            logger.info(f"Getting columns for {key}")
                            column_set = get_file_columns_s3_select(
                                s3=s3,
                                bucket=bucket_name,
                                key=key
                            )
                            logger.info(f"Got key {column_set}")
                            local_file_groups[frozenset(column_set)].append((folder_name, key))
                        except Exception as e:
                            logger.info(f"Error reading columns from {key}: {str(e)}")
        return local_file_groups

    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = []
        for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix, Delimiter="/"):
            futures.append(executor.submit(process_page, page))
        
        for future in concurrent.futures.as_completed(futures):
            for column_set, files in future.result().items():
                file_groups[column_set].extend(files)

    return file_groups

def get_file_columns_s3_select(s3: boto3.client, bucket: str, key: str) -> Set[str]:
    head = s3.head_object(Bucket=bucket, Key=key)
    content_type = head.get('ContentType', '').lower()

    input_serialization = {
        "CSV": {"FileHeaderInfo": "Use"},
    }

    if 'gzip' in content_type:
        input_serialization["CompressionType"] = "GZIP"

    response = s3.select_object_content(
        Bucket=bucket,
        Key=key,
        ExpressionType="SQL",
        Expression="SELECT * FROM s3object LIMIT 1",
        InputSerialization=input_serialization,
        OutputSerialization={"CSV": {}},
    )

    for event in response["Payload"]:
        if "Records" in event:
            content = event["Records"]["Payload"].decode("utf-8")
            reader = csv.reader(StringIO(content))
            header = next(reader)
            return set(header)
    return set()

def parse_group_map(file_groups: Dict[Set[str], List[Tuple[str, str]]]) -> Dict[str, Dict[str, List[str]]]:
    result = {}
    for i, (column_set, files) in enumerate(file_groups.items(), 1):
        result[f"group{i}"] = {
            "columnNames": list(column_set),
            "businessDates": [business_date for business_date, _ in files],
            "filePaths": [file_path for _, file_path in files],
        }
    return result

def is_valid_date_format(folder_name: str) -> bool:
    # Implement your date format validation logic here
    pass

```
