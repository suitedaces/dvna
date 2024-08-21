```

import concurrent.futures
from functools import partial
import boto3
import csv
import gzip
from io import StringIO, BytesIO
import magic

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

def parse_group_map(
    file_groups: Dict[Set[str], List[str]],
) -> Dict[str, Dict[str, str]]:
    result = {}
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = []
        for column_set in file_groups.keys():
            futures.append(executor.submit(process_column_set, column_set, file_groups))
        
        for i, future in enumerate(concurrent.futures.as_completed(futures)):
            result[f"group{i+1}"] = future.result()
    
    return result

def process_column_set(column_set, file_groups):
    return {
        "columnNames": list(column_set),
        "businessDates": [
            business_date for business_date, _ in file_groups[column_set]
        ],
        "filePaths": [file_path for _, file_path in file_groups[column_set]],
    }

def get_file_columns_s3_select(s3: boto3.client, bucket: str, key: str) -> Tuple[str]:
    # Get file content type
    head = s3.head_object(Bucket=bucket, Key=key)
    content_type = head.get('ContentType', '')

    if 'csv' in content_type.lower():
        return get_csv_columns(s3, bucket, key)
    elif 'gzip' in content_type.lower():
        return get_gzip_columns(s3, bucket, key)
    else:
        raise ValueError(f"Unsupported file type: {content_type}")

def get_csv_columns(s3: boto3.client, bucket: str, key: str) -> Tuple[str]:
    response = s3.select_object_content(
        Bucket=bucket,
        Key=key,
        ExpressionType="SQL",
        Expression="SELECT * FROM s3object LIMIT 1",
        InputSerialization={"CSV": {"FileHeaderInfo": "Use"}},
        OutputSerialization={"CSV": {}},
    )
    for event in response["Payload"]:
        if "Records" in event:
            content = event["Records"]["Payload"].decode("utf-8")
            reader = csv.reader(StringIO(content))
            header = next(reader)
            return tuple(header)
    return tuple()

def get_gzip_columns(s3: boto3.client, bucket: str, key: str) -> Tuple[str]:
    obj = s3.get_object(Bucket=bucket, Key=key)
    with gzip.GzipFile(fileobj=BytesIO(obj['Body'].read())) as gzipfile:
        content = gzipfile.read().decode('utf-8')
        reader = csv.reader(StringIO(content))
        header = next(reader)
        return tuple(header)

```
