```

import concurrent.futures
import boto3
import csv
from io import StringIO
from typing import Dict, List, Tuple, Set, FrozenSet
from collections import defaultdict

# Configure boto3 to use max connections
s3_client = boto3.client('s3', config=boto3.session.Config(max_pool_connections=100))

def group_files_by_columns(bucket_name: str, prefix: str, dataset_file_name: str) -> Dict[FrozenSet[str], List[Tuple[str, str]]]:
    file_groups = defaultdict(list)
    logger.info(f"Processing prefix: {prefix}")
    paginator = s3_client.get_paginator("list_objects_v2")

    def process_file(bucket: str, key: str, folder_name: str):
        try:
            column_set = get_file_columns_s3_select(bucket, key)
            if column_set:
                return column_set, (folder_name, key)
        except Exception as e:
            logger.error(f"Error processing file {key}: {str(e)}")
        return None

    def process_prefix(prefix_data):
        folder_name = prefix_data.get("Prefix", "").split("/")[-2]
        if not is_valid_date_format(folder_name):
            return []

        data_prefix = prefix_data.get("Prefix")
        try:
            response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=data_prefix)
            if "Contents" not in response:
                return []

            objects_to_process = [
                obj for obj in response["Contents"]
                if obj["Key"].split("/")[-1] == dataset_file_name
            ]

            with concurrent.futures.ThreadPoolExecutor(max_workers=10) as file_executor:
                file_futures = [
                    file_executor.submit(process_file, bucket_name, obj["Key"], folder_name)
                    for obj in objects_to_process
                ]
                results = []
                for future in concurrent.futures.as_completed(file_futures):
                    result = future.result()
                    if result:
                        results.append(result)
                
                return results

        except Exception as e:
            logger.error(f"Error processing prefix {data_prefix}: {str(e)}")
            return []

    with concurrent.futures.ThreadPoolExecutor(max_workers=100) as prefix_executor:
        prefix_futures = []
        for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix, Delimiter="/"):
            for prefix_data in page.get("CommonPrefixes", []):
                prefix_futures.append(prefix_executor.submit(process_prefix, prefix_data))

        for future in concurrent.futures.as_completed(prefix_futures):
            try:
                results = future.result()
                for column_set, file_info in results:
                    file_groups[column_set].append(file_info)
            except Exception as e:
                logger.error(f"Error processing future: {str(e)}")

    return file_groups

def get_file_columns_s3_select(bucket: str, key: str) -> FrozenSet[str]:
    try:
        input_serialization = {
            "CSV": {"FileHeaderInfo": "NONE"},
        }
        if key.lower().endswith('.gz'):
            input_serialization["CompressionType"] = "GZIP"

        response = s3_client.select_object_content(
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
                return frozenset(next(reader))
        
        return frozenset()
    except Exception as e:
        logger.error(f"Error reading columns from {key}: {str(e)}")
        return frozenset()

def parse_group_map(file_groups: Dict[FrozenSet[str], List[Tuple[str, str]]]) -> Dict[str, Dict[str, List[str]]]:
    return {
        f"group{i}": {
            "columnNames": list(column_set),
            "businessDates": [business_date for business_date, _ in files],
            "filePaths": [file_path for _, file_path in files],
        }
        for i, (column_set, files) in enumerate(file_groups.items(), 1)
    }

def is_valid_date_format(folder_name: str) -> bool:
    # Implement your date format validation logic here
    pass

```
