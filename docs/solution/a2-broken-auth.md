```
import boto3
import csv
from io import StringIO
from collections import defaultdict
import argparse
import re

# Initialize S3 client
s3 = boto3.client('s3')

def get_file_columns_s3_select(bucket, key):
    """Get the column names of a CSV file in S3 using S3 Select."""
    response = s3.select_object_content(
        Bucket=bucket,
        Key=key,
        ExpressionType='SQL',
        Expression="SELECT * FROM s3object s LIMIT 1",
        InputSerialization = {'CSV': {"FileHeaderInfo": "Use"}},
        OutputSerialization = {'CSV': {}}
    )

    for event in response['Payload']:
        if 'Records' in event:
            result = event['Records']['Payload'].decode('utf-8')
            reader = csv.reader(StringIO(result))
            header = next(reader)
            return tuple(header)

    raise Exception("No header found in the file")

def is_valid_date_folder(folder_name):
    """Check if the folder name matches the YYYYMMDD format."""
    return re.match(r'^\d{8}$', folder_name) is not None

def group_files_by_columns(bucket_name, file_name, dataset_name):
    """Group files by their columns across all date folders."""
    file_groups = defaultdict(list)
    
    prefix = f'vendor/{dataset_name}/'
    paginator = s3.get_paginator('list_objects_v2')
    
    for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix, Delimiter='/'):
        for prefix in page.get('CommonPrefixes', []):
            folder_name = prefix.get('Prefix').split('/')[-2]
            
            if is_valid_date_folder(folder_name):
                date_prefix = prefix.get('Prefix')
                response = s3.list_objects_v2(Bucket=bucket_name, Prefix=date_prefix)
                
                if 'Contents' in response:
                    for obj in response['Contents']:
                        key = obj['Key']
                        if key.endswith(file_name):
                            try:
                                columns = get_file_columns_s3_select(bucket_name, key)
                                group_key = (file_name, dataset_name, columns)
                                file_groups[group_key].append((folder_name, key))
                            except Exception as e:
                                print(f"Error reading columns from {key}: {str(e)}")

    return file_groups

def main():
    parser = argparse.ArgumentParser(description='Group S3 files by columns across all date folders.')
    parser.add_argument('bucket_name', help='Name of the S3 bucket')
    parser.add_argument('file_name', help='Name of the file to search for')
    parser.add_argument('dataset_name', help='Name of the dataset')
    args = parser.parse_args()

    grouped_files = group_files_by_columns(args.bucket_name, args.file_name, args.dataset_name)

    # Print the results
    for (filename, dataset_name, columns), files in grouped_files.items():
        print(f"Group: {filename} - {dataset_name}")
        print(f"Columns: {columns}")
        print("Files:")
        for date_folder, key in files:
            print(f"  {date_folder}: {key}")
        print()

if __name__ == "__main__":
    main()

```
