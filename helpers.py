import boto3, botocore
from config import S3_KEY, S3_SECRET, S3_BUCKET
import os

"""
Instaniate the S3 client
"""
s3 = boto3.client(
   "s3",
   aws_access_key_id=S3_KEY,
   aws_secret_access_key=S3_SECRET
)

"""
Upload an object to an S3 bucket in a multipart way
"""
def upload_object(key, file_object, acl="public-read"):

    try:
        s3.upload_fileobj(Fileobj=file_object, Bucket=S3_BUCKET, Key=key)

    except Exception as e:
        print("Error uploading to S3: ", e)
        return False

    return True

"""
Get the size of an existing file in bytes
"""
def get_total_bytes(file_name):

    result = s3.list_objects(Bucket=S3_BUCKET)
    for item in result['Contents']:
        if item['Key'] == file_name:
            return item['Size']

"""
Return files smaller than 1 mb in one chunk
"""
def get_object(file_name):

    total_bytes = get_total_bytes(file_name)

    if total_bytes > 1000000:
        return get_object_range(file_name, total_bytes)
    return s3.get_object(Bucket=S3_BUCKET, Key=file_name)['Body'].read()

"""
Return files bigger than 1 mb in multiple chunks
"""
def get_object_range(file_name, total_bytes):
    offset = 0
    while total_bytes > 0:
        end = offset + 999999 if total_bytes > 1000000 else ""
        total_bytes -= 1000000
        byte_range = 'bytes={offset}-{end}'.format(offset=offset, end=end)
        offset = end + 1 if not isinstance(end, str) else None
        yield s3.get_object(Bucket=S3_BUCKET, Key=file_name, Range=byte_range)['Body'].read()
