import boto3, botocore
from config import S3_KEY, S3_SECRET, S3_BUCKET, UPLOAD_FOLDER
import os
s3 = boto3.client(
   "s3",
   aws_access_key_id=S3_KEY,
   aws_secret_access_key=S3_SECRET
)

def upload_file_to_s3(filename, bucket_name, acl="public-read"):

    try:

        _file_path = os.path.join(UPLOAD_FOLDER, filename)

        s3.upload_file(
            _file_path,
            bucket_name,
            filename,
            ExtraArgs={
                "ACL": acl
            }
        )

    except Exception as e:
        # This is a catch all exception, edit this part to fit your needs.
        print("Error uploading to S3: ", e)
        return False

    """
    Remove the file locally
    """

    os.remove(_file_path)

    return True
