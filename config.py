import os
import redis

os.environ["MODIN_ENGINE"] = "ray"

S3_BUCKET                 = os.environ.get("S3_BUCKET_NAME")
S3_KEY                    = os.environ.get("S3_ACCESS_KEY")
S3_SECRET                 = os.environ.get("S3_SECRET_ACCESS_KEY")
S3_LOCATION               = 'http://{}.s3.amazonaws.com/'.format(S3_BUCKET)

SESSION_TYPE              = "redis"
SESSION_REDIS             = redis.from_url("redis://127.0.0.1:6379")

SECRET_KEY                = os.urandom(32)
DEBUG                     = True
PORT                      = 5000
MAX_UPLOAD_PROCESSES      = 2
MAX_DOWNLOAD_PROCESSES    = 2
ALLOWED_EXTENSIONS        = {'csv'}
UPLOAD_FOLDER             = "/root/uploads/"
MAX_CONTENT_LENGTH        = 100 * 1024 * 1024