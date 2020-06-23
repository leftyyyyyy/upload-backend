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
ALLOWED_EXTENSIONS        = {'csv'}
MAX_CONTENT_LENGTH        = 100 * 1024 * 1024