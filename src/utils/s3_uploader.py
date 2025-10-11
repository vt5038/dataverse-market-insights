"""
S3 uploader with retries, logging, and optional server-side encryption.
"""

import os, logging
from dotenv import load_dotenv
import boto3
from botocore.exceptions import ClientError
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

load_dotenv()

print("✅ Loaded ENV values:")
print("Region:", os.getenv("AWS_REGION"))
print("Bucket:", os.getenv("AWS_S3_BRONZE_BUCKET"))




AWS_REGION = os.getenv("AWS_REGION")
BRONZE_BUCKET = os.getenv("AWS_S3_BRONZE_BUCKET")

s3 = boto3.client(
    "s3",
    region_name=AWS_REGION,
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
)

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type(ClientError)
)
def upload_to_s3(local_path: str, s3_key: str, encrypt: bool = True) -> None:
    """
    Upload a file to S3 (Bronze bucket) with optional SSE-S3 encryption.
    """

    print(f"🪣 DEBUG: Preparing upload for {local_path} → {BRONZE_BUCKET}/{s3_key}")

    extra_args = {"ServerSideEncryption": "AES256"} if encrypt else None
    try:
        print(f"🪣 Uploading {local_path} to S3 → {BRONZE_BUCKET}/{s3_key}")

        s3.upload_file(local_path, BRONZE_BUCKET, s3_key, ExtraArgs=extra_args or {})
        logging.info(f"Uploaded to s3://{BRONZE_BUCKET}/{s3_key}")
        print(f"Uploaded to s3://{BRONZE_BUCKET}/{s3_key}")
    except ClientError as e:
        logging.error(f"❌ S3 upload failed: {e}")
        raise
