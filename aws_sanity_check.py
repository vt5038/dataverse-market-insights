import os
from dotenv import load_dotenv
import boto3

load_dotenv()
sts = boto3.client("sts",
    region_name=os.getenv("AWS_REGION"),
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
)
print("Caller identity:", sts.get_caller_identity())
