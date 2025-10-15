"""
transform_coingecko.py
----------------------
Pulls raw crypto data from S3 (Bronze),
cleans and enriches it,
and uploads the transformed dataset to the Silver bucket.
"""

import os
import pandas as pd
import boto3
import logging
from io import StringIO
from datetime import datetime
from dotenv import load_dotenv
from src.utils.metadata_logger import log_metadata


# Load environment variables
load_dotenv()

# Initialize S3
AWS_REGION = os.getenv("AWS_REGION")
BRONZE_BUCKET = os.getenv("AWS_S3_BRONZE_BUCKET")
SILVER_BUCKET = os.getenv("AWS_S3_SILVER_BUCKET")

s3 = boto3.client(
    "s3",
    region_name=AWS_REGION,
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
)

# Setup logging
LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)
logging.basicConfig(
    filename=os.path.join(LOG_DIR, "transform_coingecko.log"),
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

def get_latest_bronze_file():
    """Fetch the most recent CSV file from the Bronze bucket."""
    response = s3.list_objects_v2(Bucket=BRONZE_BUCKET, Prefix="bronze/crypto/")
    files = [obj["Key"] for obj in response.get("Contents", []) if obj["Key"].endswith(".csv")]
    if not files:
        raise FileNotFoundError("No Bronze CSV files found in S3.")
    latest_file = max(files, key=lambda x: x.split("_")[-1])  # based on timestamp
    logging.info(f"Latest Bronze file found: {latest_file}")
    return latest_file

def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    """Perform data cleaning and enrichment."""
    df = df.drop_duplicates(subset=["id"])  # remove duplicates
    df = df.dropna(subset=["current_price", "market_cap"])  # drop nulls
    df["price_to_volume_ratio"] = df["current_price"] / df["total_volume"].replace(0, pd.NA)
    df["market_cap_category"] = pd.cut(
        df["market_cap"],
        bins=[0, 1e9, 10e9, 100e9, 1e12],
        labels=["Small Cap", "Mid Cap", "Large Cap", "Mega Cap"],
    )
    df["processed_timestamp"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    return df

def upload_to_silver(df: pd.DataFrame):
    """Upload the transformed dataframe to Silver bucket."""
    ingest_date = datetime.utcnow().strftime("%Y-%m-%d")
    filename = f"crypto_data_cleaned_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.csv"
    s3_key = f"silver/crypto/ingest_date={ingest_date}/{filename}"

    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)

    s3.put_object(Bucket=SILVER_BUCKET, Key=s3_key, Body=csv_buffer.getvalue())
    logging.info(f"✅ Transformed data uploaded to s3://{SILVER_BUCKET}/{s3_key}")
    return s3_key

def main():
    logging.info("🚀 Starting transformation for CoinGecko data.")
    start_time = datetime.utcnow()

    try:
        # Get latest Bronze file
        latest_file = get_latest_bronze_file()
        obj = s3.get_object(Bucket=BRONZE_BUCKET, Key=latest_file)
        df = pd.read_csv(obj["Body"])

        # Transform data
        transformed_df = transform_data(df)

        # Upload to Silver
        silver_key = upload_to_silver(transformed_df)

        # Log metadata
        runtime = (datetime.utcnow() - start_time).total_seconds()
        log_metadata(
            source_api="coingecko_silver",
            record_count=len(transformed_df),
            status="SUCCESS",
            s3_path=f"s3://{SILVER_BUCKET}/{silver_key}",
            runtime_seconds=runtime
        )

        print(f"✅ Transformation complete. Uploaded to {silver_key}")
    except Exception as e:
        logging.error(f"❌ Transformation failed: {e}")
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
