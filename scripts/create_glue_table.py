"""
create_glue_table.py
--------------------
Triggers AWS Glue crawler to update table metadata for the Silver bucket.
"""

import boto3
import os
import time
import logging
from dotenv import load_dotenv

load_dotenv()

GLUE_CRAWLER_NAME = os.getenv("GLUE_CRAWLER_NAME")
AWS_REGION = os.getenv("AWS_REGION")

# Logging setup
LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)
logging.basicConfig(
    filename=os.path.join(LOG_DIR, "create_glue_table.log"),
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

def main():
    logging.info(f"Starting Glue Crawler: {GLUE_CRAWLER_NAME}")
    glue = boto3.client("glue", region_name=AWS_REGION)

    try:
        glue.start_crawler(Name=GLUE_CRAWLER_NAME)
        logging.info(f"Crawler {GLUE_CRAWLER_NAME} started successfully")

        # Optional: wait until crawler finishes
        while True:
            status = glue.get_crawler(Name=GLUE_CRAWLER_NAME)["Crawler"]["State"]
            if status == "READY":
                logging.info(f"Crawler {GLUE_CRAWLER_NAME} completed successfully")
                break
            logging.info(f"⏳ Crawler status: {status}")
            time.sleep(10)

    except Exception as e:
        logging.error(f"Failed to run crawler: {e}")
        print(f"Error running crawler: {e}")

if __name__ == "__main__":
    main()
