"""
extract_coingecko.py
--------------------
Fetches cryptocurrency market data from CoinGecko API
and stores it locally as timestamped CSV (Bronze layer data).
"""

import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
import logging
import requests
import time
import pandas as pd
from datetime import datetime
from datetime import datetime
from src.utils.s3_uploader import upload_to_s3
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from dotenv import load_dotenv
load_dotenv()
print("✅ .env loaded successfully")

# -------------------------------------------------------
# Setup Logging
# -------------------------------------------------------
LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)

logging.basicConfig(
    filename=os.path.join(LOG_DIR, "extract_coingecko.log"),
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

# -------------------------------------------------------
# API Call with Retry Logic
# -------------------------------------------------------


# Remove duplicate imports and clean up

from requests.exceptions import HTTPError

# Retry if temporary failures occur (network issues or rate limits)
@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type(HTTPError)
)
def fetch_crypto_data():
    """
    Fetches top cryptocurrencies from CoinGecko API with rate limit handling.
    """
    url = "https://api.coingecko.com/api/v3/coins/markets"
    params = {
        "vs_currency": "usd",
        "order": "market_cap_desc",
        "per_page": 10,
        "page": 1,
        "sparkline": False
    }

    # Introduce a rate-limit-aware delay
    time.sleep(2)  # Wait 2 seconds before each call (CoinGecko allows ~30/min)

    response = requests.get(url, params=params, timeout=10)

    # Handle HTTP errors
    if response.status_code == 429:  # Too Many Requests
        retry_after = response.headers.get("Retry-After", 30)  # default 30 seconds
        logging.warning(f"⚠️ Server throttled request — waiting {retry_after}s before retrying...")
        time.sleep(int(retry_after))
        raise HTTPError("Server throttled request — retrying after wait.")
    elif response.status_code >= 400:
        logging.error(f"❌ HTTP Error {response.status_code}: {response.text}")
        response.raise_for_status()

    data = response.json()
    logging.info(f"✅ Successfully fetched {len(data)} records.")
    return data


# -------------------------------------------------------
# Main Execution
# -------------------------------------------------------
def main():
    logging.info("Starting CoinGecko data extraction...")

    try:
        data = fetch_crypto_data()
        logging.info(f"✅ Successfully fetched {len(data)} records from CoinGecko API.")

        # Convert JSON → DataFrame
        df = pd.DataFrame(data)[["id", "symbol", "current_price", "market_cap", "total_volume"]]
        df["timestamp"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

        # Create output directory
        os.makedirs("data/bronze", exist_ok=True)
        filename = f"data/bronze/crypto_data_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.csv"

        # Save as CSV
        df.to_csv(filename, index=False)
        print("🚀 DEBUG: Calling upload_to_s3 now...")
        logging.info("🚀 DEBUG: Calling upload_to_s3 now...")

        ingest_date = datetime.utcnow().strftime("%Y-%m-%d")
        s3_key = f"bronze/crypto/ingest_date={ingest_date}/{os.path.basename(filename)}"

        upload_to_s3(filename, s3_key, encrypt=True)

        logging.info(f"💾 Data saved successfully → {filename}")

        print("\n📊 Top Cryptocurrencies:")
        print(df.head())
        print(f"\n✅ Data saved to {filename}")

    except Exception as e:
        logging.error(f"Extraction failed: {e}")
        print(f"Error during extraction: {e}")

# -------------------------------------------------------
# 4️⃣ Script Entry Point
# -------------------------------------------------------
if __name__ == "__main__":
    main()
