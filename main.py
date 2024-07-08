import os
import logging
from datetime import datetime, timezone, timedelta
import time
from google.cloud import storage
from modules.api_request import make_request_and_process
from modules.utils import save_to_csv_stream, store_data_to_gcs, store_data_locally

# Constants
GCS_BUCKET_NAME = "bitsode"
BASE_URL = "https://stage.bitso.com/api/v3/order_book"
HTTP_METHOD = "GET"
STORE_IN_GCS = True
MINUTE_MULTIPLE = 2

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Initialize GCS client
gcs_client = storage.Client()

# Load API credentials from environment variables
api_key = os.getenv("BITSO_API_KEY")
api_secret = os.getenv("BITSO_API_SECRET")
book = "btc_mxn"


def round_down_minute(current_time):
    """Round down the current time to the nearest multiple of MINUTE_MULTIPLE"""
    minutes = (current_time.minute // MINUTE_MULTIPLE) * MINUTE_MULTIPLE
    return current_time.replace(minute=minutes, second=0, microsecond=0)


def store_data(filename):
    """Store data either in GCS or locally"""
    if STORE_IN_GCS:
        store_data_to_gcs(GCS_BUCKET_NAME, filename)
    else:
        store_data_locally(filename)


def main_loop(book, api_key, api_secret):
    """Main execution loop"""
    current_interval = round_down_minute(datetime.now(timezone.utc))
    next_interval = current_interval + timedelta(minutes=MINUTE_MULTIPLE)
    filename = f"{book}_{current_interval.strftime('%Y-%m-%d-%H-%M-%S')}.csv"

    while True:
        now = datetime.now(timezone.utc)

        data_tuple = make_request_and_process(api_key, api_secret, book)

        if not data_tuple:
            time.sleep(1 - datetime.now(timezone.utc).microsecond / 1_000_000)
            continue

        if now < next_interval:
            save_to_csv_stream(data_tuple, filename)
        else:
            # Store the previous interval's data
            store_data(filename)
            logger.info(f"Data for interval {current_interval} to {next_interval} stored.")

            # Update intervals and filename for the new interval
            current_interval = round_down_minute(now)
            next_interval = current_interval + timedelta(minutes=MINUTE_MULTIPLE)
            filename = f"{book}_{current_interval.strftime('%Y-%m-%d-%H-%M-%S')}.csv"

            # Save the first data point of the new interval
            save_to_csv_stream(data_tuple, filename)

        time.sleep(1 - datetime.now(timezone.utc).microsecond / 1_000_000)


if __name__ == "__main__":
    main_loop(book, api_key, api_secret)
