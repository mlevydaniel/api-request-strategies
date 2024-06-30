from modules.api_request import make_request, make_request_and_process
from modules.utils import (save_to_csv_stream, store_data_to_gcs, store_data_locally)
import os
import logging
from datetime import datetime, timezone
import time
from threading import Thread
from google.cloud import storage

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Load API credentials from environment variables
api_key = os.getenv("BITSO_API_KEY")
api_secret = os.getenv("BITSO_API_SECRET")
GCS_BUCKET_NAME = 'bitsode'

# Request details
base_url = "https://stage.bitso.com/api/v3/order_book"
http_method = "GET"
STORE_IN_GCS = True

# Initialize GCS client
gcs_client = storage.Client()

# List of exchange rates to process
exchange_rates = ["btc_mxn", "eth_mxn", "xrp_mxn"]

def process_exchange_rate(book):
    params = {"book": book}
    data_list = []

    # Get minute when the script started
    now = datetime.now(timezone.utc)
    start_minute = now.minute
    rounded_minute = now.replace(minute=(now.minute // 1), second=0, microsecond=0)

    # Main loop
    while True:
        now = datetime.now(timezone.utc)
        current_minute = now.minute

        # Make the request and print the response
        data_tuple = make_request_and_process(api_key, api_secret, http_method, base_url, params)

        if not data_tuple:
            logging.error(f"Error fetching data for {book}")
            time.sleep(1 - datetime.now(timezone.utc).microsecond / 1_000_000)
            continue

        # Check if the current minute is a multiple of 1 and not the start minute
        if current_minute % 1 == 0 and current_minute != start_minute:

            # Save data
            filename = f"{book}_{rounded_minute.strftime('%Y-%m-%d-%H-%M-%S')}.csv"

            # Upload data to GCS or save locally
            if STORE_IN_GCS:
                upload_thread = Thread(target=store_data_to_gcs, args=(GCS_BUCKET_NAME, filename))
                upload_thread.start()
            else:
                store_data_locally(filename)

            # update the start and rounded minute and reset the data list
            start_minute = current_minute
            rounded_minute = now.replace(minute=(now.minute // 1), second=0, microsecond=0)

        if data_tuple:
            filename = f"{book}_{rounded_minute.strftime('%Y-%m-%d-%H-%M-%S')}.csv"
            save_to_csv_stream(data_tuple, filename)

        # Wait until next second
        time.sleep(1 - datetime.now(timezone.utc).microsecond / 1_000_000)

if __name__ == "__main__":
    threads = []
    for book in exchange_rates:
        thread = Thread(target=process_exchange_rate, args=(book,))
        thread.start()
        threads.append(thread)

    for thread in threads:
        thread.join()
