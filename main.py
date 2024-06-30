from modules.api_request import make_request
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
aws_access_key = os.getenv("AWS_ACCESS_KEY")
book="btc_mxn"
GCS_BUCKET_NAME = 'bitsode'

# Request details
base_url = "https://stage.bitso.com/api/v3/order_book"
params = {
    "book": book
}
http_method = "GET"
STORE_IN_GCS = True

# Initialize GCS client
gcs_client = storage.Client()


if __name__ == "__main__":

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
        orderbook_timestamp = now.strftime("%Y-%m-%dT%H:%M:%S%z")
        response = make_request(api_key, api_secret, http_method, base_url, params)

        response = response.json().get('payload')

        if not response:
            logging.error(f"Error fetching data for {book}")
            time.sleep(1 - datetime.now(timezone.utc).microsecond / 1_000_000)
            continue

        best_bid = float(response.get('bids')[0]['price'])
        best_ask = float(response.get('asks')[0]['price'])
        spread = float(format((best_bid/best_ask) * 100 / best_ask, '.4f'))

        data_tuple = (orderbook_timestamp, book, best_bid, best_ask, spread)
        logging.info(f"Bid-Ask spread: {data_tuple}")

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
