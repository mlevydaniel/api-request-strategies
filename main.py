from modules.api_request import make_request_and_process
from modules.utils import save_to_csv_stream, store_data_to_gcs, store_data_locally
import os
import logging
from datetime import datetime, timezone
import time
from google.cloud import storage

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# Load API credentials from environment variables
api_key = os.getenv("BITSO_API_KEY")
api_secret = os.getenv("BITSO_API_SECRET")
book = "btc_mxn"
GCS_BUCKET_NAME = "bitsode"

# Request details
base_url = "https://stage.bitso.com/api/v3/order_book"
params = {"book": book}
http_method = "GET"
STORE_IN_GCS = True
MINUTE_MULTIPLE = 2

# Initialize GCS client
gcs_client = storage.Client()


def round_down_minute(current_time):
    '''
    Round down the current time to the nearest multiple of MINUTE_MULTIPLE
    '''
    minutes = (current_time.minute + (MINUTE_MULTIPLE // 2)) // MINUTE_MULTIPLE * MINUTE_MULTIPLE
    return now.replace(minute=minutes % 60, second=0, microsecond=0)


if __name__ == "__main__":

    data_list = []

    # Get minute when the script started
    now = datetime.now(timezone.utc)
    start_minute = now.minute
    interval_minute = round_down_minute(now)

    # Main loop
    while True:
        now = datetime.now(timezone.utc)
        current_minute = now.minute

        # Make the request and print the response
        data_tuple = make_request_and_process(
            api_key, api_secret, http_method, base_url, params
        )

        if not data_tuple:
            logging.error(f"Error fetching data for {book}")
            time.sleep(1 - datetime.now(timezone.utc).microsecond / 1_000_000)
            continue

        # Check if the current minute is a multiple of MINUTE_MULTIPLE and not the start minute
        if current_minute % MINUTE_MULTIPLE == 0 and current_minute != start_minute:

            # name file
            filename = f"{book}_{interval_minute.strftime('%Y-%m-%d-%H-%M-%S')}.csv"

            # Upload data to GCS or save locally
            if STORE_IN_GCS:
                store_data_to_gcs(GCS_BUCKET_NAME, filename)
            else:
                store_data_locally(filename)

            # update the start and interval minute and reset the data list
            start_minute = current_minute
            interval_minute = round_down_minute(now)

        if data_tuple:
            filename = f"{book}_{interval_minute.strftime('%Y-%m-%d-%H-%M-%S')}.csv"
            save_to_csv_stream(data_tuple, filename)

        # Wait until next second
        time.sleep(1 - datetime.now(timezone.utc).microsecond / 1_000_000)
