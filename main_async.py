import os
import logging
import asyncio
import aiohttp
from datetime import datetime, timezone, timedelta
from google.cloud import storage
from modules.api_request_async import make_request_and_process
from modules.utils_async import (
    save_to_csv_stream_async,
    store_data_to_gcs_async,
    store_data_locally_async
)

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Load API credentials from environment variables
api_key = os.getenv("BITSO_API_KEY")
api_secret = os.getenv("BITSO_API_SECRET")
# books = ["btc_mxn", "ltc_mxn", "xrp_mxn"]  # List of book parameters
books = ['btc_mxn']

# Constants
GCS_BUCKET_NAME = "bitsode"
BASE_URL = "https://stage.bitso.com/api/v3/order_book"
HTTP_METHOD = "GET"
STORE_IN_GCS = True
MINUTE_MULTIPLE = 2

# Initialize GCS client
gcs_client = storage.Client()


def round_down_minute(current_time):
    """
    Round down the current time to the nearest multiple of MINUTE_MULTIPLE
    """
    minutes = (current_time.minute + (MINUTE_MULTIPLE // 2)) // MINUTE_MULTIPLE * MINUTE_MULTIPLE
    return current_time.replace(minute=minutes % 60, second=0, microsecond=0)


async def process_book(book, api_key, api_secret, HTTP_METHOD, BASE_URL, session, current_interval):
    params = {"book": book}
    logging.info(f"Processing book: {book} with params: {params}")

    try:
        data_tuple = await make_request_and_process(api_key, api_secret, HTTP_METHOD, BASE_URL, params, session)

        if data_tuple:
            filename = f"{book}_{current_interval.strftime('%Y-%m-%d-%H-%M-%S')}.csv"
            await save_to_csv_stream_async(data_tuple, filename)
            return book, filename
        else:
            logging.error(f"No data fetched for {book}")
            return book, None
    except Exception as e:
        logging.error(f"Error processing {book}: {e}")
        return book, None


async def store_files(files_to_store):
    store_tasks = []
    for book, filename in files_to_store:
        if filename:
            if STORE_IN_GCS:
                store_tasks.append(store_data_to_gcs_async(GCS_BUCKET_NAME, filename))
            else:
                store_tasks.append(store_data_locally_async(filename))

    if store_tasks:
        await asyncio.gather(*store_tasks)


async def main():
    current_interval = round_down_minute(datetime.now(timezone.utc))
    next_interval = current_interval + timedelta(minutes=MINUTE_MULTIPLE)

    async with aiohttp.ClientSession() as session:
        while True:
            now = datetime.now(timezone.utc)

            tasks = [process_book(book, api_key, api_secret, HTTP_METHOD, BASE_URL, session, current_interval) for book in books]
            results = await asyncio.gather(*tasks)

            if now > next_interval:
                await store_files(results)
                current_interval = round_down_minute(now)
                next_interval = current_interval + timedelta(minutes=MINUTE_MULTIPLE)

            await asyncio.sleep(1 - datetime.now(timezone.utc).microsecond / 1_000_000)


if __name__ == "__main__":
    asyncio.run(main())
