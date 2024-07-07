import os
import logging
import asyncio
import aiohttp
from datetime import datetime, timezone
from google.cloud import storage
from modules.api_request_async import make_request_and_process
from modules.utils import save_to_csv_stream, store_data_to_gcs, store_data_locally

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# Load API credentials from environment variables
api_key = os.getenv("BITSO_API_KEY")
api_secret = os.getenv("BITSO_API_SECRET")
books = ["btc_mxn", "ltc_mxn", "xrp_mxn"]  # List of book parameters
GCS_BUCKET_NAME = "bitsode"

# Request details
base_url = "https://stage.bitso.com/api/v3/order_book"
http_method = "GET"
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


async def main():

    # Get minute when the script started
    now = datetime.now(timezone.utc)
    start_minute = now.minute
    interval_minute = round_down_minute(now)

    async with aiohttp.ClientSession() as session:
        while True:
            now = datetime.now(timezone.utc)
            current_minute = now.minute

            tasks = []
            for book in books:
                params = {"book": book}
                logging.info(f"Creating task for book: {book} with params: {params}")
                tasks.append(make_request_and_process(api_key, api_secret, http_method, base_url, params, session))

            results = await asyncio.gather(*tasks, return_exceptions=True)

            for book, result in zip(books, results):
                if isinstance(result, Exception):
                    logging.error(f"Error fetching data for {book}: {result}")
                    continue

                data_tuple = result

                if not data_tuple:
                    logging.error(f"No data fetched for {book}")
                    continue

                if current_minute % MINUTE_MULTIPLE == 0 and current_minute != start_minute:
                    filename = f"{book}_{interval_minute.strftime('%Y-%m-%d-%H-%M-%S')}.csv"

                    if STORE_IN_GCS:
                        store_data_to_gcs(GCS_BUCKET_NAME, filename)
                    else:
                        store_data_locally(filename)

                    start_minute = current_minute
                    interval_minute = round_down_minute(now)

                if data_tuple:
                    filename = f"{book}_{interval_minute.strftime('%Y-%m-%d-%H-%M-%S')}.csv"
                    save_to_csv_stream(data_tuple, filename)

            await asyncio.sleep(1 - datetime.now(timezone.utc).microsecond / 1_000_000)


if __name__ == "__main__":
    asyncio.run(main())
