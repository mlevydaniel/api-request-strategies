import logging
import csv
import os
import shutil
from google.cloud import storage
import asyncio
import aiofiles
from google.api_core import retry


gcs_client = storage.Client()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

async def save_to_csv_batch_async(data_list, filename):
    """
    Save data to a CSV file asynchronously
    """
    async with aiofiles.open(filename, mode='w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        for row in data_list:
            await csvfile.write(','.join(map(str, row)) + '\n')
    logging.info(f"Data saved to {filename}")

async def save_to_csv_stream_async(data_tuple, filename):
    """
    Append data to a CSV file asynchronously
    """
    async with aiofiles.open(filename, mode='a', newline='') as csvfile:
        await csvfile.write(','.join(map(str, data_tuple)) + '\n')
    logging.info(f"Data appended to {filename}")

@retry.Retry(predicate=retry.if_exception_type(Exception))
async def store_data_to_gcs_async(gcs_bucket_name, filename):
    """
    Store a file in Google Cloud Storage asynchronously
    """
    try:
        year, month, day, hour = map(int, filename.split("_")[2].split("-")[0:4])
        gcs_key = f"{year}/{month}/{day}/{hour}/{filename}"
        bucket = gcs_client.bucket(gcs_bucket_name)
        blob = bucket.blob(gcs_key)

        await asyncio.to_thread(blob.upload_from_filename, filename)
        logging.info(f"File {filename} uploaded successfully to {gcs_key}")

        # Remove the file after uploading
        await asyncio.to_thread(os.remove, filename)

    except Exception as e:
        logging.error(f"Error uploading {filename} to GCS: {e}")
        raise


async def store_data_locally_async(filename):
    """
    Store data locally asynchronously
    """
    year, month, day, hour = filename.split("_")[2].split("-")[0:4]
    local_dir = os.path.join("data", year, month, day, hour)
    await asyncio.to_thread(os.makedirs, local_dir, exist_ok=True)

    local_filepath = os.path.join(local_dir, os.path.basename(filename))
    await asyncio.to_thread(shutil.move, filename, local_filepath)
    logging.info(f"File {local_filepath} stored locally")

if __name__ == "__main__":
    # Sample data
    data_list = [
        ('2023-06-30T10:00:00Z', 'btc_mxn', 600000, 605000, 0.83),
        ('2023-06-30T10:01:00Z', 'btc_mxn', 601000, 606000, 0.82)
    ]
    data_tuple = ('2023-06-30T10:02:00Z', 'btc_mxn', 601000, 607000, 0.85)
    filename = "btc_mxn_2023-06-30-10-00-00.csv"

    async def run_tests():
        await save_to_csv_batch_async(data_list, filename)
        await save_to_csv_stream_async(data_tuple, filename)

        # Store data in GCS
        gcs_bucket_name = 'bitsode'
        await store_data_to_gcs_async(gcs_bucket_name, filename)

    asyncio.run(run_tests())
