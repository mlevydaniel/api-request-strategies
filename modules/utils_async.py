import logging
import csv
import os
import shutil
import aiofiles
from google.cloud import storage
import asyncio
from functools import partial

gcs_client = storage.Client()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

async def save_to_csv_batch_async(data_list, filename):
    '''
    Save data to a CSV file

    Args:
    data_list (list): List with the data to be saved
    filename (str): Name of the file where the data will be saved
    '''
    loop = asyncio.get_running_loop()
    write_csv = partial(_write_csv, data_list, filename)
    await loop.run_in_executor(None, write_csv)
    logging.info(f"Data saved to {filename}")

def _write_csv(data_list, filename):
    with open(filename, mode='w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerows(data_list)


async def save_to_csv_stream_async(data_tuple, filename):
    '''
    Save data to a CSV file

    Args:
    data_tuple (tuple): Tuple with the data to be saved
    filename (str): Name of the file where the data will be saved
    '''
    loop = asyncio.get_running_loop()
    write_csv = partial(_write_row, data_tuple, filename)
    await loop.run_in_executor(None, write_csv)
    logging.info(f"Data saved to {filename}")

def _write_row(data_tuple, filename):
    with open(filename, mode='a', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(data_tuple)


async def store_data_to_gcs_async(gcs_bucket_name, filename):
    '''
    Store a file in Google Cloud Storage

    Args:
    gcs_bucket_name (str): Name of the GCS bucket
    filename (str): Name of the file to be stored in GCS
    '''
    year, month, day, hour = map(int, filename.split("_")[2].split("-")[0:4])
    gcs_key = f"{year}/{month}/{day}/{hour}/{filename}"

    bucket = gcs_client.bucket(gcs_bucket_name)
    blob = bucket.blob(gcs_key)

    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, blob.upload_from_filename, filename)
    logging.info(f"File {filename} uploaded successfully to {gcs_key}")

    # Remove the file after uploading
    os.remove(filename)


async def store_data_locally_async(filename):
    # Extract year, month, day, and hour from the filename
    year, month, day, hour = filename.split("_")[2].split("-")[0:4]

    # Create local directory structure
    local_dir = os.path.join("data", year, month, day, hour)
    os.makedirs(local_dir, exist_ok=True)

    # Move the file to the local directory
    local_filepath = os.path.join(local_dir, os.path.basename(filename))
    shutil.move(filename, local_filepath)

    logging.info(f"File {local_filepath} stored locally")


if __name__ == "__main__":
    # Sample data
    data_list = [
        ('2023-06-30T10:00:00Z', 'btc_mxn', 600000, 605000, 0.83),
        ('2023-06-30T10:01:00Z', 'btc_mxn', 601000, 606000, 0.82)
    ]

    data_tuple = ('2023-06-30T10:02:00Z', 'btc_mxn', 601000, 607000, 0.85)

    filename = "btc_mxn_2023-06-30-10-00-00.csv"

    asyncio.run(save_to_csv_batch_async(data_list, filename))

    asyncio.run(save_to_csv_stream_async(data_tuple, filename))

    # Store data in GCS
    gcs_bucket_name = 'bitsode'
    asyncio.run(store_data_to_gcs_async(gcs_bucket_name, filename))