import logging
import csv
import os
import shutil
from google.cloud import storage

gcs_client = storage.Client()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)


def save_to_csv_batch(data_list, filename):
    with open(filename, "w", newline='') as csvfile:
        csv_writer = csv.writer(csvfile)
        for data in data_list:
            csv_writer.writerow(data)
    logging.info(f"Data saved to {filename}")


def save_to_csv_stream(data_tuple, filename):
    with open(filename, "a", newline='') as csvfile:
        csv_writer = csv.writer(csvfile)
        csv_writer.writerow(data_tuple)
    logging.info(f"Data saved to {filename}")


def store_data_to_gcs(gcs_bucket_name, filename):
    year, month, day, hour = map(int, filename.split("_")[2].split("-")[0:4])
    gcs_key = f"{year}/{month}/{day}/{hour}/{filename}"

    bucket = gcs_client.bucket(gcs_bucket_name)
    blob = bucket.blob(gcs_key)
    blob.upload_from_filename(filename)
    logging.info(f"File {filename} uploaded successfully to {gcs_key}")

    # Remove the file after uploading
    os.remove(filename)


def store_data_locally(filename):
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
    filename = "btc_mxn_2023-06-30-10-00-00.csv"

    save_to_csv_batch(data_list, filename)

    # Store data in GCS
    gcs_bucket_name = 'bitsode'
    store_data_to_gcs(gcs_bucket_name, filename)
