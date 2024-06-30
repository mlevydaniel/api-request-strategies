# Data Extraction Strategies

## Overview

This project pulls order book data from Bitso's API, processes it, and stores it either locally or in an GCS bucket, partitioned by year, month, day, and hour.

## Setup Environment

### Install Python and dependencies

To set up the environment, follow these steps:

1. Create a virtual environment:
    ```sh
    python3.10 -m venv venv
    ```

2. Install the required packages:
    ```sh
    pip install -r requirements.txt
    ```

3. Activate the virtual environment:
    ```sh
    source venv/bin/activate
    ```

### Define Environment Variables

To use the Bitso API and optionally store data in a GCS bucket, you need to set up the required environment variables in a .env file. This file should include your Bitso API Key and Secret, along with your GCP Servie Account Key if you plan to store data in GCS.

```yaml
BITSO_API_KEY=your_bitso_api_key
BITSO_API_SECRET=your_bitso_api_secret
GCP_CREDENTIALS=your_gcp_service_account_json
```


## Project Structure

```yaml
.
├── data/ # Directory where data is stored
├── modules/ # Directory containing scripts
│── main.py # Script for pulling one book per second
│── main_threading.py # Script using threading to handle latency
│── main_multiple_books.py # Script for pulling data for multiple books simultaneously
├── requirements.txt # List of dependencies
└── README.md # Project documentation
```


## Execute code


The code is divided in three different versions. The first one called main.py runs the script pulling only one book per second

```
python3 main.py
```

Because there could be some latency issues, there is another version called main_threading.py that runs the same but using threadings to improve the data extraction. This one is more robust to latency issues

```
python3 main_threading.py
```

And finally, the main_multiple_books.py pulls data for multiple books simultaneously. However, because the API let you pull up to 60 times per minute, it may raise and error due to limits

```
python3 main_multiple_books.py
```

## Partitioning

The data is stored in the same folder partitioned by Year/Month/Day/Hour. This way it is possible to filter at hour level keeping a good balance between granularity and spreaded data

Benefits of this Partitioning Strategy:

Efficient Filtering: By partitioning the data at the hourly level is useful for time-series analysis and reporting.

Improved Performance: This approach minimizes the amount of data that needs to be scanned during queries.

Scalability: can be particularly beneficial when working with distributed storage systems like S3.


Example of directory structure:

```yaml
data/
├── 2023/
│   ├── 01/
│   │   ├── 01/
│   │   │   ├── 00/
│   │   │   │   ├── datafile1.json
│   │   │   │   └── datafile2.json
│   │   │   ├── 01/
│   │   │   │   ├── datafile1.json
│   │   │   │   └── datafile2.json
│   │   │   └── ...
│   ├── 02/
│   └── ...
├── 2024/
└── ...
```
