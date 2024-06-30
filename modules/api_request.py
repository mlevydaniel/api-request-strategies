import time
import hmac
import hashlib
import requests
from urllib.parse import urlparse, urlencode
from dotenv import load_dotenv
import os
import logging
from datetime import datetime, timezone
from google.cloud import storage

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Load environment variables from .env file
load_dotenv()


# Function to generate the signature
def generate_signature(api_secret, nonce, http_method, request_path, json_payload):
    data = f"{nonce}{http_method}{request_path}{json_payload}"
    signature = hmac.new(api_secret.encode('utf-8'), data.encode('utf-8'), hashlib.sha256).hexdigest()
    return signature


# Function to make a signed request to Bitso API
def make_request(api_key, api_secret, http_method, base_url, params):
    url = f"{base_url}?{urlencode(params)}"
    request_path = f"{urlparse(base_url).path}?{urlencode(params)}"
    json_payload = ""

    # Generate nonce
    nonce = str(int(time.time() * 1000))

    # Generate signature
    signature = generate_signature(api_secret, nonce, http_method, request_path, json_payload)

    # Build the authorization header
    auth_header = f"Bitso {api_key}:{nonce}:{signature}"

    # Make the request
    headers = {
        'Authorization': auth_header,
        'Content-Type': 'application/json'
    }

    if http_method == "GET":
        response = requests.get(url, headers=headers)
    elif http_method == "POST":
        response = requests.post(url, headers=headers, data=json_payload)
    else:
        raise ValueError("Unsupported HTTP method")

    return response


def make_request_and_process(api_key, api_secret, http_method, base_url, params):

    '''
    Function to make a signed request to Bitso API and process the response

    Parameters:
    api_key (str): Bitso API key
    api_secret (str): Bitso API secret
    http_method (str): HTTP method (GET or POST)
    base_url (str): Base URL for the request
    params (dict): Request parameters
    '''

    orderbook_timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S%z")
    response = make_request(api_key, api_secret, http_method, base_url, params)

    # logging.info("Status Code:", response.status_code)
    response = response.json().get('payload')

    best_bid = float(response.get('bids')[0]['price'])
    best_ask = float(response.get('asks')[0]['price'])
    spread = float(format((best_bid/best_ask) * 100 / best_ask, '.4f'))

    data_tuple = (orderbook_timestamp, params['book'], best_bid, best_ask, spread)
    logging.info(f"Bid-Ask spread: {data_tuple}")
    return data_tuple


if __name__ == "__main__":
    # Load API credentials from environment variables
    api_key = os.getenv("BITSO_API_KEY")
    api_secret = os.getenv("BITSO_API_SECRET")

    # Initialize GCS client
    gcs_client = storage.Client()

    # Request details
    base_url = "https://stage.bitso.com/api/v3/order_book"
    params = {
        "book": "btc_mxn"
    }
    http_method = "GET"

    # Make the request and print the response
    response = make_request(api_key, api_secret, http_method, base_url, params)
    print("Status Code:", response.status_code)
    print("Response Body:", response.json())
