import time
import hmac
import hashlib
import aiohttp
import asyncio
from urllib.parse import urlparse, urlencode
from dotenv import load_dotenv
import os
import logging
from datetime import datetime, timezone

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


# Function to make a signed request to Bitso API using aiohttp
async def make_request(api_key, api_secret, http_method, base_url, params, session):
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
        async with session.get(url, headers=headers) as response:
            return await response.json()
    elif http_method == "POST":
        async with session.post(url, headers=headers, data=json_payload) as response:
            return await response.json()
    else:
        raise ValueError("Unsupported HTTP method")


# Function to make a signed request to Bitso API and process the response
async def make_request_and_process(api_key, api_secret, http_method, base_url, params, session):
    orderbook_timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S%z")
    response = await make_request(api_key, api_secret, http_method, base_url, params, session)

    payload = response.get('payload')
    if not payload:
        logging.error(f"Error in response payload: {response}")
        return None

    best_bid = float(payload.get('bids')[0]['price'])
    best_ask = float(payload.get('asks')[0]['price'])
    spread = float(format((best_bid/best_ask) * 100 / best_ask, '.4f'))

    data_tuple = (orderbook_timestamp, params['book'], best_bid, best_ask, spread)
    logging.info(f"Bid-Ask spread: {data_tuple}")
    return data_tuple


if __name__ == "__main__":
    async def main():
        # Load API credentials from environment variables
        api_key = os.getenv("BITSO_API_KEY")
        api_secret = os.getenv("BITSO_API_SECRET")

        # Request details
        base_url = "https://stage.bitso.com/api/v3/order_book"
        params = {
            "book": "xrp_mxn"
        }
        http_method = "GET"

        # Make the request and print the response
        async with aiohttp.ClientSession() as session:
            response = await make_request_and_process(api_key, api_secret, http_method, base_url, params, session)
            print("Response Body:", response)

    asyncio.run(main())
