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

# Constants
BASE_URL = "https://stage.bitso.com/api/v3/order_book"
DEFAULT_BOOK = "xrp_mxn"

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)


# Load environment variables
load_dotenv()


def generate_signature(api_secret, nonce, http_method, request_path, json_payload):
    data = f"{nonce}{http_method}{request_path}{json_payload}"
    return hmac.new(api_secret.encode('utf-8'), data.encode('utf-8'), hashlib.sha256).hexdigest()


def build_auth_header(api_key, api_secret, http_method, request_path, json_payload):
    nonce = str(int(time.time() * 1000))
    signature = generate_signature(api_secret, nonce, http_method, request_path, json_payload)
    return f"Bitso {api_key}:{nonce}:{signature}"


async def make_request(api_key, api_secret, http_method, base_url, params, session):
    url = f"{base_url}?{urlencode(params)}"
    request_path = f"{urlparse(base_url).path}?{urlencode(params)}"
    json_payload = ""

    auth_header = build_auth_header(api_key, api_secret, http_method, request_path, json_payload)

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


async def process_response(response, book):
    payload = response.get('payload')
    if not payload:
        logging.error(f"Error in response payload: {response}")
        return None

    best_bid = float(payload['bids'][0]['price'])
    best_ask = float(payload['asks'][0]['price'])
    spread = float(format((best_bid/best_ask) * 100 / best_ask, '.4f'))

    orderbook_timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S%z")
    return (orderbook_timestamp, book, best_bid, best_ask, spread)


async def make_request_and_process(api_key, api_secret, params, session=None):

    if session is None:
        async with aiohttp.ClientSession() as new_session:
            response = await make_request(api_key, api_secret, "GET", BASE_URL, params, new_session)
    else:
        response = await make_request(api_key, api_secret, "GET", BASE_URL, params, session)

    book = params.get('book')
    data_tuple = await process_response(response, book)
    if data_tuple:
        logging.info(f"Bid-Ask spread: {data_tuple}")
    return data_tuple


async def main():
    api_key = os.getenv("BITSO_API_KEY")
    api_secret = os.getenv("BITSO_API_SECRET")

    if not api_key or not api_secret:
        logging.error("API credentials not found in environment variables")
        return

    async with aiohttp.ClientSession() as session:
        result = await make_request_and_process(api_key, api_secret, session=session)
        if result:
            print("Processed Data:", result)


if __name__ == "__main__":
    asyncio.run(main())
