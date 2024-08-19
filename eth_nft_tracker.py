import requests
import json
import signal
import sys
import logging
from time import sleep
from retrying import retry

# Configuration
ETHERSCAN_API_KEY = 'YOUR_ETHERSCAN_API_KEY'
ETHERSCAN_BASE_URL = 'https://api.etherscan.io/api'
SLEEP_INTERVAL = 60  # Interval between requests, in seconds
OUTPUT_FILE = 'nft_addresses.txt'

# Logging configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Signal handler for graceful shutdown
def signal_handler(sig, frame):
    logging.info('Stopping the script...')
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)

# Function to retry requests upon failure
@retry(wait_fixed=2000, stop_max_attempt_number=5)
def make_request(url):
    response = requests.get(url)
    response.raise_for_status()
    return response.json()

def get_latest_block():
    url = f'{ETHERSCAN_BASE_URL}?module=proxy&action=eth_blockNumber&apikey={ETHERSCAN_API_KEY}'
    data = make_request(url)
    return int(data['result'], 16)

def get_block_transactions(block_number):
    url = f'{ETHERSCAN_BASE_URL}?module=proxy&action=eth_getBlockByNumber&tag=0x{block_number:x}&boolean=true&apikey={ETHERSCAN_API_KEY}'
    data = make_request(url)
    transactions = data['result']['transactions']
    addresses = {tx['from'] for tx in transactions if tx['from']}
    addresses.update({tx['to'] for tx in transactions if tx['to']})
    return addresses

def get_nfts_for_address(address):
    url = f'{ETHERSCAN_BASE_URL}?module=account&action=tokennfttx&address={address}&startblock=0&endblock=999999999&sort=asc&apikey={ETHERSCAN_API_KEY}'
    data = make_request(url)
    return {nft['contractAddress'] for nft in data['result']}

def save_nft_addresses(nft_addresses):
    with open(OUTPUT_FILE, 'w') as f:
        f.write('\n'.join(nft_addresses))
    logging.info(f'NFT addresses saved to {OUTPUT_FILE}')

def main():
    try:
        latest_block = get_latest_block()
        logging.info(f'Latest Block: {latest_block}')

        addresses = get_block_transactions(latest_block)
        if not addresses:
            logging.info("No addresses found in the latest block.")
            return

        logging.info(f'Addresses in Block: {addresses}')

        nft_addresses = set()
        for address in addresses:
            nft_addresses.update(get_nfts_for_address(address))

        if nft_addresses:
            logging.info(f'NFT Addresses: {nft_addresses}')
            save_nft_addresses(nft_addresses)
        else:
            logging.info('No NFT addresses found.')

    except requests.RequestException as e:
        logging.error(f"Network error: {e}")
    except Exception as e:
        logging.error(f"Unexpected error: {e}")

if __name__ == '__main__':
    while True:
        main()
        logging.info(f'Waiting for {SLEEP_INTERVAL} seconds before the next iteration...')
        sleep(SLEEP_INTERVAL)
