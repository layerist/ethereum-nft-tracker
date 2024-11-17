import requests
import json
import signal
import sys
import logging
from time import sleep
from retrying import retry
from urllib.parse import urljoin

# Configuration
ETHERSCAN_API_KEY = 'YOUR_ETHERSCAN_API_KEY'
ETHERSCAN_BASE_URL = 'https://api.etherscan.io/api'
SLEEP_INTERVAL = 60  # Interval between requests in seconds
OUTPUT_FILE = 'nft_addresses.txt'
REQUEST_TIMEOUT = 10  # Timeout for HTTP requests in seconds

# Logging configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def signal_handler(sig: int, frame) -> None:
    """Handle graceful shutdown on interrupt signals."""
    logging.info('Stopping the script...')
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)

@retry(wait_fixed=2000, stop_max_attempt_number=5)
def make_request(url: str) -> dict:
    """
    Make a GET request to the specified URL with retries on failure.

    Args:
        url (str): The URL to request.

    Returns:
        dict: The JSON response.
    """
    try:
        logging.debug(f"Making request to URL: {url}")
        response = requests.get(url, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        logging.error(f"Request failed: {e}")
        raise

def get_latest_block() -> int:
    """Retrieve the latest Ethereum block number."""
    url = urljoin(ETHERSCAN_BASE_URL, f'?module=proxy&action=eth_blockNumber&apikey={ETHERSCAN_API_KEY}')
    data = make_request(url)
    try:
        return int(data.get('result', '0'), 16)
    except (ValueError, KeyError):
        logging.error("Failed to parse block number from response.")
        return 0

def get_block_transactions(block_number: int) -> set[str]:
    """Retrieve unique addresses involved in transactions from a specific block."""
    url = urljoin(ETHERSCAN_BASE_URL, f'?module=proxy&action=eth_getBlockByNumber&tag=0x{block_number:x}&boolean=true&apikey={ETHERSCAN_API_KEY}')
    data = make_request(url)
    transactions = data.get('result', {}).get('transactions', [])

    return {tx.get('from') for tx in transactions if tx.get('from')} | {tx.get('to') for tx in transactions if tx.get('to')}

def get_nfts_for_address(address: str) -> set[str]:
    """Retrieve NFT contract addresses associated with a specific Ethereum address."""
    url = urljoin(ETHERSCAN_BASE_URL, f'?module=account&action=tokennfttx&address={address}&startblock=0&endblock=999999999&sort=asc&apikey={ETHERSCAN_API_KEY}')
    data = make_request(url)
    return {nft.get('contractAddress') for nft in data.get('result', []) if 'contractAddress' in nft}

def save_nft_addresses(nft_addresses: set[str]) -> None:
    """Save NFT contract addresses to a file."""
    if nft_addresses:
        try:
            with open(OUTPUT_FILE, 'a') as f:
                f.write('\n'.join(nft_addresses) + '\n')
            logging.info(f"Saved {len(nft_addresses)} NFT addresses to {OUTPUT_FILE}")
        except IOError as e:
            logging.error(f"Failed to save NFT addresses: {e}")
    else:
        logging.info("No new NFT addresses to save.")

def process_block() -> None:
    """Process the latest block and extract NFT addresses."""
    latest_block = get_latest_block()
    if latest_block == 0:
        logging.error("Failed to retrieve a valid block number.")
        return

    logging.info(f"Processing block: {latest_block}")
    addresses = get_block_transactions(latest_block)
    if not addresses:
        logging.info("No addresses found in the latest block.")
        return

    nft_addresses = set()
    for address in addresses:
        nft_addresses.update(get_nfts_for_address(address))

    save_nft_addresses(nft_addresses)

def main() -> None:
    """Continuously fetch and save NFT addresses from the blockchain."""
    while True:
        try:
            process_block()
        except requests.RequestException as e:
            logging.error(f"Network error: {e}")
        except Exception as e:
            logging.error(f"Unexpected error: {e}")
        
        logging.info(f"Waiting {SLEEP_INTERVAL} seconds for the next iteration...")
        sleep(SLEEP_INTERVAL)

if __name__ == '__main__':
    main()
