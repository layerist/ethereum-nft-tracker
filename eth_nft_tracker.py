import requests
import json
import signal
import sys
import logging
from time import sleep
from retrying import retry
from urllib.parse import urljoin
from typing import Set

# Configuration
ETHERSCAN_API_KEY = 'YOUR_ETHERSCAN_API_KEY'
ETHERSCAN_BASE_URL = 'https://api.etherscan.io/api'
SLEEP_INTERVAL = 60  # Interval between processing blocks (seconds)
OUTPUT_FILE = 'nft_addresses.txt'
REQUEST_TIMEOUT = 10  # HTTP request timeout (seconds)
MAX_RETRIES = 5
RETRY_DELAY_MS = 2000

# Logging configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def signal_handler(sig: int, frame) -> None:
    """Gracefully exit the script on interruption signals."""
    logging.info('Terminating the script...')
    sys.exit(0)


signal.signal(signal.SIGINT, signal_handler)


@retry(wait_fixed=RETRY_DELAY_MS, stop_max_attempt_number=MAX_RETRIES)
def make_request(url: str) -> dict:
    """
    Perform a GET request to the given URL with retry logic on failure.

    Args:
        url (str): The API endpoint to query.

    Returns:
        dict: Parsed JSON response.
    """
    try:
        logging.debug(f"Requesting URL: {url}")
        response = requests.get(url, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        logging.error(f"Error during request: {e}")
        raise


def get_latest_block() -> int:
    """Fetch the latest Ethereum block number."""
    url = urljoin(ETHERSCAN_BASE_URL, f'?module=proxy&action=eth_blockNumber&apikey={ETHERSCAN_API_KEY}')
    data = make_request(url)
    try:
        block_number = int(data.get('result', '0'), 16)
        logging.info(f"Retrieved latest block number: {block_number}")
        return block_number
    except (ValueError, KeyError):
        logging.error("Failed to parse the block number from the API response.")
        return 0


def get_block_transactions(block_number: int) -> Set[str]:
    """
    Retrieve unique Ethereum addresses from transactions in a specific block.

    Args:
        block_number (int): Block number to query.

    Returns:
        Set[str]: Set of unique Ethereum addresses.
    """
    url = urljoin(ETHERSCAN_BASE_URL, f'?module=proxy&action=eth_getBlockByNumber&tag=0x{block_number:x}&boolean=true&apikey={ETHERSCAN_API_KEY}')
    data = make_request(url)
    transactions = data.get('result', {}).get('transactions', [])
    addresses = {tx.get('from') for tx in transactions if tx.get('from')} | \
                {tx.get('to') for tx in transactions if tx.get('to')}
    logging.debug(f"Extracted {len(addresses)} addresses from block {block_number}.")
    return addresses


def get_nfts_for_address(address: str) -> Set[str]:
    """
    Fetch NFT contract addresses associated with a specific Ethereum address.

    Args:
        address (str): Ethereum address to query.

    Returns:
        Set[str]: Set of NFT contract addresses.
    """
    url = urljoin(ETHERSCAN_BASE_URL, f'?module=account&action=tokennfttx&address={address}&startblock=0&endblock=999999999&sort=asc&apikey={ETHERSCAN_API_KEY}')
    data = make_request(url)
    nft_addresses = {nft.get('contractAddress') for nft in data.get('result', []) if nft.get('contractAddress')}
    logging.debug(f"Found {len(nft_addresses)} NFT contract addresses for {address}.")
    return nft_addresses


def save_nft_addresses(nft_addresses: Set[str]) -> None:
    """
    Persist NFT contract addresses to a file.

    Args:
        nft_addresses (Set[str]): Set of NFT addresses to save.
    """
    if nft_addresses:
        try:
            with open(OUTPUT_FILE, 'a') as file:
                file.write('\n'.join(nft_addresses) + '\n')
            logging.info(f"Saved {len(nft_addresses)} NFT addresses to {OUTPUT_FILE}.")
        except IOError as e:
            logging.error(f"Failed to write NFT addresses to file: {e}")
    else:
        logging.info("No NFT addresses to save.")


def process_block() -> None:
    """Process the latest Ethereum block and extract NFT contract addresses."""
    latest_block = get_latest_block()
    if latest_block == 0:
        logging.error("Invalid block number retrieved. Skipping this iteration.")
        return

    logging.info(f"Processing block: {latest_block}")
    addresses = get_block_transactions(latest_block)
    if not addresses:
        logging.info("No transactions found in the block.")
        return

    nft_addresses = set()
    for address in addresses:
        nft_addresses.update(get_nfts_for_address(address))

    save_nft_addresses(nft_addresses)


def main() -> None:
    """Continuously fetch and process blocks for NFT contract addresses."""
    while True:
        try:
            process_block()
        except requests.RequestException as e:
            logging.error(f"Network error: {e}")
        except Exception as e:
            logging.error(f"Unexpected error: {e}")
        
        logging.info(f"Sleeping for {SLEEP_INTERVAL} seconds...")
        sleep(SLEEP_INTERVAL)


if __name__ == '__main__':
    main()
