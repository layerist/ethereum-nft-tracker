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
REQUEST_TIMEOUT = 10  # Timeout for HTTP requests in seconds

# Logging configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def signal_handler(sig, frame):
    """Handle graceful shutdown on interrupt signals."""
    logging.info('Stopping the script...')
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)

@retry(wait_fixed=2000, stop_max_attempt_number=5)
def make_request(url):
    """
    Make a GET request to the specified URL with retries on failure.

    Args:
        url (str): The URL to request.

    Returns:
        dict: The JSON response.
    """
    try:
        response = requests.get(url, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        logging.error(f"Request failed: {e}")
        raise

def get_latest_block():
    """
    Retrieve the latest Ethereum block number.

    Returns:
        int: The latest block number.
    """
    url = f'{ETHERSCAN_BASE_URL}?module=proxy&action=eth_blockNumber&apikey={ETHERSCAN_API_KEY}'
    data = make_request(url)
    return int(data['result'], 16)

def get_block_transactions(block_number):
    """
    Retrieve transactions from a specific Ethereum block.

    Args:
        block_number (int): The block number to query.

    Returns:
        set: A set of addresses involved in transactions in the block.
    """
    url = f'{ETHERSCAN_BASE_URL}?module=proxy&action=eth_getBlockByNumber&tag=0x{block_number:x}&boolean=true&apikey={ETHERSCAN_API_KEY}'
    data = make_request(url)
    transactions = data['result']['transactions']
    
    return {tx['from'] for tx in transactions if tx.get('from')} | {tx['to'] for tx in transactions if tx.get('to')}

def get_nfts_for_address(address):
    """
    Retrieve NFT contract addresses associated with a specific Ethereum address.

    Args:
        address (str): The Ethereum address to query.

    Returns:
        set: A set of NFT contract addresses.
    """
    url = f'{ETHERSCAN_BASE_URL}?module=account&action=tokennfttx&address={address}&startblock=0&endblock=999999999&sort=asc&apikey={ETHERSCAN_API_KEY}'
    data = make_request(url)
    return {nft['contractAddress'] for nft in data['result'] if 'contractAddress' in nft}

def save_nft_addresses(nft_addresses):
    """
    Save NFT contract addresses to a file.

    Args:
        nft_addresses (set): A set of NFT contract addresses.
    """
    if not nft_addresses:
        logging.info("No NFT addresses to save.")
        return

    with open(OUTPUT_FILE, 'a') as f:
        f.write('\n'.join(nft_addresses) + '\n')

    logging.info(f'NFT addresses saved to {OUTPUT_FILE}')

def process_block():
    """Process a single block and extract NFT addresses."""
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

def main():
    """Main function to continually fetch and save NFT addresses."""
    while True:
        try:
            process_block()
        except requests.RequestException as e:
            logging.error(f"Network error: {e}")
        except Exception as e:
            logging.error(f"Unexpected error: {e}")
        
        logging.info(f'Waiting for {SLEEP_INTERVAL} seconds before the next iteration...')
        sleep(SLEEP_INTERVAL)

if __name__ == '__main__':
    main()
