import requests
import json
import signal
import sys
import logging
from time import sleep

# Your Etherscan API key
ETHERSCAN_API_KEY = 'YOUR_ETHERSCAN_API_KEY'
ETHERSCAN_BASE_URL = 'https://api.etherscan.io/api'

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Signal handler for graceful shutdown
def signal_handler(sig, frame):
    logging.info('Stopping the script...')
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)

def get_latest_block():
    try:
        url = f'{ETHERSCAN_BASE_URL}?module=proxy&action=eth_blockNumber&apikey={ETHERSCAN_API_KEY}'
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        return int(data['result'], 16)
    except requests.RequestException as e:
        logging.error(f"Error fetching latest block: {e}")
        return None

def get_block_transactions(block_number):
    try:
        url = f'{ETHERSCAN_BASE_URL}?module=proxy&action=eth_getBlockByNumber&tag=0x{block_number:x}&boolean=true&apikey={ETHERSCAN_API_KEY}'
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        transactions = data['result']['transactions']
        addresses = set()
        for tx in transactions:
            if tx['from']:
                addresses.add(tx['from'])
            if tx['to']:
                addresses.add(tx['to'])
        return addresses
    except requests.RequestException as e:
        logging.error(f"Error fetching transactions for block {block_number}: {e}")
        return set()

def get_nfts_for_address(address):
    try:
        url = f'{ETHERSCAN_BASE_URL}?module=account&action=tokennfttx&address={address}&startblock=0&endblock=999999999&sort=asc&apikey={ETHERSCAN_API_KEY}'
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        return data['result']
    except requests.RequestException as e:
        logging.error(f"Error fetching NFTs for address {address}: {e}")
        return []

def main():
    latest_block = get_latest_block()
    if latest_block is None:
        logging.error("Failed to retrieve the latest block.")
        return
    
    logging.info(f'Latest Block: {latest_block}')
    
    addresses = get_block_transactions(latest_block)
    if not addresses:
        logging.info("No addresses found in the latest block.")
        return
    
    logging.info(f'Addresses in Block: {addresses}')
    
    nft_addresses = set()
    for address in addresses:
        nfts = get_nfts_for_address(address)
        for nft in nfts:
            nft_addresses.add(nft['contractAddress'])
    
    logging.info(f'NFT Addresses: {nft_addresses}')
    
    try:
        with open('nft_addresses.txt', 'w') as f:
            for nft_address in nft_addresses:
                f.write(f'{nft_address}\n')
        logging.info('NFT addresses written to nft_addresses.txt')
    except IOError as e:
        logging.error(f"Error writing to file: {e}")

if __name__ == '__main__':
    while True:
        main()
        sleep(60)  # Pause for 60 seconds before the next iteration
