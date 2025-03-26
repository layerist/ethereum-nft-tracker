import aiohttp
import asyncio
import logging
import signal
import sys
import aiofiles
from time import time
from typing import Set

# Configuration
ETHERSCAN_API_KEY = 'YOUR_ETHERSCAN_API_KEY'
ETHERSCAN_BASE_URL = 'https://api.etherscan.io/api'
SLEEP_INTERVAL = 60  # Time between processing blocks (seconds)
OUTPUT_FILE = 'nft_addresses.txt'
REQUEST_TIMEOUT = 10  # HTTP request timeout (seconds)
MAX_RETRIES = 5
CONCURRENT_REQUESTS = 5  # Limit simultaneous API requests

# Logging setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Graceful exit handler
def signal_handler(sig, frame):
    logging.info("Terminating the script...")
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)

async def fetch(session: aiohttp.ClientSession, url: str, retries: int = MAX_RETRIES) -> dict:
    """Asynchronously fetch data from API with retry logic."""
    for attempt in range(retries):
        try:
            async with session.get(url, timeout=REQUEST_TIMEOUT) as response:
                response.raise_for_status()
                return await response.json()
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            logging.warning(f"Request failed ({attempt + 1}/{retries}): {e}")
            await asyncio.sleep(2 ** attempt)  # Exponential backoff
    logging.error(f"Request failed after {retries} attempts: {url}")
    return {}

async def get_latest_block(session: aiohttp.ClientSession) -> int:
    """Fetch the latest Ethereum block number."""
    url = f"{ETHERSCAN_BASE_URL}?module=proxy&action=eth_blockNumber&apikey={ETHERSCAN_API_KEY}"
    data = await fetch(session, url)
    return int(data.get('result', '0'), 16) if data.get('result') else 0

async def get_block_transactions(session: aiohttp.ClientSession, block_number: int) -> Set[str]:
    """Retrieve unique Ethereum addresses from transactions in a block."""
    url = f"{ETHERSCAN_BASE_URL}?module=proxy&action=eth_getBlockByNumber&tag=0x{block_number:x}&boolean=true&apikey={ETHERSCAN_API_KEY}"
    data = await fetch(session, url)
    transactions = data.get('result', {}).get('transactions', [])
    return {tx.get('from') for tx in transactions if tx.get('from')} | {tx.get('to') for tx in transactions if tx.get('to')}

async def get_nfts_for_address(session: aiohttp.ClientSession, address: str, semaphore: asyncio.Semaphore) -> Set[str]:
    """Fetch NFT contract addresses associated with a specific Ethereum address."""
    async with semaphore:  # Limit concurrent requests
        url = f"{ETHERSCAN_BASE_URL}?module=account&action=tokennfttx&address={address}&startblock=0&endblock=latest&sort=asc&apikey={ETHERSCAN_API_KEY}"
        data = await fetch(session, url)
        return {nft.get('contractAddress') for nft in data.get('result', []) if nft.get('contractAddress')}

async def save_nft_addresses(nft_addresses: Set[str]) -> None:
    """Persist NFT contract addresses to a file asynchronously."""
    if nft_addresses:
        async with aiofiles.open(OUTPUT_FILE, 'a') as file:
            await file.write('\n'.join(nft_addresses) + '\n')
        logging.info(f"Saved {len(nft_addresses)} NFT addresses.")

async def process_block(session: aiohttp.ClientSession, semaphore: asyncio.Semaphore):
    """Process the latest Ethereum block and extract NFT contract addresses."""
    latest_block = await get_latest_block(session)
    if latest_block == 0:
        logging.error("Invalid block number. Skipping.")
        return
    
    logging.info(f"Processing block {latest_block}")
    addresses = await get_block_transactions(session, latest_block)
    if not addresses:
        logging.info("No transactions found in this block.")
        return

    nft_addresses = set()
    tasks = [get_nfts_for_address(session, address, semaphore) for address in addresses]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    for result in results:
        if isinstance(result, set):
            nft_addresses.update(result)
        elif isinstance(result, Exception):
            logging.error(f"Error retrieving NFT addresses: {result}")

    await save_nft_addresses(nft_addresses)

async def main():
    """Continuously fetch and process blocks for NFT contract addresses."""
    semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS)
    async with aiohttp.ClientSession() as session:
        while True:
            try:
                start_time = time()
                await process_block(session, semaphore)
                elapsed_time = time() - start_time
                sleep_time = max(SLEEP_INTERVAL - elapsed_time, 0)
                logging.info(f"Sleeping for {sleep_time:.2f} seconds...")
                await asyncio.sleep(sleep_time)
            except Exception as e:
                logging.error(f"Unexpected error: {e}")

if __name__ == '__main__':
    asyncio.run(main())
