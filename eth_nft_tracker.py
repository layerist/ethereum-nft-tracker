import aiohttp
import asyncio
import logging
import signal
import sys
import aiofiles
from time import time
from typing import Set, Dict, Any

# Configuration
ETHERSCAN_API_KEY = 'YOUR_ETHERSCAN_API_KEY'
ETHERSCAN_BASE_URL = 'https://api.etherscan.io/api'
SLEEP_INTERVAL = 60  # Time between processing blocks (seconds)
OUTPUT_FILE = 'nft_addresses.txt'
REQUEST_TIMEOUT = 10  # HTTP request timeout (seconds)
MAX_RETRIES = 5
CONCURRENT_REQUESTS = 5  # Limit simultaneous API requests

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Graceful shutdown
def handle_exit(sig, frame):
    logging.info("Shutting down gracefully...")
    sys.exit(0)

signal.signal(signal.SIGINT, handle_exit)
signal.signal(signal.SIGTERM, handle_exit)


async def fetch(session: aiohttp.ClientSession, url: str, retries: int = MAX_RETRIES) -> Dict[str, Any]:
    """Fetch JSON data from a URL with retries."""
    for attempt in range(1, retries + 1):
        try:
            async with session.get(url, timeout=REQUEST_TIMEOUT) as response:
                response.raise_for_status()
                return await response.json()
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            logging.warning(f"Attempt {attempt}/{retries} failed: {e}")
            await asyncio.sleep(2 ** (attempt - 1))  # Exponential backoff
    logging.error(f"Request failed after {retries} retries: {url}")
    return {}


async def get_latest_block(session: aiohttp.ClientSession) -> int:
    """Get the latest Ethereum block number."""
    url = f"{ETHERSCAN_BASE_URL}?module=proxy&action=eth_blockNumber&apikey={ETHERSCAN_API_KEY}"
    data = await fetch(session, url)
    block_hex = data.get('result')
    return int(block_hex, 16) if block_hex else 0


async def get_block_transactions(session: aiohttp.ClientSession, block_number: int) -> Set[str]:
    """Extract sender and recipient addresses from a block."""
    url = f"{ETHERSCAN_BASE_URL}?module=proxy&action=eth_getBlockByNumber&tag=0x{block_number:x}&boolean=true&apikey={ETHERSCAN_API_KEY}"
    data = await fetch(session, url)
    transactions = data.get('result', {}).get('transactions', [])
    return {tx.get('from') for tx in transactions if tx.get('from')} | \
           {tx.get('to') for tx in transactions if tx.get('to')}


async def get_nfts_for_address(session: aiohttp.ClientSession, address: str, semaphore: asyncio.Semaphore) -> Set[str]:
    """Fetch NFT contract addresses for a given wallet address."""
    async with semaphore:
        url = f"{ETHERSCAN_BASE_URL}?module=account&action=tokennfttx&address={address}&startblock=0&endblock=latest&sort=asc&apikey={ETHERSCAN_API_KEY}"
        data = await fetch(session, url)
        return {tx.get('contractAddress') for tx in data.get('result', []) if tx.get('contractAddress')}


async def save_nft_addresses(addresses: Set[str]) -> None:
    """Append NFT contract addresses to a file."""
    if addresses:
        async with aiofiles.open(OUTPUT_FILE, 'a') as f:
            await f.write('\n'.join(addresses) + '\n')
        logging.info(f"Saved {len(addresses)} NFT addresses.")


async def process_block(session: aiohttp.ClientSession, semaphore: asyncio.Semaphore) -> None:
    """Process a single Ethereum block to find NFT contracts."""
    latest_block = await get_latest_block(session)
    if latest_block == 0:
        logging.error("Failed to retrieve a valid block number.")
        return

    logging.info(f"Processing block #{latest_block}")
    addresses = await get_block_transactions(session, latest_block)
    if not addresses:
        logging.info("No transactions found in block.")
        return

    nft_addresses: Set[str] = set()
    tasks = [get_nfts_for_address(session, addr, semaphore) for addr in addresses]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    for res in results:
        if isinstance(res, set):
            nft_addresses.update(res)
        else:
            logging.error(f"NFT fetch failed: {res}")

    await save_nft_addresses(nft_addresses)


async def main():
    """Main loop for continuously processing new blocks."""
    semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS)
    async with aiohttp.ClientSession() as session:
        while True:
            try:
                start = time()
                await process_block(session, semaphore)
                elapsed = time() - start
                sleep_for = max(0, SLEEP_INTERVAL - elapsed)
                logging.info(f"Sleeping for {sleep_for:.2f} seconds...")
                await asyncio.sleep(sleep_for)
            except Exception as e:
                logging.error(f"Unexpected error: {e}", exc_info=True)


if __name__ == '__main__':
    asyncio.run(main())
