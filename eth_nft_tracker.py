import aiohttp
import asyncio
import logging
import signal
import sys
import aiofiles
import random
from time import time
from typing import Set, Dict, Any, Optional

# Configuration
class Config:
    API_KEY = 'YOUR_ETHERSCAN_API_KEY'
    BASE_URL = 'https://api.etherscan.io/api'
    SLEEP_INTERVAL = 60
    OUTPUT_FILE = 'nft_addresses.txt'
    REQUEST_TIMEOUT = 10
    MAX_RETRIES = 5
    CONCURRENT_REQUESTS = 5


# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)


# Graceful shutdown
def handle_exit(sig, frame):
    logging.info("Received shutdown signal. Exiting...")
    sys.exit(0)

signal.signal(signal.SIGINT, handle_exit)
signal.signal(signal.SIGTERM, handle_exit)


async def fetch(session: aiohttp.ClientSession, url: str, retries: int = Config.MAX_RETRIES) -> Dict[str, Any]:
    for attempt in range(1, retries + 1):
        try:
            async with session.get(url, timeout=Config.REQUEST_TIMEOUT) as response:
                response.raise_for_status()
                return await response.json()
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            delay = (2 ** (attempt - 1)) + random.uniform(0, 1)
            logging.warning(f"Attempt {attempt}/{retries} failed: {e}. Retrying in {delay:.2f}s...")
            await asyncio.sleep(delay)
    logging.error(f"Request failed after {retries} retries: {url}")
    return {}


async def get_latest_block(session: aiohttp.ClientSession) -> Optional[int]:
    url = f"{Config.BASE_URL}?module=proxy&action=eth_blockNumber&apikey={Config.API_KEY}"
    data = await fetch(session, url)
    block_hex = data.get('result')
    return int(block_hex, 16) if block_hex else None


async def get_block_transactions(session: aiohttp.ClientSession, block_number: int) -> Set[str]:
    url = f"{Config.BASE_URL}?module=proxy&action=eth_getBlockByNumber&tag=0x{block_number:x}&boolean=true&apikey={Config.API_KEY}"
    data = await fetch(session, url)
    txs = data.get('result', {}).get('transactions', [])
    return {tx.get('from') for tx in txs if tx.get('from')} | {tx.get('to') for tx in txs if tx.get('to')}


async def get_nfts_for_address(session: aiohttp.ClientSession, address: str, semaphore: asyncio.Semaphore) -> Set[str]:
    async with semaphore:
        url = f"{Config.BASE_URL}?module=account&action=tokennfttx&address={address}&startblock=0&endblock=latest&sort=asc&apikey={Config.API_KEY}"
        data = await fetch(session, url)
        return {tx['contractAddress'] for tx in data.get('result', []) if tx.get('contractAddress')}


async def save_nft_addresses(addresses: Set[str]) -> None:
    if addresses:
        async with aiofiles.open(Config.OUTPUT_FILE, 'a') as f:
            await f.write('\n'.join(addresses) + '\n')
        logging.info(f"Saved {len(addresses)} unique NFT addresses.")


async def process_block(session: aiohttp.ClientSession, semaphore: asyncio.Semaphore) -> None:
    latest_block = await get_latest_block(session)
    if latest_block is None:
        logging.error("Failed to retrieve latest block.")
        return

    logging.info(f"Processing block #{latest_block}")
    addresses = await get_block_transactions(session, latest_block)
    if not addresses:
        logging.info(f"No transactions found in block #{latest_block}")
        return

    logging.info(f"Found {len(addresses)} unique addresses. Fetching NFT data...")

    tasks = [get_nfts_for_address(session, addr, semaphore) for addr in addresses]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    nft_addresses = set()
    for res in results:
        if isinstance(res, set):
            nft_addresses.update(res)
        else:
            logging.error(f"Error fetching NFT data: {res}")

    await save_nft_addresses(nft_addresses)


async def main():
    semaphore = asyncio.Semaphore(Config.CONCURRENT_REQUESTS)
    async with aiohttp.ClientSession() as session:
        while True:
            try:
                start = time()
                await process_block(session, semaphore)
                elapsed = time() - start
                sleep_time = max(0, Config.SLEEP_INTERVAL - elapsed)
                logging.info(f"Sleeping for {sleep_time:.2f} seconds...")
                await asyncio.sleep(sleep_time)
            except asyncio.CancelledError:
                logging.info("Main loop cancelled. Exiting...")
                break
            except Exception as e:
                logging.error(f"Unexpected error: {e}", exc_info=True)


if __name__ == '__main__':
    asyncio.run(main())
