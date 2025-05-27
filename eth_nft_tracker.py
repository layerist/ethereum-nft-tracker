import aiohttp
import asyncio
import logging
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
    CONCURRENT_REQUESTS = 10
    LOG_LEVEL = logging.INFO

# Setup logging
logging.basicConfig(
    level=Config.LOG_LEVEL,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

async def fetch(session: aiohttp.ClientSession, url: str, retries: int = Config.MAX_RETRIES) -> Dict[str, Any]:
    """
    Fetch JSON data from the given URL with retries and exponential backoff.
    """
    for attempt in range(1, retries + 1):
        try:
            async with session.get(url, timeout=Config.REQUEST_TIMEOUT) as response:
                response.raise_for_status()
                return await response.json()
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            delay = (2 ** (attempt - 1)) + random.uniform(0, 1)
            logging.warning(f"Attempt {attempt}/{retries} failed for {url}: {e}. Retrying in {delay:.2f}s...")
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
    """
    Append unique NFT addresses to the output file.
    """
    if addresses:
        async with aiofiles.open(Config.OUTPUT_FILE, 'a') as f:
            await f.write('\n'.join(addresses) + '\n')
        logging.info(f"Saved {len(addresses)} new NFT addresses.")

async def process_block(session: aiohttp.ClientSession, semaphore: asyncio.Semaphore, seen_addresses: Set[str]) -> None:
    latest_block = await get_latest_block(session)
    if latest_block is None:
        logging.error("Could not retrieve the latest block number.")
        return

    logging.info(f"Processing block #{latest_block}")
    addresses = await get_block_transactions(session, latest_block)
    if not addresses:
        logging.info(f"No transactions found in block #{latest_block}")
        return

    logging.info(f"Found {len(addresses)} addresses. Fetching NFT data...")
    tasks = [asyncio.create_task(get_nfts_for_address(session, addr, semaphore)) for addr in addresses]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    nft_addresses = {contract for result in results if isinstance(result, set) for contract in result}
    new_addresses = nft_addresses - seen_addresses
    if new_addresses:
        await save_nft_addresses(new_addresses)
        seen_addresses.update(new_addresses)

async def main():
    semaphore = asyncio.Semaphore(Config.CONCURRENT_REQUESTS)
    seen_addresses: Set[str] = set()

    async with aiohttp.ClientSession() as session:
        while True:
            try:
                start = time()
                await process_block(session, semaphore, seen_addresses)
                elapsed = time() - start
                sleep_time = max(0, Config.SLEEP_INTERVAL - elapsed)
                logging.info(f"Sleeping for {sleep_time:.2f} seconds...")
                await asyncio.sleep(sleep_time)
            except asyncio.CancelledError:
                logging.info("Main loop cancelled. Exiting gracefully...")
                break
            except Exception as e:
                logging.exception(f"Unexpected error occurred: {e}")

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Received keyboard interrupt. Exiting...")
        sys.exit(0)
