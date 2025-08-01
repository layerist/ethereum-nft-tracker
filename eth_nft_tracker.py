import aiohttp
import asyncio
import logging
import sys
import aiofiles
import random
from dataclasses import dataclass
from time import time
from typing import Set, Dict, Any, Optional

@dataclass(frozen=True)
class Config:
    API_KEY: str = 'YOUR_ETHERSCAN_API_KEY'
    BASE_URL: str = 'https://api.etherscan.io/api'
    SLEEP_INTERVAL: int = 60
    OUTPUT_FILE: str = 'nft_addresses.txt'
    REQUEST_TIMEOUT: int = 10
    MAX_RETRIES: int = 5
    CONCURRENT_REQUESTS: int = 10
    LOG_LEVEL: int = logging.INFO

# Logging setup
logging.basicConfig(
    level=Config.LOG_LEVEL,
    format='%(asctime)s | %(levelname)s | %(message)s'
)

async def retry_request(session: aiohttp.ClientSession, url: str, retries: int) -> Optional[Dict[str, Any]]:
    for attempt in range(1, retries + 1):
        try:
            async with session.get(url, timeout=Config.REQUEST_TIMEOUT) as response:
                response.raise_for_status()
                return await response.json()
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            delay = min(30, (2 ** attempt) + random.random())
            logging.warning(f"[Retry {attempt}/{retries}] Failed to fetch {url}: {e}. Retrying in {delay:.2f}s.")
            await asyncio.sleep(delay)
    logging.error(f"[Final failure] Could not fetch URL: {url}")
    return None

async def fetch(session: aiohttp.ClientSession, url: str) -> Dict[str, Any]:
    data = await retry_request(session, url, Config.MAX_RETRIES)
    return data or {}

async def get_latest_block(session: aiohttp.ClientSession) -> Optional[int]:
    url = f"{Config.BASE_URL}?module=proxy&action=eth_blockNumber&apikey={Config.API_KEY}"
    data = await fetch(session, url)
    hex_block = data.get("result")
    if hex_block:
        return int(hex_block, 16)
    logging.error("No 'result' field in blockNumber response.")
    return None

async def get_block_transactions(session: aiohttp.ClientSession, block_number: int) -> Set[str]:
    hex_block = f"0x{block_number:x}"
    url = f"{Config.BASE_URL}?module=proxy&action=eth_getBlockByNumber&tag={hex_block}&boolean=true&apikey={Config.API_KEY}"
    data = await fetch(session, url)
    txs = data.get("result", {}).get("transactions", [])
    addresses = {tx.get("from") for tx in txs if tx.get("from")}
    addresses |= {tx.get("to") for tx in txs if tx.get("to")}
    return addresses

async def get_nfts_for_address(session: aiohttp.ClientSession, address: str, semaphore: asyncio.Semaphore) -> Set[str]:
    async with semaphore:
        url = (
            f"{Config.BASE_URL}?module=account&action=tokennfttx"
            f"&address={address}&startblock=0&endblock=latest&sort=asc&apikey={Config.API_KEY}"
        )
        data = await fetch(session, url)
        if not isinstance(data.get("result"), list):
            logging.warning(f"Invalid NFT data for address: {address}")
            return set()
        return {
            tx.get("contractAddress")
            for tx in data["result"]
            if tx.get("contractAddress")
        }

async def save_nft_addresses(addresses: Set[str]) -> None:
    if not addresses:
        return
    async with aiofiles.open(Config.OUTPUT_FILE, "a") as file:
        await file.write('\n'.join(addresses) + '\n')
    logging.info(f"Saved {len(addresses)} new NFT contract addresses.")

async def process_latest_block(
    session: aiohttp.ClientSession,
    semaphore: asyncio.Semaphore,
    seen_addresses: Set[str],
    seen_blocks: Set[int]
) -> None:
    latest_block = await get_latest_block(session)
    if latest_block is None or latest_block in seen_blocks:
        logging.info(f"Skipping block #{latest_block} (already processed or unavailable).")
        return

    logging.info(f"Fetching transactions from block #{latest_block}...")
    unique_addresses = await get_block_transactions(session, latest_block)
    if not unique_addresses:
        logging.info(f"No addresses found in block #{latest_block}.")
        seen_blocks.add(latest_block)
        return

    logging.info(f"Found {len(unique_addresses)} unique addresses.")

    tasks = [get_nfts_for_address(session, addr, semaphore) for addr in unique_addresses]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    nft_addresses = {
        contract
        for res in results
        if isinstance(res, set)
        for contract in res
    }

    new_contracts = nft_addresses - seen_addresses
    if new_contracts:
        await save_nft_addresses(new_contracts)
        seen_addresses.update(new_contracts)

    seen_blocks.add(latest_block)

async def main():
    semaphore = asyncio.Semaphore(Config.CONCURRENT_REQUESTS)
    seen_addresses: Set[str] = set()
    seen_blocks: Set[int] = set()

    async with aiohttp.ClientSession() as session:
        while True:
            try:
                start = time()
                await process_latest_block(session, semaphore, seen_addresses, seen_blocks)
                elapsed = time() - start
                sleep_time = max(0, Config.SLEEP_INTERVAL - elapsed)
                logging.info(f"Sleeping for {sleep_time:.2f}s before next block check.")
                await asyncio.sleep(sleep_time)
            except asyncio.CancelledError:
                logging.info("Script cancelled by user.")
                break
            except Exception as e:
                logging.exception(f"Unhandled error: {e}")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Interrupted by user. Exiting.")
        sys.exit(0)
