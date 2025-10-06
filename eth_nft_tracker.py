import aiohttp
import asyncio
import logging
import sys
import aiofiles
import random
from dataclasses import dataclass
from time import time
from typing import Set, Dict, Any, Optional, List


@dataclass(frozen=True)
class Config:
    API_KEY: str = "YOUR_ETHERSCAN_API_KEY"
    BASE_URL: str = "https://api.etherscan.io/api"
    SLEEP_INTERVAL: int = 60
    OUTPUT_FILE: str = "nft_addresses.txt"
    REQUEST_TIMEOUT: int = 10
    MAX_RETRIES: int = 5
    CONCURRENT_REQUESTS: int = 10
    LOG_LEVEL: int = logging.INFO
    MAX_BACKOFF: int = 30
    SAVE_INTERVAL: int = 10  # Save every N blocks to reduce I/O overhead


# Logging setup
logging.basicConfig(
    level=Config.LOG_LEVEL,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)


async def retry_request(session: aiohttp.ClientSession, url: str, retries: int) -> Optional[Dict[str, Any]]:
    """Perform GET request with retry, exponential backoff, and rate limit handling."""
    for attempt in range(1, retries + 1):
        try:
            async with session.get(url, timeout=Config.REQUEST_TIMEOUT) as response:
                response.raise_for_status()
                data = await response.json()

                if data.get("status") == "0":
                    result_text = str(data.get("result", "")).lower()
                    if "rate limit" in result_text:
                        delay = 5 + attempt * 2
                        logging.warning(f"[Rate limit] Delaying {delay}s.")
                        await asyncio.sleep(delay)
                        continue

                return data

        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            delay = min(Config.MAX_BACKOFF, (2 ** attempt) + random.random())
            logging.warning(f"[Retry {attempt}/{retries}] {e.__class__.__name__}: {e}. Retrying in {delay:.2f}s.")
            await asyncio.sleep(delay)

    logging.error(f"[Final failure] Could not fetch URL: {url}")
    return None


async def fetch(session: aiohttp.ClientSession, url: str) -> Dict[str, Any]:
    """Fetch data with retry wrapper."""
    return await retry_request(session, url, Config.MAX_RETRIES) or {}


async def get_latest_block(session: aiohttp.ClientSession) -> Optional[int]:
    """Fetch the latest Ethereum block number."""
    url = f"{Config.BASE_URL}?module=proxy&action=eth_blockNumber&apikey={Config.API_KEY}"
    data = await fetch(session, url)
    try:
        return int(data["result"], 16)
    except (KeyError, TypeError, ValueError):
        logging.error("Invalid response when fetching latest block.")
        return None


async def get_block_transactions(session: aiohttp.ClientSession, block_number: int) -> Set[str]:
    """Get unique Ethereum addresses from all transactions in a block."""
    hex_block = f"0x{block_number:x}"
    url = (
        f"{Config.BASE_URL}?module=proxy&action=eth_getBlockByNumber"
        f"&tag={hex_block}&boolean=true&apikey={Config.API_KEY}"
    )
    data = await fetch(session, url)
    txs = data.get("result", {}).get("transactions", [])
    return {
        addr
        for tx in txs
        for addr in (tx.get("from"), tx.get("to"))
        if addr
    }


async def get_nfts_for_address(session: aiohttp.ClientSession, address: str, semaphore: asyncio.Semaphore) -> Set[str]:
    """Fetch all NFT contract addresses associated with a given address."""
    async with semaphore:
        url = (
            f"{Config.BASE_URL}?module=account&action=tokennfttx"
            f"&address={address}&startblock=0&endblock=latest&sort=asc&apikey={Config.API_KEY}"
        )
        data = await fetch(session, url)
        result = data.get("result")
        if not isinstance(result, list):
            msg = data.get("message", "Unknown")
            logging.warning(f"Invalid NFT data for {address}: {msg}")
            return set()

        return {tx["contractAddress"] for tx in result if "contractAddress" in tx}


async def save_nft_addresses(addresses: Set[str]) -> None:
    """Append NFT contract addresses to file."""
    if not addresses:
        return
    async with aiofiles.open(Config.OUTPUT_FILE, "a") as file:
        await file.write("\n".join(addresses) + "\n")
    logging.info(f"Saved {len(addresses)} new NFT contract addresses.")


async def load_seen_addresses() -> Set[str]:
    """Load NFT addresses already stored on disk."""
    try:
        async with aiofiles.open(Config.OUTPUT_FILE, "r") as file:
            lines = await file.readlines()
        return {line.strip() for line in lines if line.strip()}
    except FileNotFoundError:
        return set()


async def process_block(
    session: aiohttp.ClientSession,
    block_number: int,
    semaphore: asyncio.Semaphore,
    seen_addresses: Set[str],
) -> Set[str]:
    """Process a single block and return new NFT contract addresses."""
    logging.info(f"Processing block #{block_number}...")
    unique_addresses = await get_block_transactions(session, block_number)

    if not unique_addresses:
        logging.info(f"No transactions in block #{block_number}.")
        return set()

    logging.info(f"Found {len(unique_addresses)} unique addresses in block #{block_number}.")
    tasks = [get_nfts_for_address(session, addr, semaphore) for addr in unique_addresses]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    nft_addresses = {
        contract
        for res in results
        if isinstance(res, set)
        for contract in res
    }

    new_contracts = nft_addresses - seen_addresses
    return new_contracts


async def main() -> None:
    semaphore = asyncio.Semaphore(Config.CONCURRENT_REQUESTS)
    seen_addresses = await load_seen_addresses()
    seen_blocks: Set[int] = set()
    processed_count = 0

    logging.info(f"Loaded {len(seen_addresses)} previously saved NFT addresses.")

    async with aiohttp.ClientSession() as session:
        while True:
            try:
                latest_block = await get_latest_block(session)
                if not latest_block:
                    await asyncio.sleep(5)
                    continue

                if latest_block in seen_blocks:
                    logging.debug(f"Block #{latest_block} already processed. Skipping.")
                    await asyncio.sleep(Config.SLEEP_INTERVAL)
                    continue

                start = time()
                new_contracts = await process_block(session, latest_block, semaphore, seen_addresses)

                if new_contracts:
                    seen_addresses.update(new_contracts)
                    await save_nft_addresses(new_contracts)

                seen_blocks.add(latest_block)
                processed_count += 1

                elapsed = time() - start
                sleep_time = max(0, Config.SLEEP_INTERVAL - elapsed)
                logging.info(f"Sleeping {sleep_time:.2f}s before next block check.")
                await asyncio.sleep(sleep_time)

                if processed_count % Config.SAVE_INTERVAL == 0:
                    await save_nft_addresses(seen_addresses)

            except asyncio.CancelledError:
                logging.info("Script cancelled by user.")
                break
            except Exception as e:
                logging.exception(f"Unhandled error: {e}")
                await asyncio.sleep(5)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Interrupted by user. Exiting.")
        sys.exit(0)
