import aiohttp
import asyncio
import logging
import sys
import aiofiles
import random
from dataclasses import dataclass
from time import time
from typing import Set, Dict, Any, Optional, Union


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


# Logging setup
logging.basicConfig(
    level=Config.LOG_LEVEL,
    format="%(asctime)s | %(levelname)s | %(message)s",
)


async def retry_request(
    session: aiohttp.ClientSession, url: str, retries: int
) -> Optional[Dict[str, Any]]:
    """Perform GET request with retry and exponential backoff."""
    for attempt in range(1, retries + 1):
        try:
            async with session.get(url, timeout=Config.REQUEST_TIMEOUT) as response:
                response.raise_for_status()
                data = await response.json()

                # Handle API-specific errors (rate limits, etc.)
                if data.get("status") == "0":
                    result = str(data.get("result", "")).lower()
                    if "rate limit" in result:
                        delay = 5 + attempt * 2
                        logging.warning(f"[Rate limit] Delaying {delay}s.")
                        await asyncio.sleep(delay)
                        continue

                return data

        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            delay = min(Config.MAX_BACKOFF, (2**attempt) + random.random())
            logging.warning(
                f"[Retry {attempt}/{retries}] Failed {url}: {e}. Retrying in {delay:.2f}s."
            )
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


async def get_block_transactions(
    session: aiohttp.ClientSession, block_number: int
) -> Set[str]:
    """Get unique addresses from all transactions in a block."""
    hex_block = f"0x{block_number:x}"
    url = (
        f"{Config.BASE_URL}?module=proxy&action=eth_getBlockByNumber"
        f"&tag={hex_block}&boolean=true&apikey={Config.API_KEY}"
    )
    data = await fetch(session, url)
    txs = data.get("result", {}).get("transactions", [])
    addresses = {tx.get("from") for tx in txs if tx.get("from")}
    addresses |= {tx.get("to") for tx in txs if tx.get("to")}
    return {addr for addr in addresses if addr}


async def get_nfts_for_address(
    session: aiohttp.ClientSession, address: str, semaphore: asyncio.Semaphore
) -> Set[str]:
    """Get all NFT contract addresses for a given Ethereum address."""
    async with semaphore:
        url = (
            f"{Config.BASE_URL}?module=account&action=tokennfttx"
            f"&address={address}&startblock=0&endblock=latest&sort=asc&apikey={Config.API_KEY}"
        )
        data = await fetch(session, url)
        result = data.get("result")
        if not isinstance(result, list):
            logging.warning(f"Invalid NFT data for {address}: {data.get('message')}")
            return set()
        return {tx.get("contractAddress") for tx in result if tx.get("contractAddress")}


async def save_nft_addresses(addresses: Set[str]) -> None:
    """Append new NFT addresses to file."""
    if not addresses:
        return
    async with aiofiles.open(Config.OUTPUT_FILE, "a") as file:
        await file.write("\n".join(addresses) + "\n")
    logging.info(f"Saved {len(addresses)} new NFT contract addresses.")


async def load_seen_addresses() -> Set[str]:
    """Load already saved NFT addresses from file (for resuming)."""
    try:
        async with aiofiles.open(Config.OUTPUT_FILE, "r") as file:
            lines = await file.readlines()
        return {line.strip() for line in lines if line.strip()}
    except FileNotFoundError:
        return set()


async def process_latest_block(
    session: aiohttp.ClientSession,
    semaphore: asyncio.Semaphore,
    seen_addresses: Set[str],
    seen_blocks: Set[int],
) -> None:
    """Process the latest block: extract addresses, fetch NFT contracts, save new ones."""
    latest_block = await get_latest_block(session)
    if not latest_block or latest_block in seen_blocks:
        logging.info(
            f"Skipping block #{latest_block} (already processed or unavailable)."
        )
        return

    logging.info(f"Fetching transactions from block #{latest_block}...")
    unique_addresses = await get_block_transactions(session, latest_block)
    if not unique_addresses:
        logging.info(f"No addresses found in block #{latest_block}.")
        seen_blocks.add(latest_block)
        return

    logging.info(f"Found {len(unique_addresses)} unique addresses.")

    tasks = [get_nfts_for_address(session, addr, semaphore) for addr in unique_addresses]
    results: list[Union[Set[str], Exception]] = await asyncio.gather(
        *tasks, return_exceptions=True
    )

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


async def main() -> None:
    semaphore = asyncio.Semaphore(Config.CONCURRENT_REQUESTS)
    seen_addresses: Set[str] = await load_seen_addresses()
    seen_blocks: Set[int] = set()

    logging.info(f"Loaded {len(seen_addresses)} previously saved NFT addresses.")

    async with aiohttp.ClientSession() as session:
        while True:
            try:
                start = time()
                await process_latest_block(session, semaphore, seen_addresses, seen_blocks)
                elapsed = time() - start
                sleep_time = max(0, Config.SLEEP_INTERVAL - elapsed)
                logging.info(f"Sleeping {sleep_time:.2f}s before next check.")
                await asyncio.sleep(sleep_time)

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
