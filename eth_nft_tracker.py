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
    MIN_BACKOFF: int = 1


# Logging setup
logging.basicConfig(
    level=Config.LOG_LEVEL,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)


async def fetch_json(session: aiohttp.ClientSession, url: str) -> Optional[Dict[str, Any]]:
    """Perform GET request with retries, exponential backoff, and rate-limit handling."""
    for attempt in range(1, Config.MAX_RETRIES + 1):
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
            delay = min(Config.MAX_BACKOFF, Config.MIN_BACKOFF * (2 ** attempt) + random.random())
            logging.warning(f"[Retry {attempt}/{Config.MAX_RETRIES}] {e.__class__.__name__}: {e}. Retrying in {delay:.1f}s.")
            await asyncio.sleep(delay)
        except Exception as e:
            logging.error(f"Unexpected error fetching {url}: {e}")
            return None

    logging.error(f"[Final failure] Could not fetch URL after {Config.MAX_RETRIES} retries: {url}")
    return None


async def get_latest_block(session: aiohttp.ClientSession) -> Optional[int]:
    """Fetch the latest Ethereum block number."""
    url = f"{Config.BASE_URL}?module=proxy&action=eth_blockNumber&apikey={Config.API_KEY}"
    data = await fetch_json(session, url)
    try:
        return int(data["result"], 16)
    except Exception:
        logging.error(f"Invalid response from latest block API: {data}")
        return None


async def get_block_transactions(session: aiohttp.ClientSession, block_number: int) -> Set[str]:
    """Extract unique Ethereum addresses from a given block."""
    hex_block = f"0x{block_number:x}"
    url = (
        f"{Config.BASE_URL}?module=proxy&action=eth_getBlockByNumber"
        f"&tag={hex_block}&boolean=true&apikey={Config.API_KEY}"
    )
    data = await fetch_json(session, url)
    if not data or "result" not in data:
        logging.warning(f"No data for block #{block_number}")
        return set()

    txs = data["result"].get("transactions", [])
    return {addr for tx in txs for addr in (tx.get("from"), tx.get("to")) if addr}


async def get_nfts_for_address(session: aiohttp.ClientSession, address: str, semaphore: asyncio.Semaphore) -> Set[str]:
    """Fetch all NFT contract addresses for a given wallet."""
    async with semaphore:
        url = (
            f"{Config.BASE_URL}?module=account&action=tokennfttx"
            f"&address={address}&startblock=0&endblock=latest&sort=asc&apikey={Config.API_KEY}"
        )
        data = await fetch_json(session, url)
        if not data or not isinstance(data.get("result"), list):
            msg = data.get("message", "Unknown error")
            logging.debug(f"No NFT data for {address}: {msg}")
            return set()

        return {tx["contractAddress"] for tx in data["result"] if "contractAddress" in tx}


async def load_seen_addresses() -> Set[str]:
    """Load NFT addresses that are already saved."""
    try:
        async with aiofiles.open(Config.OUTPUT_FILE, "r") as f:
            lines = await f.readlines()
        return {line.strip() for line in lines if line.strip()}
    except FileNotFoundError:
        return set()


async def save_nft_addresses(addresses: Set[str]) -> None:
    """Append new NFT contract addresses to disk."""
    if not addresses:
        return
    async with aiofiles.open(Config.OUTPUT_FILE, "a") as f:
        await f.write("\n".join(addresses) + "\n")
    logging.info(f"Saved {len(addresses)} new NFT addresses.")


async def process_block(
    session: aiohttp.ClientSession,
    block_number: int,
    semaphore: asyncio.Semaphore,
    seen_addresses: Set[str],
) -> Set[str]:
    """Process a single block: get transactions, extract unique addresses, then fetch NFTs."""
    logging.info(f"Processing block #{block_number}...")
    addresses = await get_block_transactions(session, block_number)
    if not addresses:
        return set()

    tasks = [get_nfts_for_address(session, addr, semaphore) for addr in addresses]
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

    logging.info(f"Loaded {len(seen_addresses)} NFT addresses from disk.")

    async with aiohttp.ClientSession(raise_for_status=False) as session:
        while True:
            try:
                latest_block = await get_latest_block(session)
                if not latest_block:
                    await asyncio.sleep(5)
                    continue

                if latest_block in seen_blocks:
                    logging.debug(f"Block #{latest_block} already processed. Sleeping...")
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
                logging.info(f"Sleeping {sleep_time:.1f}s before next check...")
                await asyncio.sleep(sleep_time)

                if processed_count % Config.SAVE_INTERVAL == 0:
                    await save_nft_addresses(seen_addresses)

            except asyncio.CancelledError:
                logging.info("Task cancelled. Exiting.")
                break
            except Exception as e:
                logging.exception(f"Unhandled exception: {e}")
                await asyncio.sleep(5)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Interrupted by user. Exiting.")
        sys.exit(0)
