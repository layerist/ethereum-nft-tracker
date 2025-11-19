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
    REQUEST_TIMEOUT: int = 12
    MAX_RETRIES: int = 5
    CONCURRENT_REQUESTS: int = 10
    LOG_LEVEL: int = logging.INFO
    MAX_BACKOFF: int = 30
    SAVE_INTERVAL: int = 5
    MIN_BACKOFF: int = 1
    GLOBAL_RPS: int = 4  # max global requests per second for safety


# Logging setup
logging.basicConfig(
    level=Config.LOG_LEVEL,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)

# global rate limiter
rate_semaphore = asyncio.Semaphore(Config.GLOBAL_RPS)


async def rate_limited():
    """Limit total requests per second globally."""
    async with rate_semaphore:
        await asyncio.sleep(1 / Config.GLOBAL_RPS)


async def fetch_json(session: aiohttp.ClientSession, url: str) -> Optional[Dict[str, Any]]:
    """Perform GET request with retries, exponential backoff, and rate-limit handling."""
    for attempt in range(1, Config.MAX_RETRIES + 1):
        try:
            await rate_limited()

            async with session.get(url, timeout=Config.REQUEST_TIMEOUT) as response:
                response.raise_for_status()
                data = await response.json()

                # detect etherscan rate limit
                if data.get("status") == "0":
                    result_text = str(data.get("result", "")).lower()
                    if "rate limit" in result_text:
                        delay = min(Config.MAX_BACKOFF, 5 + attempt * 2)
                        logging.warning(f"[Rate limit] Delaying {delay}s.")
                        await asyncio.sleep(delay)
                        continue

                return data

        except (asyncio.TimeoutError, aiohttp.ClientError) as e:
            delay = min(Config.MAX_BACKOFF, Config.MIN_BACKOFF * (2 ** attempt) + random.random())
            logging.warning(f"[Retry {attempt}/{Config.MAX_RETRIES}] {e}. Retrying in {delay:.1f}s.")
            await asyncio.sleep(delay)

    logging.error(f"[Final failure] Could not fetch URL: {url}")
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


# cache to skip addresses that have no NFTs (avoids useless queries)
empty_wallet_cache: Set[str] = set()


async def get_nfts_for_address(session: aiohttp.ClientSession, address: str, semaphore: asyncio.Semaphore) -> Set[str]:
    """Fetch all NFT contract addresses for a given wallet."""
    if address in empty_wallet_cache:
        return set()

    async with semaphore:
        url = (
            f"{Config.BASE_URL}?module=account&action=tokennfttx"
            f"&address={address}&startblock=0&endblock=latest&sort=asc&apikey={Config.API_KEY}"
        )
        data = await fetch_json(session, url)
        if not data or not isinstance(data.get("result"), list):
            empty_wallet_cache.add(address)
            return set()

        result = {tx["contractAddress"] for tx in data["result"] if "contractAddress" in tx}
        if not result:
            empty_wallet_cache.add(address)

        return result


async def load_seen_addresses() -> Set[str]:
    try:
        async with aiofiles.open(Config.OUTPUT_FILE, "r") as f:
            lines = await f.readlines()
        return {line.strip() for line in lines if line.strip()}
    except FileNotFoundError:
        return set()


async def save_nft_addresses(addresses: Set[str]) -> None:
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
    logging.info(f"Processing block #{block_number}...")
    addresses = await get_block_transactions(session, block_number)
    if not addresses:
        return set()

    # remove addresses already checked with zero results
    addresses = addresses - empty_wallet_cache

    tasks = [get_nfts_for_address(session, addr, semaphore) for addr in addresses]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    nft_addresses = {c for res in results if isinstance(res, set) for c in res}

    return nft_addresses - seen_addresses


async def main() -> None:
    semaphore = asyncio.Semaphore(Config.CONCURRENT_REQUESTS)
    seen_addresses = await load_seen_addresses()
    seen_blocks: Set[int] = set()
    processed_count = 0

    logging.info(f"Loaded {len(seen_addresses)} NFT addresses.")

    # graceful shutdown support
    stop_event = asyncio.Event()

    def handle_stop(*_):
        stop_event.set()

    loop = asyncio.get_event_loop()
    loop.add_signal_handler(getattr(signal, "SIGINT"), handle_stop)
    loop.add_signal_handler(getattr(signal, "SIGTERM"), handle_stop)

    async with aiohttp.ClientSession(raise_for_status=False) as session:
        while not stop_event.is_set():
            try:
                latest_block = await get_latest_block(session)
                if not latest_block:
                    await asyncio.sleep(5)
                    continue

                if latest_block in seen_blocks:
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
                await asyncio.sleep(max(0, Config.SLEEP_INTERVAL - elapsed))

                if processed_count % Config.SAVE_INTERVAL == 0:
                    # safety save (idempotent)
                    await save_nft_addresses(seen_addresses)

            except Exception as e:
                logging.exception(f"Unhandled exception: {e}")
                await asyncio.sleep(5)

    logging.info("Shutdown complete.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Interrupted by user.")
        sys.exit(0)
