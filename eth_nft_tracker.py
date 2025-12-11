import aiohttp
import asyncio
import logging
import sys
import aiofiles
import random
import signal
from dataclasses import dataclass
from time import time, monotonic
from typing import Set, Dict, Any, Optional


# ============================================================
# Configuration
# ============================================================

@dataclass(frozen=True)
class Config:
    API_KEY: str = "YOUR_ETHERSCAN_API_KEY"
    BASE_URL: str = "https://api.etherscan.io/api"

    SLEEP_INTERVAL: int = 60
    OUTPUT_FILE: str = "nft_addresses.txt"

    REQUEST_TIMEOUT: int = 12
    MAX_RETRIES: int = 5
    MAX_BACKOFF: int = 30
    MIN_BACKOFF: int = 1

    CONCURRENT_REQUESTS: int = 10

    GLOBAL_RPS: int = 4        # max global requests per second
    TOKEN_BUCKET_SIZE: int = 4 # same as RPS

    SAVE_INTERVAL: int = 5
    LOG_LEVEL: int = logging.INFO


# Logging
logging.basicConfig(
    level=Config.LOG_LEVEL,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)


# ============================================================
# Rate Limiter (Token Bucket)
# ============================================================

class TokenBucket:
    def __init__(self, rate: int, capacity: int):
        self.rate = rate
        self.capacity = capacity
        self.tokens = capacity
        self.timestamp = monotonic()
        self._lock = asyncio.Lock()

    async def consume(self) -> None:
        async with self._lock:
            now = monotonic()
            elapsed = now - self.timestamp

            # refill tokens
            refill = elapsed * self.rate
            if refill > 0:
                self.tokens = min(self.capacity, self.tokens + refill)
                self.timestamp = now

            if self.tokens < 1:
                sleep_time = (1 - self.tokens) / self.rate
                await asyncio.sleep(sleep_time)
                self.tokens = 0  # will refill next iteration

            self.tokens -= 1


bucket = TokenBucket(Config.GLOBAL_RPS, Config.TOKEN_BUCKET_SIZE)


async def rate_limited():
    """Async token bucket limiter."""
    await bucket.consume()


# ============================================================
# HTTP helpers
# ============================================================

def build_url(module: str, action: str, **params) -> str:
    params_str = "&".join(f"{k}={v}" for k, v in params.items())
    return f"{Config.BASE_URL}?module={module}&action={action}&{params_str}&apikey={Config.API_KEY}"


async def fetch_json(session: aiohttp.ClientSession, url: str) -> Optional[Dict[str, Any]]:
    """GET JSON with retries, backoff, and rate limit."""
    for attempt in range(1, Config.MAX_RETRIES + 1):
        try:
            await rate_limited()

            async with session.get(url, timeout=Config.REQUEST_TIMEOUT) as response:
                response.raise_for_status()
                data = await response.json()

                # etherscan rate limit detection
                if data.get("status") == "0":
                    msg = str(data.get("result", "")).lower()
                    if "rate limit" in msg:
                        delay = min(Config.MAX_BACKOFF, 3 + attempt * 2)
                        logging.warning(f"[Rate limit] Sleeping {delay}s.")
                        await asyncio.sleep(delay)
                        continue

                return data

        except (asyncio.TimeoutError, aiohttp.ClientError) as e:
            delay = min(Config.MAX_BACKOFF, Config.MIN_BACKOFF * (2 ** attempt) + random.random())
            logging.warning(f"[Retry {attempt}/{Config.MAX_RETRIES}] {e}. Retrying in {delay:.1f}s...")
            await asyncio.sleep(delay)

    logging.error(f"[Final failure] URL failed after retries: {url}")
    return None


# ============================================================
# Etherscan API calls
# ============================================================

async def get_latest_block(session: aiohttp.ClientSession) -> Optional[int]:
    url = build_url("proxy", "eth_blockNumber")
    data = await fetch_json(session, url)
    try:
        return int(data["result"], 16)
    except Exception:
        logging.error(f"Invalid response from blockNumber API: {data}")
        return None


async def get_block_transactions(session: aiohttp.ClientSession, block_number: int) -> Set[str]:
    hex_block = f"0x{block_number:x}"
    url = build_url("proxy", "eth_getBlockByNumber", tag=hex_block, boolean="true")
    data = await fetch_json(session, url)

    if not data or "result" not in data:
        logging.warning(f"Empty block data #{block_number}")
        return set()

    txs = data["result"].get("transactions", [])
    return {addr for tx in txs for addr in (tx.get("from"), tx.get("to")) if addr}


empty_wallet_cache: Set[str] = set()


async def get_nfts_for_address(
    session: aiohttp.ClientSession,
    address: str,
    semaphore: asyncio.Semaphore
) -> Set[str]:

    if address in empty_wallet_cache:
        return set()

    async with semaphore:
        url = build_url(
            "account", "tokennfttx",
            address=address, startblock=0, endblock="latest", sort="asc"
        )
        data = await fetch_json(session, url)

        if not data or not isinstance(data.get("result"), list):
            empty_wallet_cache.add(address)
            return set()

        contracts = {tx.get("contractAddress") for tx in data["result"] if tx.get("contractAddress")}
        if not contracts:
            empty_wallet_cache.add(address)

        return contracts


# ============================================================
# Persistence
# ============================================================

async def load_seen_addresses() -> Set[str]:
    try:
        async with aiofiles.open(Config.OUTPUT_FILE, "r") as f:
            return {line.strip() for line in await f.readlines() if line.strip()}
    except FileNotFoundError:
        return set()


async def save_nft_addresses(addresses: Set[str]) -> None:
    if not addresses:
        return
    async with aiofiles.open(Config.OUTPUT_FILE, "a") as f:
        await f.write("\n".join(addresses) + "\n")
    logging.info(f"Saved {len(addresses)} NFT contract addresses.")


# ============================================================
# Block Processor
# ============================================================

async def process_block(
    session: aiohttp.ClientSession,
    block_number: int,
    semaphore: asyncio.Semaphore,
    seen_addresses: Set[str],
) -> Set[str]:

    logging.info(f"Processing block #{block_number}...")
    addrs = await get_block_transactions(session, block_number)
    addrs -= empty_wallet_cache

    if not addrs:
        return set()

    tasks = [get_nfts_for_address(session, addr, semaphore) for addr in addrs]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    found = {c for res in results if isinstance(res, set) for c in res}
    return found - seen_addresses


# ============================================================
# Main Loop
# ============================================================

async def main() -> None:
    semaphore = asyncio.Semaphore(Config.CONCURRENT_REQUESTS)
    seen_addresses = await load_seen_addresses()
    seen_blocks: Set[int] = set()
    processed_count = 0

    logging.info(f"Loaded {len(seen_addresses)} known NFT contracts.")

    stop_event = asyncio.Event()

    def request_stop(*_):
        logging.info("Shutdown signal received...")
        stop_event.set()

    loop = asyncio.get_event_loop()
    loop.add_signal_handler(signal.SIGINT, request_stop)
    loop.add_signal_handler(signal.SIGTERM, request_stop)

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

                new_contracts = await process_block(
                    session, latest_block, semaphore, seen_addresses
                )
                if new_contracts:
                    seen_addresses.update(new_contracts)
                    await save_nft_addresses(new_contracts)

                seen_blocks.add(latest_block)
                processed_count += 1

                elapsed = time() - start
                await asyncio.sleep(max(0, Config.SLEEP_INTERVAL - elapsed))

                if processed_count % Config.SAVE_INTERVAL == 0:
                    await save_nft_addresses(seen_addresses)

            except Exception as e:
                logging.exception(f"Unhandled exception in main loop: {e}")
                await asyncio.sleep(5)

    logging.info("Shutdown complete.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Interrupted by user.")
        sys.exit(0)
