import aiohttp
import asyncio
import logging
import sys
import aiofiles
import random
import signal
from dataclasses import dataclass
from time import monotonic
from typing import Set, Dict, Any, Optional
from collections import OrderedDict


# ============================================================
# Configuration
# ============================================================

@dataclass(frozen=True)
class Config:
    API_KEY: str = "YOUR_ETHERSCAN_API_KEY"
    BASE_URL: str = "https://api.etherscan.io/api"

    OUTPUT_FILE: str = "nft_addresses.txt"

    REQUEST_TIMEOUT: int = 15
    MAX_RETRIES: int = 5

    MIN_BACKOFF: float = 1.0
    MAX_BACKOFF: float = 30.0

    CONCURRENT_REQUESTS: int = 10
    GLOBAL_RPS: int = 4
    TOKEN_BUCKET_SIZE: int = 4

    EMPTY_WALLET_CACHE_SIZE: int = 50_000
    BLOCK_POLL_INTERVAL: int = 60

    LOG_LEVEL: int = logging.INFO


# ============================================================
# Logging
# ============================================================

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
        self.tokens = float(capacity)
        self.last = monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        async with self._lock:
            now = monotonic()
            elapsed = now - self.last
            self.last = now

            self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)

            if self.tokens < 1:
                wait = (1 - self.tokens) / self.rate
                await asyncio.sleep(wait)
                self.tokens = 0

            self.tokens -= 1


bucket = TokenBucket(Config.GLOBAL_RPS, Config.TOKEN_BUCKET_SIZE)


async def rate_limit() -> None:
    await bucket.acquire()


# ============================================================
# HTTP helpers
# ============================================================

def build_url(module: str, action: str, **params) -> str:
    query = "&".join(f"{k}={v}" for k, v in params.items())
    return f"{Config.BASE_URL}?module={module}&action={action}&{query}&apikey={Config.API_KEY}"


async def fetch_json(
    session: aiohttp.ClientSession,
    url: str
) -> Optional[Dict[str, Any]]:

    for attempt in range(1, Config.MAX_RETRIES + 1):
        try:
            await rate_limit()
            async with session.get(url) as resp:
                data = await resp.json(content_type=None)

                if isinstance(data, dict) and data.get("status") == "0":
                    msg = str(data.get("result", "")).lower()
                    if "rate limit" in msg:
                        delay = min(Config.MAX_BACKOFF, 3 + attempt * 2)
                        logging.warning(f"[Rate limit] Sleeping {delay:.1f}s")
                        await asyncio.sleep(delay)
                        continue

                return data

        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            delay = min(
                Config.MAX_BACKOFF,
                Config.MIN_BACKOFF * (2 ** (attempt - 1)) + random.random()
            )
            logging.warning(f"[Retry {attempt}/{Config.MAX_RETRIES}] {e} → {delay:.1f}s")
            await asyncio.sleep(delay)

    logging.error(f"[Failed] {url}")
    return None


# ============================================================
# Etherscan API
# ============================================================

async def get_latest_block(session: aiohttp.ClientSession) -> Optional[int]:
    data = await fetch_json(session, build_url("proxy", "eth_blockNumber"))
    try:
        return int(data["result"], 16)
    except Exception:
        logging.error("Invalid blockNumber response")
        return None


async def get_block_addresses(
    session: aiohttp.ClientSession,
    block_number: int
) -> Set[str]:

    tag = f"0x{block_number:x}"
    data = await fetch_json(
        session,
        build_url("proxy", "eth_getBlockByNumber", tag=tag, boolean="true")
    )

    if not data or "result" not in data:
        return set()

    txs = data["result"].get("transactions", [])
    return {
        addr
        for tx in txs
        for addr in (tx.get("from"), tx.get("to"))
        if addr
    }


# ============================================================
# Empty wallet LRU cache
# ============================================================

class EmptyWalletCache:
    def __init__(self, max_size: int):
        self.max_size = max_size
        self.data: OrderedDict[str, None] = OrderedDict()

    def __contains__(self, addr: str) -> bool:
        return addr in self.data

    def add(self, addr: str) -> None:
        self.data[addr] = None
        self.data.move_to_end(addr)
        if len(self.data) > self.max_size:
            self.data.popitem(last=False)


empty_wallets = EmptyWalletCache(Config.EMPTY_WALLET_CACHE_SIZE)


async def get_nfts_for_address(
    session: aiohttp.ClientSession,
    address: str,
    sem: asyncio.Semaphore
) -> Set[str]:

    if address in empty_wallets:
        return set()

    async with sem:
        data = await fetch_json(
            session,
            build_url(
                "account",
                "tokennfttx",
                address=address,
                startblock=0,
                endblock="latest",
                sort="asc",
            ),
        )

    if not data or not isinstance(data.get("result"), list):
        empty_wallets.add(address)
        return set()

    contracts = {
        tx["contractAddress"]
        for tx in data["result"]
        if tx.get("contractAddress")
    }

    if not contracts:
        empty_wallets.add(address)

    return contracts


# ============================================================
# Persistence
# ============================================================

async def load_seen() -> Set[str]:
    try:
        async with aiofiles.open(Config.OUTPUT_FILE, "r") as f:
            return {line.strip() for line in await f.readlines() if line.strip()}
    except FileNotFoundError:
        return set()


async def append_new(addresses: Set[str]) -> None:
    if not addresses:
        return
    async with aiofiles.open(Config.OUTPUT_FILE, "a") as f:
        await f.write("\n".join(addresses) + "\n")
    logging.info(f"Saved {len(addresses)} NFT contracts")


# ============================================================
# Block processor
# ============================================================

async def process_block(
    session: aiohttp.ClientSession,
    block: int,
    sem: asyncio.Semaphore,
    seen: Set[str],
) -> Set[str]:

    logging.info(f"Processing block #{block}")
    addresses = await get_block_addresses(session, block)
    addresses -= empty_wallets.data.keys()

    tasks = [
        get_nfts_for_address(session, addr, sem)
        for addr in addresses
    ]

    results = await asyncio.gather(*tasks, return_exceptions=True)
    found = {
        c for r in results
        if isinstance(r, set)
        for c in r
    }

    return found - seen


# ============================================================
# Main
# ============================================================

async def main() -> None:
    sem = asyncio.Semaphore(Config.CONCURRENT_REQUESTS)
    seen = await load_seen()
    seen_blocks: Set[int] = set()

    stop = asyncio.Event()

    def shutdown(*_):
        logging.info("Shutdown signal received")
        stop.set()

    loop = asyncio.get_running_loop()
    loop.add_signal_handler(signal.SIGINT, shutdown)
    loop.add_signal_handler(signal.SIGTERM, shutdown)

    timeout = aiohttp.ClientTimeout(total=Config.REQUEST_TIMEOUT)

    async with aiohttp.ClientSession(timeout=timeout) as session:
        while not stop.is_set():
            latest = await get_latest_block(session)
            if not latest or latest in seen_blocks:
                await asyncio.sleep(Config.BLOCK_POLL_INTERVAL)
                continue

            new = await process_block(session, latest, sem, seen)
            if new:
                seen.update(new)
                await append_new(new)

            seen_blocks.add(latest)
            await asyncio.sleep(Config.BLOCK_POLL_INTERVAL)

    logging.info("Shutdown complete")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
