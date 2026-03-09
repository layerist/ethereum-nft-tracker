import asyncio
import aiohttp
import aiofiles
import logging
import signal
import sys
import random
from dataclasses import dataclass
from time import monotonic
from typing import Set, Dict, Any, Optional, Iterable
from collections import OrderedDict
from urllib.parse import urlencode


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

    BLOCK_POLL_INTERVAL: int = 10

    LOG_LEVEL: int = logging.INFO


# ============================================================
# Logging
# ============================================================

logging.basicConfig(
    level=Config.LOG_LEVEL,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)

logger = logging.getLogger("nft-scanner")


# ============================================================
# Token Bucket Rate Limiter
# ============================================================

class TokenBucket:

    def __init__(self, rate: int, capacity: int):
        self.rate = rate
        self.capacity = capacity
        self.tokens = capacity
        self.updated = monotonic()
        self.lock = asyncio.Lock()

    async def acquire(self):

        while True:

            async with self.lock:

                now = monotonic()
                elapsed = now - self.updated
                self.updated = now

                self.tokens = min(
                    self.capacity,
                    self.tokens + elapsed * self.rate
                )

                if self.tokens >= 1:
                    self.tokens -= 1
                    return

                sleep_for = (1 - self.tokens) / self.rate

            await asyncio.sleep(sleep_for)


bucket = TokenBucket(Config.GLOBAL_RPS, Config.TOKEN_BUCKET_SIZE)


async def rate_limit():
    await bucket.acquire()


# ============================================================
# HTTP Helpers
# ============================================================

def build_url(module: str, action: str, **params: Any) -> str:

    params.update({
        "module": module,
        "action": action,
        "apikey": Config.API_KEY
    })

    return f"{Config.BASE_URL}?{urlencode(params)}"


async def fetch_json(
    session: aiohttp.ClientSession,
    url: str,
) -> Optional[Dict[str, Any]]:

    for attempt in range(1, Config.MAX_RETRIES + 1):

        try:

            await rate_limit()

            async with session.get(
                url,
                timeout=aiohttp.ClientTimeout(total=Config.REQUEST_TIMEOUT)
            ) as resp:

                if resp.status >= 500:
                    raise aiohttp.ClientError(f"Server error {resp.status}")

                if resp.status == 429:
                    raise aiohttp.ClientError("HTTP rate limit")

                data = await resp.json(content_type=None)

                if not isinstance(data, dict):
                    raise ValueError("Invalid JSON")

                if data.get("status") == "0":

                    msg = str(data.get("result", "")).lower()

                    if "rate limit" in msg:

                        delay = min(Config.MAX_BACKOFF, 2 ** attempt)
                        logger.warning(f"Etherscan rate limit → sleep {delay}s")

                        await asyncio.sleep(delay)
                        continue

                return data

        except (aiohttp.ClientError, asyncio.TimeoutError, ValueError) as e:

            delay = min(
                Config.MAX_BACKOFF,
                Config.MIN_BACKOFF * (2 ** (attempt - 1)) + random.random()
            )

            logger.warning(
                f"Retry {attempt}/{Config.MAX_RETRIES} | {e} | sleep {delay:.1f}s"
            )

            await asyncio.sleep(delay)

    logger.error(f"Permanent failure: {url}")
    return None


# ============================================================
# Etherscan API
# ============================================================

async def get_latest_block(session: aiohttp.ClientSession) -> Optional[int]:

    data = await fetch_json(session, build_url("proxy", "eth_blockNumber"))

    if not data:
        return None

    try:
        return int(data["result"], 16)
    except Exception:
        logger.error("Invalid blockNumber response")
        return None


async def get_block_addresses(
    session: aiohttp.ClientSession,
    block: int
) -> Set[str]:

    data = await fetch_json(
        session,
        build_url(
            "proxy",
            "eth_getBlockByNumber",
            tag=f"0x{block:x}",
            boolean="true"
        )
    )

    if not data:
        return set()

    txs = data.get("result", {}).get("transactions") or []

    addresses = set()

    for tx in txs:

        if not isinstance(tx, dict):
            continue

        frm = tx.get("from")
        to = tx.get("to")

        if frm:
            addresses.add(frm.lower())

        if to:
            addresses.add(to.lower())

    return addresses


# ============================================================
# Empty Wallet LRU Cache
# ============================================================

class LRUCache:

    def __init__(self, max_size: int):
        self.max_size = max_size
        self.data = OrderedDict()

    def __contains__(self, key):

        if key not in self.data:
            return False

        self.data.move_to_end(key)
        return True

    def add(self, key):

        self.data[key] = None
        self.data.move_to_end(key)

        if len(self.data) > self.max_size:
            self.data.popitem(last=False)


empty_wallets = LRUCache(Config.EMPTY_WALLET_CACHE_SIZE)


# ============================================================
# NFT Scanner
# ============================================================

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
                page=1,
                offset=100,
                sort="desc"
            )
        )

    result = data.get("result") if isinstance(data, dict) else None

    if not isinstance(result, list):

        empty_wallets.add(address)
        return set()

    contracts = {
        tx.get("contractAddress").lower()
        for tx in result
        if isinstance(tx, dict) and tx.get("contractAddress")
    }

    if not contracts:
        empty_wallets.add(address)

    return contracts


# ============================================================
# Persistence
# ============================================================

async def load_seen() -> Set[str]:

    try:
        async with aiofiles.open(Config.OUTPUT_FILE) as f:
            return {line.strip() for line in await f.readlines() if line.strip()}

    except FileNotFoundError:
        return set()


async def append_new(addresses: Iterable[str]):

    addresses = list(addresses)

    if not addresses:
        return

    async with aiofiles.open(Config.OUTPUT_FILE, "a") as f:
        await f.write("\n".join(addresses) + "\n")

    logger.info(f"Saved {len(addresses)} new NFT contracts")


# ============================================================
# Block Processing
# ============================================================

async def process_block(
    session,
    block,
    sem,
    seen
):

    logger.info(f"Processing block {block}")

    addresses = await get_block_addresses(session, block)

    addresses = [
        a for a in addresses
        if a not in empty_wallets
    ]

    tasks = [
        get_nfts_for_address(session, a, sem)
        for a in addresses
    ]

    found = set()

    for chunk in range(0, len(tasks), 100):

        batch = tasks[chunk:chunk + 100]

        results = await asyncio.gather(*batch, return_exceptions=True)

        for r in results:
            if isinstance(r, set):
                found.update(r)

    return found - seen


# ============================================================
# Main
# ============================================================

async def main():

    sem = asyncio.Semaphore(Config.CONCURRENT_REQUESTS)

    seen_contracts = await load_seen()

    last_block = None

    stop_event = asyncio.Event()

    def shutdown():
        logger.info("Shutdown signal received")
        stop_event.set()

    loop = asyncio.get_running_loop()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, shutdown)
        except NotImplementedError:
            pass

    connector = aiohttp.TCPConnector(
        limit=Config.CONCURRENT_REQUESTS,
        ttl_dns_cache=300
    )

    async with aiohttp.ClientSession(connector=connector) as session:

        while not stop_event.is_set():

            latest = await get_latest_block(session)

            if not latest:
                await asyncio.sleep(5)
                continue

            if last_block is None:
                last_block = latest - 1

            for block in range(last_block + 1, latest + 1):

                new_contracts = await process_block(
                    session,
                    block,
                    sem,
                    seen_contracts
                )

                if new_contracts:

                    seen_contracts.update(new_contracts)

                    await append_new(new_contracts)

                last_block = block

            await asyncio.sleep(Config.BLOCK_POLL_INTERVAL)

    logger.info("Shutdown complete")


if __name__ == "__main__":

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
