import asyncio
import aiohttp
import aiofiles
import logging
import signal
import sys
import random
from dataclasses import dataclass
from time import monotonic
from typing import Set, Dict, Any, Optional
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
logger = logging.getLogger("nft-scanner")


# ============================================================
# Token Bucket Rate Limiter
# ============================================================

class TokenBucket:
    def __init__(self, rate: int, capacity: int):
        self.rate = rate
        self.capacity = capacity
        self.tokens = float(capacity)
        self.last_refill = monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        async with self._lock:
            now = monotonic()
            elapsed = now - self.last_refill
            self.last_refill = now

            # Refill tokens
            self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)

            if self.tokens < 1:
                sleep_time = (1 - self.tokens) / self.rate
                await asyncio.sleep(sleep_time)
                self.tokens = 0

            self.tokens -= 1


_bucket = TokenBucket(Config.GLOBAL_RPS, Config.TOKEN_BUCKET_SIZE)


async def rate_limit() -> None:
    await _bucket.acquire()


# ============================================================
# HTTP Helpers
# ============================================================

def build_url(module: str, action: str, **params: Any) -> str:
    params.update({
        "module": module,
        "action": action,
        "apikey": Config.API_KEY,
    })
    return f"{Config.BASE_URL}?{urlencode(params)}"


async def fetch_json(
    session: aiohttp.ClientSession,
    url: str,
) -> Optional[Dict[str, Any]]:

    for attempt in range(1, Config.MAX_RETRIES + 1):
        try:
            await rate_limit()

            async with session.get(url) as resp:
                resp.raise_for_status()
                data = await resp.json(content_type=None)

                if not isinstance(data, dict):
                    raise ValueError("Invalid JSON response")

                # Etherscan explicit rate-limit message
                if data.get("status") == "0":
                    msg = str(data.get("result", "")).lower()
                    if "rate limit" in msg:
                        delay = min(Config.MAX_BACKOFF, 2 ** attempt)
                        logger.warning(f"API rate limit hit → sleeping {delay:.1f}s")
                        await asyncio.sleep(delay)
                        continue

                return data

        except (aiohttp.ClientError, asyncio.TimeoutError, ValueError) as e:
            delay = min(
                Config.MAX_BACKOFF,
                Config.MIN_BACKOFF * (2 ** (attempt - 1)) + random.random(),
            )
            logger.warning(
                f"Retry {attempt}/{Config.MAX_RETRIES} → {e} | sleeping {delay:.1f}s"
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
    except (KeyError, TypeError, ValueError):
        logger.error("Invalid eth_blockNumber response")
        return None


async def get_block_addresses(
    session: aiohttp.ClientSession,
    block_number: int,
) -> Set[str]:

    data = await fetch_json(
        session,
        build_url(
            "proxy",
            "eth_getBlockByNumber",
            tag=f"0x{block_number:x}",
            boolean="true",
        ),
    )

    if not data or "result" not in data:
        return set()

    txs = data["result"].get("transactions") or []
    addresses: Set[str] = set()

    for tx in txs:
        if isinstance(tx, dict):
            if tx.get("from"):
                addresses.add(tx["from"])
            if tx.get("to"):
                addresses.add(tx["to"])

    return addresses


# ============================================================
# Empty Wallet LRU Cache
# ============================================================

class EmptyWalletCache:
    def __init__(self, max_size: int):
        self.max_size = max_size
        self.data: OrderedDict[str, None] = OrderedDict()

    def __contains__(self, address: str) -> bool:
        return address in self.data

    def add(self, address: str) -> None:
        self.data[address] = None
        self.data.move_to_end(address)
        if len(self.data) > self.max_size:
            self.data.popitem(last=False)


empty_wallets = EmptyWalletCache(Config.EMPTY_WALLET_CACHE_SIZE)


async def get_nfts_for_address(
    session: aiohttp.ClientSession,
    address: str,
    sem: asyncio.Semaphore,
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

    result = data.get("result") if isinstance(data, dict) else None
    if not isinstance(result, list):
        empty_wallets.add(address)
        return set()

    contracts = {
        tx.get("contractAddress")
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
        async with aiofiles.open(Config.OUTPUT_FILE, "r") as f:
            return {line.strip() for line in await f if line.strip()}
    except FileNotFoundError:
        return set()


async def append_new(addresses: Set[str]) -> None:
    if not addresses:
        return

    async with aiofiles.open(Config.OUTPUT_FILE, "a") as f:
        await f.write("\n".join(addresses) + "\n")

    logger.info(f"Saved {len(addresses)} new NFT contracts")


# ============================================================
# Block Processing
# ============================================================

async def process_block(
    session: aiohttp.ClientSession,
    block: int,
    sem: asyncio.Semaphore,
    seen: Set[str],
) -> Set[str]:

    logger.info(f"Processing block #{block}")

    addresses = await get_block_addresses(session, block)
    addresses = {
        addr for addr in addresses
        if addr and addr not in empty_wallets
    }

    tasks = [
        get_nfts_for_address(session, addr, sem)
        for addr in addresses
    ]

    results = await asyncio.gather(*tasks, return_exceptions=True)

    found: Set[str] = set()

    for result in results:
        if isinstance(result, set):
            found.update(result)

    return found - seen


# ============================================================
# Main
# ============================================================

async def main() -> None:
    sem = asyncio.Semaphore(Config.CONCURRENT_REQUESTS)
    seen_contracts = await load_seen()
    processed_blocks: Set[int] = set()

    stop_event = asyncio.Event()

    def shutdown() -> None:
        logger.info("Shutdown signal received")
        stop_event.set()

    loop = asyncio.get_running_loop()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, shutdown)
        except NotImplementedError:
            pass  # Windows fallback

    timeout = aiohttp.ClientTimeout(total=Config.REQUEST_TIMEOUT)
    connector = aiohttp.TCPConnector(limit=Config.CONCURRENT_REQUESTS)

    async with aiohttp.ClientSession(
        timeout=timeout,
        connector=connector,
    ) as session:

        while not stop_event.is_set():

            latest = await get_latest_block(session)

            if not latest or latest in processed_blocks:
                await asyncio.sleep(Config.BLOCK_POLL_INTERVAL)
                continue

            new_contracts = await process_block(
                session,
                latest,
                sem,
                seen_contracts,
            )

            if new_contracts:
                seen_contracts.update(new_contracts)
                await append_new(new_contracts)

            processed_blocks.add(latest)
            await asyncio.sleep(Config.BLOCK_POLL_INTERVAL)

    logger.info("Shutdown complete")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
