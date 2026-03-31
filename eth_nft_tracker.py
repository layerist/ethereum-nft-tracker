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
    MAX_RETRIES: int = 6

    MIN_BACKOFF: float = 0.5
    MAX_BACKOFF: float = 20.0

    CONCURRENT_REQUESTS: int = 20
    GLOBAL_RPS: int = 5
    TOKEN_BUCKET_SIZE: int = 5

    EMPTY_WALLET_CACHE_SIZE: int = 100_000

    BLOCK_POLL_INTERVAL: int = 8

    ADDRESS_QUEUE_SIZE: int = 50_000
    WRITE_BUFFER_SIZE: int = 200

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


async def fetch_json(session: aiohttp.ClientSession, url: str) -> Optional[Dict[str, Any]]:
    for attempt in range(1, Config.MAX_RETRIES + 1):
        try:
            await rate_limit()

            async with session.get(
                url,
                timeout=aiohttp.ClientTimeout(total=Config.REQUEST_TIMEOUT)
            ) as resp:

                if resp.status >= 500:
                    raise aiohttp.ClientError(f"{resp.status}")

                if resp.status == 429:
                    raise aiohttp.ClientError("rate limit")

                data = await resp.json(content_type=None)

                if not isinstance(data, dict):
                    raise ValueError("bad json")

                if data.get("status") == "0":
                    msg = str(data.get("result", "")).lower()
                    if "rate limit" in msg:
                        raise aiohttp.ClientError("etherscan rate limit")

                return data

        except (aiohttp.ClientError, asyncio.TimeoutError, ValueError) as e:
            delay = min(
                Config.MAX_BACKOFF,
                Config.MIN_BACKOFF * (2 ** (attempt - 1)) + random.uniform(0, 1)
            )
            logger.warning(f"[retry {attempt}] {e} → {delay:.2f}s")
            await asyncio.sleep(delay)

    logger.error(f"FAILED: {url}")
    return None


# ============================================================
# Etherscan API
# ============================================================

async def get_latest_block(session: aiohttp.ClientSession) -> Optional[int]:
    data = await fetch_json(session, build_url("proxy", "eth_blockNumber"))
    try:
        return int(data["result"], 16) if data else None
    except Exception:
        return None


async def get_block_addresses(session: aiohttp.ClientSession, block: int) -> Set[str]:
    data = await fetch_json(
        session,
        build_url("proxy", "eth_getBlockByNumber", tag=f"0x{block:x}", boolean="true")
    )

    txs = data.get("result", {}).get("transactions") if data else []
    result = set()

    for tx in txs or []:
        if not isinstance(tx, dict):
            continue
        if tx.get("from"):
            result.add(tx["from"].lower())
        if tx.get("to"):
            result.add(tx["to"].lower())

    return result


# ============================================================
# LRU Cache (faster)
# ============================================================

class LRUCache:
    def __init__(self, max_size: int):
        self.max_size = max_size
        self.data = OrderedDict()

    def contains(self, key: str) -> bool:
        return key in self.data

    def add(self, key: str):
        self.data[key] = None
        if len(self.data) > self.max_size:
            self.data.popitem(last=False)


empty_wallets = LRUCache(Config.EMPTY_WALLET_CACHE_SIZE)


# ============================================================
# NFT Fetch
# ============================================================

async def fetch_nfts(session, address: str) -> Set[str]:
    if empty_wallets.contains(address):
        return set()

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
# Persistence (buffered writer)
# ============================================================

class AsyncWriter:
    def __init__(self, path: str):
        self.path = path
        self.buffer = set()
        self.lock = asyncio.Lock()

    async def add(self, items: Set[str]):
        async with self.lock:
            self.buffer.update(items)

            if len(self.buffer) >= Config.WRITE_BUFFER_SIZE:
                await self.flush()

    async def flush(self):
        if not self.buffer:
            return

        async with aiofiles.open(self.path, "a") as f:
            await f.write("\n".join(self.buffer) + "\n")

        logger.info(f"Saved {len(self.buffer)} contracts")
        self.buffer.clear()


# ============================================================
# Worker Pipeline
# ============================================================

async def worker(name, queue, session, seen, writer, stop_event):
    while not stop_event.is_set():
        try:
            address = await asyncio.wait_for(queue.get(), timeout=1)
        except asyncio.TimeoutError:
            continue

        if address in seen:
            queue.task_done()
            continue

        contracts = await fetch_nfts(session, address)

        new = contracts - seen
        if new:
            seen.update(new)
            await writer.add(new)

        queue.task_done()


# ============================================================
# Main
# ============================================================

async def main():
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

    connector = aiohttp.TCPConnector(limit=Config.CONCURRENT_REQUESTS)

    queue = asyncio.Queue(maxsize=Config.ADDRESS_QUEUE_SIZE)
    writer = AsyncWriter(Config.OUTPUT_FILE)
    seen: Set[str] = set()

    async with aiohttp.ClientSession(connector=connector) as session:

        # workers
        workers = [
            asyncio.create_task(worker(i, queue, session, seen, writer, stop_event))
            for i in range(Config.CONCURRENT_REQUESTS)
        ]

        last_block = None

        while not stop_event.is_set():

            latest = await get_latest_block(session)
            if not latest:
                await asyncio.sleep(5)
                continue

            if last_block is None:
                last_block = latest - 1

            for block in range(last_block + 1, latest + 1):

                addresses = await get_block_addresses(session, block)

                for addr in addresses:
                    if not empty_wallets.contains(addr):
                        try:
                            queue.put_nowait(addr)
                        except asyncio.QueueFull:
                            await queue.put(addr)

                last_block = block

            await asyncio.sleep(Config.BLOCK_POLL_INTERVAL)

        await queue.join()
        await writer.flush()

        for w in workers:
            w.cancel()

    logger.info("Shutdown complete")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
