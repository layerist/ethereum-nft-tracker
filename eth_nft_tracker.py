import asyncio
import aiohttp
import aiofiles
import logging
import signal
import sys
import random
import os
from dataclasses import dataclass
from time import monotonic
from typing import Set, Dict, Any, Optional, List
from collections import OrderedDict


# ============================================================
# Configuration
# ============================================================

@dataclass(frozen=True)
class Config:
    # ========================================================
    # API
    # ========================================================

    API_KEYS: List[str] = (
        "YOUR_ETHERSCAN_API_KEY",
    )

    BASE_URL: str = "https://api.etherscan.io/api"

    # ========================================================
    # Files
    # ========================================================

    OUTPUT_FILE: str = "nft_contracts.txt"
    KNOWN_CONTRACTS_FILE: str = "known_contracts.txt"

    # ========================================================
    # Networking
    # ========================================================

    REQUEST_TIMEOUT: int = 15
    MAX_RETRIES: int = 7

    MIN_BACKOFF: float = 0.4
    MAX_BACKOFF: float = 30.0

    TCP_LIMIT: int = 100
    TCP_LIMIT_PER_HOST: int = 50

    DNS_CACHE_TTL: int = 300

    # ========================================================
    # Concurrency
    # ========================================================

    WORKERS: int = 40

    GLOBAL_RPS: int = 25
    TOKEN_BUCKET_SIZE: int = 50

    ADDRESS_QUEUE_SIZE: int = 100_000

    # ========================================================
    # Caching
    # ========================================================

    EMPTY_WALLET_CACHE_SIZE: int = 200_000

    # ========================================================
    # Runtime
    # ========================================================

    BLOCK_POLL_INTERVAL: float = 3.0

    WRITE_BUFFER_SIZE: int = 1000

    LOG_LEVEL: int = logging.INFO

    USER_AGENT: str = (
        "Mozilla/5.0 NFTScanner/2.0 "
        "(aiohttp async high-performance scanner)"
    )


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
# Optimized Token Bucket
# ============================================================

class TokenBucket:
    def __init__(self, rate: int, capacity: int):
        self.rate = rate
        self.capacity = capacity

        self.tokens = float(capacity)
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


bucket = TokenBucket(
    Config.GLOBAL_RPS,
    Config.TOKEN_BUCKET_SIZE
)


# ============================================================
# API Key Rotation
# ============================================================

class APIKeyManager:
    def __init__(self, keys: List[str]):
        self.keys = [k for k in keys if k and k != "YOUR_ETHERSCAN_API_KEY"]

        if not self.keys:
            raise ValueError("No valid API keys provided")

        self.index = 0
        self.lock = asyncio.Lock()

    async def get_key(self) -> str:
        async with self.lock:
            key = self.keys[self.index]
            self.index = (self.index + 1) % len(self.keys)
            return key


api_keys = APIKeyManager(list(Config.API_KEYS))


# ============================================================
# URL Builder
# ============================================================

def build_url(module: str, action: str, apikey: str, **params) -> str:
    query = {
        "module": module,
        "action": action,
        "apikey": apikey,
        **params
    }

    return (
        Config.BASE_URL
        + "?"
        + "&".join(f"{k}={v}" for k, v in query.items())
    )


# ============================================================
# Optimized LRU Cache
# ============================================================

class LRUCache:
    def __init__(self, max_size: int):
        self.data = OrderedDict()
        self.max_size = max_size

    def contains(self, key: str) -> bool:
        if key in self.data:
            self.data.move_to_end(key)
            return True
        return False

    def add(self, key: str):
        self.data[key] = None
        self.data.move_to_end(key)

        if len(self.data) > self.max_size:
            self.data.popitem(last=False)


empty_wallets = LRUCache(
    Config.EMPTY_WALLET_CACHE_SIZE
)


# ============================================================
# Async Writer
# ============================================================

class AsyncWriter:
    def __init__(self, path: str):
        self.path = path

        self.buffer = set()
        self.lock = asyncio.Lock()

        self.total_written = 0

    async def add(self, items: Set[str]):
        flush_data = None

        async with self.lock:
            self.buffer.update(items)

            if len(self.buffer) >= Config.WRITE_BUFFER_SIZE:
                flush_data = self.buffer
                self.buffer = set()

        if flush_data:
            await self._write(flush_data)

    async def flush(self):
        async with self.lock:
            data = self.buffer
            self.buffer = set()

        if data:
            await self._write(data)

    async def _write(self, data: Set[str]):
        async with aiofiles.open(self.path, "a") as f:
            await f.write("\n".join(data) + "\n")

        self.total_written += len(data)

        logger.info(
            f"Saved {len(data)} contracts "
            f"(total: {self.total_written})"
        )


# ============================================================
# Statistics
# ============================================================

class Stats:
    def __init__(self):
        self.start_time = monotonic()

        self.blocks = 0
        self.addresses = 0
        self.contracts = 0
        self.requests = 0
        self.errors = 0

        self.lock = asyncio.Lock()

    async def log(self):
        while True:
            await asyncio.sleep(30)

            uptime = monotonic() - self.start_time

            logger.info(
                f"[stats] "
                f"uptime={uptime:.0f}s "
                f"blocks={self.blocks} "
                f"addresses={self.addresses} "
                f"contracts={self.contracts} "
                f"requests={self.requests} "
                f"errors={self.errors}"
            )


stats = Stats()


# ============================================================
# HTTP
# ============================================================

async def fetch_json(
    session: aiohttp.ClientSession,
    url: str
) -> Optional[Dict[str, Any]]:

    for attempt in range(1, Config.MAX_RETRIES + 1):

        try:
            await bucket.acquire()

            async with session.get(
                url,
                timeout=aiohttp.ClientTimeout(
                    total=Config.REQUEST_TIMEOUT
                )
            ) as response:

                stats.requests += 1

                if response.status == 429:
                    raise aiohttp.ClientError("HTTP 429")

                if response.status >= 500:
                    raise aiohttp.ClientError(
                        f"HTTP {response.status}"
                    )

                data = await response.json(
                    content_type=None
                )

                if not isinstance(data, dict):
                    raise ValueError("invalid json")

                result = str(data.get("result", "")).lower()

                if "rate limit" in result:
                    raise aiohttp.ClientError(
                        "etherscan rate limit"
                    )

                return data

        except (
            aiohttp.ClientError,
            asyncio.TimeoutError,
            ValueError
        ) as e:

            stats.errors += 1

            delay = min(
                Config.MAX_BACKOFF,
                (
                    Config.MIN_BACKOFF
                    * (2 ** (attempt - 1))
                ) + random.uniform(0, 1)
            )

            logger.warning(
                f"[retry {attempt}/{Config.MAX_RETRIES}] "
                f"{e} -> sleep {delay:.2f}s"
            )

            await asyncio.sleep(delay)

    return None


# ============================================================
# API
# ============================================================

async def get_latest_block(
    session: aiohttp.ClientSession
) -> Optional[int]:

    key = await api_keys.get_key()

    data = await fetch_json(
        session,
        build_url(
            "proxy",
            "eth_blockNumber",
            apikey=key
        )
    )

    try:
        return int(data["result"], 16)
    except Exception:
        return None


async def get_block_addresses(
    session: aiohttp.ClientSession,
    block: int
) -> Set[str]:

    key = await api_keys.get_key()

    data = await fetch_json(
        session,
        build_url(
            "proxy",
            "eth_getBlockByNumber",
            apikey=key,
            tag=f"0x{block:x}",
            boolean="true"
        )
    )

    result = set()

    if not data:
        return result

    txs = (
        data.get("result", {})
        .get("transactions", [])
    )

    for tx in txs:

        if not isinstance(tx, dict):
            continue

        from_addr = tx.get("from")
        to_addr = tx.get("to")

        if from_addr:
            result.add(from_addr.lower())

        if to_addr:
            result.add(to_addr.lower())

    return result


async def fetch_nfts(
    session: aiohttp.ClientSession,
    address: str
) -> Set[str]:

    if empty_wallets.contains(address):
        return set()

    key = await api_keys.get_key()

    data = await fetch_json(
        session,
        build_url(
            "account",
            "tokennfttx",
            apikey=key,
            address=address,
            page=1,
            offset=100,
            sort="desc"
        )
    )

    if not data:
        return set()

    result = data.get("result")

    if not isinstance(result, list):
        empty_wallets.add(address)
        return set()

    contracts = set()

    for tx in result:

        if not isinstance(tx, dict):
            continue

        contract = tx.get("contractAddress")

        if not contract:
            continue

        contract = contract.lower()

        if (
            contract != "0x0000000000000000000000000000000000000000"
            and len(contract) == 42
        ):
            contracts.add(contract)

    if not contracts:
        empty_wallets.add(address)

    return contracts


# ============================================================
# Worker
# ============================================================

async def worker(
    queue: asyncio.Queue,
    session: aiohttp.ClientSession,
    seen_contracts: Set[str],
    seen_addresses: Set[str],
    writer: AsyncWriter,
    stop_event: asyncio.Event
):

    while True:

        if stop_event.is_set() and queue.empty():
            return

        try:
            address = await asyncio.wait_for(
                queue.get(),
                timeout=1
            )

        except asyncio.TimeoutError:
            continue

        try:
            contracts = await fetch_nfts(
                session,
                address
            )

            if contracts:

                new_contracts = (
                    contracts - seen_contracts
                )

                if new_contracts:

                    seen_contracts.update(
                        new_contracts
                    )

                    stats.contracts += len(
                        new_contracts
                    )

                    await writer.add(
                        new_contracts
                    )

        except Exception as e:
            logger.exception(
                f"worker error: {e}"
            )

        finally:
            seen_addresses.discard(address)
            queue.task_done()


# ============================================================
# Load Existing Contracts
# ============================================================

def load_existing_contracts() -> Set[str]:

    contracts = set()

    for path in (
        Config.OUTPUT_FILE,
        Config.KNOWN_CONTRACTS_FILE
    ):

        if not os.path.exists(path):
            continue

        try:
            with open(path, "r", encoding="utf-8") as f:

                for line in f:
                    line = line.strip().lower()

                    if line:
                        contracts.add(line)

            logger.info(
                f"Loaded {len(contracts)} "
                f"contracts from {path}"
            )

        except Exception as e:
            logger.warning(
                f"Failed to load {path}: {e}"
            )

    return contracts


# ============================================================
# Main
# ============================================================

async def main():

    stop_event = asyncio.Event()

    def shutdown():
        logger.warning("Shutdown signal received")
        stop_event.set()

    loop = asyncio.get_running_loop()

    for sig in (
        signal.SIGINT,
        signal.SIGTERM
    ):
        try:
            loop.add_signal_handler(
                sig,
                shutdown
            )
        except NotImplementedError:
            pass

    connector = aiohttp.TCPConnector(
        limit=Config.TCP_LIMIT,
        limit_per_host=Config.TCP_LIMIT_PER_HOST,
        ttl_dns_cache=Config.DNS_CACHE_TTL,
        enable_cleanup_closed=True,
        ssl=False,
    )

    headers = {
        "User-Agent": Config.USER_AGENT,
        "Accept": "application/json",
        "Connection": "keep-alive",
    }

    queue = asyncio.Queue(
        maxsize=Config.ADDRESS_QUEUE_SIZE
    )

    writer = AsyncWriter(
        Config.OUTPUT_FILE
    )

    seen_contracts = load_existing_contracts()

    seen_addresses = set()

    logger.info(
        f"Loaded {len(seen_contracts)} "
        f"existing contracts"
    )

    async with aiohttp.ClientSession(
        connector=connector,
        headers=headers
    ) as session:

        workers = [
            asyncio.create_task(
                worker(
                    queue,
                    session,
                    seen_contracts,
                    seen_addresses,
                    writer,
                    stop_event
                )
            )
            for _ in range(Config.WORKERS)
        ]

        stats_task = asyncio.create_task(
            stats.log()
        )

        latest_processed = None

        try:

            while not stop_event.is_set():

                latest_block = await get_latest_block(
                    session
                )

                if latest_block is None:
                    await asyncio.sleep(3)
                    continue

                if latest_processed is None:
                    latest_processed = latest_block - 1

                if latest_block > latest_processed:

                    logger.info(
                        f"Processing blocks "
                        f"{latest_processed + 1}"
                        f" -> {latest_block}"
                    )

                for block in range(
                    latest_processed + 1,
                    latest_block + 1
                ):

                    addresses = await get_block_addresses(
                        session,
                        block
                    )

                    stats.blocks += 1
                    stats.addresses += len(addresses)

                    for address in addresses:

                        if (
                            address in seen_addresses
                            or empty_wallets.contains(address)
                        ):
                            continue

                        seen_addresses.add(address)

                        try:
                            queue.put_nowait(address)

                        except asyncio.QueueFull:
                            await queue.put(address)

                    latest_processed = block

                await asyncio.sleep(
                    Config.BLOCK_POLL_INTERVAL
                )

        finally:

            logger.info(
                "Waiting for queue to finish..."
            )

            await queue.join()

            await writer.flush()

            stats_task.cancel()

            for w in workers:
                w.cancel()

            await asyncio.gather(
                *workers,
                return_exceptions=True
            )

    logger.info("Shutdown complete")


# ============================================================
# Entry
# ============================================================

if __name__ == "__main__":

    if sys.platform.startswith("win"):
        asyncio.set_event_loop_policy(
            asyncio.WindowsSelectorEventLoopPolicy()
        )

    try:
        asyncio.run(main())

    except KeyboardInterrupt:
        pass
