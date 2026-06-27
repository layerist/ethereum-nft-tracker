#!/usr/bin/env python3
"""
NFT Scanner v4 — safer async Etherscan scanner

What it does:
- Reads new Ethereum blocks through Etherscan proxy API.
- Extracts sender/recipient addresses from block transactions.
- Checks recent ERC-721 transfers for those addresses.
- Saves newly discovered NFT contract addresses.

Main improvements over the provided v3:
- Real checkpoint/resume with atomic JSON writes.
- Proper URL encoding via aiohttp params, not manual string concat.
- API-key rotation with per-key cooldown on 429/rate-limit responses.
- Safer Etherscan response handling: status/message/result are interpreted.
- Confirmation lag to reduce reorg-related false progress.
- Bounded in-flight address dedupe to avoid unlimited memory growth.
- Atomic-ish append writer with stable sorted output batches.
- Async-safe stats increments and periodic rate reporting.
- Cleaner env-based configuration and validation.

Environment variables:
  ETHERSCAN_API_KEYS=key1,key2,key3
  START_BLOCK=0                    # optional; 0 = start from latest safe block
  GLOBAL_RPS=5                     # keep realistic for your Etherscan plan
  WORKERS=20
  CONFIRMATIONS=2
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import random
import signal
import sys
from collections import OrderedDict
from dataclasses import dataclass
from pathlib import Path
from time import monotonic
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

import aiofiles
import aiohttp

ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"
PLACEHOLDER_KEYS = {"", "YOUR_ETHERSCAN_API_KEY", "changeme", "none", "null"}


# ============================================================
# Configuration
# ============================================================


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        return int(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer, got {raw!r}") from exc


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        return float(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be a float, got {raw!r}") from exc


def _env_list(name: str, default: Iterable[str]) -> List[str]:
    raw = os.getenv(name)
    values = list(default) if raw is None else [x.strip() for x in raw.split(",")]
    return [x for x in values if x and x not in PLACEHOLDER_KEYS]


@dataclass(frozen=True)
class Config:
    api_keys: Tuple[str, ...]
    base_url: str = "https://api.etherscan.io/api"

    output_file: Path = Path("nft_contracts.txt")
    known_contracts_file: Path = Path("known_contracts.txt")
    checkpoint_file: Path = Path("nft_scanner_checkpoint.json")

    request_timeout: float = 15.0
    max_retries: int = 7
    min_backoff: float = 0.4
    max_backoff: float = 30.0

    tcp_limit: int = 100
    tcp_limit_per_host: int = 50
    dns_cache_ttl: int = 300

    workers: int = 20
    global_rps: float = 5.0
    token_bucket_size: int = 10
    address_queue_size: int = 100_000

    empty_wallet_cache_size: int = 200_000
    in_flight_address_cache_size: int = 300_000

    block_poll_interval: float = 3.0
    confirmations: int = 2
    max_blocks_per_poll: int = 3

    write_buffer_size: int = 1000
    stats_interval: float = 30.0

    api_key_cooldown_sec: float = 30.0
    start_block: int = 0

    log_level: str = "INFO"
    user_agent: str = "Mozilla/5.0 NFTScanner/4.0 (aiohttp async scanner)"

    @classmethod
    def from_env(cls) -> "Config":
        return cls(
            api_keys=tuple(_env_list("ETHERSCAN_API_KEYS", ["YOUR_ETHERSCAN_API_KEY"])),
            base_url=os.getenv("ETHERSCAN_BASE_URL", cls.base_url),
            output_file=Path(os.getenv("OUTPUT_FILE", str(cls.output_file))),
            known_contracts_file=Path(os.getenv("KNOWN_CONTRACTS_FILE", str(cls.known_contracts_file))),
            checkpoint_file=Path(os.getenv("CHECKPOINT_FILE", str(cls.checkpoint_file))),
            request_timeout=_env_float("REQUEST_TIMEOUT", cls.request_timeout),
            max_retries=_env_int("MAX_RETRIES", cls.max_retries),
            min_backoff=_env_float("MIN_BACKOFF", cls.min_backoff),
            max_backoff=_env_float("MAX_BACKOFF", cls.max_backoff),
            tcp_limit=_env_int("TCP_LIMIT", cls.tcp_limit),
            tcp_limit_per_host=_env_int("TCP_LIMIT_PER_HOST", cls.tcp_limit_per_host),
            dns_cache_ttl=_env_int("DNS_CACHE_TTL", cls.dns_cache_ttl),
            workers=_env_int("WORKERS", cls.workers),
            global_rps=_env_float("GLOBAL_RPS", cls.global_rps),
            token_bucket_size=_env_int("TOKEN_BUCKET_SIZE", cls.token_bucket_size),
            address_queue_size=_env_int("ADDRESS_QUEUE_SIZE", cls.address_queue_size),
            empty_wallet_cache_size=_env_int("EMPTY_WALLET_CACHE_SIZE", cls.empty_wallet_cache_size),
            in_flight_address_cache_size=_env_int("IN_FLIGHT_ADDRESS_CACHE_SIZE", cls.in_flight_address_cache_size),
            block_poll_interval=_env_float("BLOCK_POLL_INTERVAL", cls.block_poll_interval),
            confirmations=_env_int("CONFIRMATIONS", cls.confirmations),
            max_blocks_per_poll=_env_int("MAX_BLOCKS_PER_POLL", cls.max_blocks_per_poll),
            write_buffer_size=_env_int("WRITE_BUFFER_SIZE", cls.write_buffer_size),
            stats_interval=_env_float("STATS_INTERVAL", cls.stats_interval),
            api_key_cooldown_sec=_env_float("API_KEY_COOLDOWN_SEC", cls.api_key_cooldown_sec),
            start_block=_env_int("START_BLOCK", cls.start_block),
            log_level=os.getenv("LOG_LEVEL", cls.log_level).upper(),
            user_agent=os.getenv("USER_AGENT", cls.user_agent),
        )

    def validate(self) -> None:
        if not self.api_keys:
            raise ValueError("No valid Etherscan keys. Set ETHERSCAN_API_KEYS=key1,key2")
        if self.workers < 1:
            raise ValueError("WORKERS must be >= 1")
        if self.global_rps <= 0:
            raise ValueError("GLOBAL_RPS must be > 0")
        if self.token_bucket_size < 1:
            raise ValueError("TOKEN_BUCKET_SIZE must be >= 1")
        if self.confirmations < 0:
            raise ValueError("CONFIRMATIONS must be >= 0")
        if self.max_blocks_per_poll < 1:
            raise ValueError("MAX_BLOCKS_PER_POLL must be >= 1")


# ============================================================
# Logging
# ============================================================


def setup_logging(level: str) -> logging.Logger:
    logging.basicConfig(
        level=getattr(logging, level, logging.INFO),
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )
    return logging.getLogger("nft-scanner")


# ============================================================
# Helpers
# ============================================================


def is_hex_address(value: Any) -> bool:
    if not isinstance(value, str):
        return False
    if len(value) != 42 or not value.startswith("0x"):
        return False
    try:
        int(value[2:], 16)
        return True
    except ValueError:
        return False


class LRUCache:
    def __init__(self, max_size: int):
        self.data: OrderedDict[str, None] = OrderedDict()
        self.max_size = max_size

    def contains(self, key: str) -> bool:
        if key in self.data:
            self.data.move_to_end(key)
            return True
        return False

    def add(self, key: str) -> None:
        self.data[key] = None
        self.data.move_to_end(key)
        if len(self.data) > self.max_size:
            self.data.popitem(last=False)

    def discard(self, key: str) -> None:
        self.data.pop(key, None)

    def __len__(self) -> int:
        return len(self.data)


class TokenBucket:
    def __init__(self, rate: float, capacity: int):
        self.rate = float(rate)
        self.capacity = int(capacity)
        self.tokens = float(capacity)
        self.updated = monotonic()
        self.lock = asyncio.Lock()

    async def acquire(self) -> None:
        while True:
            async with self.lock:
                now = monotonic()
                elapsed = now - self.updated
                self.updated = now
                self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)
                if self.tokens >= 1:
                    self.tokens -= 1
                    return
                sleep_for = max(0.0, (1 - self.tokens) / self.rate)
            await asyncio.sleep(sleep_for)


class APIKeyManager:
    def __init__(self, keys: Iterable[str], cooldown_sec: float):
        self.keys = list(keys)
        self.cooldown_sec = cooldown_sec
        self.index = 0
        self.cooldowns: Dict[str, float] = {key: 0.0 for key in self.keys}
        self.lock = asyncio.Lock()

    async def get_key(self) -> str:
        while True:
            async with self.lock:
                now = monotonic()
                available = [key for key in self.keys if self.cooldowns.get(key, 0.0) <= now]
                if available:
                    for _ in range(len(self.keys)):
                        key = self.keys[self.index]
                        self.index = (self.index + 1) % len(self.keys)
                        if key in available:
                            return key

                sleep_for = min(max(min(self.cooldowns.values()) - now, 0.1), self.cooldown_sec)

            await asyncio.sleep(sleep_for)

    async def cooldown(self, key: str, reason: str, logger: logging.Logger) -> None:
        async with self.lock:
            self.cooldowns[key] = monotonic() + self.cooldown_sec
        logger.warning("API key put on cooldown for %.1fs: %s", self.cooldown_sec, reason)


class Stats:
    def __init__(self) -> None:
        self.start_time = monotonic()
        self.blocks = 0
        self.addresses_seen = 0
        self.addresses_enqueued = 0
        self.contracts = 0
        self.requests = 0
        self.errors = 0
        self.rate_limits = 0
        self.empty_cached = 0
        self.lock = asyncio.Lock()

    async def inc(self, **kwargs: int) -> None:
        async with self.lock:
            for key, value in kwargs.items():
                setattr(self, key, getattr(self, key) + value)

    async def snapshot(self) -> Dict[str, Any]:
        async with self.lock:
            return {
                "uptime": monotonic() - self.start_time,
                "blocks": self.blocks,
                "addresses_seen": self.addresses_seen,
                "addresses_enqueued": self.addresses_enqueued,
                "contracts": self.contracts,
                "requests": self.requests,
                "errors": self.errors,
                "rate_limits": self.rate_limits,
                "empty_cached": self.empty_cached,
            }

    async def log_loop(self, interval: float, logger: logging.Logger, queue: asyncio.Queue, in_flight: LRUCache) -> None:
        while True:
            await asyncio.sleep(interval)
            s = await self.snapshot()
            uptime = max(s["uptime"], 1.0)
            logger.info(
                "[stats] uptime=%.0fs blocks=%d contracts=%d req=%d err=%d rate_limits=%d "
                "addr_seen=%d addr_enqueued=%d queue=%d in_flight=%d req/s=%.2f",
                s["uptime"],
                s["blocks"],
                s["contracts"],
                s["requests"],
                s["errors"],
                s["rate_limits"],
                s["addresses_seen"],
                s["addresses_enqueued"],
                queue.qsize(),
                len(in_flight),
                s["requests"] / uptime,
            )


class Checkpoint:
    def __init__(self, path: Path, logger: logging.Logger):
        self.path = path
        self.logger = logger

    def load(self) -> Optional[int]:
        if not self.path.exists():
            return None
        try:
            with self.path.open("r", encoding="utf-8") as f:
                data = json.load(f)
            block = data.get("last_processed_block")
            return int(block) if block is not None else None
        except Exception as exc:
            self.logger.warning("Failed to load checkpoint %s: %s", self.path, exc)
            return None

    async def save(self, block: int) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_suffix(self.path.suffix + ".tmp")
        payload = {
            "last_processed_block": int(block),
            "saved_at_monotonic": monotonic(),
        }
        async with aiofiles.open(tmp, "w", encoding="utf-8") as f:
            await f.write(json.dumps(payload, ensure_ascii=False, indent=2))
        os.replace(tmp, self.path)


class AsyncContractWriter:
    def __init__(self, path: Path, buffer_size: int, logger: logging.Logger):
        self.path = path
        self.buffer_size = buffer_size
        self.logger = logger
        self.buffer: Set[str] = set()
        self.lock = asyncio.Lock()
        self.total_written = 0

    async def add(self, items: Set[str]) -> None:
        if not items:
            return
        flush_data: Optional[Set[str]] = None
        async with self.lock:
            self.buffer.update(items)
            if len(self.buffer) >= self.buffer_size:
                flush_data = self.buffer
                self.buffer = set()
        if flush_data:
            await self._write(flush_data)

    async def flush(self) -> None:
        async with self.lock:
            data = self.buffer
            self.buffer = set()
        if data:
            await self._write(data)

    async def _write(self, data: Set[str]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        lines = "\n".join(sorted(data)) + "\n"
        async with aiofiles.open(self.path, "a", encoding="utf-8") as f:
            await f.write(lines)
        self.total_written += len(data)
        self.logger.info("Saved %d contracts (session total: %d)", len(data), self.total_written)


# ============================================================
# Etherscan client
# ============================================================


class EtherscanClient:
    def __init__(
        self,
        session: aiohttp.ClientSession,
        config: Config,
        api_keys: APIKeyManager,
        bucket: TokenBucket,
        stats: Stats,
        logger: logging.Logger,
    ):
        self.session = session
        self.config = config
        self.api_keys = api_keys
        self.bucket = bucket
        self.stats = stats
        self.logger = logger
        self.timeout = aiohttp.ClientTimeout(total=config.request_timeout)

    async def request(self, module: str, action: str, **params: Any) -> Optional[Dict[str, Any]]:
        last_error: Optional[Exception] = None

        for attempt in range(1, self.config.max_retries + 1):
            key = await self.api_keys.get_key()
            query = {
                "module": module,
                "action": action,
                "apikey": key,
                **{k: v for k, v in params.items() if v is not None},
            }

            try:
                await self.bucket.acquire()
                async with self.session.get(self.config.base_url, params=query, timeout=self.timeout) as response:
                    await self.stats.inc(requests=1)

                    if response.status == 429:
                        await self.stats.inc(rate_limits=1)
                        await self.api_keys.cooldown(key, "HTTP 429", self.logger)
                        raise aiohttp.ClientResponseError(
                            response.request_info,
                            response.history,
                            status=response.status,
                            message="HTTP 429",
                        )

                    if response.status >= 500:
                        raise aiohttp.ClientResponseError(
                            response.request_info,
                            response.history,
                            status=response.status,
                            message=f"HTTP {response.status}",
                        )

                    data = await response.json(content_type=None)
                    if not isinstance(data, dict):
                        raise ValueError("Etherscan returned non-object JSON")

                    result_text = str(data.get("result", "")).lower()
                    message_text = str(data.get("message", "")).lower()
                    status_text = str(data.get("status", "")).lower()
                    combined = f"{status_text} {message_text} {result_text}"

                    if any(x in combined for x in ("rate limit", "max rate", "too many requests")):
                        await self.stats.inc(rate_limits=1)
                        await self.api_keys.cooldown(key, combined[:120], self.logger)
                        raise aiohttp.ClientError("Etherscan rate limit")

                    return data

            except (aiohttp.ClientError, asyncio.TimeoutError, ValueError) as exc:
                last_error = exc
                await self.stats.inc(errors=1)
                delay = min(
                    self.config.max_backoff,
                    self.config.min_backoff * (2 ** (attempt - 1)) + random.uniform(0.0, 1.0),
                )
                self.logger.warning(
                    "[retry %d/%d] %s.%s failed: %s -> sleep %.2fs",
                    attempt,
                    self.config.max_retries,
                    module,
                    action,
                    exc,
                    delay,
                )
                await asyncio.sleep(delay)

        self.logger.error("Request failed after retries: %s.%s (%s)", module, action, last_error)
        return None

    async def latest_block(self) -> Optional[int]:
        data = await self.request("proxy", "eth_blockNumber")
        try:
            result = data["result"] if data else None
            return int(result, 16) if isinstance(result, str) else None
        except Exception:
            return None

    async def block_addresses(self, block: int) -> Set[str]:
        data = await self.request(
            "proxy",
            "eth_getBlockByNumber",
            tag=f"0x{block:x}",
            boolean="true",
        )
        if not data:
            return set()

        block_data = data.get("result")
        if not isinstance(block_data, dict):
            return set()

        txs = block_data.get("transactions", [])
        if not isinstance(txs, list):
            return set()

        addresses: Set[str] = set()
        for tx in txs:
            if not isinstance(tx, dict):
                continue
            for field in ("from", "to"):
                address = tx.get(field)
                if is_hex_address(address) and address.lower() != ZERO_ADDRESS:
                    addresses.add(address.lower())
        return addresses

    async def nft_contracts_for_address(self, address: str) -> Set[str]:
        data = await self.request(
            "account",
            "tokennfttx",
            address=address,
            page=1,
            offset=100,
            sort="desc",
        )
        if not data:
            return set()

        result = data.get("result")
        if not isinstance(result, list):
            return set()

        contracts: Set[str] = set()
        for tx in result:
            if not isinstance(tx, dict):
                continue
            contract = tx.get("contractAddress")
            if is_hex_address(contract) and contract.lower() != ZERO_ADDRESS:
                contracts.add(contract.lower())
        return contracts


# ============================================================
# Loading existing data
# ============================================================


def load_existing_contracts(paths: Iterable[Path], logger: logging.Logger) -> Set[str]:
    contracts: Set[str] = set()
    for path in paths:
        if not path.exists():
            continue
        before = len(contracts)
        try:
            with path.open("r", encoding="utf-8") as f:
                for line in f:
                    value = line.strip().lower()
                    if is_hex_address(value) and value != ZERO_ADDRESS:
                        contracts.add(value)
            logger.info("Loaded %d contracts from %s", len(contracts) - before, path)
        except Exception as exc:
            logger.warning("Failed to load %s: %s", path, exc)
    return contracts


# ============================================================
# Workers
# ============================================================


async def address_worker(
    name: str,
    queue: asyncio.Queue[str],
    client: EtherscanClient,
    seen_contracts: Set[str],
    empty_wallets: LRUCache,
    in_flight_addresses: LRUCache,
    writer: AsyncContractWriter,
    stats: Stats,
    stop_event: asyncio.Event,
    logger: logging.Logger,
) -> None:
    while True:
        if stop_event.is_set() and queue.empty():
            return

        try:
            address = await asyncio.wait_for(queue.get(), timeout=1.0)
        except asyncio.TimeoutError:
            continue

        try:
            if empty_wallets.contains(address):
                continue

            contracts = await client.nft_contracts_for_address(address)
            if not contracts:
                empty_wallets.add(address)
                await stats.inc(empty_cached=1)
                continue

            new_contracts = contracts - seen_contracts
            if new_contracts:
                seen_contracts.update(new_contracts)
                await stats.inc(contracts=len(new_contracts))
                await writer.add(new_contracts)

        except Exception as exc:
            await stats.inc(errors=1)
            logger.exception("%s worker error for %s: %s", name, address, exc)
        finally:
            in_flight_addresses.discard(address)
            queue.task_done()


# ============================================================
# Main loop
# ============================================================


async def run(config: Config) -> None:
    config.validate()
    logger = setup_logging(config.log_level)
    logger.info("Starting NFT scanner: workers=%d rps=%.2f confirmations=%d", config.workers, config.global_rps, config.confirmations)

    stop_event = asyncio.Event()

    def shutdown() -> None:
        logger.warning("Shutdown signal received")
        stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, shutdown)
        except (NotImplementedError, RuntimeError):
            pass

    queue: asyncio.Queue[str] = asyncio.Queue(maxsize=config.address_queue_size)
    stats = Stats()
    bucket = TokenBucket(config.global_rps, config.token_bucket_size)
    key_manager = APIKeyManager(config.api_keys, config.api_key_cooldown_sec)
    empty_wallets = LRUCache(config.empty_wallet_cache_size)
    in_flight_addresses = LRUCache(config.in_flight_address_cache_size)
    writer = AsyncContractWriter(config.output_file, config.write_buffer_size, logger)
    checkpoint = Checkpoint(config.checkpoint_file, logger)
    seen_contracts = load_existing_contracts((config.output_file, config.known_contracts_file), logger)

    connector = aiohttp.TCPConnector(
        limit=config.tcp_limit,
        limit_per_host=config.tcp_limit_per_host,
        ttl_dns_cache=config.dns_cache_ttl,
        enable_cleanup_closed=True,
    )
    headers = {
        "User-Agent": config.user_agent,
        "Accept": "application/json",
        "Connection": "keep-alive",
    }

    async with aiohttp.ClientSession(connector=connector, headers=headers) as session:
        client = EtherscanClient(session, config, key_manager, bucket, stats, logger)

        workers = [
            asyncio.create_task(
                address_worker(
                    f"worker-{i + 1}",
                    queue,
                    client,
                    seen_contracts,
                    empty_wallets,
                    in_flight_addresses,
                    writer,
                    stats,
                    stop_event,
                    logger,
                )
            )
            for i in range(config.workers)
        ]
        stats_task = asyncio.create_task(stats.log_loop(config.stats_interval, logger, queue, in_flight_addresses))

        last_processed = checkpoint.load()
        if last_processed is not None:
            logger.info("Resuming from checkpoint: last_processed_block=%d", last_processed)
        elif config.start_block > 0:
            last_processed = config.start_block - 1
            logger.info("Starting from START_BLOCK=%d", config.start_block)

        try:
            while not stop_event.is_set():
                latest = await client.latest_block()
                if latest is None:
                    await asyncio.sleep(config.block_poll_interval)
                    continue

                latest_safe = max(0, latest - config.confirmations)
                if last_processed is None:
                    last_processed = latest_safe
                    await checkpoint.save(last_processed)
                    logger.info("Initialized checkpoint at latest safe block: %d", last_processed)
                    await asyncio.sleep(config.block_poll_interval)
                    continue

                if latest_safe <= last_processed:
                    await asyncio.sleep(config.block_poll_interval)
                    continue

                batch_to = min(latest_safe, last_processed + config.max_blocks_per_poll)
                logger.info("Processing blocks %d -> %d (latest=%d, safe=%d)", last_processed + 1, batch_to, latest, latest_safe)

                for block in range(last_processed + 1, batch_to + 1):
                    if stop_event.is_set():
                        break

                    addresses = await client.block_addresses(block)
                    await stats.inc(blocks=1, addresses_seen=len(addresses))

                    enqueued = 0
                    for address in addresses:
                        if empty_wallets.contains(address) or in_flight_addresses.contains(address):
                            continue
                        in_flight_addresses.add(address)
                        await queue.put(address)
                        enqueued += 1

                    await stats.inc(addresses_enqueued=enqueued)
                    logger.info("Block %d: addresses=%d enqueued=%d queue=%d", block, len(addresses), enqueued, queue.qsize())
                    last_processed = block

                # Do not checkpoint until queued addresses from this block batch are processed.
                # This prevents losing a block's addresses on crash after enqueue but before worker completion.
                await queue.join()
                await writer.flush()
                await checkpoint.save(last_processed)
                logger.info("Checkpoint saved at block %d", last_processed)

        finally:
            logger.info("Stopping: waiting for queued work to finish...")
            stop_event.set()
            await queue.join()
            await writer.flush()

            stats_task.cancel()
            for task in workers:
                task.cancel()

            await asyncio.gather(*workers, return_exceptions=True)
            await asyncio.gather(stats_task, return_exceptions=True)

    logger.info("Shutdown complete")


# ============================================================
# Entry
# ============================================================


def main() -> None:
    if sys.platform.startswith("win"):
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    config = Config.from_env()
    try:
        asyncio.run(run(config))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
