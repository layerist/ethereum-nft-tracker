#!/usr/bin/env python3
"""
NFT Scanner v5 — reliable asynchronous Etherscan V2 scanner.

Scans confirmed Ethereum blocks, extracts transaction participants, requests
recent ERC-721 transfers for those addresses and appends newly discovered NFT
contract addresses to a text file.

Reliability changes over v4:
- Etherscan API V2 endpoint and explicit CHAIN_ID.
- Request failure is never treated as an empty wallet.
- A failed block is never checkpointed or silently skipped.
- Exact in-flight address set: queued items cannot be evicted and duplicated.
- Per-key cooldown honours Retry-After and separates rate-limit failures.
- Explicit Etherscan envelope/error classification.
- Atomic checkpoint with UTC timestamp and optional directory fsync.
- Serialized append writer with flush + fsync before checkpoint advancement.
- Graceful Windows/POSIX shutdown and fatal worker monitoring.
- Stronger configuration validation and secret-safe logging.

Required environment variable:
  ETHERSCAN_API_KEYS=key1,key2

Useful variables:
  CHAIN_ID=1
  ETHERSCAN_BASE_URL=https://api.etherscan.io/v2/api
  START_BLOCK=0
  GLOBAL_RPS=3
  WORKERS=20
  CONFIRMATIONS=3
  NFT_TX_OFFSET=100
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
from datetime import datetime, timezone
from pathlib import Path
from time import monotonic
from typing import Any, Iterable, Mapping, Optional, TypeVar

import aiofiles
import aiohttp

ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"
PLACEHOLDER_KEYS = {"", "your_etherscan_api_key", "changeme", "none", "null"}
T = TypeVar("T")


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------


def env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    try:
        return int(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer, got {raw!r}") from exc


def env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    try:
        return float(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be a number, got {raw!r}") from exc


def env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    value = raw.strip().lower()
    if value in {"1", "true", "yes", "on"}:
        return True
    if value in {"0", "false", "no", "off"}:
        return False
    raise ValueError(f"{name} must be true/false, got {raw!r}")


def env_keys(name: str) -> tuple[str, ...]:
    raw = os.getenv(name, "")
    result: list[str] = []
    seen: set[str] = set()
    for item in raw.split(","):
        key = item.strip()
        if key.lower() in PLACEHOLDER_KEYS or key in seen:
            continue
        seen.add(key)
        result.append(key)
    return tuple(result)


@dataclass(frozen=True, slots=True)
class Config:
    api_keys: tuple[str, ...]
    chain_id: int = 1
    base_url: str = "https://api.etherscan.io/v2/api"

    output_file: Path = Path("nft_contracts.txt")
    known_contracts_file: Path = Path("known_contracts.txt")
    checkpoint_file: Path = Path("nft_scanner_checkpoint.json")

    request_timeout: float = 20.0
    connect_timeout: float = 10.0
    max_retries: int = 7
    min_backoff: float = 0.5
    max_backoff: float = 30.0

    workers: int = 20
    global_rps: float = 3.0
    token_bucket_size: int = 3
    address_queue_size: int = 50_000

    tcp_limit: int = 100
    tcp_limit_per_host: int = 50
    dns_cache_ttl: int = 300

    confirmations: int = 3
    max_blocks_per_batch: int = 3
    block_poll_interval: float = 3.0
    nft_tx_offset: int = 100

    empty_wallet_cache_size: int = 200_000
    write_buffer_size: int = 500
    stats_interval: float = 30.0

    api_key_cooldown_sec: float = 30.0
    start_block: int = 0
    fsync_writes: bool = True

    log_level: str = "INFO"
    user_agent: str = "NFTScanner/5.0 (+aiohttp; Etherscan-V2)"

    @classmethod
    def from_env(cls) -> "Config":
        return cls(
            api_keys=env_keys("ETHERSCAN_API_KEYS"),
            chain_id=env_int("CHAIN_ID", cls.chain_id),
            base_url=os.getenv("ETHERSCAN_BASE_URL", cls.base_url).strip(),
            output_file=Path(os.getenv("OUTPUT_FILE", str(cls.output_file))),
            known_contracts_file=Path(os.getenv("KNOWN_CONTRACTS_FILE", str(cls.known_contracts_file))),
            checkpoint_file=Path(os.getenv("CHECKPOINT_FILE", str(cls.checkpoint_file))),
            request_timeout=env_float("REQUEST_TIMEOUT", cls.request_timeout),
            connect_timeout=env_float("CONNECT_TIMEOUT", cls.connect_timeout),
            max_retries=env_int("MAX_RETRIES", cls.max_retries),
            min_backoff=env_float("MIN_BACKOFF", cls.min_backoff),
            max_backoff=env_float("MAX_BACKOFF", cls.max_backoff),
            workers=env_int("WORKERS", cls.workers),
            global_rps=env_float("GLOBAL_RPS", cls.global_rps),
            token_bucket_size=env_int("TOKEN_BUCKET_SIZE", cls.token_bucket_size),
            address_queue_size=env_int("ADDRESS_QUEUE_SIZE", cls.address_queue_size),
            tcp_limit=env_int("TCP_LIMIT", cls.tcp_limit),
            tcp_limit_per_host=env_int("TCP_LIMIT_PER_HOST", cls.tcp_limit_per_host),
            dns_cache_ttl=env_int("DNS_CACHE_TTL", cls.dns_cache_ttl),
            confirmations=env_int("CONFIRMATIONS", cls.confirmations),
            max_blocks_per_batch=env_int("MAX_BLOCKS_PER_BATCH", cls.max_blocks_per_batch),
            block_poll_interval=env_float("BLOCK_POLL_INTERVAL", cls.block_poll_interval),
            nft_tx_offset=env_int("NFT_TX_OFFSET", cls.nft_tx_offset),
            empty_wallet_cache_size=env_int("EMPTY_WALLET_CACHE_SIZE", cls.empty_wallet_cache_size),
            write_buffer_size=env_int("WRITE_BUFFER_SIZE", cls.write_buffer_size),
            stats_interval=env_float("STATS_INTERVAL", cls.stats_interval),
            api_key_cooldown_sec=env_float("API_KEY_COOLDOWN_SEC", cls.api_key_cooldown_sec),
            start_block=env_int("START_BLOCK", cls.start_block),
            fsync_writes=env_bool("FSYNC_WRITES", cls.fsync_writes),
            log_level=os.getenv("LOG_LEVEL", cls.log_level).upper(),
            user_agent=os.getenv("USER_AGENT", cls.user_agent),
        )

    def validate(self) -> None:
        if not self.api_keys:
            raise ValueError("Set ETHERSCAN_API_KEYS=key1,key2")
        if self.chain_id <= 0:
            raise ValueError("CHAIN_ID must be > 0")
        if not self.base_url.startswith(("https://", "http://")):
            raise ValueError("ETHERSCAN_BASE_URL must be an HTTP(S) URL")
        positive_ints = {
            "WORKERS": self.workers,
            "TOKEN_BUCKET_SIZE": self.token_bucket_size,
            "ADDRESS_QUEUE_SIZE": self.address_queue_size,
            "TCP_LIMIT": self.tcp_limit,
            "TCP_LIMIT_PER_HOST": self.tcp_limit_per_host,
            "MAX_RETRIES": self.max_retries,
            "MAX_BLOCKS_PER_BATCH": self.max_blocks_per_batch,
            "NFT_TX_OFFSET": self.nft_tx_offset,
            "WRITE_BUFFER_SIZE": self.write_buffer_size,
        }
        for name, value in positive_ints.items():
            if value < 1:
                raise ValueError(f"{name} must be >= 1")
        if self.nft_tx_offset > 1000:
            raise ValueError("NFT_TX_OFFSET must be <= 1000")
        if self.global_rps <= 0 or self.request_timeout <= 0 or self.connect_timeout <= 0:
            raise ValueError("RPS and timeouts must be > 0")
        if self.confirmations < 0 or self.start_block < 0:
            raise ValueError("CONFIRMATIONS and START_BLOCK must be >= 0")
        if self.min_backoff < 0 or self.max_backoff < self.min_backoff:
            raise ValueError("Backoff values are invalid")


# ---------------------------------------------------------------------------
# Logging and helpers
# ---------------------------------------------------------------------------


def setup_logging(level: str) -> logging.Logger:
    logging.basicConfig(
        level=getattr(logging, level, logging.INFO),
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
        force=True,
    )
    return logging.getLogger("nft-scanner")


def normalize_address(value: Any) -> Optional[str]:
    if not isinstance(value, str):
        return None
    value = value.strip().lower()
    if len(value) != 42 or not value.startswith("0x") or value == ZERO_ADDRESS:
        return None
    try:
        int(value[2:], 16)
    except ValueError:
        return None
    return value


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def retry_delay(attempt: int, minimum: float, maximum: float) -> float:
    ceiling = min(maximum, minimum * (2 ** max(0, attempt - 1)))
    return random.uniform(minimum, max(minimum, ceiling))


async def fsync_path(path: Path) -> None:
    await asyncio.to_thread(_fsync_path_sync, path)


def _fsync_path_sync(path: Path) -> None:
    with path.open("rb") as handle:
        os.fsync(handle.fileno())


class LRUSet:
    def __init__(self, max_size: int):
        self.max_size = max_size
        self._data: OrderedDict[str, None] = OrderedDict()

    def __contains__(self, key: str) -> bool:
        if key not in self._data:
            return False
        self._data.move_to_end(key)
        return True

    def add(self, key: str) -> None:
        if self.max_size <= 0:
            return
        self._data[key] = None
        self._data.move_to_end(key)
        while len(self._data) > self.max_size:
            self._data.popitem(last=False)

    def __len__(self) -> int:
        return len(self._data)


class TokenBucket:
    def __init__(self, rate: float, capacity: int):
        self.rate = rate
        self.capacity = float(capacity)
        self.tokens = float(capacity)
        self.updated = monotonic()
        self.lock = asyncio.Lock()

    async def acquire(self) -> None:
        while True:
            async with self.lock:
                now = monotonic()
                self.tokens = min(self.capacity, self.tokens + (now - self.updated) * self.rate)
                self.updated = now
                if self.tokens >= 1.0:
                    self.tokens -= 1.0
                    return
                delay = (1.0 - self.tokens) / self.rate
            await asyncio.sleep(delay)


class APIKeyManager:
    def __init__(self, keys: tuple[str, ...], default_cooldown: float):
        self.keys = keys
        self.default_cooldown = default_cooldown
        self.cooldowns = {key: 0.0 for key in keys}
        self.cursor = 0
        self.lock = asyncio.Lock()

    async def acquire(self) -> str:
        while True:
            async with self.lock:
                now = monotonic()
                for _ in range(len(self.keys)):
                    key = self.keys[self.cursor]
                    self.cursor = (self.cursor + 1) % len(self.keys)
                    if self.cooldowns[key] <= now:
                        return key
                next_ready = min(self.cooldowns.values())
                delay = max(0.05, next_ready - now)
            await asyncio.sleep(delay)

    async def cooldown(self, key: str, seconds: Optional[float] = None) -> None:
        duration = self.default_cooldown if seconds is None else max(0.1, seconds)
        async with self.lock:
            self.cooldowns[key] = max(self.cooldowns[key], monotonic() + duration)


class Stats:
    FIELDS = (
        "blocks", "addresses_seen", "addresses_enqueued", "addresses_ok",
        "empty_wallets", "contracts", "requests", "retries", "errors", "rate_limits",
    )

    def __init__(self) -> None:
        self.started = monotonic()
        self.values = {name: 0 for name in self.FIELDS}
        self.lock = asyncio.Lock()

    async def inc(self, **increments: int) -> None:
        async with self.lock:
            for name, amount in increments.items():
                if name not in self.values:
                    raise KeyError(name)
                self.values[name] += amount

    async def snapshot(self) -> dict[str, float | int]:
        async with self.lock:
            return {"uptime": monotonic() - self.started, **self.values}

    async def log_loop(
        self,
        interval: float,
        logger: logging.Logger,
        queue: asyncio.Queue[str],
        in_flight: set[str],
        empty_cache: LRUSet,
    ) -> None:
        while True:
            await asyncio.sleep(interval)
            s = await self.snapshot()
            uptime = max(float(s["uptime"]), 1.0)
            logger.info(
                "[stats] up=%.0fs blocks=%d contracts=%d req=%d req/s=%.2f retries=%d "
                "errors=%d limits=%d addr=%d/%d ok=%d empty=%d queue=%d in_flight=%d cache=%d",
                s["uptime"], s["blocks"], s["contracts"], s["requests"],
                int(s["requests"]) / uptime, s["retries"], s["errors"], s["rate_limits"],
                s["addresses_enqueued"], s["addresses_seen"], s["addresses_ok"],
                s["empty_wallets"], queue.qsize(), len(in_flight), len(empty_cache),
            )


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------


class Checkpoint:
    def __init__(self, path: Path, fsync: bool, logger: logging.Logger):
        self.path = path
        self.fsync = fsync
        self.logger = logger

    def load(self) -> Optional[int]:
        if not self.path.exists():
            return None
        try:
            with self.path.open("r", encoding="utf-8") as handle:
                payload = json.load(handle)
            value = int(payload["last_processed_block"])
            if value < 0:
                raise ValueError("negative block")
            return value
        except (OSError, ValueError, TypeError, KeyError, json.JSONDecodeError) as exc:
            broken = self.path.with_suffix(self.path.suffix + ".broken")
            self.logger.error("Invalid checkpoint %s: %s", self.path, exc)
            try:
                os.replace(self.path, broken)
                self.logger.error("Moved invalid checkpoint to %s", broken)
            except OSError:
                pass
            return None

    async def save(self, block: int) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_name(self.path.name + ".tmp")
        payload = {
            "version": 1,
            "last_processed_block": block,
            "saved_at_utc": utc_now_iso(),
        }
        async with aiofiles.open(tmp, "w", encoding="utf-8", newline="\n") as handle:
            await handle.write(json.dumps(payload, ensure_ascii=False, indent=2) + "\n")
            await handle.flush()
        if self.fsync:
            await fsync_path(tmp)
        os.replace(tmp, self.path)


class ContractWriter:
    def __init__(self, path: Path, buffer_size: int, fsync: bool, logger: logging.Logger):
        self.path = path
        self.buffer_size = buffer_size
        self.fsync = fsync
        self.logger = logger
        self.buffer: set[str] = set()
        self.lock = asyncio.Lock()
        self.total_written = 0

    async def add(self, contracts: set[str]) -> None:
        if not contracts:
            return
        async with self.lock:
            self.buffer.update(contracts)
            if len(self.buffer) >= self.buffer_size:
                await self._flush_locked()

    async def flush(self) -> None:
        async with self.lock:
            await self._flush_locked()

    async def _flush_locked(self) -> None:
        if not self.buffer:
            return
        data = sorted(self.buffer)
        self.buffer.clear()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        async with aiofiles.open(self.path, "a", encoding="utf-8", newline="\n") as handle:
            await handle.write("\n".join(data) + "\n")
            await handle.flush()
        if self.fsync:
            await fsync_path(self.path)
        self.total_written += len(data)
        self.logger.info("Saved %d new contracts (session=%d)", len(data), self.total_written)


def load_contracts(paths: Iterable[Path], logger: logging.Logger) -> set[str]:
    result: set[str] = set()
    for path in paths:
        if not path.exists():
            continue
        before = len(result)
        try:
            with path.open("r", encoding="utf-8-sig") as handle:
                for line in handle:
                    address = normalize_address(line)
                    if address:
                        result.add(address)
            logger.info("Loaded %d contracts from %s", len(result) - before, path)
        except OSError as exc:
            raise RuntimeError(f"Cannot read {path}: {exc}") from exc
    return result


# ---------------------------------------------------------------------------
# Etherscan client
# ---------------------------------------------------------------------------


class RetryableAPIError(RuntimeError):
    pass


class RateLimitError(RetryableAPIError):
    def __init__(self, message: str, retry_after: Optional[float] = None):
        super().__init__(message)
        self.retry_after = retry_after


class PermanentAPIError(RuntimeError):
    pass


class EtherscanClient:
    RATE_LIMIT_MARKERS = ("rate limit", "max rate", "too many requests", "daily limit")
    TRANSIENT_MARKERS = ("timeout", "temporarily unavailable", "server busy", "try again")

    def __init__(
        self,
        session: aiohttp.ClientSession,
        config: Config,
        keys: APIKeyManager,
        limiter: TokenBucket,
        stats: Stats,
        logger: logging.Logger,
    ):
        self.session = session
        self.config = config
        self.keys = keys
        self.limiter = limiter
        self.stats = stats
        self.logger = logger

    async def request(self, module: str, action: str, **params: Any) -> Mapping[str, Any]:
        last_error: Optional[BaseException] = None
        for attempt in range(1, self.config.max_retries + 1):
            key = await self.keys.acquire()
            query = {
                "chainid": str(self.config.chain_id),
                "module": module,
                "action": action,
                "apikey": key,
                **{name: value for name, value in params.items() if value is not None},
            }
            try:
                await self.limiter.acquire()
                async with self.session.get(self.config.base_url, params=query) as response:
                    await self.stats.inc(requests=1)
                    retry_after = self._retry_after(response.headers.get("Retry-After"))
                    body = await response.text()

                    if response.status == 429:
                        raise RateLimitError("HTTP 429", retry_after)
                    if response.status in {408, 425} or response.status >= 500:
                        raise RetryableAPIError(f"HTTP {response.status}: {body[:160]}")
                    if response.status >= 400:
                        raise PermanentAPIError(f"HTTP {response.status}: {body[:300]}")

                    try:
                        data = json.loads(body)
                    except json.JSONDecodeError as exc:
                        raise RetryableAPIError(f"Invalid JSON: {body[:160]}") from exc
                    if not isinstance(data, dict):
                        raise RetryableAPIError("API returned non-object JSON")
                    self._classify_envelope(data)
                    return data

            except RateLimitError as exc:
                last_error = exc
                await self.stats.inc(rate_limits=1, retries=1)
                cooldown = exc.retry_after or self.config.api_key_cooldown_sec
                await self.keys.cooldown(key, cooldown)
                delay = min(cooldown, self.config.max_backoff)
                self.logger.warning(
                    "%s.%s rate-limited; key cooldown %.1fs, retry %d/%d",
                    module, action, cooldown, attempt, self.config.max_retries,
                )
            except PermanentAPIError:
                await self.stats.inc(errors=1)
                raise
            except (aiohttp.ClientError, asyncio.TimeoutError, RetryableAPIError) as exc:
                last_error = exc
                await self.stats.inc(errors=1, retries=1)
                delay = retry_delay(attempt, self.config.min_backoff, self.config.max_backoff)
                self.logger.warning(
                    "%s.%s failed (%s), retry %d/%d in %.2fs",
                    module, action, exc, attempt, self.config.max_retries, delay,
                )
            if attempt < self.config.max_retries:
                await asyncio.sleep(delay)

        raise RetryableAPIError(
            f"{module}.{action} failed after {self.config.max_retries} attempts: {last_error}"
        )

    @staticmethod
    def _retry_after(value: Optional[str]) -> Optional[float]:
        if not value:
            return None
        try:
            return max(0.1, float(value))
        except ValueError:
            return None

    def _classify_envelope(self, data: Mapping[str, Any]) -> None:
        result = data.get("result")
        combined = " ".join(
            str(data.get(name, "")).lower() for name in ("status", "message", "result")
        )
        if any(marker in combined for marker in self.RATE_LIMIT_MARKERS):
            raise RateLimitError(combined[:240])
        if any(marker in combined for marker in self.TRANSIENT_MARKERS):
            raise RetryableAPIError(combined[:240])
        # JSON-RPC proxy errors can be returned in a successful HTTP response.
        if data.get("error") is not None:
            raise RetryableAPIError(f"JSON-RPC error: {data['error']!r}")
        # Account endpoints use status=0 for both a valid empty result and errors.
        if str(data.get("status", "")) == "0" and not self._is_empty_account_result(data):
            raise PermanentAPIError(f"Etherscan API error: {combined[:300]}")
        if result is None:
            raise RetryableAPIError("API response has no result")

    @staticmethod
    def _is_empty_account_result(data: Mapping[str, Any]) -> bool:
        result = data.get("result")
        message = str(data.get("message", "")).lower()
        result_text = str(result).lower()
        return (
            result == []
            or "no transactions found" in result_text
            or (message == "no transactions found" and (result is None or result == "" or result == []))
        )

    async def latest_block(self) -> int:
        data = await self.request("proxy", "eth_blockNumber")
        result = data.get("result")
        if not isinstance(result, str):
            raise RetryableAPIError(f"Invalid latest block result: {result!r}")
        try:
            return int(result, 16)
        except ValueError as exc:
            raise RetryableAPIError(f"Invalid block hex: {result!r}") from exc

    async def block_addresses(self, block_number: int) -> set[str]:
        data = await self.request(
            "proxy", "eth_getBlockByNumber", tag=f"0x{block_number:x}", boolean="true"
        )
        block = data.get("result")
        if block is None:
            raise RetryableAPIError(f"Block {block_number} is temporarily unavailable")
        if not isinstance(block, dict):
            raise RetryableAPIError(f"Invalid block {block_number} payload")
        transactions = block.get("transactions")
        if not isinstance(transactions, list):
            raise RetryableAPIError(f"Block {block_number} has invalid transactions")

        addresses: set[str] = set()
        for tx in transactions:
            if not isinstance(tx, dict):
                continue
            for field in ("from", "to"):
                address = normalize_address(tx.get(field))
                if address:
                    addresses.add(address)
        return addresses

    async def nft_contracts_for_address(self, address: str) -> set[str]:
        data = await self.request(
            "account",
            "tokennfttx",
            address=address,
            page=1,
            offset=self.config.nft_tx_offset,
            sort="desc",
        )
        result = data.get("result")
        if isinstance(result, str) and "no transactions found" in result.lower():
            return set()
        if not isinstance(result, list):
            raise RetryableAPIError(f"Invalid tokennfttx result for {address}: {type(result).__name__}")

        contracts: set[str] = set()
        for transfer in result:
            if not isinstance(transfer, dict):
                continue
            contract = normalize_address(transfer.get("contractAddress"))
            if contract:
                contracts.add(contract)
        return contracts


# ---------------------------------------------------------------------------
# Workers and orchestration
# ---------------------------------------------------------------------------


async def address_worker(
    worker_id: int,
    queue: asyncio.Queue[str],
    client: EtherscanClient,
    seen_contracts: set[str],
    seen_lock: asyncio.Lock,
    empty_wallets: LRUSet,
    in_flight: set[str],
    writer: ContractWriter,
    stats: Stats,
    logger: logging.Logger,
) -> None:
    name = f"worker-{worker_id}"
    while True:
        address = await queue.get()
        try:
            contracts = await client.nft_contracts_for_address(address)
            if not contracts:
                empty_wallets.add(address)
                await stats.inc(empty_wallets=1, addresses_ok=1)
                continue

            async with seen_lock:
                new_contracts = contracts - seen_contracts
                seen_contracts.update(new_contracts)
            if new_contracts:
                await writer.add(new_contracts)
                await stats.inc(contracts=len(new_contracts))
            await stats.inc(addresses_ok=1)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            await stats.inc(errors=1)
            logger.critical(
                "%s exhausted retries for %s; stopping before checkpoint: %s",
                name, address, exc, exc_info=True,
            )
            raise
        finally:
            in_flight.discard(address)
            queue.task_done()


async def wait_for_queue_or_worker_failure(
    queue: asyncio.Queue[str], workers: list[asyncio.Task[None]]
) -> None:
    join_task = asyncio.create_task(queue.join(), name="queue-join")
    done, _ = await asyncio.wait([join_task, *workers], return_when=asyncio.FIRST_COMPLETED)
    if join_task in done:
        return
    join_task.cancel()
    await asyncio.gather(join_task, return_exceptions=True)
    failed = next(task for task in done if task is not join_task)
    exc = failed.exception()
    raise RuntimeError(f"Address worker stopped unexpectedly: {exc!r}") from exc


async def run(config: Config) -> None:
    config.validate()
    logger = setup_logging(config.log_level)
    logger.info(
        "Starting NFT Scanner v5: chain=%d workers=%d rps=%.2f confirmations=%d keys=%d",
        config.chain_id, config.workers, config.global_rps, config.confirmations, len(config.api_keys),
    )

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()

    def request_shutdown() -> None:
        if not stop_event.is_set():
            logger.warning("Shutdown requested")
            stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, request_shutdown)
        except (NotImplementedError, RuntimeError):
            pass

    queue: asyncio.Queue[str] = asyncio.Queue(maxsize=config.address_queue_size)
    in_flight: set[str] = set()
    empty_wallets = LRUSet(config.empty_wallet_cache_size)
    seen_contracts = load_contracts((config.output_file, config.known_contracts_file), logger)
    seen_lock = asyncio.Lock()
    stats = Stats()
    writer = ContractWriter(config.output_file, config.write_buffer_size, config.fsync_writes, logger)
    checkpoint = Checkpoint(config.checkpoint_file, config.fsync_writes, logger)
    limiter = TokenBucket(config.global_rps, config.token_bucket_size)
    keys = APIKeyManager(config.api_keys, config.api_key_cooldown_sec)

    timeout = aiohttp.ClientTimeout(
        total=config.request_timeout,
        connect=config.connect_timeout,
        sock_connect=config.connect_timeout,
    )
    connector = aiohttp.TCPConnector(
        limit=config.tcp_limit,
        limit_per_host=config.tcp_limit_per_host,
        ttl_dns_cache=config.dns_cache_ttl,
        enable_cleanup_closed=True,
    )
    headers = {"User-Agent": config.user_agent, "Accept": "application/json"}

    async with aiohttp.ClientSession(connector=connector, timeout=timeout, headers=headers) as session:
        client = EtherscanClient(session, config, keys, limiter, stats, logger)
        workers = [
            asyncio.create_task(
                address_worker(
                    i + 1, queue, client, seen_contracts, seen_lock, empty_wallets,
                    in_flight, writer, stats, logger,
                ),
                name=f"address-worker-{i + 1}",
            )
            for i in range(config.workers)
        ]
        stats_task = asyncio.create_task(
            stats.log_loop(config.stats_interval, logger, queue, in_flight, empty_wallets),
            name="stats",
        )

        last_processed = checkpoint.load()
        if last_processed is not None:
            logger.info("Resuming after block %d", last_processed)
        elif config.start_block > 0:
            last_processed = config.start_block - 1
            logger.info("Starting at configured block %d", config.start_block)

        try:
            while not stop_event.is_set():
                try:
                    latest = await client.latest_block()
                except Exception as exc:
                    logger.error("Cannot obtain latest block: %s", exc)
                    await asyncio.sleep(config.block_poll_interval)
                    continue

                safe_head = max(0, latest - config.confirmations)
                if last_processed is None:
                    last_processed = safe_head
                    await checkpoint.save(last_processed)
                    logger.info("Initialized at safe head %d; historical blocks are not scanned", safe_head)
                    await asyncio.sleep(config.block_poll_interval)
                    continue

                if safe_head <= last_processed:
                    try:
                        await asyncio.wait_for(stop_event.wait(), timeout=config.block_poll_interval)
                    except asyncio.TimeoutError:
                        pass
                    continue

                batch_end = min(safe_head, last_processed + config.max_blocks_per_batch)
                logger.info("Processing blocks %d..%d (head=%d safe=%d)", last_processed + 1, batch_end, latest, safe_head)
                batch_last = last_processed
                batch_failed = False

                for block_number in range(last_processed + 1, batch_end + 1):
                    if stop_event.is_set():
                        break
                    try:
                        addresses = await client.block_addresses(block_number)
                    except Exception as exc:
                        logger.error("Block %d was not processed and will be retried: %s", block_number, exc)
                        batch_failed = True
                        break

                    await stats.inc(blocks=1, addresses_seen=len(addresses))
                    enqueued = 0
                    for address in addresses:
                        if address in empty_wallets or address in in_flight:
                            continue
                        in_flight.add(address)
                        try:
                            await queue.put(address)
                        except BaseException:
                            in_flight.discard(address)
                            raise
                        enqueued += 1
                    await stats.inc(addresses_enqueued=enqueued)
                    batch_last = block_number
                    logger.info(
                        "Block %d: participants=%d enqueued=%d queue=%d",
                        block_number, len(addresses), enqueued, queue.qsize(),
                    )

                # A checkpoint is durable only after all associated address requests and
                # contract writes have completed. Failed address requests are not cached;
                # replaying the batch after a crash remains safe because output is loaded.
                if batch_last > last_processed:
                    await wait_for_queue_or_worker_failure(queue, workers)
                    await writer.flush()
                    await checkpoint.save(batch_last)
                    last_processed = batch_last
                    logger.info("Checkpoint committed at block %d", last_processed)

                if batch_failed:
                    await asyncio.sleep(config.block_poll_interval)

        finally:
            stop_event.set()
            logger.info("Stopping producer; draining %d queued addresses", queue.qsize())
            try:
                await wait_for_queue_or_worker_failure(queue, workers)
                await writer.flush()
            finally:
                stats_task.cancel()
                for task in workers:
                    task.cancel()
                await asyncio.gather(*workers, stats_task, return_exceptions=True)

    final = await stats.snapshot()
    logger.info(
        "Shutdown complete: blocks=%d addresses_ok=%d contracts=%d requests=%d errors=%d",
        final["blocks"], final["addresses_ok"], final["contracts"], final["requests"], final["errors"],
    )


def main() -> int:
    if sys.platform.startswith("win"):
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    try:
        config = Config.from_env()
        asyncio.run(run(config))
        return 0
    except KeyboardInterrupt:
        return 130
    except Exception as exc:
        logging.basicConfig(level=logging.ERROR, format="%(asctime)s | %(levelname)s | %(message)s")
        logging.getLogger("nft-scanner").critical("Fatal error: %s", exc, exc_info=True)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
