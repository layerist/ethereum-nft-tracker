#!/usr/bin/env python3
"""
NFT Scanner v7 — crash-safe asynchronous Etherscan V2 scanner.

Scans confirmed blocks, extracts top-level transaction participants, requests
recent ERC-721 transfers for those addresses, and appends newly discovered NFT
contract addresses to a text file.

Major changes over v6:
- Fail-fast producer: queue.put() can no longer hang forever after a worker dies.
- Per-key rate-limit rotation: a throttled API key is cooled down without forcing
  a global 30-second sleep when another key is available.
- Crash-safe checkpoint v2 stores the canonical block hash and verifies it on
  restart, detecting deep reorgs instead of silently continuing on another fork.
- Durable checkpoint writes use a unique temp file + fsync + atomic os.replace.
- Contract append writes keep the in-memory buffer until the append/fsync succeeds.
- Interruptible sleeps improve SIGINT/SIGTERM shutdown responsiveness.
- Stronger response/schema validation and pagination-loop detection.
- Old v1 checkpoints remain readable; they are upgraded after the next commit.
- aiofiles dependency removed; durable filesystem operations run via asyncio.to_thread.
- More detailed runtime statistics, including cache hits, NFT pages and key throttles.

Required:
    pip install aiohttp

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
    NFT_TX_PAGES=1
    EMPTY_WALLET_CACHE_TTL=3600
    VERIFY_CHECKPOINT_HASH=true
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import random
import signal
import sys
import tempfile
from collections import OrderedDict
from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from time import monotonic
from typing import Any, Iterable, Mapping, Optional
from urllib.parse import urlparse

import aiohttp


ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"
PLACEHOLDER_KEYS = {"", "your_etherscan_api_key", "changeme", "none", "null"}


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
    nft_tx_pages: int = 1

    empty_wallet_cache_size: int = 200_000
    empty_wallet_cache_ttl: float = 3600.0
    write_buffer_size: int = 500
    stats_interval: float = 30.0

    api_key_cooldown_sec: float = 30.0
    start_block: int = 0
    fsync_writes: bool = True
    verify_checkpoint_hash: bool = True

    log_level: str = "INFO"
    user_agent: str = "NFTScanner/7.0 (+aiohttp; Etherscan-V2)"

    @classmethod
    def from_env(cls) -> "Config":
        return cls(
            api_keys=env_keys("ETHERSCAN_API_KEYS"),
            chain_id=env_int("CHAIN_ID", cls.chain_id),
            base_url=os.getenv("ETHERSCAN_BASE_URL", cls.base_url).strip(),
            output_file=Path(os.getenv("OUTPUT_FILE", str(cls.output_file))),
            known_contracts_file=Path(
                os.getenv("KNOWN_CONTRACTS_FILE", str(cls.known_contracts_file))
            ),
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
            max_blocks_per_batch=env_int(
                "MAX_BLOCKS_PER_BATCH", cls.max_blocks_per_batch
            ),
            block_poll_interval=env_float(
                "BLOCK_POLL_INTERVAL", cls.block_poll_interval
            ),
            nft_tx_offset=env_int("NFT_TX_OFFSET", cls.nft_tx_offset),
            nft_tx_pages=env_int("NFT_TX_PAGES", cls.nft_tx_pages),
            empty_wallet_cache_size=env_int(
                "EMPTY_WALLET_CACHE_SIZE", cls.empty_wallet_cache_size
            ),
            empty_wallet_cache_ttl=env_float(
                "EMPTY_WALLET_CACHE_TTL", cls.empty_wallet_cache_ttl
            ),
            write_buffer_size=env_int("WRITE_BUFFER_SIZE", cls.write_buffer_size),
            stats_interval=env_float("STATS_INTERVAL", cls.stats_interval),
            api_key_cooldown_sec=env_float(
                "API_KEY_COOLDOWN_SEC", cls.api_key_cooldown_sec
            ),
            start_block=env_int("START_BLOCK", cls.start_block),
            fsync_writes=env_bool("FSYNC_WRITES", cls.fsync_writes),
            verify_checkpoint_hash=env_bool(
                "VERIFY_CHECKPOINT_HASH", cls.verify_checkpoint_hash
            ),
            log_level=os.getenv("LOG_LEVEL", cls.log_level).upper(),
            user_agent=os.getenv("USER_AGENT", cls.user_agent).strip()
            or cls.user_agent,
        )

    def validate(self) -> None:
        if not self.api_keys:
            raise ValueError("Set ETHERSCAN_API_KEYS=key1,key2")
        if self.chain_id <= 0:
            raise ValueError("CHAIN_ID must be > 0")

        parsed = urlparse(self.base_url)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            raise ValueError("ETHERSCAN_BASE_URL must be a valid HTTP(S) URL")

        positive_ints = {
            "WORKERS": self.workers,
            "TOKEN_BUCKET_SIZE": self.token_bucket_size,
            "ADDRESS_QUEUE_SIZE": self.address_queue_size,
            "TCP_LIMIT": self.tcp_limit,
            "TCP_LIMIT_PER_HOST": self.tcp_limit_per_host,
            "DNS_CACHE_TTL": self.dns_cache_ttl,
            "MAX_RETRIES": self.max_retries,
            "MAX_BLOCKS_PER_BATCH": self.max_blocks_per_batch,
            "NFT_TX_OFFSET": self.nft_tx_offset,
            "NFT_TX_PAGES": self.nft_tx_pages,
            "WRITE_BUFFER_SIZE": self.write_buffer_size,
        }
        for name, value in positive_ints.items():
            if value < 1:
                raise ValueError(f"{name} must be >= 1")

        if self.nft_tx_offset > 1000:
            raise ValueError("NFT_TX_OFFSET must be <= 1000")
        if self.tcp_limit_per_host > self.tcp_limit:
            raise ValueError("TCP_LIMIT_PER_HOST must be <= TCP_LIMIT")
        if self.global_rps <= 0:
            raise ValueError("GLOBAL_RPS must be > 0")
        if self.request_timeout <= 0 or self.connect_timeout <= 0:
            raise ValueError("REQUEST_TIMEOUT and CONNECT_TIMEOUT must be > 0")
        if self.connect_timeout > self.request_timeout:
            raise ValueError("CONNECT_TIMEOUT must be <= REQUEST_TIMEOUT")
        if self.block_poll_interval <= 0 or self.stats_interval <= 0:
            raise ValueError("BLOCK_POLL_INTERVAL and STATS_INTERVAL must be > 0")
        if self.api_key_cooldown_sec <= 0:
            raise ValueError("API_KEY_COOLDOWN_SEC must be > 0")
        if self.empty_wallet_cache_size < 0 or self.empty_wallet_cache_ttl < 0:
            raise ValueError(
                "EMPTY_WALLET_CACHE_SIZE and EMPTY_WALLET_CACHE_TTL must be >= 0"
            )
        if self.confirmations < 0 or self.start_block < 0:
            raise ValueError("CONFIRMATIONS and START_BLOCK must be >= 0")
        if self.min_backoff < 0 or self.max_backoff < self.min_backoff:
            raise ValueError("Backoff values are invalid")

        level = getattr(logging, self.log_level, None)
        if not isinstance(level, int):
            raise ValueError(f"Invalid LOG_LEVEL: {self.log_level!r}")


# ---------------------------------------------------------------------------
# Logging and helpers
# ---------------------------------------------------------------------------


def setup_logging(level: str) -> logging.Logger:
    logging.basicConfig(
        level=getattr(logging, level),
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


def normalize_hash(value: Any) -> Optional[str]:
    if not isinstance(value, str):
        return None
    value = value.strip().lower()
    if len(value) != 66 or not value.startswith("0x"):
        return None
    try:
        int(value[2:], 16)
    except ValueError:
        return None
    return value


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def retry_delay(attempt: int, minimum: float, maximum: float) -> float:
    if minimum <= 0:
        return 0.0
    ceiling = min(maximum, minimum * (2 ** max(0, attempt - 1)))
    return random.uniform(minimum, max(minimum, ceiling))


async def sleep_or_stop(stop_event: asyncio.Event, seconds: float) -> bool:
    """Sleep up to seconds. Return True if shutdown was requested."""
    if stop_event.is_set():
        return True
    try:
        await asyncio.wait_for(stop_event.wait(), timeout=max(0.0, seconds))
        return True
    except asyncio.TimeoutError:
        return False


class ExpiringLRUSet:
    """Bounded LRU set whose entries expire after ttl seconds."""

    def __init__(self, max_size: int, ttl: float):
        self.max_size = max_size
        self.ttl = ttl
        self._data: OrderedDict[str, float] = OrderedDict()

    def __contains__(self, key: str) -> bool:
        if self.max_size <= 0 or self.ttl <= 0:
            return False
        expires_at = self._data.get(key)
        if expires_at is None:
            return False
        now = monotonic()
        if expires_at <= now:
            self._data.pop(key, None)
            return False
        self._data.move_to_end(key)
        return True

    def add(self, key: str) -> None:
        if self.max_size <= 0 or self.ttl <= 0:
            return
        now = monotonic()
        self._data[key] = now + self.ttl
        self._data.move_to_end(key)
        self._prune(now)

    def _prune(self, now: Optional[float] = None) -> None:
        if not self._data:
            return
        now = monotonic() if now is None else now

        # Membership hits move entries, so expired items are not guaranteed to
        # be clustered at the front.
        expired = [
            key for key, expires_at in self._data.items() if expires_at <= now
        ]
        for key in expired:
            self._data.pop(key, None)

        while len(self._data) > self.max_size:
            self._data.popitem(last=False)

    def __len__(self) -> int:
        self._prune()
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
                self.tokens = min(
                    self.capacity, self.tokens + (now - self.updated) * self.rate
                )
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
        duration = (
            self.default_cooldown if seconds is None else max(0.1, float(seconds))
        )
        async with self.lock:
            self.cooldowns[key] = max(
                self.cooldowns[key], monotonic() + duration
            )

    async def available_count(self) -> int:
        async with self.lock:
            now = monotonic()
            return sum(1 for ready_at in self.cooldowns.values() if ready_at <= now)


class Stats:
    FIELDS = (
        "blocks",
        "addresses_seen",
        "addresses_enqueued",
        "cache_hits",
        "addresses_ok",
        "empty_wallets",
        "contracts",
        "nft_pages",
        "requests",
        "retries",
        "errors",
        "rate_limits",
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
        empty_cache: ExpiringLRUSet,
        keys: APIKeyManager,
    ) -> None:
        while True:
            await asyncio.sleep(interval)
            s = await self.snapshot()
            uptime = max(float(s["uptime"]), 1.0)
            available_keys = await keys.available_count()
            logger.info(
                "[stats] up=%.0fs blocks=%d contracts=%d req=%d req/s=%.2f "
                "pages=%d retries=%d errors=%d limits=%d keys_ready=%d "
                "addr=%d/%d ok=%d empty=%d cache_hits=%d queue=%d "
                "in_flight=%d empty_cache=%d",
                s["uptime"],
                s["blocks"],
                s["contracts"],
                s["requests"],
                int(s["requests"]) / uptime,
                s["nft_pages"],
                s["retries"],
                s["errors"],
                s["rate_limits"],
                available_keys,
                s["addresses_enqueued"],
                s["addresses_seen"],
                s["addresses_ok"],
                s["empty_wallets"],
                s["cache_hits"],
                queue.qsize(),
                len(in_flight),
                len(empty_cache),
            )


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class CheckpointState:
    last_processed_block: int
    last_processed_block_hash: Optional[str] = None


def _fsync_directory_sync(path: Path) -> None:
    if os.name == "nt":
        return
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _atomic_json_write_sync(path: Path, payload: Mapping[str, Any], fsync: bool) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd: Optional[int] = None
    tmp_path: Optional[Path] = None
    try:
        fd, tmp_name = tempfile.mkstemp(
            prefix=f".{path.name}.",
            suffix=".tmp",
            dir=str(path.parent),
            text=True,
        )
        tmp_path = Path(tmp_name)
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as handle:
            fd = None
            json.dump(payload, handle, ensure_ascii=False, indent=2)
            handle.write("\n")
            handle.flush()
            if fsync:
                os.fsync(handle.fileno())

        os.replace(tmp_path, path)
        tmp_path = None
        if fsync:
            _fsync_directory_sync(path.parent)
    finally:
        if fd is not None:
            try:
                os.close(fd)
            except OSError:
                pass
        if tmp_path is not None:
            try:
                tmp_path.unlink(missing_ok=True)
            except OSError:
                pass


def _append_lines_sync(path: Path, lines: list[str], fsync: bool) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8", newline="\n") as handle:
        handle.write("\n".join(lines))
        handle.write("\n")
        handle.flush()
        if fsync:
            os.fsync(handle.fileno())


class Checkpoint:
    def __init__(self, path: Path, fsync: bool, logger: logging.Logger):
        self.path = path
        self.fsync = fsync
        self.logger = logger

    def load(self) -> Optional[CheckpointState]:
        if not self.path.exists():
            return None
        try:
            with self.path.open("r", encoding="utf-8") as handle:
                payload = json.load(handle)

            if not isinstance(payload, dict):
                raise ValueError("checkpoint root must be an object")

            version = int(payload.get("version", 1))
            if version not in {1, 2}:
                raise ValueError(f"unsupported checkpoint version {version}")

            block = int(payload["last_processed_block"])
            if block < 0:
                raise ValueError("negative block")

            raw_hash = payload.get("last_processed_block_hash")
            block_hash = None
            if raw_hash is not None:
                block_hash = normalize_hash(raw_hash)
                if block_hash is None:
                    raise ValueError("invalid last_processed_block_hash")

            return CheckpointState(block, block_hash)

        except (OSError, ValueError, TypeError, KeyError, json.JSONDecodeError) as exc:
            stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
            broken = self.path.with_name(f"{self.path.name}.broken.{stamp}")
            self.logger.critical("Invalid checkpoint %s: %s", self.path, exc)
            moved_to: Optional[Path] = None
            try:
                os.replace(self.path, broken)
                moved_to = broken
                self.logger.critical("Moved invalid checkpoint to %s", broken)
            except OSError as move_exc:
                self.logger.critical(
                    "Could not preserve invalid checkpoint: %s", move_exc
                )

            detail = f"; preserved as {moved_to}" if moved_to else ""
            raise RuntimeError(
                f"Checkpoint {self.path} is invalid{detail}. Refusing to "
                "initialize at the current head because that could skip blocks. "
                "Restore/fix it, or delete it intentionally and set START_BLOCK."
            ) from exc

    async def save(self, block: int, block_hash: Optional[str]) -> None:
        payload = {
            "version": 2,
            "last_processed_block": block,
            "last_processed_block_hash": block_hash,
            "saved_at_utc": utc_now_iso(),
        }
        await asyncio.to_thread(
            _atomic_json_write_sync, self.path, payload, self.fsync
        )


class ContractWriter:
    def __init__(
        self,
        path: Path,
        buffer_size: int,
        fsync: bool,
        logger: logging.Logger,
    ):
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

        # Do not clear the buffer before a successful append/fsync. If the write
        # fails, the uncheckpointed batch will be replayed and the buffer remains
        # available for a final flush attempt.
        data = sorted(self.buffer)
        await asyncio.to_thread(_append_lines_sync, self.path, data, self.fsync)
        self.buffer.difference_update(data)
        self.total_written += len(data)
        self.logger.info(
            "Saved %d new contracts (session=%d)",
            len(data),
            self.total_written,
        )


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
            logger.info(
                "Loaded %d contracts from %s", len(result) - before, path
            )
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


@dataclass(frozen=True, slots=True)
class BlockData:
    number: int
    block_hash: str
    addresses: set[str]


class EtherscanClient:
    RATE_LIMIT_MARKERS = (
        "rate limit",
        "max rate",
        "too many requests",
        "daily limit",
    )
    TRANSIENT_MARKERS = (
        "timeout",
        "temporarily unavailable",
        "server busy",
        "try again",
        "gateway",
    )

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

    async def request(
        self, module: str, action: str, **params: Any
    ) -> Mapping[str, Any]:
        last_error: Optional[BaseException] = None

        for attempt in range(1, self.config.max_retries + 1):
            key = await self.keys.acquire()
            query = {
                "chainid": str(self.config.chain_id),
                "module": module,
                "action": action,
                "apikey": key,
                **{
                    name: str(value)
                    for name, value in params.items()
                    if value is not None
                },
            }

            delay = 0.0
            try:
                await self.limiter.acquire()
                async with self.session.get(
                    self.config.base_url, params=query
                ) as response:
                    await self.stats.inc(requests=1)
                    retry_after = self._retry_after(
                        response.headers.get("Retry-After")
                    )
                    body = await response.text()

                    if response.status == 429:
                        raise RateLimitError("HTTP 429", retry_after)
                    if response.status in {408, 425} or response.status >= 500:
                        raise RetryableAPIError(
                            f"HTTP {response.status}: {body[:160]}"
                        )
                    if response.status >= 400:
                        raise PermanentAPIError(
                            f"HTTP {response.status}: {body[:300]}"
                        )

                    try:
                        data = json.loads(body)
                    except json.JSONDecodeError as exc:
                        raise RetryableAPIError(
                            f"Invalid JSON: {body[:160]}"
                        ) from exc
                    if not isinstance(data, dict):
                        raise RetryableAPIError("API returned non-object JSON")

                    self._classify_envelope(data)
                    return data

            except RateLimitError as exc:
                last_error = exc
                await self.stats.inc(rate_limits=1)
                if attempt < self.config.max_retries:
                    await self.stats.inc(retries=1)

                cooldown = (
                    exc.retry_after
                    if exc.retry_after is not None
                    else self.config.api_key_cooldown_sec
                )
                await self.keys.cooldown(key, cooldown)

                # Important: do not sleep for the throttled key's whole cooldown.
                # acquire() will immediately rotate to another healthy key. If all
                # keys are cooling down, acquire() itself waits until the first one
                # becomes ready.
                self.logger.warning(
                    "%s.%s rate-limited; key cooldown %.1fs, retry %d/%d",
                    module,
                    action,
                    cooldown,
                    attempt,
                    self.config.max_retries,
                )
                delay = 0.0

            except PermanentAPIError:
                await self.stats.inc(errors=1)
                raise

            except (aiohttp.ClientError, asyncio.TimeoutError, RetryableAPIError) as exc:
                last_error = exc
                await self.stats.inc(errors=1)
                if attempt < self.config.max_retries:
                    await self.stats.inc(retries=1)
                delay = retry_delay(
                    attempt,
                    self.config.min_backoff,
                    self.config.max_backoff,
                )
                self.logger.warning(
                    "%s.%s failed (%s), retry %d/%d in %.2fs",
                    module,
                    action,
                    exc,
                    attempt,
                    self.config.max_retries,
                    delay,
                )

            if attempt < self.config.max_retries and delay > 0:
                await asyncio.sleep(delay)

        raise RetryableAPIError(
            f"{module}.{action} failed after {self.config.max_retries} "
            f"attempts: {last_error}"
        )

    @staticmethod
    def _retry_after(value: Optional[str]) -> Optional[float]:
        if not value:
            return None
        value = value.strip()
        try:
            return max(0.1, float(value))
        except ValueError:
            pass

        try:
            when = parsedate_to_datetime(value)
            if when.tzinfo is None:
                when = when.replace(tzinfo=timezone.utc)
            seconds = (
                when - datetime.now(timezone.utc)
            ).total_seconds()
            return max(0.1, seconds)
        except (TypeError, ValueError, OverflowError):
            return None

    def _classify_envelope(self, data: Mapping[str, Any]) -> None:
        result = data.get("result")
        combined = " ".join(
            str(data.get(name, "")).lower()
            for name in ("status", "message", "result")
        )

        if any(marker in combined for marker in self.RATE_LIMIT_MARKERS):
            raise RateLimitError(combined[:240])
        if any(marker in combined for marker in self.TRANSIENT_MARKERS):
            raise RetryableAPIError(combined[:240])

        if data.get("error") is not None:
            raise RetryableAPIError(f"JSON-RPC error: {data['error']!r}")

        if str(data.get("status", "")) == "0" and not self._is_empty_account_result(
            data
        ):
            raise PermanentAPIError(
                f"Etherscan API error: {combined[:300]}"
            )

        if result is None:
            raise RetryableAPIError("API response has no result")

    @staticmethod
    def _is_empty_account_result(data: Mapping[str, Any]) -> bool:
        result = data.get("result")
        message = str(data.get("message", "")).strip().lower()
        result_text = str(result).strip().lower()
        return (
            (result == [] and message in {"", "ok", "no transactions found"})
            or "no transactions found" in result_text
            or (
                message == "no transactions found"
                and result in (None, "", [])
            )
        )

    async def latest_block(self) -> int:
        data = await self.request("proxy", "eth_blockNumber")
        result = data.get("result")
        if not isinstance(result, str):
            raise RetryableAPIError(
                f"Invalid latest block result: {result!r}"
            )
        try:
            value = int(result, 16)
        except ValueError as exc:
            raise RetryableAPIError(
                f"Invalid block hex: {result!r}"
            ) from exc
        if value < 0:
            raise RetryableAPIError(f"Negative latest block: {value}")
        return value

    async def block_hash(self, block_number: int) -> str:
        data = await self.request(
            "proxy",
            "eth_getBlockByNumber",
            tag=f"0x{block_number:x}",
            boolean="false",
        )
        block = data.get("result")
        if block is None:
            raise RetryableAPIError(
                f"Block {block_number} is temporarily unavailable"
            )
        if not isinstance(block, dict):
            raise RetryableAPIError(
                f"Invalid block {block_number} payload"
            )
        block_hash = normalize_hash(block.get("hash"))
        if block_hash is None:
            raise RetryableAPIError(
                f"Block {block_number} has invalid hash"
            )
        return block_hash

    async def block_data(self, block_number: int) -> BlockData:
        data = await self.request(
            "proxy",
            "eth_getBlockByNumber",
            tag=f"0x{block_number:x}",
            boolean="true",
        )
        block = data.get("result")
        if block is None:
            raise RetryableAPIError(
                f"Block {block_number} is temporarily unavailable"
            )
        if not isinstance(block, dict):
            raise RetryableAPIError(
                f"Invalid block {block_number} payload"
            )

        block_hash = normalize_hash(block.get("hash"))
        if block_hash is None:
            raise RetryableAPIError(
                f"Block {block_number} has invalid hash"
            )

        transactions = block.get("transactions")
        if not isinstance(transactions, list):
            raise RetryableAPIError(
                f"Block {block_number} has invalid transactions"
            )

        addresses: set[str] = set()
        for tx in transactions:
            if not isinstance(tx, dict):
                continue
            for field in ("from", "to"):
                address = normalize_address(tx.get(field))
                if address:
                    addresses.add(address)

        return BlockData(
            number=block_number,
            block_hash=block_hash,
            addresses=addresses,
        )

    async def nft_contracts_for_address(self, address: str) -> set[str]:
        contracts: set[str] = set()
        seen_page_fingerprints: set[tuple[str, str, int]] = set()

        for page in range(1, self.config.nft_tx_pages + 1):
            data = await self.request(
                "account",
                "tokennfttx",
                address=address,
                page=page,
                offset=self.config.nft_tx_offset,
                sort="desc",
            )
            await self.stats.inc(nft_pages=1)

            result = data.get("result")
            if (
                isinstance(result, str)
                and "no transactions found" in result.lower()
            ):
                break
            if not isinstance(result, list):
                raise RetryableAPIError(
                    f"Invalid tokennfttx result for {address}: "
                    f"{type(result).__name__}"
                )

            if result:
                def row_signature(row: Any) -> str:
                    if not isinstance(row, dict):
                        return repr(row)[:200]
                    return "|".join(
                        str(row.get(field, ""))
                        for field in (
                            "blockNumber", "transactionIndex", "hash",
                            "contractAddress", "tokenID", "from", "to",
                        )
                    )

                fingerprint = (
                    row_signature(result[0]),
                    row_signature(result[-1]),
                    len(result),
                )
                if fingerprint in seen_page_fingerprints:
                    raise RetryableAPIError(
                        f"Pagination loop detected for {address} at page {page}"
                    )
                seen_page_fingerprints.add(fingerprint)

            valid_rows = 0
            for transfer in result:
                if not isinstance(transfer, dict):
                    continue
                contract = normalize_address(
                    transfer.get("contractAddress")
                )
                if contract:
                    contracts.add(contract)
                    valid_rows += 1

            # If Etherscan claims it returned rows but every row has lost the
            # expected contractAddress schema, fail closed instead of caching the
            # wallet as empty.
            if result and valid_rows == 0:
                raise RetryableAPIError(
                    f"tokennfttx returned {len(result)} rows for {address} "
                    "but none had a valid contractAddress"
                )

            if len(result) < self.config.nft_tx_offset:
                break

        return contracts


# ---------------------------------------------------------------------------
# Worker supervision
# ---------------------------------------------------------------------------


class WorkerFailureMonitor:
    """A future that becomes failed as soon as any address worker dies."""

    def __init__(self) -> None:
        loop = asyncio.get_running_loop()
        self.future: asyncio.Future[None] = loop.create_future()

    def watch(self, task: asyncio.Task[None]) -> None:
        task.add_done_callback(self._on_done)

    def _on_done(self, task: asyncio.Task[None]) -> None:
        if self.future.done():
            return

        if task.cancelled():
            self.future.set_exception(
                RuntimeError(
                    f"Address worker {task.get_name()} was cancelled unexpectedly"
                )
            )
            # Mark exception as observed if nobody is currently awaiting it.
            self.future.exception()
            return

        try:
            exc = task.exception()
        except asyncio.CancelledError:
            exc = RuntimeError(
                f"Address worker {task.get_name()} was cancelled unexpectedly"
            )

        if exc is None:
            failure = RuntimeError(
                f"Address worker {task.get_name()} stopped unexpectedly"
            )
        else:
            failure = RuntimeError(
                f"Address worker {task.get_name()} stopped unexpectedly: {exc!r}"
            )
            failure.__cause__ = exc

        self.future.set_exception(failure)
        # Retrieval here prevents "Future exception was never retrieved" during
        # shutdown. Awaiting the future still raises the stored exception.
        self.future.exception()

    def raise_if_failed(self) -> None:
        if not self.future.done():
            return
        exc = self.future.exception()
        if exc is not None:
            raise exc
        raise RuntimeError("Address worker monitor completed unexpectedly")

    async def wait(self) -> None:
        await asyncio.shield(self.future)


async def guarded_queue_put(
    queue: asyncio.Queue[str],
    address: str,
    monitor: WorkerFailureMonitor,
) -> None:
    """Put without risking a permanent producer hang after worker failure."""
    monitor.raise_if_failed()

    put_task = asyncio.create_task(
        queue.put(address),
        name=f"queue-put-{address[-8:]}",
    )
    monitor_task = asyncio.create_task(
        monitor.wait(),
        name="worker-failure-wait",
    )

    try:
        done, _ = await asyncio.wait(
            {put_task, monitor_task},
            return_when=asyncio.FIRST_COMPLETED,
        )

        # Worker failure wins if both became ready in the same event-loop turn.
        monitor.raise_if_failed()

        if put_task in done:
            await put_task
            return

        # Defensive fallback; normally raise_if_failed() above has raised.
        await monitor_task
        raise RuntimeError("Worker monitor stopped unexpectedly")

    finally:
        for task in (put_task, monitor_task):
            if not task.done():
                task.cancel()
        await asyncio.gather(
            put_task,
            monitor_task,
            return_exceptions=True,
        )


async def wait_for_queue_or_worker_failure(
    queue: asyncio.Queue[str],
    monitor: WorkerFailureMonitor,
) -> None:
    """Wait for a drained queue, while making worker failure take precedence."""
    monitor.raise_if_failed()

    join_task = asyncio.create_task(queue.join(), name="queue-join")
    monitor_task = asyncio.create_task(
        monitor.wait(), name="worker-failure-wait"
    )
    try:
        done, _ = await asyncio.wait(
            {join_task, monitor_task},
            return_when=asyncio.FIRST_COMPLETED,
        )

        monitor.raise_if_failed()

        if join_task in done:
            await join_task
            monitor.raise_if_failed()
            return

        await monitor_task
        raise RuntimeError("Worker monitor stopped unexpectedly")

    finally:
        for task in (join_task, monitor_task):
            if not task.done():
                task.cancel()
        await asyncio.gather(
            join_task,
            monitor_task,
            return_exceptions=True,
        )


async def address_worker(
    worker_id: int,
    queue: asyncio.Queue[str],
    client: EtherscanClient,
    seen_contracts: set[str],
    seen_lock: asyncio.Lock,
    empty_wallets: ExpiringLRUSet,
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

                # Reserve before releasing the lock so two workers cannot both
                # append the same newly discovered contract.
                seen_contracts.update(new_contracts)

            try:
                if new_contracts:
                    await writer.add(new_contracts)
                    await stats.inc(contracts=len(new_contracts))
            except BaseException:
                # If persistence failed, undo only our reservation. This is not
                # needed for process-restart safety, but keeps in-process state
                # truthful while shutdown performs its final flush attempt.
                if new_contracts:
                    async with seen_lock:
                        seen_contracts.difference_update(new_contracts)
                raise

            await stats.inc(addresses_ok=1)

        except asyncio.CancelledError:
            raise

        except Exception as exc:
            logger.critical(
                "%s exhausted retries for %s; stopping before checkpoint: %s",
                name,
                address,
                exc,
                exc_info=True,
            )
            raise

        finally:
            in_flight.discard(address)
            queue.task_done()


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------


async def verify_checkpoint(
    state: CheckpointState,
    client: EtherscanClient,
    config: Config,
    logger: logging.Logger,
) -> None:
    if not config.verify_checkpoint_hash:
        return

    if state.last_processed_block_hash is None:
        logger.warning(
            "Checkpoint has no block hash (legacy v1); reorg verification will "
            "become active after the next successful checkpoint."
        )
        return

    current_hash = await client.block_hash(state.last_processed_block)
    if current_hash != state.last_processed_block_hash:
        raise RuntimeError(
            "Checkpoint reorg mismatch at block "
            f"{state.last_processed_block}: stored "
            f"{state.last_processed_block_hash}, canonical {current_hash}. "
            "Refusing to continue because already-checkpointed blocks may have "
            "changed. Choose an earlier START_BLOCK/checkpoint and rescan."
        )

    logger.info(
        "Checkpoint canonical hash verified at block %d",
        state.last_processed_block,
    )


async def run(config: Config) -> None:
    config.validate()
    logger = setup_logging(config.log_level)
    logger.info(
        "Starting NFT Scanner v7: chain=%d workers=%d rps=%.2f "
        "confirmations=%d keys=%d pages=%d",
        config.chain_id,
        config.workers,
        config.global_rps,
        config.confirmations,
        len(config.api_keys),
        config.nft_tx_pages,
    )

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()

    def request_shutdown() -> None:
        if not stop_event.is_set():
            logger.warning("Shutdown requested")
            stop_event.set()

    installed_signals: list[signal.Signals] = []
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, request_shutdown)
            installed_signals.append(sig)
        except (NotImplementedError, RuntimeError):
            pass

    queue: asyncio.Queue[str] = asyncio.Queue(
        maxsize=config.address_queue_size
    )
    in_flight: set[str] = set()
    empty_wallets = ExpiringLRUSet(
        config.empty_wallet_cache_size,
        config.empty_wallet_cache_ttl,
    )
    seen_contracts = load_contracts(
        (config.output_file, config.known_contracts_file),
        logger,
    )
    seen_lock = asyncio.Lock()
    stats = Stats()
    writer = ContractWriter(
        config.output_file,
        config.write_buffer_size,
        config.fsync_writes,
        logger,
    )
    checkpoint = Checkpoint(
        config.checkpoint_file,
        config.fsync_writes,
        logger,
    )
    limiter = TokenBucket(
        config.global_rps,
        config.token_bucket_size,
    )
    keys = APIKeyManager(
        config.api_keys,
        config.api_key_cooldown_sec,
    )

    timeout = aiohttp.ClientTimeout(
        total=config.request_timeout,
        connect=config.connect_timeout,
        sock_connect=config.connect_timeout,
        sock_read=config.request_timeout,
    )
    connector = aiohttp.TCPConnector(
        limit=config.tcp_limit,
        limit_per_host=config.tcp_limit_per_host,
        ttl_dns_cache=config.dns_cache_ttl,
        enable_cleanup_closed=True,
    )
    headers = {
        "User-Agent": config.user_agent,
        "Accept": "application/json",
    }

    final_snapshot: Optional[dict[str, float | int]] = None

    try:
        async with aiohttp.ClientSession(
            connector=connector,
            timeout=timeout,
            headers=headers,
        ) as session:
            client = EtherscanClient(
                session,
                config,
                keys,
                limiter,
                stats,
                logger,
            )

            monitor = WorkerFailureMonitor()
            workers = [
                asyncio.create_task(
                    address_worker(
                        i + 1,
                        queue,
                        client,
                        seen_contracts,
                        seen_lock,
                        empty_wallets,
                        in_flight,
                        writer,
                        stats,
                        logger,
                    ),
                    name=f"address-worker-{i + 1}",
                )
                for i in range(config.workers)
            ]
            for task in workers:
                monitor.watch(task)

            stats_task = asyncio.create_task(
                stats.log_loop(
                    config.stats_interval,
                    logger,
                    queue,
                    in_flight,
                    empty_wallets,
                    keys,
                ),
                name="stats",
            )

            state = checkpoint.load()
            last_processed: Optional[int]
            last_processed_hash: Optional[str]

            if state is not None:
                last_processed = state.last_processed_block
                last_processed_hash = state.last_processed_block_hash
                logger.info("Resuming after block %d", last_processed)
                await verify_checkpoint(state, client, config, logger)

            elif config.start_block > 0:
                last_processed = config.start_block - 1
                last_processed_hash = None
                logger.info(
                    "Starting at configured block %d",
                    config.start_block,
                )

            else:
                last_processed = None
                last_processed_hash = None

            try:
                while not stop_event.is_set():
                    monitor.raise_if_failed()

                    try:
                        latest = await client.latest_block()
                    except Exception as exc:
                        logger.error(
                            "Cannot obtain latest block: %s",
                            exc,
                        )
                        await sleep_or_stop(
                            stop_event,
                            config.block_poll_interval,
                        )
                        continue

                    safe_head = max(0, latest - config.confirmations)

                    if last_processed is None:
                        # Establish both number and canonical hash, so even an
                        # automatically initialized scanner is reorg-verifiable.
                        try:
                            safe_hash = await client.block_hash(safe_head)
                            await checkpoint.save(safe_head, safe_hash)
                        except Exception as exc:
                            logger.error(
                                "Cannot initialize checkpoint at safe head %d: %s",
                                safe_head,
                                exc,
                            )
                            await sleep_or_stop(
                                stop_event,
                                config.block_poll_interval,
                            )
                            continue

                        last_processed = safe_head
                        last_processed_hash = safe_hash
                        logger.info(
                            "Initialized at safe head %d; historical blocks are "
                            "not scanned (set START_BLOCK for history)",
                            safe_head,
                        )
                        await sleep_or_stop(
                            stop_event,
                            config.block_poll_interval,
                        )
                        continue

                    if latest < last_processed:
                        raise RuntimeError(
                            f"Latest chain head {latest} is behind checkpoint "
                            f"{last_processed}; refusing to continue."
                        )

                    if safe_head <= last_processed:
                        await sleep_or_stop(
                            stop_event,
                            config.block_poll_interval,
                        )
                        continue

                    batch_end = min(
                        safe_head,
                        last_processed + config.max_blocks_per_batch,
                    )
                    logger.info(
                        "Processing blocks %d..%d (head=%d safe=%d)",
                        last_processed + 1,
                        batch_end,
                        latest,
                        safe_head,
                    )

                    batch_last = last_processed
                    batch_last_hash = last_processed_hash
                    batch_failed = False

                    for block_number in range(
                        last_processed + 1,
                        batch_end + 1,
                    ):
                        if stop_event.is_set():
                            break
                        monitor.raise_if_failed()

                        try:
                            block = await client.block_data(block_number)
                        except Exception as exc:
                            logger.error(
                                "Block %d was not processed and will be retried: %s",
                                block_number,
                                exc,
                            )
                            batch_failed = True
                            break

                        await stats.inc(
                            blocks=1,
                            addresses_seen=len(block.addresses),
                        )

                        enqueued = 0
                        cache_hits = 0

                        for address in block.addresses:
                            if stop_event.is_set():
                                break
                            monitor.raise_if_failed()

                            if address in empty_wallets:
                                cache_hits += 1
                                continue
                            if address in in_flight:
                                continue

                            in_flight.add(address)
                            try:
                                await guarded_queue_put(
                                    queue,
                                    address,
                                    monitor,
                                )
                            except BaseException:
                                in_flight.discard(address)
                                raise
                            enqueued += 1

                        await stats.inc(
                            addresses_enqueued=enqueued,
                            cache_hits=cache_hits,
                        )

                        # If shutdown interrupted this block while enqueueing,
                        # do not mark it complete. The subset already queued may
                        # finish and write contracts, but this block will replay.
                        if stop_event.is_set():
                            logger.info(
                                "Shutdown interrupted block %d enqueue; "
                                "checkpoint remains before this block",
                                block_number,
                            )
                            break

                        batch_last = block.number
                        batch_last_hash = block.block_hash
                        logger.info(
                            "Block %d: participants=%d enqueued=%d "
                            "cache_hits=%d queue=%d",
                            block_number,
                            len(block.addresses),
                            enqueued,
                            cache_hits,
                            queue.qsize(),
                        )

                    # A checkpoint is durable only after all address queries and
                    # contract appends associated with completed blocks finish.
                    if batch_last > last_processed:
                        await wait_for_queue_or_worker_failure(
                            queue,
                            monitor,
                        )
                        await writer.flush()
                        await checkpoint.save(
                            batch_last,
                            batch_last_hash,
                        )
                        last_processed = batch_last
                        last_processed_hash = batch_last_hash
                        logger.info(
                            "Checkpoint committed at block %d",
                            last_processed,
                        )

                    if batch_failed:
                        await sleep_or_stop(
                            stop_event,
                            config.block_poll_interval,
                        )

            finally:
                stop_event.set()
                logger.info(
                    "Stopping producer; draining %d queued addresses",
                    queue.qsize(),
                )

                try:
                    try:
                        await wait_for_queue_or_worker_failure(
                            queue,
                            monitor,
                        )
                    except Exception as exc:
                        # A failed worker means the current uncheckpointed work
                        # will replay on restart. Still persist discoveries that
                        # are already buffered.
                        logger.error(
                            "Queue did not drain cleanly during shutdown: %s",
                            exc,
                        )

                    try:
                        await writer.flush()
                    except Exception as exc:
                        logger.critical(
                            "Final contract flush failed: %s",
                            exc,
                            exc_info=True,
                        )
                finally:
                    stats_task.cancel()
                    for task in workers:
                        task.cancel()
                    await asyncio.gather(
                        *workers,
                        stats_task,
                        return_exceptions=True,
                    )

            final_snapshot = await stats.snapshot()

    finally:
        for sig in installed_signals:
            try:
                loop.remove_signal_handler(sig)
            except (NotImplementedError, RuntimeError):
                pass

    if final_snapshot is None:
        final_snapshot = await stats.snapshot()

    logger.info(
        "Shutdown complete: blocks=%d addresses_ok=%d contracts=%d "
        "requests=%d retries=%d errors=%d limits=%d",
        final_snapshot["blocks"],
        final_snapshot["addresses_ok"],
        final_snapshot["contracts"],
        final_snapshot["requests"],
        final_snapshot["retries"],
        final_snapshot["errors"],
        final_snapshot["rate_limits"],
    )


def main() -> int:
    if sys.platform.startswith("win"):
        asyncio.set_event_loop_policy(
            asyncio.WindowsSelectorEventLoopPolicy()
        )

    try:
        config = Config.from_env()
        asyncio.run(run(config))
        return 0
    except KeyboardInterrupt:
        return 130
    except Exception as exc:
        logging.basicConfig(
            level=logging.ERROR,
            format="%(asctime)s | %(levelname)s | %(message)s",
        )
        logging.getLogger("nft-scanner").critical(
            "Fatal error: %s",
            exc,
            exc_info=True,
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
