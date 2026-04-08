"""
RpcRateLimiter — shared per-endpoint semaphore that prevents workers from
hammering RPC nodes and triggering 429 rate-limit responses.

Features:
  - Semaphore: limits concurrent in-flight calls per endpoint
  - Exponential backoff: tracks 429 history, increases wait time per repeat
  - Hard ban: endpoints with extreme ban duration (>1 hour) are blacklisted
    and skipped entirely until the process restarts
"""

import asyncio
import time
import logging
from contextlib import asynccontextmanager

log = logging.getLogger(__name__)

# If an endpoint returns a backoff this long, consider it dead for this session.
# ANKR free tier returns ~21 day bans — we immediately blacklist and rotate.
HARD_BAN_THRESHOLD_S = 3600   # 1 hour


class RpcRateLimiter:
    def __init__(self, max_concurrent: int = 10, base_backoff: float = 0.5, max_backoff: float = 60.0):
        self._max_concurrent  = max_concurrent
        self._base_backoff    = base_backoff
        self._max_backoff     = max_backoff
        self._semaphores:     dict[str, asyncio.Semaphore] = {}
        self._backoff_until:  dict[str, float] = {}
        self._backoff_exp:    dict[str, int]   = {}
        # Permanently banned for this session (extreme quota exhaustion)
        self._blacklisted:    set[str] = set()

    def _get_semaphore(self, endpoint: str) -> asyncio.Semaphore:
        if endpoint not in self._semaphores:
            self._semaphores[endpoint] = asyncio.Semaphore(self._max_concurrent)
        return self._semaphores[endpoint]

    def is_blacklisted(self, endpoint: str) -> bool:
        """True if the endpoint has been session-banned (e.g. ANKR 21-day quota)."""
        return endpoint in self._blacklisted

    @asynccontextmanager
    async def acquire(self, endpoint: str):
        if endpoint in self._blacklisted:
            raise RuntimeError(f"RPC blacklisted (quota exhausted): {endpoint[:40]}")
        sem = self._get_semaphore(endpoint)
        async with sem:
            wait_until = self._backoff_until.get(endpoint, 0)
            now = time.monotonic()
            if wait_until > now:
                sleep_s = wait_until - now
                if sleep_s > HARD_BAN_THRESHOLD_S:
                    # Too long to wait — blacklist for this session
                    self._blacklisted.add(endpoint)
                    log.error(
                        "[RateLimiter] %s... backoff %.0fs > threshold \u2014 session-blacklisted. "
                        "Use ANKR_API_KEY or switch RPC.", endpoint[:40], sleep_s
                    )
                    raise RuntimeError(f"RPC session-blacklisted (backoff {sleep_s:.0f}s)")
                await asyncio.sleep(min(sleep_s, self._max_backoff))
            yield

    def report_rate_limit(self, endpoint: str, ban_seconds: float | None = None):
        """
        Call when an RPC returns HTTP 429 or a quota-exhaustion error.
        If ban_seconds is provided (e.g. parsed from the error message),
        uses it directly — otherwise applies exponential backoff.
        """
        if ban_seconds is not None and ban_seconds > HARD_BAN_THRESHOLD_S:
            self._blacklisted.add(endpoint)
            log.error(
                "[RateLimiter] %s... has a %.0f-second ban \u2014 session-blacklisted.",
                endpoint[:40], ban_seconds
            )
            return

        exp   = self._backoff_exp.get(endpoint, 0)
        delay = ban_seconds if ban_seconds is not None else min(self._base_backoff * (2 ** exp), self._max_backoff)
        self._backoff_until[endpoint] = time.monotonic() + delay
        self._backoff_exp[endpoint]   = exp + 1
        log.warning(
            "[RateLimiter] %s... rate-limited \u2014 backing off %.1fs (attempt %d)",
            endpoint[:40], delay, exp + 1
        )

    def report_success(self, endpoint: str):
        """Reset backoff on successful call."""
        self._backoff_exp.pop(endpoint, None)
        self._backoff_until.pop(endpoint, None)

    def get_status(self) -> dict:
        return {
            "blacklisted": list(self._blacklisted),
            "backed_off":  {k: round(v - time.monotonic(), 1)
                            for k, v in self._backoff_until.items()
                            if v > time.monotonic()},
        }


# Singleton shared across all workers
rpc_limiter = RpcRateLimiter(max_concurrent=10)
