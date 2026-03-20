"""
RpcRateLimiter — shared per-endpoint semaphore that prevents Workers from
hammering RPC nodes and triggering 429 rate-limit responses.
"""

import asyncio
import time
from contextlib import asynccontextmanager


class RpcRateLimiter:
    """
    Limits concurrent in-flight RPC calls per endpoint.
    Uses a semaphore with exponential backoff on 429-style errors.
    """

    def __init__(self, max_concurrent: int = 10, base_backoff: float = 0.5, max_backoff: float = 30.0):
        self._max_concurrent = max_concurrent
        self._base_backoff = base_backoff
        self._max_backoff = max_backoff
        # semaphore per endpoint URL
        self._semaphores: dict[str, asyncio.Semaphore] = {}
        # backoff tracking per endpoint
        self._backoff_until: dict[str, float] = {}
        self._backoff_exponent: dict[str, int] = {}

    def _get_semaphore(self, endpoint: str) -> asyncio.Semaphore:
        if endpoint not in self._semaphores:
            self._semaphores[endpoint] = asyncio.Semaphore(self._max_concurrent)
        return self._semaphores[endpoint]

    @asynccontextmanager
    async def acquire(self, endpoint: str):
        sem = self._get_semaphore(endpoint)
        async with sem:
            # Respect backoff window
            wait_until = self._backoff_until.get(endpoint, 0)
            now = time.monotonic()
            if wait_until > now:
                await asyncio.sleep(wait_until - now)
            yield

    def report_rate_limit(self, endpoint: str):
        """Call this when an RPC returns HTTP 429 or equivalent."""
        exp = self._backoff_exponent.get(endpoint, 0)
        delay = min(self._base_backoff * (2 ** exp), self._max_backoff)
        self._backoff_until[endpoint] = time.monotonic() + delay
        self._backoff_exponent[endpoint] = exp + 1

    def report_success(self, endpoint: str):
        """Reset backoff on successful call."""
        self._backoff_exponent[endpoint] = 0
        self._backoff_until.pop(endpoint, None)


# Singleton shared across all workers
rpc_limiter = RpcRateLimiter(max_concurrent=10)
