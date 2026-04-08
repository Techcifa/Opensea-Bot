"""
RpcHealthChecker — pings every configured RPC every 15 seconds and removes
unresponsive endpoints from the active pool automatically.

FIX H-01: Replaced per-check aiohttp session creation with a single shared
           persistent session+connector to prevent file-descriptor exhaustion.
"""

import asyncio
import aiohttp
import time
import logging
from src.config.settings import NETWORKS

log = logging.getLogger(__name__)


class RpcHealthChecker:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if self._initialized:
            return
        self._initialized = True
        self.healthy_rpcs: dict[str, list[str]] = {
            ticker: list(info["rpc"]) for ticker, info in NETWORKS.items()
        }
        self.stats: dict[str, dict] = {}
        self._task: asyncio.Task | None = None
        # FIX H-01: single persistent connector shared across all health checks
        self._connector: aiohttp.TCPConnector | None = None
        self._session: aiohttp.ClientSession | None = None

    def start(self):
        if self._task is None or self._task.done():
            self._task = asyncio.create_task(self._loop())

    def stop(self):
        if self._task:
            self._task.cancel()
            self._task = None
        # Schedule session cleanup
        asyncio.get_event_loop().create_task(self._close_session())

    async def _close_session(self):
        if self._session and not self._session.closed:
            await self._session.close()
            self._session = None
        if self._connector and not self._connector.closed:
            await self._connector.close()
            self._connector = None

    def _get_session(self) -> aiohttp.ClientSession:
        """FIX H-01: Lazily create and reuse a single session with shared connector."""
        if self._session is None or self._session.closed:
            # One connector for the life of the health checker
            self._connector = aiohttp.TCPConnector(limit=50, ttl_dns_cache=300)
            timeout = aiohttp.ClientTimeout(total=4)
            self._session = aiohttp.ClientSession(connector=self._connector, timeout=timeout)
        return self._session

    def get_rpcs(self, ticker: str) -> list[str]:
        return self.get_prioritized_rpcs(ticker)

    def get_prioritized_rpcs(self, ticker: str) -> list[str]:
        """Return URLs sorted by performance (score / latency)."""
        pool = self.healthy_rpcs.get(ticker, [])
        if not pool:
            # Fallback: restore from NETWORKS so we're never truly empty
            self.healthy_rpcs[ticker] = list(NETWORKS.get(ticker, {}).get("rpc", []))
            pool = self.healthy_rpcs[ticker]

        def sort_key(url):
            s = self.stats.get(url, {"latency": 9999, "score": 0.5})
            return (-s["score"], s["latency"])

        return sorted(pool, key=sort_key)

    async def _loop(self):
        while True:
            try:
                await self._check_all()
            except asyncio.CancelledError:
                await self._close_session()
                return
            except Exception as exc:
                log.debug("RPC health check error: %s", exc)
            await asyncio.sleep(15)

    async def _check_all(self):
        session = self._get_session()
        tasks = []
        for ticker, info in NETWORKS.items():
            for url in info["rpc"]:
                tasks.append(self._check_one(session, ticker, url))
        await asyncio.gather(*tasks, return_exceptions=True)

    async def _check_one(self, session: aiohttp.ClientSession, ticker: str, url: str):
        """FIX H-01: Accepts the shared session instead of creating a new one."""
        payload = {"jsonrpc": "2.0", "method": "eth_blockNumber", "params": [], "id": 1}
        start = time.perf_counter()
        try:
            async with session.post(url, json=payload) as resp:
                data = await resp.json()
                if "result" in data:
                    latency = int((time.perf_counter() - start) * 1000)
                    pool = self.healthy_rpcs.setdefault(ticker, [])
                    if url not in pool:
                        pool.append(url)
                    curr = self.stats.get(url, {"latency": latency, "score": 0.5})
                    curr["latency"] = int(curr["latency"] * 0.7 + latency * 0.3)
                    curr["score"]   = min(1.0, curr["score"] + 0.1)
                    self.stats[url] = curr
                    return
        except Exception:
            pass

        # Degrade score; remove only if score hits 0 and pool has backups
        pool = self.healthy_rpcs.get(ticker, [])
        if url in pool:
            curr = self.stats.get(url, {"latency": 9999, "score": 0.5})
            curr["score"] = max(0.0, curr["score"] - 0.2)
            self.stats[url] = curr
            if curr["score"] <= 0 and len(pool) > 1:
                pool.remove(url)
                log.warning("RPC removed from pool (score 0): %s [%s]", url[:40], ticker)


# Singleton instance
rpc_health = RpcHealthChecker()
