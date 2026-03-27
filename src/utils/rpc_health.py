"""
RpcHealthChecker — pings every configured RPC every 30 seconds and removes
unresponsive endpoints from the active pool automatically.
"""

import asyncio
import aiohttp
import time
from src.config.settings import NETWORKS


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
        # healthy_rpcs[network_ticker] = [url, ...]
        self.healthy_rpcs: dict[str, list[str]] = {
            ticker: list(info["rpc"]) for ticker, info in NETWORKS.items()
        }
        # stats[url] = {"latency": ms, "score": 0.0-1.0}
        self.stats: dict[str, dict] = {}
        self._task: asyncio.Task | None = None

    def start(self):
        if self._task is None or self._task.done():
            self._task = asyncio.create_task(self._loop())

    def stop(self):
        if self._task:
            self._task.cancel()

    def get_rpcs(self, ticker: str) -> list[str]:
        return self.get_prioritized_rpcs(ticker)

    def get_prioritized_rpcs(self, ticker: str) -> list[str]:
        """Return URLs sorted by performance (score / latency)."""
        pool = self.healthy_rpcs.get(ticker, [])
        if not pool:
            # Fallback: restore from NETWORKS
            self.healthy_rpcs[ticker] = list(NETWORKS.get(ticker, {}).get("rpc", []))
            pool = self.healthy_rpcs[ticker]

        def sort_key(url):
            s = self.stats.get(url, {"latency": 9999, "score": 0.5})
            # Sort by score descending, then latency ascending
            return (-s["score"], s["latency"])

        return sorted(pool, key=sort_key)

    async def _loop(self):
        while True:
            await self._check_all()
            await asyncio.sleep(15)  # Faster health checks for racing

    async def _check_all(self):
        tasks = []
        for ticker, info in NETWORKS.items():
            for url in info["rpc"]:
                tasks.append(self._check_one(ticker, url))
        await asyncio.gather(*tasks, return_exceptions=True)

    async def _check_one(self, ticker: str, url: str):
        payload = {"jsonrpc": "2.0", "method": "eth_blockNumber", "params": [], "id": 1}
        start = time.perf_counter()
        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=4)) as s:
                async with s.post(url, json=payload) as resp:
                    data = await resp.json()
                    if "result" in data:
                        latency = int((time.perf_counter() - start) * 1000)
                        # Mark healthy
                        pool = self.healthy_rpcs.setdefault(ticker, [])
                        if url not in pool:
                            pool.append(url)
                        
                        # Update stats
                        curr = self.stats.get(url, {"latency": latency, "score": 0.5})
                        # Smoothing latency and score
                        curr["latency"] = int(curr["latency"] * 0.7 + latency * 0.3)
                        curr["score"] = min(1.0, curr["score"] + 0.1)
                        self.stats[url] = curr
                        return
        except Exception:
            pass
            
        # Mark unhealthy or degradation
        pool = self.healthy_rpcs.get(ticker, [])
        if url in pool:
            curr = self.stats.get(url, {"latency": 9999, "score": 0.0})
            curr["score"] = max(0.0, curr["score"] - 0.2)
            self.stats[url] = curr
            if curr["score"] <= 0 and len(pool) > 1:
                pool.remove(url)


# Singleton instance
rpc_health = RpcHealthChecker()
