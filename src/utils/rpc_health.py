"""
RpcHealthChecker — pings every configured RPC every 30 seconds and removes
unresponsive endpoints from the active pool automatically.
"""

import asyncio
import aiohttp
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
        self._task: asyncio.Task | None = None

    def start(self):
        if self._task is None or self._task.done():
            self._task = asyncio.create_task(self._loop())

    def stop(self):
        if self._task:
            self._task.cancel()

    def get_rpcs(self, ticker: str) -> list[str]:
        rpcs = self.healthy_rpcs.get(ticker, [])
        if not rpcs:
            # Fallback: restore all RPCs if every node somehow went offline
            self.healthy_rpcs[ticker] = list(NETWORKS.get(ticker, {}).get("rpc", []))
            rpcs = self.healthy_rpcs[ticker]
        return rpcs

    async def _loop(self):
        while True:
            await self._check_all()
            await asyncio.sleep(30)

    async def _check_all(self):
        tasks = []
        for ticker, info in NETWORKS.items():
            for url in info["rpc"]:
                tasks.append(self._check_one(ticker, url))
        await asyncio.gather(*tasks, return_exceptions=True)

    async def _check_one(self, ticker: str, url: str):
        payload = {"jsonrpc": "2.0", "method": "eth_blockNumber", "params": [], "id": 1}
        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=5)) as s:
                async with s.post(url, json=payload) as resp:
                    data = await resp.json()
                    if "result" in data:
                        # Mark healthy
                        pool = self.healthy_rpcs.setdefault(ticker, [])
                        if url not in pool:
                            pool.append(url)
                        return
        except Exception:
            pass
        # Mark unhealthy
        pool = self.healthy_rpcs.get(ticker, [])
        if url in pool and len(pool) > 1:
            pool.remove(url)


# Singleton instance
rpc_health = RpcHealthChecker()
