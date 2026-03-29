"""
BotOrchestrator — manages all worker tasks, broadcasts state to WebSocket clients,
and coordinates startup/shutdown.
"""

import asyncio
import json
import time

from src.config.settings import ConfigurationManager
from src.engine.execution import ExecutionUnit
from src.engine.dead_letter_queue import dlq
from src.features.funder import MassFunder
from src.gas_oracle import gas_oracle
from src.utils.rpc_health import rpc_health


class BotOrchestrator:
    def __init__(self):
        self._tasks: list[asyncio.Task] = []
        self._clients: set = set()  # WebSocket connections
        self._running = False
        self._start_time: float | None = None

    # ------------------------------------------------------------------
    # WebSocket client management
    # ------------------------------------------------------------------
    def add_client(self, ws):
        self._clients.add(ws)

    def remove_client(self, ws):
        self._clients.discard(ws)

    async def _broadcast(self, payload: dict):
        """Send a JSON message to all connected WebSocket clients."""
        if not self._clients:
            return
        msg = json.dumps(payload)
        dead = set()
        for ws in self._clients:
            try:
                await ws.send_text(msg)
            except Exception:
                dead.add(ws)
        self._clients -= dead

    # ------------------------------------------------------------------
    # Startup
    # ------------------------------------------------------------------
    async def start(self, keys: list[str], encrypted_keys: bool = False):
        if self._running:
            await self._broadcast({"type": "log", "level": "WARNING", "worker_id": "SYS",
                                    "message": "Bot is already running.", "timestamp": int(time.time())})
            return

        self._running = True
        self._start_time = time.time()
        cfg = ConfigurationManager()

        await self._broadcast({"type": "status", "status": "STARTING",
                                "timestamp": int(time.time())})
        await self._broadcast({"type": "log", "level": "INFO", "worker_id": "SYS",
                                "message": f"Initializing infrastructure for {cfg.rpc_ticker}...", "timestamp": int(time.time())})

        # Start infrastructure
        try:
            rpc_health.start()
            gas_oracle.set_broadcast_callback(self._broadcast)
            gas_oracle.start()
            
            # Wait a moment for RPC health check to hit first results
            await asyncio.sleep(1)
            rpcs = rpc_health.get_rpcs(cfg.rpc_ticker)
            await self._broadcast({"type": "log", "level": "INFO", "worker_id": "SYS",
                                    "message": f"RPC pool ready ({len(rpcs)} nodes).", "timestamp": int(time.time())})
        except Exception as e:
            await self._broadcast({"type": "log", "level": "FATAL", "worker_id": "SYS",
                                    "message": f"Infrastructure failure: {e}", "timestamp": int(time.time())})
            self._running = False
            return

        # Auto-funder
        if cfg.fund_enabled:
            await self._broadcast({"type": "log", "level": "INFO", "worker_id": "SYS",
                                    "message": "Starting Auto-Funder...", "timestamp": int(time.time())})
            try:
                funder = MassFunder()
                await funder.check_and_fund(keys, broadcast=self._broadcast)
            except Exception as e:
                await self._broadcast({"type": "log", "level": "ERROR", "worker_id": "SYS",
                                        "message": f"Auto-Funder error: {e}", "timestamp": int(time.time())})

        # Clear dead letter queue from last run
        await dlq.clear()

        # Priority sorting: lower number = higher priority = spawned first
        indexed_keys = list(enumerate(keys, 1))
        prio = cfg.worker_priority
        if prio:
            indexed_keys.sort(key=lambda x: prio[x[0]-1] if x[0] <= len(prio) else 99)

        # Worker semaphore
        semaphore = asyncio.Semaphore(cfg.max_threads)

        await self._broadcast({"type": "log", "level": "INFO", "worker_id": "SYS",
                                "message": f"Spawning {len(keys)} workers (max {cfg.max_threads} concurrent)...",
                                "timestamp": int(time.time())})
        await self._broadcast({"type": "status", "status": "RUNNING",
                                "timestamp": int(time.time())})

        async def _worker_wrapper(pk: str, uid: int):
            async with semaphore:
                try:
                    unit = ExecutionUnit(pk, uid, cfg, broadcast=self._broadcast)
                    await unit.run_protocol()
                except asyncio.CancelledError:
                    pass
                except Exception as e:
                    await self._broadcast({
                        "type": "log", "level": "FATAL", "worker_id": uid,
                        "message": f"Unit crashed: {str(e)[:80]}", "timestamp": int(time.time())
                    })
                    await dlq.push(uid, pk, str(e))

        self._tasks = [
            asyncio.create_task(_worker_wrapper(pk, uid))
            for uid, pk in indexed_keys
        ]

        # Watch for completion in background
        asyncio.create_task(self._watch_completion())

    async def _watch_completion(self):
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        self._running = False
        await self._broadcast({"type": "status", "status": "DONE",
                                "timestamp": int(time.time())})
        await self._broadcast({"type": "log", "level": "INFO", "worker_id": "SYS",
                                "message": "All operations complete.", "timestamp": int(time.time())})

    # ------------------------------------------------------------------
    # Shutdown
    # ------------------------------------------------------------------
    async def stop(self):
        for task in self._tasks:
            if not task.done():
                task.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()
        self._running = False
        gas_oracle.stop()

        await self._broadcast({"type": "status", "status": "STOPPED",
                                "timestamp": int(time.time())})
        await self._broadcast({"type": "log", "level": "INFO", "worker_id": "SYS",
                                "message": "Bot stopped gracefully.", "timestamp": int(time.time())})

    # ------------------------------------------------------------------
    # Status snapshot
    # ------------------------------------------------------------------
    def get_status(self) -> dict:
        return {
            "running": self._running,
            "workers": len(self._tasks),
            "active_workers": sum(1 for t in self._tasks if not t.done()),
            "uptime": int(time.time() - self._start_time) if self._start_time else 0,
            "gas": gas_oracle.get_current(),
        }


# Singleton
orchestrator = BotOrchestrator()
