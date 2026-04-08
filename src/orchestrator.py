"""
BotOrchestrator — manages all worker tasks, broadcasts state to WebSocket clients,
and coordinates startup/shutdown.

FIXES:
  C-05  TOCTOU race: asyncio.Lock prevents two concurrent start() calls from
        spawning duplicate worker sets (e.g. lifespan resume + HTTP POST race).
  H-07  DLQ push now uses 'retry_count' field to match the Pydantic model.
"""

import asyncio
import json
import time
import random
import traceback
import logging

from src.config.settings import ConfigurationManager
from src.engine.execution import ExecutionUnit
from src.engine.dead_letter_queue import dlq
from src.gas_oracle import gas_oracle
from src.utils.rpc_health import rpc_health

log = logging.getLogger(__name__)


class BotOrchestrator:
    def __init__(self):
        self._tasks: list[asyncio.Task] = []
        self._clients: set = set()      # WebSocket connections
        self._running  = False
        self._start_time: float | None = None
        self._shutdown_event = asyncio.Event()
        # FIX C-05: prevents TOCTOU race between lifespan resume + HTTP start
        self._start_lock = asyncio.Lock()

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
        msg  = json.dumps(payload)
        dead = set()
        for ws in list(self._clients):
            try:
                await asyncio.wait_for(ws.send_text(msg), timeout=5.0)
            except Exception:
                dead.add(ws)
        self._clients -= dead

    # ------------------------------------------------------------------
    # Startup
    # ------------------------------------------------------------------
    async def start(self, keys: list[str]):
        # FIX C-05: lock prevents two concurrent callers both passing the
        # _running guard before either sets _running = True
        async with self._start_lock:
            if self._running:
                await self._broadcast({
                    "type": "log", "level": "WARNING", "worker_id": "SYS",
                    "message": "Bot is already running.", "timestamp": int(time.time()),
                })
                return

            self._running    = True
            self._start_time = time.time()
            self._shutdown_event.clear()
            cfg = ConfigurationManager()

        await self._broadcast({"type": "status", "status": "STARTING", "timestamp": int(time.time())})
        await self._broadcast({
            "type": "log", "level": "INFO", "worker_id": "SYS",
            "message": f"Initializing infrastructure for {cfg.rpc_ticker}...",
            "timestamp": int(time.time()),
        })

        # Start infrastructure
        try:
            rpc_health.start()
            gas_oracle.set_broadcast_callback(self._broadcast)
            gas_oracle.start()
            await asyncio.sleep(1)   # let first health-check run
            rpcs = rpc_health.get_rpcs(cfg.rpc_ticker)
            await self._broadcast({
                "type": "log", "level": "INFO", "worker_id": "SYS",
                "message": f"RPC pool ready ({len(rpcs)} nodes).",
                "timestamp": int(time.time()),
            })
        except Exception as exc:
            await self._broadcast({
                "type": "log", "level": "FATAL", "worker_id": "SYS",
                "message": f"Infrastructure failure: {exc}", "timestamp": int(time.time()),
            })
            self._running = False
            return

        # Auto-funder
        if cfg.fund_enabled:
            await self._broadcast({
                "type": "log", "level": "INFO", "worker_id": "SYS",
                "message": "Starting Auto-Funder...", "timestamp": int(time.time()),
            })
            try:
                from src.features.funder import MassFunder
                funder = MassFunder()
                await funder.check_and_fund(keys, broadcast=self._broadcast)
            except Exception as exc:
                await self._broadcast({
                    "type": "log", "level": "ERROR", "worker_id": "SYS",
                    "message": f"Auto-Funder error: {exc}", "timestamp": int(time.time()),
                })

        # Clear dead letter queue from last run
        await dlq.clear()

        # Priority sorting: lower number = higher priority = spawned first
        indexed_keys = list(enumerate(keys, 1))
        prio = cfg.worker_priority
        if prio:
            indexed_keys.sort(key=lambda x: prio[x[0] - 1] if x[0] <= len(prio) else 99)

        # Spawn workers
        for _idx, (key_idx, key) in enumerate(indexed_keys):
            task = asyncio.create_task(self._worker_loop(key_idx, key, cfg))
            self._tasks.append(task)

        await self._broadcast({
            "type": "log", "level": "INFO", "worker_id": "SYS",
            "message": f"Spawned {len(keys)} workers. Bot is LIVE.",
            "timestamp": int(time.time()),
        })
        await self._broadcast({"type": "status", "status": "RUNNING", "timestamp": int(time.time())})

    # ------------------------------------------------------------------
    # Worker loop (with full error isolation)
    # ------------------------------------------------------------------
    async def _worker_loop(self, uid: int, key: str, cfg: ConfigurationManager):
        """Worker loop with comprehensive error handling."""
        worker_id      = f"W{uid}"
        execution_unit = ExecutionUnit(key, uid, cfg, broadcast=self._broadcast)
        attempt        = 0

        while self._running:
            try:
                attempt += 1

                await self._broadcast({
                    "type": "log", "level": "DEBUG", "worker_id": worker_id,
                    "message": f"Starting mint attempt #{attempt}...",
                    "timestamp": int(time.time()),
                })

                # Check gas ceiling before minting
                if gas_oracle.is_ceiling_exceeded():
                    gwei = gas_oracle.get_current().get("max_fee_gwei", "?")
                    await self._broadcast({
                        "type": "log", "level": "WARNING", "worker_id": worker_id,
                        "message": f"Gas ceiling exceeded ({gwei} > {cfg.gas_ceiling_gwei} Gwei). Pausing...",
                        "timestamp": int(time.time()),
                    })
                    await asyncio.sleep(10)
                    continue

                # Run the mint cycle
                result = await execution_unit.run()

                # Post-mint actions if successful
                if result.success and result.tx_hash:
                    await self._broadcast({
                        "type": "log", "level": "INFO", "worker_id": worker_id,
                        "message": f"Mint successful! TX: {result.tx_hash}",
                        "timestamp": int(time.time()),
                    })
                    try:
                        await execution_unit.post_mint_actions(result)
                    except Exception as exc:
                        await self._broadcast({
                            "type": "log", "level": "WARNING", "worker_id": worker_id,
                            "message": f"Post-mint action error (non-critical): {exc}",
                            "timestamp": int(time.time()),
                        })

                    if not cfg.force_start:
                        break

                # Delay before retry
                delay = random.uniform(cfg.delay_range[0], cfg.delay_range[1])
                await asyncio.sleep(delay)

            except asyncio.CancelledError:
                await self._broadcast({
                    "type": "log", "level": "INFO", "worker_id": worker_id,
                    "message": "Worker cancelled.", "timestamp": int(time.time()),
                })
                return

            except Exception as exc:
                tb = traceback.format_exc()
                await self._broadcast({
                    "type": "log", "level": "ERROR", "worker_id": worker_id,
                    "message": f"Execution error: {str(exc)[:200]}",
                    "traceback": tb[:500],
                    "timestamp": int(time.time()),
                })
                log.error("Worker %s error (attempt %d): %s", worker_id, attempt, exc)

                # Push to DLQ
                try:
                    await dlq.push({
                        "worker_id":   worker_id,
                        "wallet":      execution_unit._acct.address,
                        "error":       str(exc)[:200],
                        "timestamp":   time.time(),
                        "retry_count": attempt,      # FIX H-07: was 'attempt' key
                        "traceback":   tb[:500],
                    })
                except Exception:
                    pass

                # Delay before retry
                delay = random.uniform(cfg.delay_range[0], cfg.delay_range[1])
                await asyncio.sleep(delay)

    # ------------------------------------------------------------------
    # Status
    # ------------------------------------------------------------------
    def get_status(self) -> dict:
        uptime = (time.time() - self._start_time) if self._start_time else 0
        return {
            "status":    "RUNNING" if self._running else "STOPPED",
            "running":   self._running,
            "uptime":    uptime,
            "workers":   len([t for t in self._tasks if not t.done()]),
            "timestamp": int(time.time()),
        }

    # ------------------------------------------------------------------
    # Graceful shutdown
    # ------------------------------------------------------------------
    async def stop(self):
        """Graceful shutdown with timeout and proper cleanup."""
        if not self._running:
            return

        self._running = False
        self._shutdown_event.set()

        await self._broadcast({"type": "status", "status": "STOPPING", "timestamp": int(time.time())})
        await self._broadcast({
            "type": "log", "level": "INFO", "worker_id": "SYS",
            "message": "Initiating graceful shutdown...", "timestamp": int(time.time()),
        })

        # Stop infrastructure
        try:
            gas_oracle.stop()
        except Exception as exc:
            log.warning("Error stopping gas oracle: %s", exc)

        try:
            rpc_health.stop()
        except Exception as exc:
            log.warning("Error stopping RPC health: %s", exc)

        # Give workers up to 10 seconds to finish current operations
        if self._tasks:
            try:
                await asyncio.wait_for(
                    asyncio.gather(*self._tasks, return_exceptions=True),
                    timeout=10.0,
                )
                await self._broadcast({
                    "type": "log", "level": "INFO", "worker_id": "SYS",
                    "message": "All workers completed gracefully.", "timestamp": int(time.time()),
                })
            except asyncio.TimeoutError:
                await self._broadcast({
                    "type": "log", "level": "WARNING", "worker_id": "SYS",
                    "message": "Shutdown timeout: force-cancelling remaining workers.",
                    "timestamp": int(time.time()),
                })
                for task in self._tasks:
                    if not task.done():
                        task.cancel()
                try:
                    await asyncio.gather(*self._tasks, return_exceptions=True)
                except Exception:
                    pass

        self._tasks.clear()
        await self._broadcast({"type": "status", "status": "STOPPED", "timestamp": int(time.time())})


# Singleton
orchestrator = BotOrchestrator()