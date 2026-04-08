"""
BotOrchestrator — manages all worker tasks, broadcasts state to WebSocket clients,
and coordinates startup/shutdown. (FIXED VERSION)

Key improvements:
- Graceful shutdown with timeout
- Proper task cancellation
- Error isolation in worker loop
"""

import asyncio
import json
import time
import random
import traceback

from src.config.settings import ConfigurationManager
from src.engine.execution import ExecutionUnit
from src.engine.dead_letter_queue import dlq
from src.gas_oracle import gas_oracle
from src.utils.rpc_health import rpc_health


class BotOrchestrator:
    def __init__(self):
        self._tasks: list[asyncio.Task] = []
        self._clients: set = set()  # WebSocket connections
        self._running = False
        self._start_time: float | None = None
        self._shutdown_event = asyncio.Event()

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
                await asyncio.wait_for(ws.send_text(msg), timeout=5.0)
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
        self._shutdown_event.clear()
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
                from src.features.funder import MassFunder
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

        # Spawn workers
        for idx, (key_idx, key) in enumerate(indexed_keys):
            worker_id = f"W{key_idx}"
            task = asyncio.create_task(self._worker_loop(worker_id, key, cfg))
            self._tasks.append(task)

        await self._broadcast({"type": "log", "level": "INFO", "worker_id": "SYS",
                                "message": f"Spawned {len(keys)} workers. Bot is LIVE.", "timestamp": int(time.time())})
        await self._broadcast({"type": "status", "status": "RUNNING",
                                "timestamp": int(time.time())})

    # ------------------------------------------------------------------
    # Worker loop (with full error isolation)
    # ------------------------------------------------------------------
    async def _worker_loop(self, worker_id: str, key: str, cfg: ConfigurationManager):
        """Worker loop with comprehensive error handling."""
        execution_unit = ExecutionUnit(worker_id, key, broadcast=self._broadcast)
        attempt = 0
        
        while self._running:
            try:
                attempt += 1
                
                await self._broadcast({"type": "log", "level": "DEBUG", "worker_id": worker_id,
                                      "message": f"Starting mint attempt #{attempt}...", "timestamp": int(time.time())})
                
                # Check gas ceiling before minting
                if gas_oracle.is_ceiling_exceeded():
                    await self._broadcast({"type": "log", "level": "WARNING", "worker_id": worker_id,
                                          "message": f"Gas ceiling exceeded ({gas_oracle.get_current()['max_fee_gwei']} > {cfg.gas_ceiling_gwei}). Pausing...", 
                                          "timestamp": int(time.time())})
                    await asyncio.sleep(10)
                    continue
                
                # Run the mint cycle
                result = await execution_unit.run()
                
                # Post-mint actions if successful
                if result.success and result.tx_hash:
                    await self._broadcast({"type": "log", "level": "INFO", "worker_id": worker_id,
                                          "message": f"Mint successful! TX: {result.tx_hash}", "timestamp": int(time.time())})
                    
                    try:
                        await execution_unit.post_mint_actions(result)
                    except Exception as e:
                        await self._broadcast({"type": "log", "level": "WARNING", "worker_id": worker_id,
                                              "message": f"Post-mint action error (non-critical): {e}", 
                                              "timestamp": int(time.time())})
                    
                    # On success, worker can exit or wait for next cycle
                    if not cfg.force_start:
                        break
                
                # Delay before retry
                delay = random.uniform(cfg.delay_range[0], cfg.delay_range[1])
                await asyncio.sleep(delay)
            
            except asyncio.CancelledError:
                await self._broadcast({"type": "log", "level": "INFO", "worker_id": worker_id,
                                      "message": "Worker cancelled.", "timestamp": int(time.time())})
                return
            
            except Exception as e:
                await self._broadcast({"type": "log", "level": "ERROR", "worker_id": worker_id,
                                      "message": f"Execution error: {str(e)[:200]}", 
                                      "traceback": traceback.format_exc()[:500],
                                      "timestamp": int(time.time())})
                
                # Push to DLQ for retry
                try:
                    await dlq.push({
                        "worker_id": worker_id,
                        "error": str(e)[:200],
                        "timestamp": time.time(),
                        "traceback": traceback.format_exc()[:500],
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
        uptime = 0
        if self._start_time:
            uptime = time.time() - self._start_time
        
        return {
            "status": "RUNNING" if self._running else "STOPPED",
            "running": self._running,
            "uptime": uptime,
            "workers": len([t for t in self._tasks if not t.done()]),
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
        await self._broadcast({"type": "log", "level": "INFO", "worker_id": "SYS",
                              "message": "Initiating graceful shutdown...", "timestamp": int(time.time())})
        
        # Stop infrastructure
        try:
            gas_oracle.stop()
        except Exception as e:
            print(f"[-] Error stopping gas oracle: {e}")
        
        try:
            rpc_health.stop()
        except Exception as e:
            print(f"[-] Error stopping RPC health: {e}")
        
        # Give workers up to 10 seconds to finish current operations
        if self._tasks:
            try:
                await asyncio.wait_for(
                    asyncio.gather(*self._tasks, return_exceptions=True),
                    timeout=10.0
                )
                await self._broadcast({"type": "log", "level": "INFO", "worker_id": "SYS",
                                      "message": "All workers completed gracefully.", "timestamp": int(time.time())})
            except asyncio.TimeoutError:
                await self._broadcast({"type": "log", "level": "WARNING", "worker_id": "SYS",
                                      "message": "Shutdown timeout: force-cancelling remaining workers.", 
                                      "timestamp": int(time.time())})
                # Force cancel any still running
                for task in self._tasks:
                    if not task.done():
                        task.cancel()
                
                # Wait a moment for cancellation to propagate
                try:
                    await asyncio.gather(*self._tasks, return_exceptions=True)
                except Exception:
                    pass
        
        self._tasks.clear()
        await self._broadcast({"type": "status", "status": "STOPPED", "timestamp": int(time.time())})


# Singleton
orchestrator = BotOrchestrator()