"""
GasOracle — EIP-1559 aware gas price oracle.

Uses eth_feeHistory to compute optimal maxFeePerGas + maxPriorityFeePerGas.
Maintains a 30-minute rolling history for the dashboard chart.
Enforces GAS_CEILING_GWEI — pauses worker execution if exceeded.
"""

import asyncio
import time
from collections import deque
from web3 import AsyncWeb3, AsyncHTTPProvider

from src.config.settings import ConfigurationManager, NETWORKS


class GasOracle:
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
        self._cfg = ConfigurationManager()
        self._current: dict = {}
        # rolling 30-min history: [(timestamp, gwei_value), ...]
        self._history: deque = deque(maxlen=360)  # 30min @ 5s interval
        self._task: asyncio.Task | None = None
        self._broadcast_cb = None  # set by orchestrator
        self._w3: AsyncWeb3 | None = None

    def set_broadcast_callback(self, cb):
        """Register a coroutine callback to broadcast gas updates."""
        self._broadcast_cb = cb

    def start(self):
        if self._task is None or self._task.done():
            self._task = asyncio.create_task(self._loop())

    def stop(self):
        if self._task:
            self._task.cancel()

    def get_current(self) -> dict:
        return self._current.copy()

    def get_history(self) -> list:
        return list(self._history)

    def is_ceiling_exceeded(self) -> bool:
        gwei = self._current.get("max_fee_gwei", 0)
        return gwei > self._cfg.gas_ceiling_gwei

    async def get_tx_params(self) -> dict:
        """Return EIP-1559 gas params for building a transaction."""
        if self._current:
            return {
                "maxFeePerGas": self._current.get("max_fee_wei", 0),
                "maxPriorityFeePerGas": self._current.get("priority_fee_wei", 0),
            }
        # Fallback: fetch right now
        await self._fetch()
        return {
            "maxFeePerGas": self._current.get("max_fee_wei", 0),
            "maxPriorityFeePerGas": self._current.get("priority_fee_wei", 0),
        }

    async def bump_gas_params(self, original_params: dict, attempt: int = 1) -> dict:
        """Exponentially bump gas for stuck-TX resubmission."""
        factor = min(1.0 + 0.12 * attempt, self._cfg.max_gas_bump_multiplier)
        bumped = {
            "maxFeePerGas": int(original_params.get("maxFeePerGas", 0) * factor),
            "maxPriorityFeePerGas": int(original_params.get("maxPriorityFeePerGas", 0) * factor),
        }
        cap_wei = int(self._cfg.max_gas_limit * 1e9) if self._cfg.max_gas_limit > 0 else 0
        if cap_wei > 0:
            bumped["maxFeePerGas"] = min(bumped["maxFeePerGas"], cap_wei)
            if bumped["maxPriorityFeePerGas"] > bumped["maxFeePerGas"]:
                bumped["maxPriorityFeePerGas"] = bumped["maxFeePerGas"]
        return bumped

    # ------------------------------------------------------------------
    async def _loop(self):
        while True:
            try:
                await self._fetch()
            except asyncio.CancelledError:
                return
            except Exception:
                pass
            await asyncio.sleep(5)

    async def _fetch(self):
        from src.utils.rpc_health import rpc_health
        rpcs = rpc_health.get_rpcs(self._cfg.rpc_ticker)
        
        # Try each healthy RPC until one works
        for url in rpcs:
            self._w3 = AsyncWeb3(AsyncHTTPProvider(url))
            try:
                # EIP-1559 fee history — last 5 blocks, 25th / 75th percentile priority fees
                history = await asyncio.wait_for(self._w3.eth.fee_history(5, "latest", [25, 75]), timeout=4)
                base_fees = history.get("baseFeePerGas", [])
                prio_fees = history.get("reward", [])

                if base_fees:
                    latest_base = base_fees[-1]
                else:
                    latest_base = await asyncio.wait_for(self._w3.eth.gas_price, timeout=4)

                if prio_fees:
                    # User priority fee override or network average
                    if self._cfg.priority_fee_gwei > 0:
                        avg_prio = int(self._cfg.priority_fee_gwei * 1e9)
                    else:
                        avg_prio = sum(row[0] for row in prio_fees) // len(prio_fees)
                else:
                    avg_prio = int(self._cfg.priority_fee_gwei * 1e9)

                # Use configurable base fee multiplier (default 1.25x) instead of hardcoded 2.0x
                max_fee = int(latest_base * self._cfg.gas_base_multiplier) + avg_prio
                cap_wei = int(self._cfg.max_gas_limit * 1e9) if self._cfg.max_gas_limit > 0 else 0
                if cap_wei > 0:
                    max_fee = min(max_fee, cap_wei)
                    if avg_prio > max_fee:
                        avg_prio = max_fee

                self._current = {
                    "base_fee_wei": latest_base,
                    "priority_fee_wei": avg_prio,
                    "max_fee_wei": max_fee,
                    "base_fee_gwei": round(latest_base / 1e9, 2),
                    "max_fee_gwei": round(max_fee / 1e9, 2),
                }

                ts = int(time.perf_counter())
                self._history.append((ts, self._current["max_fee_gwei"]))

                if self._broadcast_cb:
                    try:
                        await self._broadcast_cb({
                            "type": "gas_update",
                            "data": self._current,
                            "ts": ts,
                        })
                    except Exception:
                        pass
                
                # If we got here, success!
                return

            except Exception:
                # Fallback to legacy gas price if EIP-1559 not supported or this provider fails
                try:
                    price = await asyncio.wait_for(self._w3.eth.gas_price, timeout=4)
                    cap_wei = int(self._cfg.max_gas_limit * 1e9) if self._cfg.max_gas_limit > 0 else 0
                    if cap_wei > 0:
                        price = min(price, cap_wei)
                    gwei = round(price / 1e9, 2)
                    self._current = {
                        "base_fee_wei": price,
                        "priority_fee_wei": 0,
                        "max_fee_wei": price,
                        "base_fee_gwei": gwei,
                        "max_fee_gwei": gwei,
                    }
                    ts = int(time.perf_counter())
                    self._history.append((ts, gwei))
                    
                    if self._broadcast_cb:
                        await self._broadcast_cb({"type": "gas_update", "data": self._current, "ts": ts})
                    return # Success
                except Exception:
                    continue # Try next RPC


# Singleton
gas_oracle = GasOracle()
