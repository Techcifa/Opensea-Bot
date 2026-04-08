"""
GasOracle — EIP-1559 aware gas price oracle.

FIXES:
  C-02  time.perf_counter() → time.time() so history timestamps are real Unix epoch
  H-06  Dynamic config read so ceiling/multipliers pick up runtime overrides
"""

import asyncio
import time
import logging
from collections import deque
from web3 import AsyncWeb3, AsyncHTTPProvider

from src.config.settings import ConfigurationManager, NETWORKS

log = logging.getLogger(__name__)


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
        self._current: dict = {}
        # rolling 30-min history: [(unix_timestamp_int, gwei_value), ...]
        self._history: deque = deque(maxlen=360)   # 30 min @ 5-s intervals
        self._task: asyncio.Task | None = None
        self._broadcast_cb = None                  # set by orchestrator
        self._w3: AsyncWeb3 | None = None

    # ------------------------------------------------------------------
    # H-06: Always read fresh config so runtime overrides take effect
    # ------------------------------------------------------------------
    @property
    def _cfg(self) -> ConfigurationManager:
        return ConfigurationManager()

    # ------------------------------------------------------------------
    def set_broadcast_callback(self, cb):
        """Register a coroutine callback to broadcast gas updates."""
        self._broadcast_cb = cb

    def start(self):
        if self._task is None or self._task.done():
            self._task = asyncio.create_task(self._loop())

    def stop(self):
        if self._task:
            self._task.cancel()
            self._task = None

    def get_current(self) -> dict:
        return self._current.copy()

    def get_history(self) -> list:
        return list(self._history)

    def is_ceiling_exceeded(self) -> bool:
        """FIX H-06: always reads current cfg so overrides are respected."""
        gwei = self._current.get("max_fee_gwei", 0)
        return gwei > self._cfg.gas_ceiling_gwei

    async def get_tx_params(self) -> dict:
        """Return EIP-1559 gas params for building a transaction."""
        if self._current:
            return {
                "maxFeePerGas":         self._current.get("max_fee_wei", 0),
                "maxPriorityFeePerGas": self._current.get("priority_fee_wei", 0),
            }
        # Fallback: fetch right now
        await self._fetch()
        return {
            "maxFeePerGas":         self._current.get("max_fee_wei", 0),
            "maxPriorityFeePerGas": self._current.get("priority_fee_wei", 0),
        }

    async def bump_gas_params(self, original_params: dict, attempt: int = 1) -> dict:
        """Exponentially bump gas for stuck-TX resubmission."""
        cfg = self._cfg
        factor = min(1.0 + 0.12 * attempt, cfg.max_gas_bump_multiplier)
        bumped = {
            "maxFeePerGas":         int(original_params.get("maxFeePerGas", 0) * factor),
            "maxPriorityFeePerGas": int(original_params.get("maxPriorityFeePerGas", 0) * factor),
        }
        cap_wei = int(cfg.max_gas_limit * 1e9) if cfg.max_gas_limit > 0 else 0
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
            except Exception as exc:
                log.debug("GasOracle fetch error: %s", exc)
            await asyncio.sleep(5)

    async def _fetch(self):
        from src.utils.rpc_health import rpc_health
        cfg = self._cfg           # FIX H-06: fresh each call
        rpcs = rpc_health.get_rpcs(cfg.rpc_ticker)

        for url in rpcs:
            self._w3 = AsyncWeb3(AsyncHTTPProvider(url))
            try:
                history = await asyncio.wait_for(
                    self._w3.eth.fee_history(5, "latest", [25, 75]), timeout=4
                )
                base_fees = history.get("baseFeePerGas", [])
                prio_fees = history.get("reward", [])

                latest_base = base_fees[-1] if base_fees else await asyncio.wait_for(
                    self._w3.eth.gas_price, timeout=4
                )

                if prio_fees:
                    avg_prio = (
                        int(cfg.priority_fee_gwei * 1e9)
                        if cfg.priority_fee_gwei > 0
                        else sum(row[0] for row in prio_fees) // len(prio_fees)
                    )
                else:
                    avg_prio = int(cfg.priority_fee_gwei * 1e9)

                max_fee = int(latest_base * cfg.gas_base_multiplier) + avg_prio
                cap_wei = int(cfg.max_gas_limit * 1e9) if cfg.max_gas_limit > 0 else 0
                if cap_wei > 0:
                    max_fee = min(max_fee, cap_wei)
                    if avg_prio > max_fee:
                        avg_prio = max_fee

                self._current = {
                    "base_fee_wei":  latest_base,
                    "priority_fee_wei": avg_prio,
                    "max_fee_wei":   max_fee,
                    "base_fee_gwei": round(latest_base / 1e9, 2),
                    "max_fee_gwei":  round(max_fee / 1e9, 2),
                }

                # FIX C-02: use time.time() (Unix epoch) not time.perf_counter()
                ts = int(time.time())
                self._history.append((ts, self._current["max_fee_gwei"]))

                if self._broadcast_cb:
                    try:
                        await self._broadcast_cb({"type": "gas_update", "data": self._current, "ts": ts})
                    except Exception:
                        pass

                return   # success

            except Exception:
                # Fallback to legacy gas price
                try:
                    price = await asyncio.wait_for(self._w3.eth.gas_price, timeout=4)
                    cap_wei = int(cfg.max_gas_limit * 1e9) if cfg.max_gas_limit > 0 else 0
                    if cap_wei > 0:
                        price = min(price, cap_wei)
                    gwei = round(price / 1e9, 2)
                    self._current = {
                        "base_fee_wei": price, "priority_fee_wei": 0,
                        "max_fee_wei": price, "base_fee_gwei": gwei, "max_fee_gwei": gwei,
                    }
                    # FIX C-02: Unix timestamp
                    ts = int(time.time())
                    self._history.append((ts, gwei))
                    if self._broadcast_cb:
                        await self._broadcast_cb({"type": "gas_update", "data": self._current, "ts": ts})
                    return
                except Exception:
                    continue   # try next RPC


# Singleton
gas_oracle = GasOracle()
