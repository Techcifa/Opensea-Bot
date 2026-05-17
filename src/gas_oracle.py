"""
GasOracle — Accurate EIP-1559 gas price oracle.

Algorithm:
  1. Sample last 10 blocks via eth_feeHistory with [10, 50, 90] percentile rewards.
  2. Predict next-block base fee using the EIP-1559 adjustment formula
     (based on actual block fullness from gasUsedRatio).
  3. Cross-reference with eth_maxPriorityFeePerGas (node's own suggestion).
  4. Expose Slow / Standard / Fast speed tiers for dashboard + execution engine.

Maintains a 30-minute rolling history for the dashboard chart.
Enforces GAS_CEILING_GWEI — pauses workers if gas exceeds ceiling.
Automatically cycles through healthy RPC nodes on failure.
"""

import asyncio
import time
import logging
from collections import deque
from web3 import AsyncWeb3, AsyncHTTPProvider

from src.config.settings import ConfigurationManager, NETWORKS
from src.utils.rpc_health import rpc_health

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
        # rolling 30-min history: [(timestamp, gwei_value), ...]
        self._history: deque = deque(maxlen=360)  # 30 min @ 5s interval
        self._task: asyncio.Task | None = None
        self._broadcast_cb = None
        self._w3: AsyncWeb3 | None = None
        self._active_rpc: str = ""

    # ------------------------------------------------------------------
    # RPC resilience
    # ------------------------------------------------------------------
    async def _ensure_w3(self) -> bool:
        """Pick the first responsive RPC from the healthy pool."""
        candidates = rpc_health.get_rpcs(self._cfg.rpc_ticker)
        for url in candidates:
            if url == self._active_rpc and self._w3 is not None:
                return True  # already on this node
            try:
                if url.startswith("wss://") or url.startswith("ws://"):
                    temp_w3 = AsyncWeb3(AsyncWeb3.WebSocketProvider(url))
                else:
                    temp_w3 = AsyncWeb3(AsyncHTTPProvider(url))
                await asyncio.wait_for(temp_w3.eth.block_number, timeout=4)
                self._w3 = temp_w3
                self._active_rpc = url
                return True
            except Exception:
                continue
        return False  # all nodes failed

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def set_broadcast_callback(self, cb):
        """Register a coroutine callback to broadcast gas updates."""
        self._broadcast_cb = cb

    def reload_config(self):
        """Refresh cached config/RPC after dashboard or env overrides."""
        self._cfg = ConfigurationManager()
        self._w3 = None
        self._active_rpc = ""
        self._current = {}

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

    async def get_tx_params(self, speed: str = "fast") -> dict:
        """Return EIP-1559 gas params for building a transaction.

        Args:
            speed: "slow" | "standard" | "fast"  (default: fast for competitive mints)
        """
        if not self._current:
            await self._fetch()

        tiers = self._current.get("tiers", {})
        tier = tiers.get(speed) or tiers.get("fast", {})

        if tier:
            return {
                "maxFeePerGas":         tier["max_fee_wei"],
                "maxPriorityFeePerGas": tier["priority_wei"],
            }

        # Final safety fallback
        return {
            "maxFeePerGas":         self._current.get("max_fee_wei", 0),
            "maxPriorityFeePerGas": self._current.get("priority_fee_wei", 0),
        }

    async def bump_gas_params(self, original_params: dict, attempt: int = 1) -> dict:
        """Exponentially bump gas for stuck-TX resubmission (12 % per attempt, capped)."""
        factor = min(1.0 + 0.12 * attempt, self._cfg.max_gas_bump_multiplier)
        return {
            "maxFeePerGas":         int(original_params.get("maxFeePerGas", 0)         * factor),
            "maxPriorityFeePerGas": int(original_params.get("maxPriorityFeePerGas", 0) * factor),
        }
        cap_wei = int(cfg.max_gas_limit * 1e9) if cfg.max_gas_limit > 0 else 0
        if cap_wei > 0:
            bumped["maxFeePerGas"] = min(bumped["maxFeePerGas"], cap_wei)
            if bumped["maxPriorityFeePerGas"] > bumped["maxFeePerGas"]:
                bumped["maxPriorityFeePerGas"] = bumped["maxFeePerGas"]
        return bumped

    # ------------------------------------------------------------------
    # Internal loop
    # ------------------------------------------------------------------
    async def _loop(self):
        await self._ensure_w3()  # bootstrap before first fetch
        while True:
            try:
                await self._fetch()
            except asyncio.CancelledError:
                return
            except Exception:
                self._active_rpc = ""  # mark dead, rotate on next iteration
                await self._ensure_w3()
            await asyncio.sleep(5)

    # ------------------------------------------------------------------
    # Core fetch — accurate EIP-1559 multi-tier estimation
    # ------------------------------------------------------------------
    async def _fetch(self):
        if self._w3 is None:
            if not await self._ensure_w3():
                return  # no healthy nodes this cycle

        try:
            # ----------------------------------------------------------------
            # Step 1 — Sample last 10 blocks.
            # Percentile rewards: [10, 50, 90] → slow / standard / fast tips.
            # ----------------------------------------------------------------
            BLOCKS = 10
            history = await self._w3.eth.fee_history(BLOCKS, "latest", [10, 50, 90])

            base_fees    = history.get("baseFeePerGas", [])   # len = BLOCKS + 1
            prio_rows    = history.get("reward", [])           # len = BLOCKS
            usage_ratios = history.get("gasUsedRatio", [])     # len = BLOCKS

            if not base_fees:
                raise ValueError("Empty baseFeePerGas — node may not support EIP-1559")

            # ----------------------------------------------------------------
            # Step 2 — Predict next-block base fee using EIP-1559 formula.
            #   nextBase = currentBase × (1 + 0.125 × (blockFullness − 0.5) / 0.5)
            #   • Block 100 % full → base fee +12.5 %
            #   • Block 50 % full  → base fee unchanged
            #   • Block 0 % full   → base fee −12.5 %
            # ----------------------------------------------------------------
            current_base = base_fees[-1]

            if usage_ratios:
                last_ratio = max(0.0, min(1.0, usage_ratios[-1]))   # clamp to [0, 1]
                delta      = 0.125 * (last_ratio - 0.5) / 0.5
                next_base  = int(current_base * (1.0 + delta))
            else:
                next_base  = int(current_base * 1.10)  # conservative +10 % fallback

            # Base fee can drop at most 12.5 % per block
            next_base = max(next_base, int(current_base * 0.875))

            # ----------------------------------------------------------------
            # Step 3 — Derive priority tips from percentile histograms.
            #   Use the median of each percentile column across all sampled blocks
            #   (median is robust against outlier blocks from gas spikes).
            # ----------------------------------------------------------------
            def _median(vals: list) -> int:
                if not vals:
                    return int(1e9)
                s   = sorted(vals)
                mid = len(s) // 2
                return s[mid] if len(s) % 2 else (s[mid - 1] + s[mid]) // 2

            slow_tips     = [int(row[0]) for row in prio_rows if row]
            standard_tips = [int(row[1]) for row in prio_rows if row]
            fast_tips     = [int(row[2]) for row in prio_rows if row]

            slow_prio     = _median(slow_tips)     or int(1.0e9)
            standard_prio = _median(standard_tips) or int(1.5e9)
            fast_prio     = _median(fast_tips)     or int(2.0e9)

            # Cross-reference with eth_maxPriorityFeePerGas (node's own suggestion).
            # This accounts for mempool pressure not visible in historical blocks.
            try:
                node_prio     = int(await asyncio.wait_for(self._w3.eth.max_priority_fee, timeout=3))
                fast_prio     = max(fast_prio, node_prio)  # never bid less than node suggests
                standard_prio = max(standard_prio, slow_prio)
                fast_prio     = max(fast_prio, standard_prio)
            except Exception:
                pass  # node doesn't support eth_maxPriorityFeePerGas — use histogram only

            # ----------------------------------------------------------------
            # Step 4 — Build maxFeePerGas per tier.
            #   maxFee = (nextBase × 1.10) + priorityTip
            #   The 10 % buffer on top of predicted nextBase ensures the TX
            #   is valid even if base fee rises slightly before inclusion.
            # ----------------------------------------------------------------
            buffered_base = int(next_base * 1.10)

            slow_max     = buffered_base + slow_prio
            standard_max = buffered_base + standard_prio
            fast_max     = buffered_base + fast_prio

            # ----------------------------------------------------------------
            # Step 5 — Persist results. Fast tier is the default for execution.
            # ----------------------------------------------------------------
            block_usage = round((usage_ratios[-1] if usage_ratios else 0.5) * 100, 1)

            self._current = {
                # Default values (fast tier) — backward-compatible with execution.py
                "max_fee_wei":      fast_max,
                "priority_fee_wei": fast_prio,

                # Human-readable Gwei values shown on dashboard
                "base_fee_gwei":   round(current_base / 1e9, 3),
                "next_base_gwei":  round(next_base    / 1e9, 3),
                "max_fee_gwei":    round(fast_max     / 1e9, 3),
                "block_usage_pct": block_usage,

                # Speed tiers — available to dashboard and get_tx_params(speed=...)
                "tiers": {
                    "slow": {
                        "max_fee_gwei":  round(slow_max  / 1e9, 3),
                        "priority_gwei": round(slow_prio / 1e9, 3),
                        "max_fee_wei":   slow_max,
                        "priority_wei":  slow_prio,
                    },
                    "standard": {
                        "max_fee_gwei":  round(standard_max  / 1e9, 3),
                        "priority_gwei": round(standard_prio / 1e9, 3),
                        "max_fee_wei":   standard_max,
                        "priority_wei":  standard_prio,
                    },
                    "fast": {
                        "max_fee_gwei":  round(fast_max  / 1e9, 3),
                        "priority_gwei": round(fast_prio / 1e9, 3),
                        "max_fee_wei":   fast_max,
                        "priority_wei":  fast_prio,
                    },
                },
            }

            ts = int(time.time())
            self._history.append((ts, self._current["max_fee_gwei"]))

            if self._broadcast_cb:
                try:
                    await self._broadcast_cb({
                        "type": "gas_update",
                        "data": self._current,
                        "ts":   ts,
                    })
                except Exception:
                    pass

        except Exception:
            # Fallback: legacy gasPrice for non-EIP-1559 chains (BSC, old testnets)
            try:
                price = await self._w3.eth.gas_price
                gwei  = round(price / 1e9, 3)
                flat_tier = {
                    "max_fee_gwei":  gwei,
                    "priority_gwei": 0.0,
                    "max_fee_wei":   price,
                    "priority_wei":  0,
                }
                self._current = {
                    "max_fee_wei":      price,
                    "priority_fee_wei": 0,
                    "base_fee_gwei":    gwei,
                    "next_base_gwei":   gwei,
                    "max_fee_gwei":     gwei,
                    "block_usage_pct":  0,
                    "tiers": {
                        "slow":     flat_tier,
                        "standard": flat_tier,
                        "fast":     flat_tier,
                    },
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
