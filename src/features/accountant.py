"""
Accountant — logs successful transactions to history.csv for PnL tracking.

FIX M-03: Added asyncio.Lock() to prevent concurrent writes from multiple
           workers corrupting history.csv (interleaved rows / PermissionError).
"""

import csv
import asyncio
import os
import logging
from datetime import datetime

log = logging.getLogger(__name__)


class Accountant:
    FILE    = "history.csv"
    HEADERS = ["timestamp", "network", "wallet", "event", "tx_hash", "gas_used", "gas_price_gwei", "eth_spent"]

    # Module-level asyncio Lock shared by all concurrent callers
    _write_lock: asyncio.Lock | None = None

    @classmethod
    def _get_lock(cls) -> asyncio.Lock:
        """Lazily create the lock inside the running event loop."""
        if cls._write_lock is None:
            cls._write_lock = asyncio.Lock()
        return cls._write_lock

    @classmethod
    def _ensure_file(cls):
        if not os.path.exists(cls.FILE):
            try:
                with open(cls.FILE, "w", newline="") as f:
                    writer = csv.writer(f)
                    writer.writerow(cls.HEADERS)
            except Exception as exc:
                log.error("Accountant: could not create %s: %s", cls.FILE, exc)

    @classmethod
    async def log_transaction(
        cls,
        network: str,
        wallet: str,
        event: str,
        tx_hash: str,
        receipt,
        total_value_wei: int,
    ):
        """FIX M-03: Async-safe write via asyncio.Lock."""
        cls._ensure_file()
        gas_used       = receipt.gasUsed if receipt else 0
        effective_price = getattr(receipt, "effectiveGasPrice", 0)
        gas_cost_wei   = gas_used * effective_price
        total_eth      = (gas_cost_wei + total_value_wei) / 1e18

        row = [
            datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
            network,
            wallet,
            event,
            tx_hash,
            gas_used,
            round(effective_price / 1e9, 4),
            round(total_eth, 8),
        ]

        async with cls._get_lock():
            try:
                with open(cls.FILE, "a", newline="") as f:
                    csv.writer(f).writerow(row)
            except Exception as exc:
                log.error("Accountant: write failed: %s", exc)

    @classmethod
    def read_history(cls) -> list[dict]:
        cls._ensure_file()
        rows = []
        try:
            with open(cls.FILE, newline="") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    rows.append(dict(row))
        except Exception as exc:
            log.error("Accountant: read failed: %s", exc)
        return rows
