"""
Accountant — logs successful transactions to history.csv for PnL tracking.
"""

import csv
import os
from datetime import datetime


class Accountant:
    FILE = "history.csv"
    HEADERS = ["timestamp", "network", "wallet", "event", "tx_hash", "gas_used", "gas_price_gwei", "eth_spent"]

    @classmethod
    def _ensure_file(cls):
        if not os.path.exists(cls.FILE):
            with open(cls.FILE, "w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(cls.HEADERS)

    @classmethod
    def log_transaction(cls, network: str, wallet: str, event: str, tx_hash: str, receipt, total_value_wei: int):
        cls._ensure_file()
        gas_used = receipt.gasUsed if receipt else 0
        # gas price is not directly in receipt for EIP-1559 — use effectiveGasPrice if available
        effective_price = getattr(receipt, "effectiveGasPrice", 0)
        gas_cost_wei = gas_used * effective_price
        total_eth = (gas_cost_wei + total_value_wei) / 1e18

        with open(cls.FILE, "a", newline="") as f:
            writer = csv.writer(f)
            writer.writerow([
                datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                network,
                wallet,
                event,
                tx_hash,
                gas_used,
                round(effective_price / 1e9, 4),
                round(total_eth, 8),
            ])

    @classmethod
    def read_history(cls) -> list[dict]:
        cls._ensure_file()
        rows = []
        with open(cls.FILE, newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                rows.append(dict(row))
        return rows
