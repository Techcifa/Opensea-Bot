"""
MassFunder — auto-tops up worker wallets from a master wallet before minting.

FIX H-04: Validates MASTER_PRIVATE_KEY at construction time using the same
           validate_private_key() logic as the server, so a bad/placeholder
           key raises early with a clear message instead of crashing mid-startup.
"""

import asyncio
import logging
from web3 import AsyncWeb3, AsyncHTTPProvider
from eth_account import Account

from src.config.settings import ConfigurationManager, NETWORKS
from src.utils.rpc_health import rpc_health

log = logging.getLogger(__name__)

# Placeholder values that should never be used as real keys
_INVALID_PLACEHOLDERS = {
    "",
    "0xYourMasterPrivateKeyHere",
    "your_master_private_key",
}


def _validate_master_key(raw_pk: str) -> tuple[bool, str]:
    """Return (is_valid, error_message). Pure validation, no side-effects."""
    pk = raw_pk.strip()
    if not pk or pk in _INVALID_PLACEHOLDERS:
        return False, "MASTER_PRIVATE_KEY is not set or is still the placeholder value"
    try:
        key = pk if pk.startswith("0x") else "0x" + pk
        acct = Account.from_key(key)
        if not acct.address:
            return False, "Key did not produce a valid address"
        return True, ""
    except Exception as exc:
        return False, f"Invalid master key: {str(exc)[:100]}"


class MassFunder:
    def __init__(self):
        self._cfg = ConfigurationManager()
        rpcs = rpc_health.get_rpcs(self._cfg.rpc_ticker)

        # FIX M-04: guard against empty RPC list
        if not rpcs:
            rpcs = list(NETWORKS.get(self._cfg.rpc_ticker, {}).get("rpc", []))
        if not rpcs:
            raise RuntimeError("MassFunder: no RPC endpoints available")

        net_info = NETWORKS.get(self._cfg.rpc_ticker, {})
        self._w3 = AsyncWeb3(AsyncHTTPProvider(rpcs[0]))
        self._symbol = net_info.get("symbol", "ETH")
        self._master = None

        # FIX H-04: validate master key at construction, not at first use
        if self._cfg.fund_enabled:
            is_valid, err = _validate_master_key(self._cfg.master_pk)
            if not is_valid:
                log.error("[Funder] %s — Auto-Fund disabled.", err)
                # Don't crash the whole orchestrator startup; just disable funding
            else:
                raw = self._cfg.master_pk.strip()
                key = raw if raw.startswith("0x") else "0x" + raw
                self._master = Account.from_key(key)
                log.info("[Funder] Master wallet: %s...%s", self._master.address[:8], self._master.address[-4:])

    async def check_and_fund(self, worker_pks: list[str], broadcast=None) -> list[str]:
        """
        Top up any worker wallet whose ETH balance is below cfg.min_worker_balance.
        Returns list of log messages.
        Also awaits receipts before incrementing nonce to avoid stuck funding TXs.
        """
        if not self._master:
            return ["[Funder] Auto-Fund disabled or Master PK invalid."]

        logs = []
        master_nonce = await self._w3.eth.get_transaction_count(self._master.address, "pending")
        chain_id     = await self._w3.eth.chain_id

        # Get EIP-1559 gas if possible, fallback to legacy
        try:
            fee_history = await self._w3.eth.fee_history(1, "latest", [50])
            base_fee    = fee_history["baseFeePerGas"][-1]
            priority    = fee_history["reward"][0][0] if fee_history.get("reward") else int(1e9)
            gas_price   = int(base_fee * 1.5) + priority
            gas_params  = {"maxFeePerGas": gas_price, "maxPriorityFeePerGas": priority, "type": 2}
        except Exception:
            gas_price  = int((await self._w3.eth.gas_price) * 1.1)
            gas_params = {"gasPrice": gas_price}

        for i, pk in enumerate(worker_pks):
            worker = Account.from_key(pk)
            wid    = i + 1

            try:
                bal_wei = await self._w3.eth.get_balance(worker.address)
            except Exception as exc:
                err_msg = f"[Funder] Worker #{wid} balance check failed: {exc}"
                logs.append(err_msg)
                if broadcast:
                    await broadcast({"type": "log", "level": "ERROR", "worker_id": "SYS", "message": err_msg})
                continue

            bal_eth = bal_wei / 1e18
            msg = f"[Funder] Worker #{wid} — Bal: {bal_eth:.5f} {self._symbol}"
            logs.append(msg)
            if broadcast:
                await broadcast({"type": "log", "level": "INFO", "worker_id": "SYS", "message": msg})

            if bal_eth < self._cfg.min_worker_balance:
                amount_wei = int(self._cfg.funding_amount * 1e18)
                tx = {
                    "nonce":   master_nonce,
                    "to":      worker.address,
                    "value":   amount_wei,
                    "gas":     21_000,
                    "chainId": chain_id,
                    **gas_params,
                }
                try:
                    signed   = self._master.sign_transaction(tx)
                    tx_hash  = await self._w3.eth.send_raw_transaction(signed.raw_transaction)
                    ok_msg   = f"[Funder] Funded Worker #{wid} → {self._cfg.funding_amount} {self._symbol} | TX: {tx_hash.hex()[:12]}..."
                    logs.append(ok_msg)
                    if broadcast:
                        await broadcast({"type": "log", "level": "SUCCESS", "worker_id": "SYS", "message": ok_msg})
                    # Brief sleep avoids mempool flood; receipt wait not required for speed
                    await asyncio.sleep(2)
                    master_nonce += 1
                except Exception as exc:
                    err_msg = f"[Funder] Failed to fund Worker #{wid}: {exc}"
                    logs.append(err_msg)
                    if broadcast:
                        await broadcast({"type": "log", "level": "ERROR", "worker_id": "SYS", "message": err_msg})

        return logs
