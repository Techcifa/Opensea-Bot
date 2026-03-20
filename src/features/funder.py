"""
MassFunder — auto-tops up worker wallets from a master wallet before minting.
"""

import asyncio
from web3 import AsyncWeb3, AsyncHTTPProvider
from eth_account import Account

from src.config.settings import ConfigurationManager, NETWORKS
from src.utils.rpc_health import rpc_health


class MassFunder:
    def __init__(self):
        self._cfg = ConfigurationManager()
        rpcs = rpc_health.get_rpcs(self._cfg.rpc_ticker)
        net_info = NETWORKS.get(self._cfg.rpc_ticker, {})
        self._w3 = AsyncWeb3(AsyncHTTPProvider(rpcs[0]))
        self._symbol = net_info.get("symbol", "ETH")

        if self._cfg.fund_enabled and self._cfg.master_pk:
            self._master = Account.from_key(self._cfg.master_pk)
        else:
            self._master = None

    async def check_and_fund(self, worker_pks: list[str], broadcast=None) -> list[str]:
        """
        Top up any worker wallet whose ETH balance is below cfg.min_worker_balance.
        Returns list of log messages.
        """
        if not self._master:
            return ["[Funder] Auto-Fund disabled or Master PK missing."]

        logs = []
        master_nonce = await self._w3.eth.get_transaction_count(self._master.address)
        chain_id = await self._w3.eth.chain_id

        # Get EIP-1559 gas if possible, fallback to legacy
        try:
            fee_history = await self._w3.eth.fee_history(1, "latest", [50])
            base_fee = fee_history["baseFeePerGas"][-1]
            priority = fee_history["reward"][0][0] if fee_history.get("reward") else int(1e9)
            gas_price = int(base_fee * 1.5) + priority
            gas_params = {"maxFeePerGas": gas_price, "maxPriorityFeePerGas": priority, "type": 2}
        except Exception:
            gas_price = int((await self._w3.eth.gas_price) * 1.1)
            gas_params = {"gasPrice": gas_price}

        for i, pk in enumerate(worker_pks):
            worker = Account.from_key(pk)
            wid = i + 1

            bal_wei = await self._w3.eth.get_balance(worker.address)
            bal_eth = bal_wei / 1e18

            msg = f"[Funder] Worker #{wid} — Bal: {bal_eth:.5f} {self._symbol}"
            logs.append(msg)
            if broadcast:
                await broadcast({"type": "log", "level": "INFO", "worker_id": "SYS", "message": msg})

            if bal_eth < self._cfg.min_worker_balance:
                amount_wei = int(self._cfg.funding_amount * 1e18)
                tx = {
                    "nonce": master_nonce,
                    "to": worker.address,
                    "value": amount_wei,
                    "gas": 21000,
                    "chainId": chain_id,
                    **gas_params,
                }
                try:
                    signed = self._master.sign_transaction(tx)
                    tx_hash = await self._w3.eth.send_raw_transaction(signed.raw_transaction)
                    ok_msg = f"[Funder] Funded Worker #{wid} → {self._cfg.funding_amount} {self._symbol} | TX: {tx_hash.hex()[:12]}..."
                    logs.append(ok_msg)
                    if broadcast:
                        await broadcast({"type": "log", "level": "SUCCESS", "worker_id": "SYS", "message": ok_msg})
                    await asyncio.sleep(2)
                    master_nonce += 1
                except Exception as e:
                    err_msg = f"[Funder] Failed to fund Worker #{wid}: {e}"
                    logs.append(err_msg)
                    if broadcast:
                        await broadcast({"type": "log", "level": "ERROR", "worker_id": "SYS", "message": err_msg})

        return logs
