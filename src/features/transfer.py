"""
AssetRelay — transfers minted NFTs to a cold wallet (consolidation)
and sweeps residual ETH back to the master wallet (dust sweeper).
"""

import asyncio
from web3 import AsyncWeb3
from eth_account import Account

from src.config.settings import ContractSpecs


class AssetRelay:
    def __init__(self, w3: AsyncWeb3, acct: Account, worker_id: int, recipient: str):
        self._w3 = w3
        self._acct = acct
        self._uid = worker_id
        self._recipient = recipient

    async def execute_consolidation(self, nft_address: str, receipt) -> list[str]:
        """Transfer all NFTs minted in `receipt` to the cold wallet."""
        logs = []
        addr_c = AsyncWeb3.to_checksum_address(nft_address)
        recip_c = AsyncWeb3.to_checksum_address(self._recipient)
        nft = self._w3.eth.contract(address=addr_c, abi=ContractSpecs.ERC721_ABI)

        # Find Transfer events in the receipt to get token IDs
        try:
            transfer_topic = self._w3.keccak(text="Transfer(address,address,uint256)").hex()
            token_ids = []
            for log in receipt.logs:
                if log.topics and log.topics[0].hex() == transfer_topic:
                    token_id = int(log.topics[3].hex(), 16)
                    token_ids.append(token_id)
        except Exception:
            logs.append(f"[Transfer] Could not parse token IDs from receipt.")
            return logs

        chain_id = await self._w3.eth.chain_id
        nonce = await self._w3.eth.get_transaction_count(self._acct.address)

        for token_id in token_ids:
            try:
                gas_price = await self._w3.eth.gas_price
                tx = await nft.functions.safeTransferFrom(
                    self._acct.address, recip_c, token_id
                ).build_transaction({
                    "chainId": chain_id,
                    "from": self._acct.address,
                    "gasPrice": int(gas_price * 1.1),
                    "nonce": nonce,
                })
                tx["gas"] = await self._w3.eth.estimate_gas(tx)
                signed = self._acct.sign_transaction(tx)
                tx_hash = await self._w3.eth.send_raw_transaction(signed.raw_transaction)
                logs.append(f"[Transfer] Token #{token_id} → {recip_c[:8]}... | TX: {tx_hash.hex()[:12]}...")
                nonce += 1
                await asyncio.sleep(1)
            except Exception as e:
                logs.append(f"[Transfer] Failed token #{token_id}: {e}")

        return logs

    async def sweep_native_token(self, min_balance_eth: float) -> str | None:
        """Return residual ETH (gas change) to master / recipient wallet."""
        bal_wei = await self._w3.eth.get_balance(self._acct.address)
        bal_eth = bal_wei / 1e18
        if bal_eth < min_balance_eth:
            return None

        chain_id = await self._w3.eth.chain_id
        gas_price = await self._w3.eth.gas_price
        gas_cost = 21000 * int(gas_price * 1.1)
        send_wei = bal_wei - gas_cost
        if send_wei <= 0:
            return None

        nonce = await self._w3.eth.get_transaction_count(self._acct.address)
        recip_c = AsyncWeb3.to_checksum_address(self._recipient)
        tx = {
            "chainId": chain_id,
            "to": recip_c,
            "value": send_wei,
            "gas": 21000,
            "gasPrice": int(gas_price * 1.1),
            "nonce": nonce,
        }
        signed = self._acct.sign_transaction(tx)
        tx_hash = await self._w3.eth.send_raw_transaction(signed.raw_transaction)
        return tx_hash.hex()
