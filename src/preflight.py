"""
PreflightChecker — validates config and wallet readiness before starting.
"""

import asyncio
from web3 import AsyncWeb3, AsyncHTTPProvider

from src.config.settings import ConfigurationManager, NETWORKS, ContractSpecs
from src.utils.rpc_health import rpc_health


class PreflightChecker:
    def __init__(self, keys: list[str]):
        self._cfg = ConfigurationManager()
        self._keys = keys

        rpcs = rpc_health.get_rpcs(self._cfg.rpc_ticker)
        self._w3 = AsyncWeb3(AsyncHTTPProvider(rpcs[0]))
        net_info = NETWORKS.get(self._cfg.rpc_ticker, {})
        self._symbol = net_info.get("symbol", "ETH")
        self._explorer = net_info.get("explorer", "https://etherscan.io")

    async def run(self) -> list[dict]:
        """Returns a list of check results: {label, status, detail}"""
        checks = []
        checks.append(await self._check_network())
        checks.append(await self._check_contract())
        checks.append(await self._check_rpc())
        wallet_checks = await self._check_wallets()
        checks.extend(wallet_checks)
        return checks

    async def _check_network(self) -> dict:
        try:
            chain_id = await asyncio.wait_for(self._w3.eth.chain_id, timeout=5)
            expected = NETWORKS.get(self._cfg.rpc_ticker, {}).get("id")
            if expected and chain_id != expected:
                return {"label": "Network", "status": "warn",
                        "detail": f"Chain ID mismatch: expected {expected}, got {chain_id}"}
            return {"label": "Network", "status": "ok",
                    "detail": f"{self._cfg.rpc_ticker} (Chain ID: {chain_id})"}
        except Exception as e:
            return {"label": "Network", "status": "error", "detail": str(e)}

    async def _check_contract(self) -> dict:
        if not self._cfg.target_nft:
            return {"label": "NFT Contract", "status": "error", "detail": "No contract address set"}
        try:
            code = await asyncio.wait_for(
                self._w3.eth.get_code(AsyncWeb3.to_checksum_address(self._cfg.target_nft)),
                timeout=5
            )
            if code and code != b"":
                return {"label": "NFT Contract", "status": "ok",
                        "detail": f"Contract found on-chain ({self._cfg.target_nft[:10]}...)"}
            return {"label": "NFT Contract", "status": "error",
                    "detail": "Address exists but has no bytecode (EOA or wrong network)"}
        except Exception as e:
            return {"label": "NFT Contract", "status": "error", "detail": str(e)}

    async def _check_rpc(self) -> dict:
        rpcs = rpc_health.get_rpcs(self._cfg.rpc_ticker)
        if rpcs:
            return {"label": "RPC Health", "status": "ok",
                    "detail": f"{len(rpcs)} healthy node(s) available"}
        return {"label": "RPC Health", "status": "error", "detail": "No healthy RPC nodes"}

    async def _check_wallets(self) -> list[dict]:
        from eth_account import Account
        results = []

        # Estimate mint cost
        try:
            sea_contract = self._w3.eth.contract(
                address=AsyncWeb3.to_checksum_address(self._cfg.sea_addr),
                abi=ContractSpecs.SEA_ABI,
            )
            drop_data = await sea_contract.functions.getPublicDrop(
                AsyncWeb3.to_checksum_address(self._cfg.target_nft)
            ).call()
            mint_price_wei = int(drop_data[0])
        except Exception:
            mint_price_wei = 0

        # Rough gas estimate (300k gas @ 20 gwei)
        gas_estimate_wei = 300_000 * int(20e9)
        min_required_wei = mint_price_wei * self._cfg.qty + gas_estimate_wei

        for i, pk in enumerate(self._keys):
            wid = i + 1
            try:
                acct = Account.from_key(pk)
                bal_wei = await self._w3.eth.get_balance(acct.address)
                bal_eth = bal_wei / 1e18
                min_eth = min_required_wei / 1e18

                if bal_wei >= min_required_wei:
                    status = "ok"
                    detail = f"Balance: {bal_eth:.5f} {self._symbol} (need ≥ {min_eth:.5f})"
                else:
                    status = "warn"
                    detail = f"Low balance: {bal_eth:.5f} {self._symbol} (need ≥ {min_eth:.5f})"

                results.append({
                    "label": f"Wallet #{wid}",
                    "status": status,
                    "detail": detail,
                    "address": acct.address,
                    "explorer": f"{self._explorer}/address/{acct.address}",
                })
            except Exception as e:
                results.append({
                    "label": f"Wallet #{wid}",
                    "status": "error",
                    "detail": f"Invalid key: {str(e)[:40]}",
                })

        return results
