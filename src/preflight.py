"""
PreflightChecker — validates config and wallet readiness before starting.
"""

import asyncio
import os
from web3 import AsyncWeb3, AsyncHTTPProvider

from src.config.settings import ConfigurationManager, NETWORKS, ContractSpecs
from src.utils.rpc_health import rpc_health


class PreflightChecker:
    def __init__(self, keys: list[str]):
        self._cfg = ConfigurationManager()
        self._keys = keys
        
        # We'll initialize _w3 lazily or find a working one now
        self._rpcs = rpc_health.get_rpcs(self._cfg.rpc_ticker)
        self._w3 = AsyncWeb3(AsyncHTTPProvider(self._rpcs[0]))
        
        net_info = NETWORKS.get(self._cfg.rpc_ticker, {})
        self._symbol = net_info.get("symbol", "ETH")
        self._explorer = net_info.get("explorer", "https://etherscan.io")

    async def _ensure_w3_works(self):
        """Try available RPCs until one actually responds to a simple call."""
        for rpc in self._rpcs:
            temp_w3 = AsyncWeb3(AsyncHTTPProvider(rpc))
            try:
                # Use a lightweight call to verify authorization and connectivity
                await asyncio.wait_for(temp_w3.eth.block_number, timeout=3)
                self._w3 = temp_w3
                return True
            except Exception:
                continue
        return False

    async def _estimate_gas_cost_wei(self) -> int:
        """
        Estimate a worst-case gas cost in wei for preflight balance checks.

        Uses EIP-1559 feeHistory when available; falls back to legacy gas_price.
        Configurable via env:
          - PREFLIGHT_GAS_LIMIT (default 300000)
          - PREFLIGHT_GAS_MULTIPLIER (default 1.2)
        """
        try:
            gas_limit = int(float(os.getenv("PREFLIGHT_GAS_LIMIT", "300000")))
        except Exception:
            gas_limit = 300_000

        try:
            mult = float(os.getenv("PREFLIGHT_GAS_MULTIPLIER", "1.2"))
        except Exception:
            mult = 1.2

        try:
            history = await asyncio.wait_for(self._w3.eth.fee_history(5, "latest", [25, 75]), timeout=5)
            base_fees = history.get("baseFeePerGas", [])
            prio_fees = history.get("reward", [])
            latest_base = base_fees[-1] if base_fees else await self._w3.eth.gas_price
            avg_prio = (sum(row[0] for row in prio_fees) // len(prio_fees)) if prio_fees else int(1e9)
            max_fee = int(latest_base * 2) + avg_prio
            max_fee = int(max_fee * mult)
            return int(gas_limit * max_fee)
        except Exception:
            try:
                gas_price = await asyncio.wait_for(self._w3.eth.gas_price, timeout=5)
                return int(gas_limit * int(gas_price * mult))
            except Exception:
                return 0

    async def run(self) -> list[dict]:
        """Returns a list of check results: {label, status, detail}"""
        # First, find a working RPC node to avoid "Forbidden" errors
        await self._ensure_w3_works()
        
        checks = []
        checks.append(await self._check_network())
        checks.append(await self._check_contract())
        checks.append(await self._check_rpc())
        if self._cfg.mint_mode == "SEADROP_WL":
            checks.append(self._check_os_api())
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

    def _check_os_api(self) -> dict:
        if self._cfg.os_api_key:
            return {"label": "OpenSea API", "status": "ok", "detail": "API Key found ✓"}
        return {"label": "OpenSea API", "status": "error", "detail": "Missing OS_API_KEY for SEADROP_WL mode!"}

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

        # Gas estimate (dynamic; configurable)
        gas_estimate_wei = await self._estimate_gas_cost_wei()
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
