"""
PreflightChecker — validates config and wallet readiness before starting.
"""

import asyncio
import time
from eth_account import Account
from web3 import AsyncWeb3, AsyncHTTPProvider

from src.config.settings import ConfigurationManager, NETWORKS, ContractSpecs
from src.utils.rpc_health import rpc_health
from src.utils.revert_decoder import decode_revert_error
from src.engine.seadrop_wl import SeaDropWlService


class PreflightChecker:
    def __init__(self, keys: list[str]):
        self._cfg = ConfigurationManager()
        self._keys = keys

        rpcs = rpc_health.get_rpcs(self._cfg.rpc_ticker)
        # Will be set in _pick_rpc(); default to first as fallback
        self._rpcs = rpcs
        if rpcs and (rpcs[0].startswith("wss://") or rpcs[0].startswith("ws://")):
            self._w3 = AsyncWeb3(AsyncWeb3.WebSocketProvider(rpcs[0]))
        else:
            self._w3 = AsyncWeb3(AsyncHTTPProvider(rpcs[0] if rpcs else "http://localhost:8545"))
        net_info = NETWORKS.get(self._cfg.rpc_ticker, {})
        self._symbol = net_info.get("symbol", "ETH")
        self._explorer = net_info.get("explorer", "https://etherscan.io")

    async def _pick_rpc(self):
        """Try each RPC until one responds, then use it for all checks."""
        for url in self._rpcs:
            try:
                if url.startswith("wss://") or url.startswith("ws://"):
                    w3 = AsyncWeb3(AsyncWeb3.WebSocketProvider(url))
                else:
                    w3 = AsyncWeb3(AsyncHTTPProvider(url))
                await asyncio.wait_for(w3.eth.block_number, timeout=5)
                self._w3 = w3
                return True
            except Exception:
                continue
                
        return False

    async def run(self) -> list[dict]:
        """Returns a list of check results: {label, status, detail}"""
        # First, find a working RPC node to avoid "Forbidden" errors
        await self._ensure_w3_works()
        
        checks = []
        if not await self._pick_rpc():
            return [{"label": "RPC Connection", "status": "error", "detail": "ALL available RPC nodes failed to respond or returned 503/429 errors. Check your connection or API keys."}]
        checks.append(await self._check_network())
        checks.append(await self._check_contract())
        checks.append(await self._check_drop_state())
        checks.append(await self._check_rpc())
        if self._cfg.mint_mode == "SEADROP_WL":
            checks.append(self._check_os_api())
        wallet_checks = await self._check_wallets()
        checks.extend(wallet_checks)
        simulation_checks = await self._simulate_wallets()
        checks.extend(simulation_checks)
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

    async def _check_drop_state(self) -> dict:
        if self._cfg.mint_mode not in ("SEADROP", "SEADROP_WL"):
            return {"label": "Drop State", "status": "warn",
                    "detail": "Direct/proxy mode: sale state must be validated by simulation."}
        try:
            nft_addr = AsyncWeb3.to_checksum_address(self._cfg.target_nft)
            sea_contract = self._w3.eth.contract(
                address=AsyncWeb3.to_checksum_address(self._cfg.sea_addr),
                abi=ContractSpecs.SEA_ABI,
            )
            drop_data = await asyncio.wait_for(
                sea_contract.functions.getPublicDrop(nft_addr).call(),
                timeout=8,
            )
            mint_price_wei = int(drop_data[0])
            start_time = int(drop_data[1])
            end_time = int(drop_data[2])
            max_per_wallet = int(drop_data[3])
            now = int(time.time())

            if end_time and now > end_time:
                return {"label": "Drop State", "status": "error",
                        "detail": f"Public drop ended at {end_time}."}
            if start_time and now < start_time and not self._cfg.force_start:
                return {"label": "Drop State", "status": "warn",
                        "detail": f"Public drop starts at {start_time}; bot will wait."}
            return {"label": "Drop State", "status": "ok",
                    "detail": f"price={mint_price_wei / 1e18:.6f} {self._symbol}, max/wallet={max_per_wallet}"}
        except Exception as e:
            return {"label": "Drop State", "status": "warn",
                    "detail": f"Could not read SeaDrop public state: {str(e)[:120]}"}

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
        # Fetch actual network gas price (tighter 150k gas limit)
        try:
            current_gas_price = await self._w3.eth.gas_price
        except Exception:
            current_gas_price = int(10e9)  # 10 gwei fallback
            
        gas_limit = 150_000
        gas_estimate_wei = gas_limit * current_gas_price
        min_required_wei = mint_price_wei * self._cfg.qty + gas_estimate_wei

        # FIX M-05: Check all wallets concurrently instead of sequentially.
        # With 50 wallets and 300ms RPC latency, sequential = 15 s; concurrent = ~0.3 s.
        async def _check_one(i: int, pk: str) -> dict:
            wid = i + 1
            try:
                acct    = Account.from_key(pk)
                bal_wei = await self._w3.eth.get_balance(acct.address)
                bal_eth = bal_wei / 1e18
                min_eth = min_required_wei / 1e18
                status  = "ok" if bal_wei >= min_required_wei else "warn"
                detail  = (
                    f"Balance: {bal_eth:.5f} {self._symbol} (need ≥ {min_eth:.5f})"
                    if status == "ok"
                    else f"Low balance: {bal_eth:.5f} {self._symbol} (need ≥ {min_eth:.5f})"
                )
                return {
                    "label":    f"Wallet #{wid}",
                    "status":   status,
                    "detail":   detail,
                    "address":  acct.address,
                    "explorer": f"{self._explorer}/address/{acct.address}",
                }
            except Exception as exc:
                return {
                    "label":  f"Wallet #{wid}",
                    "status": "error",
                    "detail": f"Invalid key: {str(exc)[:40]}",
                }

        return results

    async def _simulate_wallets(self) -> list[dict]:
        results = []
        for i, pk in enumerate(self._keys):
            wid = i + 1
            try:
                acct = Account.from_key(pk)
                tx = await self._build_sim_tx(acct.address)
                await asyncio.wait_for(self._w3.eth.call(tx), timeout=12)

                try:
                    gas_estimate = await asyncio.wait_for(self._w3.eth.estimate_gas(tx), timeout=12)
                    detail = f"Simulation passed. Estimated gas: {gas_estimate:,}"
                except Exception as e:
                    detail = f"Simulation passed, gas estimate unavailable: {str(e)[:80]}"

                results.append({
                    "label": f"Simulation #{wid}",
                    "status": "ok",
                    "detail": detail,
                    "address": acct.address,
                    "explorer": f"{self._explorer}/address/{acct.address}",
                })
            except Exception as e:
                results.append({
                    "label": f"Simulation #{wid}",
                    "status": "error",
                    "detail": decode_revert_error(e)[:180],
                })
        return results

    async def _build_sim_tx(self, sender: str) -> dict:
        nft_addr = AsyncWeb3.to_checksum_address(self._cfg.target_nft)
        sea_addr = AsyncWeb3.to_checksum_address(self._cfg.sea_addr)
        multi_addr = AsyncWeb3.to_checksum_address(self._cfg.multi_addr)
        total_value = await self._get_total_value(nft_addr)

        base = {
            "from": sender,
            "value": total_value,
            "gas": 400_000,
        }

        if self._cfg.mint_mode == "DIRECT":
            contract = self._w3.eth.contract(address=nft_addr, abi=ContractSpecs.UNIVERSAL_MINT_ABI)
            fn = getattr(contract.functions, self._cfg.mint_func_name)
            return await fn(self._cfg.qty).build_transaction(base)

        if self._cfg.mint_mode == "SEADROP":
            sea_contract = self._w3.eth.contract(address=sea_addr, abi=ContractSpecs.SEA_ABI)
            return await sea_contract.functions.mintPublic(
                nft_addr,
                AsyncWeb3.to_checksum_address("0x0000a26b00c1F0DF003000390027140000fAa719"),
                AsyncWeb3.to_checksum_address("0x0000000000000000000000000000000000000000"),
                self._cfg.qty,
            ).build_transaction(base)

        if self._cfg.mint_mode == "SEADROP_WL":
            wl_service = SeaDropWlService(self._cfg.os_api_key)
            wl_data = await wl_service.fetch_proof(nft_addr, sender)
            if not wl_data:
                raise ValueError("Wallet is not eligible for SeaDrop allowlist.")
            total_value = wl_data["mintPrice"] * self._cfg.qty
            base["value"] = total_value
            sea_contract = self._w3.eth.contract(address=sea_addr, abi=ContractSpecs.SEA_ABI)
            return await sea_contract.functions.mintAllowList(
                nft_addr,
                AsyncWeb3.to_checksum_address("0x0000a26b00c1F0DF003000390027140000fAa719"),
                AsyncWeb3.to_checksum_address("0x0000000000000000000000000000000000000000"),
                self._cfg.qty,
                wl_data["mintParams"],
                wl_data["proof"],
            ).build_transaction(base)

        multi_contract = self._w3.eth.contract(address=multi_addr, abi=ContractSpecs.MULTI_ABI)
        return await multi_contract.functions.mintMulti(
            self._cfg.qty,
            nft_addr,
        ).build_transaction(base)

    async def _get_total_value(self, nft_addr: str) -> int:
        if self._cfg.mint_mode == "DIRECT":
            return 0
        try:
            sea_contract = self._w3.eth.contract(
                address=AsyncWeb3.to_checksum_address(self._cfg.sea_addr),
                abi=ContractSpecs.SEA_ABI,
            )
            drop_data = await sea_contract.functions.getPublicDrop(nft_addr).call()
            return int(drop_data[0]) * self._cfg.qty
        except Exception:
            return 0
