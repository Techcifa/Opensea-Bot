"""
ExecutionUnit — Core minting worker for a single wallet.

Features:
- EIP-1559 gas parameters via GasOracle
- TX simulation (eth_call) before broadcast
- God Mode (pre-signed TX 5s before mint)
- Time Sniper (millisecond-precision sleep to mint time)
- Gas Guardian (pause when gas > ceiling)
- Dynamic gas bumping for stuck TXs
- Flashbots mode (optional private mempool)
- DIRECT and PROXY (MultiMint) execution modes
- NonceManager with auto-heal
- Post-success: Accountant, Discord, AssetRelay, Dust Sweep
- Dead Letter Queue on failure

NO TELEMETRY. NO BACKDOORS.
"""

import asyncio
import time
import random
import aiohttp
from datetime import datetime
from typing import Callable, Any

from web3 import AsyncWeb3, AsyncHTTPProvider
from eth_account import Account

from src.config.settings import ContractSpecs, ConfigurationManager, NETWORKS
from src.utils.core import async_error_handler
from src.utils.verifier import ContractVerifier
from src.utils.rpc_health import rpc_health
from src.utils.rpc_rate_limiter import rpc_limiter
from src.gas_oracle import gas_oracle
from src.engine.nonce_manager import NonceManager
from src.engine.dead_letter_queue import dlq
from src.engine.seadrop_wl import SeaDropWlService
from src.features.transfer import AssetRelay
from src.features.accountant import Accountant
from src.ui.notifier import DiscordReporter

FLASHBOTS_URL = "https://relay.flashbots.net"


class ExecutionUnit:
    _pk: str
    _uid: int
    _cfg: ConfigurationManager
    _acct: Account
    _broadcast: Callable[[dict], Any] | None
    _chain_id: int
    _symbol: str
    _explorer: str
    _rpc_list: list[str]
    _rpc_index: int
    _proxy_url: str | None
    w3: AsyncWeb3
    _nonce_mgr: NonceManager
    _priority: int

    def __init__(self, pkey: str, uid: int, global_config: ConfigurationManager, broadcast=None):
        self._pk = pkey
        self._uid = uid
        self._cfg = global_config
        self._acct = Account.from_key(pkey)
        self._broadcast = broadcast  # async callable for WebSocket updates

        net_info = NETWORKS.get(self._cfg.rpc_ticker)
        if not net_info:
            raise ValueError(f"Invalid Network Ticker: {self._cfg.rpc_ticker}")

        self._chain_id = net_info["id"]
        self._symbol = net_info["symbol"]
        self._explorer = net_info.get("explorer", "https://etherscan.io")

        # RPC rotation
        self._rpc_list = rpc_health.get_rpcs(self._cfg.rpc_ticker)
        self._rpc_index = 0

        # Proxy selection (round-robin per worker)
        self._proxy_url = None
        if self._cfg.use_proxies and self._cfg.proxies:
            self._proxy_url = self._cfg.proxies[(self._uid - 1) % len(self._cfg.proxies)]

        self._init_web3()
        self._nonce_mgr = NonceManager(self.w3, self._acct.address, uid)

        # Priority tier
        prio = self._cfg.worker_priority
        self._priority = prio[uid - 1] if prio and uid <= len(prio) else 1

    # ------------------------------------------------------------------
    # Web3 setup & RPC rotation
    # ------------------------------------------------------------------
    def _init_web3(self):
        url = self._rpc_list[self._rpc_index % len(self._rpc_list)]
        kwargs = {}
        if self._proxy_url:
            kwargs = {"proxy": self._proxy_url}
        self.w3 = AsyncWeb3(AsyncHTTPProvider(url, request_kwargs=kwargs))

    async def _rotate_rpc(self):
        self._rpc_index += 1
        url = self._rpc_list[self._rpc_index % len(self._rpc_list)]
        await self._log("WARNING", f"[RPC Rotator] → {url[:30]}...")
        self._init_web3()
        self._nonce_mgr._w3 = self.w3

    # ------------------------------------------------------------------
    # Logging helpers
    # ------------------------------------------------------------------
    async def _log(self, level: str, message: str, tx_hash: str = ""):
        import time as t
        payload = {
            "type": "log",
            "level": level,
            "worker_id": self._uid,
            "message": message,
            "tx_hash": tx_hash,
            "timestamp": int(t.time()),
        }
        if self._broadcast is not None:
            try:
                await self._broadcast(payload)
            except Exception:
                pass

    async def _update_worker(self, status: str, balance: str = "", action: str = ""):
        if self._broadcast is not None:
            try:
                await self._broadcast({
                    "type": "worker_update",
                    "worker_id": self._uid,
                    "address": self._acct.address,
                    "status": status,
                    "balance": balance,
                    "action": action,
                    "priority": self._priority,
                    "explorer": f"{str(self._explorer)}/address/{self._acct.address}",
                })
            except Exception:
                pass

    # ------------------------------------------------------------------
    # Gas helpers
    # ------------------------------------------------------------------
    def _required_balance_wei(self, tx: dict) -> int:
        gas = int(tx.get("gas") or 0)
        value = int(tx.get("value") or 0)
        price = int(tx.get("maxFeePerGas") or tx.get("gasPrice") or 0)
        required = value + gas * price
        return int(required * 1.02)

    async def _get_gas_params(self) -> dict:
        """Return EIP-1559 gas params, respecting manual override."""
        if self._cfg.gas_gwei:
            price = int(float(self._cfg.gas_gwei) * 1e9)
            return {"gasPrice": price}
        params = await gas_oracle.get_tx_params()
        # Add 20% buffer on top of computed fee
        params["maxFeePerGas"] = int(params["maxFeePerGas"] * 1.2)
        return params

    async def _guard_gas(self):
        """Block execution while gas is above ceiling."""
        while gas_oracle.is_ceiling_exceeded():
            gwei = gas_oracle.get_current().get("max_fee_gwei", "?")
            await self._log("WARNING", f"[Gas Guardian] Gas {gwei} Gwei > ceiling. Pausing...")
            await asyncio.sleep(5)

    # ------------------------------------------------------------------
    # TX Simulation
    # ------------------------------------------------------------------
    async def _simulate_tx(self, tx_data: dict) -> tuple[bool, str]:
        """eth_call simulation. Returns (success, revert_reason)."""
        try:
            await self.w3.eth.call({
                "from": self._acct.address,
                "to": tx_data["to"],
                "data": tx_data.get("data", "0x"),
                "value": tx_data.get("value", 0),
            })
            return True, ""
        except Exception as e:
            reason = str(e)
            # Try to extract revert reason
            if "revert" in reason.lower():
                return False, reason[:120]
            # Other errors (network, etc.) — allow through
            return True, ""

    # ------------------------------------------------------------------
    # RPC Race Strategy (Parallel Broadcast)
    # ------------------------------------------------------------------
    async def _broadcast_parallel(self, signed_raw_tx: bytes) -> bytes:
        """
        Broadcast the same signed raw TX to multiple RPCs simultaneously.
        Returns the hash of the first successful propagation.
        """
        rpcs = rpc_health.get_prioritized_rpcs(self._cfg.rpc_ticker)
        # Limit to top 5 healthy nodes to avoid spamming/overhead
        targets = rpcs[:5]
        
        await self._log("INFO", f"🚀 [Race Strategy] Broadcasting to {len(targets)} nodes simultaneously...")
        
        async def _single_broadcast(url: str):
            # Create temporary w3 for this specific node
            kwargs = {"proxy": self._proxy_url} if self._proxy_url else {}
            w3_node = AsyncWeb3(AsyncHTTPProvider(url, request_kwargs=kwargs))
            try:
                # Use rate limiter if possible
                async with rpc_limiter.acquire(url):
                    tx_hash = await w3_node.eth.send_raw_transaction(signed_raw_tx)
                    rpc_health.stats[url]["score"] = min(1.0, rpc_health.stats[url].get("score", 0.5) + 0.05)
                    return tx_hash, url
            except Exception as e:
                # Penalize score for failure
                if url in rpc_health.stats:
                    rpc_health.stats[url]["score"] = max(0.0, rpc_health.stats[url]["score"] - 0.1)
                raise e

        tasks = [asyncio.create_task(_single_broadcast(url)) for url in targets]
        
        if not tasks:
            raise Exception("No healthy RPCs available for broadcast!")

        done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        
        # Clean up pending tasks (we land the first one)
        for p in pending:
            p.cancel()

        first_res: tuple[bytes, str] | None = None
        for task in done:
            try:
                res = task.result()
                if isinstance(res, tuple):
                    first_res = res
                    break
            except Exception:
                continue

        if first_res is not None:
            tx_h: bytes = first_res[0]
            win_url: str = first_res[1]
            h_str = tx_h.hex()
            u_str = str(win_url)
            await self._log("SUCCESS", f"⚡ Node Won: {u_str[:30]}... | Hash: {h_str[:10]}")
            return tx_h
        
        # If all failed, try to get error from any done task
        error_msg = "All parallel broadcast tasks failed"
        for task in done:
            try:
                task.result()
            except Exception as e:
                error_msg = str(e)
        raise Exception(error_msg)

    # ------------------------------------------------------------------
    # Flashbots submission
    # ------------------------------------------------------------------
    async def _send_via_flashbots(self, signed_raw_tx: bytes) -> str:
        """Submit a signed raw TX to Flashbots relay."""
        bundle = {
            "jsonrpc": "2.0",
            "method": "eth_sendRawTransaction",
            "params": [signed_raw_tx.hex()],
            "id": 1,
        }
        async with aiohttp.ClientSession() as session:
            async with session.post(FLASHBOTS_URL, json=bundle) as resp:
                data = await resp.json()
                if "result" in data:
                    return data["result"]
                raise Exception(f"Flashbots error: {data.get('error', data)}")

    # ------------------------------------------------------------------
    # Build mint TX (DIRECT or PROXY)
    # ------------------------------------------------------------------
    async def _build_mint_tx(self, nft_addr_c: str, total_value: int,
                              gas_params: dict, nonce: int) -> dict | None:
        sea_addr_c = AsyncWeb3.to_checksum_address(self._cfg.sea_addr)
        multi_addr_c = AsyncWeb3.to_checksum_address(self._cfg.multi_addr)

        if self._cfg.mint_mode == "DIRECT":
            target_contract = self.w3.eth.contract(
                address=nft_addr_c, abi=ContractSpecs.UNIVERSAL_MINT_ABI
            )
            try:
                fn = getattr(target_contract.functions, self._cfg.mint_func_name)
            except AttributeError:
                await self._log("FATAL", f"Function '{self._cfg.mint_func_name}' not found in ABI!")
                return None
            tx = await fn(self._cfg.qty).build_transaction({
                "chainId": self._chain_id,
                "from": self._acct.address,
                "value": total_value,
                "nonce": nonce,
                "gas": 400000,
                **gas_params,
            })
        elif self._cfg.mint_mode == "SEADROP":
            sea_contract = self.w3.eth.contract(
                address=sea_addr_c, abi=ContractSpecs.SEA_ABI
            )
            ZERO_ADDR = AsyncWeb3.to_checksum_address("0x0000000000000000000000000000000000000000")
            FEE_RECIPIENT = AsyncWeb3.to_checksum_address("0x0000a26b00c1F0DF003000390027140000fAa719")
            tx = await sea_contract.functions.mintPublic(
                nft_addr_c,
                FEE_RECIPIENT,
                ZERO_ADDR,
                self._cfg.qty
            ).build_transaction({
                "chainId": self._chain_id,
                "from": self._acct.address,
                "value": total_value,
                "nonce": nonce,
                "gas": 400000,
                **gas_params,
            })
        elif self._cfg.mint_mode == "SEADROP_WL":
            sea_contract = self.w3.eth.contract(
                address=sea_addr_c, abi=ContractSpecs.SEA_ABI
            )
            ZERO_ADDR = AsyncWeb3.to_checksum_address("0x0000000000000000000000000000000000000000")
            FEE_RECIPIENT = AsyncWeb3.to_checksum_address("0x0000a26b00c1F0DF003000390027140000fAa719")
            
            # Fetch proof dynamically
            wl_service = SeaDropWlService(self._cfg.os_api_key)
            try:
                wl_data = await wl_service.fetch_proof(nft_addr_c, self._acct.address)
                if not wl_data:
                    await self._log("FATAL", f"Wallet {self._acct.address} not eligible for SeaDrop Whitelist!")
                    return None
            except Exception as e:
                await self._log("FATAL", f"OS Allowlist Fetch Failed: {str(e)}")
                return None
                
            proof = wl_data["proof"]
            mint_params = wl_data["mintParams"]
            # Correct total value dynamically
            total_value = wl_data["mintPrice"] * self._cfg.qty
            
            tx = await sea_contract.functions.mintAllowList(
                nft_addr_c,
                FEE_RECIPIENT,
                ZERO_ADDR,
                self._cfg.qty,
                mint_params,
                proof
            ).build_transaction({
                "chainId": self._chain_id,
                "from": self._acct.address,
                "value": total_value,
                "nonce": nonce,
                "gas": 400000,
                **gas_params,
            })
        else:
            multi_contract = self.w3.eth.contract(
                address=multi_addr_c, abi=ContractSpecs.MULTI_ABI
            )
            tx = await multi_contract.functions.mintMulti(
                self._cfg.qty, nft_addr_c
            ).build_transaction({
                "chainId": self._chain_id,
                "from": self._acct.address,
                "value": total_value,
                "nonce": nonce,
                "gas": 400000,
                **gas_params,
            })

        # Gas estimation
        try:
            est_tx = tx.copy()
            est_tx.pop("gas", None)
            est = await self.w3.eth.estimate_gas(est_tx)
            tx["gas"] = int(est * 1.2)
        except Exception:
            tx["gas"] = 400_000

        return tx

    # ------------------------------------------------------------------
    # Main protocol
    # ------------------------------------------------------------------
    @async_error_handler(retries=999, delay=2.0)
    async def run_protocol(self):
        nft_addr_c = AsyncWeb3.to_checksum_address(self._cfg.target_nft)

        # Anti-honeypot check (worker 1 only)
        if self._uid == 1 and self._cfg.verifier_enabled:
            verifier = ContractVerifier(self._cfg.explorer_api_key, self._cfg.rpc_ticker)
            is_safe = await verifier.check_guard(nft_addr_c)
            if not is_safe:
                await self._log("FATAL", "⚠️ CONTRACT UNVERIFIED — Potential honeypot. Aborting.")
                raise Exception("Contract Unverified")

        # Get current mint price and start time from SeaDrop
        sea_contract = self.w3.eth.contract(
            address=AsyncWeb3.to_checksum_address(self._cfg.sea_addr),
            abi=ContractSpecs.SEA_ABI,
        )
        mint_price = 0
        start_time = 0
        try:
            drop_data = await sea_contract.functions.getPublicDrop(nft_addr_c).call()
            mint_price = int(drop_data[0])
            start_time = int(drop_data[1])
        except Exception:
            pass

        total_value = mint_price * self._cfg.qty

        # Fetch wallet balance
        bal_wei = await self.w3.eth.get_balance(self._acct.address)
        bal_eth = bal_wei / 1e18
        await self._update_worker("INIT", f"{bal_eth:.5f} {self._symbol}", "Initialized")
        await self._log("INFO", f"Wallet: {self._acct.address[:8]}... | Bal: {bal_eth:.5f} {self._symbol}")

        # ---- Time Sniper ----
        current_time = int(time.time())
        pre_signed_raw = None
        pre_signed_required_wei = None

        if start_time > current_time and not self._cfg.force_start:
            wait_seconds = start_time - current_time
            # Standardize: Show both UTC and Local to avoid machine offset confusion
            dt_utc = datetime.utcfromtimestamp(start_time).strftime("%Y-%m-%d %H:%M:%S")
            dt_loc = datetime.fromtimestamp(start_time).strftime("%H:%M:%S")
            
            await self._log("WARNING", f"[Time Sniper] Opens at: {dt_utc} UTC ({dt_loc} Local). Sleeping {wait_seconds:.0f}s...")
            await self._update_worker("WAITING", f"{bal_eth:.5f}", f"Wait until {dt_utc} UTC")

            if self._cfg.presign_enabled and wait_seconds > 10:
                # Sleep until 5s before mint, then pre-sign
                sleep_main = wait_seconds - 5
                if sleep_main > 0:
                    await asyncio.sleep(sleep_main)

                await self._log("INIT", "[God Mode] Pre-signing TX...")
                try:
                    nonce = await self._nonce_mgr.get_nonce()
                    gas_params = await self._get_gas_params()
                    # Use multiplied gas for pre-sign to avoid stuck
                    if "maxFeePerGas" in gas_params:
                        gas_params["maxFeePerGas"] = int(gas_params["maxFeePerGas"] * self._cfg.presign_gas_mult)
                    else:
                        gas_params["gasPrice"] = int(gas_params.get("gasPrice", 0) * self._cfg.presign_gas_mult)

                    tx = await self._build_mint_tx(nft_addr_c, total_value, gas_params, nonce)
                    if tx:
                        tx["gas"] = self._cfg.presign_gas_limit
                        bal_wei_now = await self.w3.eth.get_balance(self._acct.address)
                        req_wei = self._required_balance_wei(tx)
                        if bal_wei_now < req_wei:
                            await self._log("WARNING", "God Mode skipped: insufficient balance for value+gas.")
                            self._nonce_mgr.fail(nonce)
                        else:
                            signed = self._acct.sign_transaction(tx)
                            pre_signed_raw = signed.raw_transaction
                            pre_signed_required_wei = req_wei
                            await self._log("SUCCESS", "TX locked. Waiting for trigger...")
                except Exception as e:
                    await self._log("ERROR", f"God Mode failed: {e}")

                # Sleep the final few seconds
                remaining = start_time - int(time.time())
                if remaining > 0:
                    await asyncio.sleep(remaining)
            else:
                if wait_seconds > 2:
                    await asyncio.sleep(wait_seconds - 2)

            await self._log("SUCCESS", "🚀 WAKE UP! Executing mint NOW!")

        # ---- Mint loop ----
        attempt = 0
        last_gas_params = None

        while True:
            try:
                await self._guard_gas()

                bal_wei = await self.w3.eth.get_balance(self._acct.address)
                bal_eth = bal_wei / 1e18
                await self._update_worker("MINTING", f"{bal_eth:.5f}", "Initiating sequence")

                if bal_wei < total_value:
                    gap = (total_value - bal_wei) / 1e18
                    await self._log("WARNING", f"Insufficient balance. Deficit: {gap:.5f} {self._symbol}. Waiting...")
                    await self._update_worker("WAITING", f"{bal_eth:.5f}", "Waiting for funds")
                    await asyncio.sleep(random.uniform(*self._cfg.delay_range))
                    continue

                # Broadcast pre-signed TX first
                local_pre_signed = pre_signed_raw
                if local_pre_signed is not None:
                    if pre_signed_required_wei is not None and bal_wei < pre_signed_required_wei:
                        gap = (pre_signed_required_wei - bal_wei) / 1e18
                        await self._log("WARNING", f"Insufficient balance for value+gas. Deficit: {gap:.5f} {self._symbol}. Waiting...")
                        await self._update_worker("WAITING", f"{bal_eth:.5f}", "Waiting for funds (gas)")
                        await asyncio.sleep(random.uniform(*self._cfg.delay_range))
                        continue
                    await self._log("INFO", "🚀 Broadcasting pre-signed TX...")
                    tx_hash = await self._broadcast_parallel(local_pre_signed)
                    pre_signed_raw = None
                    pre_signed_required_wei = None
                else:
                    # Build and sign fresh TX
                    attempt += 1
                    nonce = await self._nonce_mgr.get_nonce()

                    # Gas bumping for stuck TXs
                    if attempt > 1 and last_gas_params:
                        gas_params = await gas_oracle.bump_gas_params(last_gas_params, attempt - 1)
                        await self._log("WARNING", f"[Gas Bump] Attempt {attempt} — bumping gas x{attempt}")
                    else:
                        gas_params = await self._get_gas_params()

                    last_gas_params = gas_params

                    tx = await self._build_mint_tx(nft_addr_c, total_value, gas_params, nonce)
                    if tx is None:
                        return

                    required_wei = self._required_balance_wei(tx)
                    if bal_wei < required_wei:
                        gap = (required_wei - bal_wei) / 1e18
                        await self._log("WARNING", f"Insufficient balance for value+gas. Deficit: {gap:.5f} {self._symbol}. Waiting...")
                        await self._update_worker("WAITING", f"{bal_eth:.5f}", "Waiting for funds (gas)")
                        self._nonce_mgr.fail(nonce)
                        await asyncio.sleep(random.uniform(*self._cfg.delay_range))
                        continue

                    # Simulate before broadcast
                    ok, reason = await self._simulate_tx(tx)
                    if not ok:
                        await self._log("ERROR", f"TX simulation reverted: {reason}. Skipping.")
                        self._nonce_mgr.fail(nonce)
                        await dlq.push(self._uid, self._acct.address, f"Sim reverted: {reason}", attempt)
                        return

                    signed = self._acct.sign_transaction(tx)

                    if self._cfg.flashbots_mode:
                        await self._log("INFO", "[Flashbots] Submitting via private mempool...")
                        tx_hash_str = await self._send_via_flashbots(signed.raw_transaction)
                        tx_hash = bytes.fromhex(tx_hash_str.lstrip("0x"))
                    else:
                        tx_hash = await self._broadcast_parallel(signed.raw_transaction)
                        self._nonce_mgr.confirm(nonce)

                await self._log("INFO", f"TX Broadcast: {tx_hash.hex()}", tx_hash.hex())
                await self._update_worker("CONFIRMING", f"{bal_eth:.5f}", f"TX: {tx_hash.hex()[:10]}...")

                # Wait for receipt
                receipt = await self.w3.eth.wait_for_transaction_receipt(tx_hash, timeout=120)

                if receipt.status == 1:
                    await self._log("SUCCESS", "✅ Mint confirmed!", tx_hash.hex())
                    await self._update_worker("SUCCESS", f"{bal_eth:.5f}", f"Minted! TX: {tx_hash.hex()[:10]}")
                    await self._handle_success(tx_hash, receipt, total_value, nft_addr_c)
                    return
                else:
                    await self._log("ERROR", "TX reverted on-chain.")
                    if self._cfg.accountant_enabled:
                        Accountant.log_transaction(
                            self._cfg.rpc_ticker, self._acct.address,
                            "REVERT", tx_hash.hex(), receipt, total_value
                        )
                    self._nonce_mgr.fail(nonce if 'nonce' in locals() else 0)
                    # Try again
                    await asyncio.sleep(random.uniform(*self._cfg.delay_range))
                    continue

            except asyncio.CancelledError:
                await self._update_worker("STOPPED", "", "Cancelled")
                return
            except Exception as e:
                await self._log("ERROR", f"Worker error: {str(e)[:80]}")
                await self._rotate_rpc()
                raise

    # ------------------------------------------------------------------
    # Post-success handler
    # ------------------------------------------------------------------
    async def _handle_success(self, tx_hash, receipt, total_value: int, nft_addr: str):
        if self._cfg.accountant_enabled:
            Accountant.log_transaction(
                self._cfg.rpc_ticker, self._acct.address,
                "MINT", tx_hash.hex(), receipt, total_value
            )

        await DiscordReporter.notify_success(
            self._uid, self._acct.address, tx_hash.hex(),
            self._cfg.qty, self._cfg.rpc_ticker
        )

        if self._cfg.transfer_enabled and self._cfg.recipient:
            relay = AssetRelay(self.w3, self._acct, self._uid, self._cfg.recipient)
            await relay.execute_consolidation(nft_addr, receipt)
            await DiscordReporter.notify_transfer(
                self._uid, self._acct.address, tx_hash.hex(), self._cfg.recipient
            )
            if self._cfg.sweep_enabled:
                await relay.sweep_native_token(self._cfg.min_sweep_eth)
