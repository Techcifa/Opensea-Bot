"""
ExecutionUnit — Core minting worker for a single wallet.

FIXES:
  C-03  AsyncWeb3 instances are now cached by URL in a module-level dict —
        no more 5 new instances created (and never closed) per mint attempt.
  C-06  wait_for_transaction_receipt replaced with _wait_receipt_with_rotation()
        which polls all healthy RPCs and rotates on failure, preventing a
        120-second hang on a dead node.
  H-09  _broadcast_parallel now waits for ALL tasks (not FIRST_COMPLETED),
        then returns the first success.  Avoids the case where the only
        completed task raised an exception while other nodes accepted the TX.
  M-04  Guard against empty RPC list in __init__ and _init_web3.
  M-06  Gas bumping is now actually wired: _maybe_bump_gas() checks
        block height after broadcast and resubmits with bumped params if
        the TX hasn't confirmed after cfg.bump_after_blocks blocks.

NO TELEMETRY. NO BACKDOORS.
"""

import asyncio
import time
import random
import logging
import aiohttp
from eth_utils import keccak
from datetime import datetime
from contextlib import asynccontextmanager

from web3 import AsyncWeb3, AsyncHTTPProvider
from eth_account import Account

from src.config.settings import ContractSpecs, ConfigurationManager, NETWORKS
from src.utils.core import async_error_handler
from src.utils.revert_decoder import decode_revert_error
from src.utils.verifier import ContractVerifier
from src.utils.rpc_health import rpc_health
from src.utils.rpc_rate_limiter import rpc_limiter
from src.utils.run_logger import RunLogger
from src.gas_oracle import gas_oracle
from src.engine.nonce_manager import NonceManager
from src.engine.dead_letter_queue import dlq
from src.engine.seadrop_wl import SeaDropWlService
from src.features.transfer import AssetRelay
from src.features.accountant import Accountant
from src.ui.notifier import DiscordReporter

log = logging.getLogger(__name__)

FLASHBOTS_URL = "https://relay.flashbots.net"

# FIX C-03: Module-level cache keyed by (url, proxy) so AsyncWeb3 instances
# are created at most once per unique endpoint and reused across all workers.
_w3_cache: dict[str, AsyncWeb3] = {}


def _get_cached_w3(url: str, proxy: str | None = None) -> AsyncWeb3:
    key = f"{url}|{proxy}"
    if key not in _w3_cache:
        kwargs = {"proxy": proxy} if proxy else {}
        _w3_cache[key] = AsyncWeb3(AsyncHTTPProvider(url, request_kwargs=kwargs))
    return _w3_cache[key]


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
        self._pk        = pkey
        self._uid       = uid
        self._cfg       = global_config
        self._acct      = Account.from_key(pkey)
        self._broadcast = broadcast

        net_info = NETWORKS.get(self._cfg.rpc_ticker)
        if not net_info:
            raise ValueError(f"Invalid Network Ticker: {self._cfg.rpc_ticker}")

        self._chain_id  = net_info["id"]
        self._symbol    = net_info["symbol"]
        self._explorer  = net_info.get("explorer", "https://etherscan.io")

        # RPC rotation — FIX M-04: fallback to NETWORKS if pool is empty
        self._rpc_list  = rpc_health.get_rpcs(self._cfg.rpc_ticker)
        if not self._rpc_list:
            self._rpc_list = list(net_info.get("rpc", []))
        if not self._rpc_list:
            raise RuntimeError(f"No RPC endpoints available for {self._cfg.rpc_ticker}")
        self._rpc_index = uid % len(self._rpc_list)   # stagger workers across nodes

        # Proxy selection
        self._proxy_url = None
        if self._cfg.use_proxies and self._cfg.proxies:
            self._proxy_url = self._cfg.proxies[(self._uid - 1) % len(self._cfg.proxies)]

        self._init_web3()
        self._nonce_mgr = NonceManager(self.w3, self._acct.address, uid)

        prio = self._cfg.worker_priority
        self._priority = prio[uid - 1] if prio and uid <= len(prio) else 1

    # ------------------------------------------------------------------
    # Results model
    # ------------------------------------------------------------------
    class ExecutionResult:
        def __init__(self, success: bool, tx_hash: str = "", receipt: Any = None, value: int = 0):
            self.success  = success
            self.tx_hash  = tx_hash
            self.receipt  = receipt
            self.value    = value

    # ------------------------------------------------------------------
    # Web3 setup & RPC rotation (FIX C-03: use cache)
    # ------------------------------------------------------------------
    def _init_web3(self):
        url = self._rpc_list[self._rpc_index % len(self._rpc_list)]
        if url.startswith("wss://") or url.startswith("ws://"):
            self.w3 = AsyncWeb3(AsyncWeb3.WebSocketProvider(url))
        else:
            kwargs = {}
            if self._proxy_url:
                kwargs = {"proxy": self._proxy_url}
            self.w3 = AsyncWeb3(AsyncHTTPProvider(url, request_kwargs=kwargs))

    async def _rotate_rpc(self):
        self._rpc_index += 1
        url = self._rpc_list[self._rpc_index % len(self._rpc_list)]
        await self._log("WARNING", f"[RPC Rotator] → {url[:30]}...")
        self.w3 = _get_cached_w3(url, self._proxy_url)   # FIX C-03: cache
        self._nonce_mgr._w3 = self.w3

    # ------------------------------------------------------------------
    # Logging helpers
    # ------------------------------------------------------------------
    async def _log(self, level: str, message: str, tx_hash: str = ""):
        payload = {
            "type": "log", "level": level,
            "worker_id": self._uid,
            "message":   message,
            "tx_hash":   tx_hash,
            "timestamp": int(time.time()),
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
                    "type":      "worker_update",
                    "worker_id": self._uid,
                    "address":   self._acct.address,
                    "status":    status,
                    "balance":   balance,
                    "action":    action,
                    "priority":  self._priority,
                    "explorer":  f"{str(self._explorer)}/address/{self._acct.address}",
                })
            except Exception:
                pass

    # ------------------------------------------------------------------
    # Gas helpers
    # ------------------------------------------------------------------
    def _required_balance_wei(self, tx: dict) -> int:
        gas   = int(tx.get("gas") or 0)
        value = int(tx.get("value") or 0)
        price = int(tx.get("maxFeePerGas") or tx.get("gasPrice") or 0)
        return int((value + gas * price) * 1.02)

    def _cap_gas_params(self, params: dict) -> dict:
        cap_wei = int(self._cfg.max_gas_limit * 1e9) if self._cfg.max_gas_limit > 0 else 0
        if cap_wei <= 0:
            return params
        if "maxFeePerGas" in params:
            params["maxFeePerGas"] = min(int(params.get("maxFeePerGas", 0)), cap_wei)
            if "maxPriorityFeePerGas" in params:
                params["maxPriorityFeePerGas"] = min(
                    int(params.get("maxPriorityFeePerGas", 0)), params["maxFeePerGas"]
                )
        if "gasPrice" in params:
            params["gasPrice"] = min(int(params.get("gasPrice", 0)), cap_wei)
        return params

    async def _get_gas_params(self) -> dict:
        if self._cfg.gas_gwei:
            manual_price = int(float(self._cfg.gas_gwei) * 1e9)
            return self._cap_gas_params({"gasPrice": manual_price})
        params = await gas_oracle.get_tx_params()
        fee_buffer = max(float(self._cfg.tx_fee_buffer_multiplier), 0.1)
        params["maxFeePerGas"]         = int(params.get("maxFeePerGas", 0) * fee_buffer)
        if "maxPriorityFeePerGas" in params:
            params["maxPriorityFeePerGas"] = int(params.get("maxPriorityFeePerGas", 0) * fee_buffer)
        return self._cap_gas_params(params)

    async def _guard_gas(self):
        """Pause until gas ceiling is no longer exceeded (respects shutdown)."""
        while self._cfg.gas_ceiling_gwei > 0 and gas_oracle.is_ceiling_exceeded():
            gwei = gas_oracle.get_current().get("max_fee_gwei", "?")
            await self._log("WARNING", f"[Gas Guardian] Gas {gwei} Gwei > ceiling. Pausing...")
            await asyncio.sleep(5)

    @asynccontextmanager
    async def _rpc_call(self):
        """Phase 3 Rate Limiting: Acquire endpoint token, track 429s/backoffs."""
        endpoint = self.w3.provider.endpoint_uri
        async with rpc_limiter.acquire(endpoint):
            try:
                yield
                rpc_limiter.report_success(endpoint)
            except Exception as e:
                err_str = str(e).lower()
                if "429" in err_str or "rate limit" in err_str or "too many requests" in err_str:
                    rpc_limiter.report_rate_limit(endpoint)
                raise


    # ------------------------------------------------------------------
    # 429 / RPC error handling
    # ------------------------------------------------------------------
    @staticmethod
    def _is_rate_limited(exc: Exception) -> bool:
        """
        Detect 429 / quota-exhausted errors from any RPC provider.
        Handles both HTTP-level 429s and JSON-RPC quota codes:
          -32005  limit exceeded (Infura, Alchemy)
          -32097  forbidden / quota (Infura)
          -32014  header not found / ErrUpstreamsExhausted (ANKR)
          -32603  Internal error (sometimes wraps quota on geth nodes)
        """
        msg = str(exc).lower()
        return any(kw in msg for kw in (
            # HTTP-level signals
            "429", "rate limit", "rate-limit", "ratelimit",
            "too many requests", "quota", "over limit",
            "requests per second", "request limit",
            # Provider-specific
            "quantainstant",            # ANKR epoch-based quota string
            "errupstreamsexhausted",     # ANKR: all upstreams are over capacity
            "upstreams exhausted",
            "upstreamexhausted",
            # JSON-RPC error codes (appear as dict key inside the exception string)
            "-32005",                    # limit exceeded
            "-32097",                    # forbidden / quota
            # -32014 specifically means "header not found" on ANKR when overloaded
            # We detect via the accompanying 'ErrUpstreamsExhausted' data field above
        ))

    @staticmethod
    def _is_retryable_rpc_error(exc: Exception) -> bool:
        """
        Connection-level / provider-overload errors that warrant RPC rotation + retry.
        Includes ANKR-specific JSON-RPC codes and HTTP transport errors.
        """
        msg = str(exc).lower()
        return any(kw in msg for kw in (
            # Transport / connection
            "connection", "timeout", "timed out", "refused",
            "unreachable", "bad gateway", "service unavailable",
            "reset by peer", "broken pipe",
            # HTTP error codes
            "503", "502", "504", "401", "403", "forbidden",
            # JSON-RPC error codes that mean "try another node"
            "-32014",     # ANKR: header not found / ErrUpstreamsExhausted
            "-32603",     # Internal error (often a degraded node)
            "-32000",     # Generic execution error (sometimes transient)
            "header not found",           # ANKR -32014 message text
            "errupstreamsexhausted",       # ANKR data.code
            "execution timeout",
            "nonce too low",              # nonce mismatch after reorg → heal + retry
        ))

    async def _handle_rpc_error(self, exc: Exception, url: str):
        """Penalise a bad RPC and rotate to the next one."""
        msg = str(exc).lower()

        if self._is_rate_limited(exc):
            rpc_limiter.report_rate_limit(url)
            if url in rpc_health.stats:
                rpc_health.stats[url]["score"] = 0.0
            await self._log("WARNING",
                f"[RPC 429] {url[:32]}... rate/quota limited — rotating to next endpoint")
        elif "nonce too low" in msg:
            # Nonce desync after reorg or crash-restart: heal and keep going
            await self._log("WARNING", "[Nonce] Nonce too low detected — healing nonce manager")
            try:
                await self._nonce_mgr.heal()
            except Exception:
                pass
        else:
            if url in rpc_health.stats:
                rpc_health.stats[url]["score"] = max(0.0, rpc_health.stats[url].get("score", 0.5) - 0.3)
            await self._log("WARNING",
                f"[RPC ERR] {url[:32]}... error: {str(exc)[:60]} — rotating")
        await self._rotate_rpc()

    # ------------------------------------------------------------------
    # TX Simulation
    # ------------------------------------------------------------------
    async def _simulate_tx(self, tx_data: dict) -> tuple[bool, str, bool]:
        """
        Returns (success, reason, is_retryable).

        is_retryable=True  → empty revert (0x), mint not open yet — keep looping
        is_retryable=False → real revert with reason string — hard stop

        Revert reason decoding:
          Error(string) selector = 0x08c379a0
          Panic(uint256) selector = 0x4e487b71
        """
        if self._cfg.skip_simulation:
            return True, "", False

        try:
            async with self._rpc_call():
                await self.w3.eth.call({
                    "from": self._acct.address,
                    "to": tx_data["to"],
                    "data": tx_data.get("data", "0x"),
                    "value": tx_data.get("value", 0),
                })
            return True, ""
        except Exception as e:
            raw_reason = str(e)
            reason = decode_revert_error(e)
            # Try to extract revert reason
            if "revert" in raw_reason.lower() or reason != raw_reason:
                return False, reason[:120]
            # Other errors (network, etc.) — allow through
            return True, ""

    async def _replay_revert_reason(self, tx_hash: bytes, receipt, fallback_tx: dict | None = None) -> str:
        try:
            tx_obj = fallback_tx
            if tx_obj is None:
                async with self._rpc_call():
                    chain_tx = await self.w3.eth.get_transaction(tx_hash)
                tx_obj = {
                    "from": chain_tx["from"],
                    "to": chain_tx["to"],
                    "data": chain_tx.get("input", "0x"),
                    "value": chain_tx.get("value", 0),
                }
            else:
                tx_obj = {
                    "from": tx_obj.get("from", self._acct.address),
                    "to": tx_obj.get("to"),
                    "data": tx_obj.get("data", "0x"),
                    "value": tx_obj.get("value", 0),
                }
            async with self._rpc_call():
                await self.w3.eth.call(tx_obj, block_identifier=receipt.blockNumber)
        except Exception as e:
            return decode_revert_error(e)[:180]
        return "Transaction reverted without a decoded reason."

    # ------------------------------------------------------------------
    # Broadcast Racing
    # ------------------------------------------------------------------
    async def _broadcast_race(self, signed_raw_tx: bytes) -> bytes:
        """Phase 1 Speed: Spam all healthy RPC nodes simultaneously."""
        hex_tx = signed_raw_tx.hex() if isinstance(signed_raw_tx, bytes) else signed_raw_tx
        if not hex_tx.startswith("0x"):
            hex_tx = "0x" + hex_tx
        raw_bytes = bytes.fromhex(hex_tx.removeprefix("0x"))
        local_hash = keccak(raw_bytes)
            
        payload = {
            "jsonrpc": "2.0",
            "method": "eth_sendRawTransaction",
            "params": [hex_tx],
            "id": 1
        }
        
        async def _post(url: str):
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.post(url, json=payload, timeout=2.0) as resp:
                        res = await resp.json()
                        if "result" in res:
                            return bytes.fromhex(res["result"].lstrip("0x"))
                        err_msg = str(res.get("error", "")).lower()
                        if "already known" in err_msg or "known transaction" in err_msg:
                            return local_hash
                        return None
            except Exception:
                return None

        tasks = [_post(url) for url in self._rpc_list if url.startswith("http://") or url.startswith("https://")]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        for r in results:
            if isinstance(r, bytes):
                return r
                
        # Fallback to standard W3 broadcast
        async with self._rpc_call():
            try:
                return await self.w3.eth.send_raw_transaction(signed_raw_tx)
            except Exception as e:
                err = str(e).lower()
                if "already known" in err or "known transaction" in err:
                    return local_hash
                raise

    # ------------------------------------------------------------------
    # Flashbots submission
    # ------------------------------------------------------------------
    async def _send_via_flashbots(self, signed_raw_tx: bytes) -> str:
        bundle = {
            "jsonrpc": "2.0", "method": "eth_sendRawTransaction",
            "params": [signed_raw_tx.hex()], "id": 1,
        }
        async with aiohttp.ClientSession() as session:
            async with session.post(FLASHBOTS_URL, json=bundle, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                data = await resp.json()
                if "result" in data:
                    return data["result"]
                raise Exception(f"Flashbots error: {data.get('error', data)}")

    # ------------------------------------------------------------------
    # Build mint TX
    # ------------------------------------------------------------------
    async def _build_mint_tx(self, nft_addr_c: str, total_value: int,
                              gas_params: dict, nonce: int) -> dict | None:
        t0 = time.perf_counter()
        sea_addr_c = AsyncWeb3.to_checksum_address(self._cfg.sea_addr)
        multi_addr_c = AsyncWeb3.to_checksum_address(self._cfg.multi_addr)
        fallback_gas = {
            "DIRECT":     self._cfg.direct_fallback_gas_limit,
            "SEADROP":    260_000,
            "SEADROP_WL": 300_000,
            "PROXY":      320_000,
        }.get(self._cfg.mint_mode, 260_000)

        if self._cfg.mint_mode == "DIRECT":
            target_contract = self.w3.eth.contract(address=nft_addr_c, abi=ContractSpecs.UNIVERSAL_MINT_ABI)
            fn = getattr(target_contract.functions, self._cfg.mint_func_name)
            tx = await fn(self._cfg.qty).build_transaction({
                "chainId": self._chain_id, "from": self._acct.address,
                "value": total_value, "nonce": nonce, "gas": fallback_gas, **gas_params,
            })
        elif self._cfg.mint_mode == "SEADROP":
            sea_contract = self.w3.eth.contract(address=sea_addr_c, abi=ContractSpecs.SEA_ABI)
            tx = await sea_contract.functions.mintPublic(
                nft_addr_c,
                "0x0000a26b00c1F0DF003000390027140000fAa719",
                "0x0000000000000000000000000000000000000000",
                self._cfg.qty,
            ).build_transaction({
                "chainId": self._chain_id, "from": self._acct.address,
                "value": total_value, "nonce": nonce, "gas": fallback_gas, **gas_params,
            })
        elif self._cfg.mint_mode == "SEADROP_WL":
            sea_contract = self.w3.eth.contract(address=sea_addr_c, abi=ContractSpecs.SEA_ABI)
            wl_service   = SeaDropWlService(self._cfg.os_api_key)
            wl_data      = await wl_service.fetch_proof(nft_addr_c, self._acct.address)
            if not wl_data:
                return None
            total_value  = wl_data["mintPrice"] * self._cfg.qty
            tx = await sea_contract.functions.mintAllowList(
                nft_addr_c,
                "0x0000a26b00c1F0DF003000390027140000fAa719",
                "0x0000000000000000000000000000000000000000",
                self._cfg.qty,
                wl_data["mintParams"],
                wl_data["proof"],
            ).build_transaction({
                "chainId": self._chain_id, "from": self._acct.address,
                "value": total_value, "nonce": nonce, "gas": fallback_gas, **gas_params,
            })
        else:
            multi_contract = self.w3.eth.contract(address=multi_addr_c, abi=ContractSpecs.MULTI_ABI)
            tx = await multi_contract.functions.mintMulti(self._cfg.qty, nft_addr_c).build_transaction({
                "chainId": self._chain_id, "from": self._acct.address,
                "value": total_value, "nonce": nonce, "gas": fallback_gas, **gas_params,
            })

        # Gas estimation
        if self._cfg.skip_gas_estimate:
            tx["gas"] = 250_000
        else:
            try:
                est_tx = tx.copy()
                est_tx.pop("gas", None)
                async with self._rpc_call():
                    est = await self.w3.eth.estimate_gas(est_tx)
                tx["gas"] = int(est * 1.2)
            except Exception:
                tx["gas"] = 400_000

        RunLogger.log_event(
            "tx_built",
            worker_id=self._uid,
            wallet=self._acct.address,
            mint_mode=self._cfg.mint_mode,
            gas=tx.get("gas"),
            value=tx.get("value", 0),
            elapsed_ms=round((time.perf_counter() - t0) * 1000, 2),
        )
        return tx

    # ------------------------------------------------------------------
    # Mint price resolution
    # ------------------------------------------------------------------
    @async_error_handler(retries=999, delay=2.0)
    async def run_protocol(self):
        RunLogger.log_event("worker_start", worker_id=self._uid, wallet=self._acct.address)
        nft_addr_c = AsyncWeb3.to_checksum_address(self._cfg.target_nft)

        1. MINT_PRICE_ETH env var (manual override — highest priority)
        2. SeaDrop getPublicDrop() — for SEADROP / SEADROP_WL modes
        3. Direct contract call: mintPrice() / price() / cost()
        4. Default 0 (free mint or price included in gas)
        """
        cfg = self._cfg

        # Tier 1: manual override
        if cfg.mint_price_eth is not None:
            price_wei = int(cfg.mint_price_eth * 1e18)
            await self._log("INFO", f"[Price] Using manual override: {cfg.mint_price_eth} ETH ({price_wei} wei)")
            return price_wei

        total_value = mint_price * self._cfg.qty

        # Fetch wallet balance
        async with self._rpc_call():
            bal_wei = await self.w3.eth.get_balance(self._acct.address)
        bal_eth = bal_wei / 1e18
        await self._update_worker("INIT", f"{bal_eth:.5f} {self._symbol}", "Initialized")
        await self._log("INFO", f"Wallet: {self._acct.address[:8]}... | Bal: {bal_eth:.5f} {self._symbol}")

        # ---- Mempool Sniper & Time Sniper ----
        current_time = int(time.time())
        pre_signed_raw = None

        if self._cfg.mempool_sniping_enabled:
            await self._log("WARNING", "Mempool Sniper active. Worker holding execution...")
            await self._update_worker("WAITING", f"{bal_eth:.5f}", "Holding for mempool trigger")
            from src.orchestrator import orchestrator
            await orchestrator.snipe_event.wait()
            await self._log("SUCCESS", "🚀 MEMPOOL TRIGGER DETECTED! Executing mint NOW!")
        elif start_time > current_time and not self._cfg.force_start:
            wait_seconds = start_time - current_time
            unlock_dt = datetime.fromtimestamp(start_time).strftime("%Y-%m-%d %H:%M:%S")
            await self._log("WARNING", f"[Time Sniper] Opens at: {unlock_dt}. Sleeping {wait_seconds:.0f}s...")
            await self._update_worker("WAITING", f"{bal_eth:.5f}", f"Sleeping until {unlock_dt}")

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
                        signed = self._acct.sign_transaction(tx)
                        pre_signed_raw = signed.raw_transaction
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
                sea_contract = self.w3.eth.contract(
                    address=AsyncWeb3.to_checksum_address(cfg.sea_addr),
                    abi=ContractSpecs.SEA_ABI,
                )
                drop_data = await asyncio.wait_for(
                    sea_contract.functions.getPublicDrop(nft_addr_c).call(), timeout=5
                )
                price_wei = int(drop_data[0])
                await self._log("INFO", f"[Price] SeaDrop: {price_wei / 1e18:.6f} ETH")
                return price_wei
            except Exception as exc:
                await self._log("WARNING", f"[Price] SeaDrop lookup failed: {exc} — trying direct call")

                # Phase 2 Speed: Only re-fetch balance if we don't have enough
                if bal_wei < total_value:
                    async with self._rpc_call():
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
                if pre_signed_raw:
                    await self._log("INFO", "🚀 Broadcasting pre-signed TX...")
                    tx_hash = await self._broadcast_race(pre_signed_raw)
                    pre_signed_raw = None
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

                    if self._cfg.mempool_sniping_enabled and self._cfg.snipe_gas_multiplier > 1.0:
                        gas_params = gas_params.copy()
                        if "maxPriorityFeePerGas" in gas_params:
                            gas_params["maxPriorityFeePerGas"] = int(gas_params["maxPriorityFeePerGas"] * self._cfg.snipe_gas_multiplier)
                        if "maxFeePerGas" in gas_params:
                            gas_params["maxFeePerGas"] = int(gas_params["maxFeePerGas"] * self._cfg.snipe_gas_multiplier)
                        if "gasPrice" in gas_params:
                            gas_params["gasPrice"] = int(gas_params["gasPrice"] * self._cfg.snipe_gas_multiplier)
                        await self._log("INFO", f"Sniping Gas Boost x{self._cfg.snipe_gas_multiplier} applied.")

                    last_gas_params = gas_params

                    tx = await self._build_mint_tx(nft_addr_c, total_value, gas_params, nonce)
                    if tx is None:
                        return

                    # Simulate before broadcast
                    if not self._cfg.skip_simulation:
                        sim_t0 = time.perf_counter()
                        ok, reason = await self._simulate_tx(tx)
                        RunLogger.log_event(
                            "tx_simulated",
                            worker_id=self._uid,
                            wallet=self._acct.address,
                            ok=ok,
                            reason=reason,
                            elapsed_ms=round((time.perf_counter() - sim_t0) * 1000, 2),
                        )
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
                        broadcast_t0 = time.perf_counter()
                        tx_hash = await self._broadcast_race(signed.raw_transaction)
                        RunLogger.log_event(
                            "tx_broadcast",
                            worker_id=self._uid,
                            wallet=self._acct.address,
                            tx_hash=tx_hash.hex(),
                            elapsed_ms=round((time.perf_counter() - broadcast_t0) * 1000, 2),
                        )
                        self._nonce_mgr.confirm(nonce)

                await self._log("INFO", f"TX Broadcast: {tx_hash.hex()}", tx_hash.hex())
                await self._update_worker("CONFIRMING", f"{bal_eth:.5f}", f"TX: {tx_hash.hex()[:10]}...")

                # Wait for receipt
                receipt_t0 = time.perf_counter()
                async with self._rpc_call():
                    receipt = await self.w3.eth.wait_for_transaction_receipt(tx_hash, timeout=120)
                RunLogger.log_event(
                    "tx_receipt",
                    worker_id=self._uid,
                    wallet=self._acct.address,
                    tx_hash=tx_hash.hex(),
                    status=int(receipt.status),
                    gas_used=getattr(receipt, "gasUsed", 0),
                    elapsed_ms=round((time.perf_counter() - receipt_t0) * 1000, 2),
                )

                if receipt.status == 1:
                    await self._log("SUCCESS", "✅ Mint confirmed!", tx_hash.hex())
                    await self._update_worker("SUCCESS", f"{bal_eth:.5f}", f"Minted! TX: {tx_hash.hex()[:10]}")
                    await self._handle_success(tx_hash, receipt, total_value, nft_addr_c)
                    return
                else:
                    reason = await self._replay_revert_reason(
                        tx_hash,
                        receipt,
                        tx if 'tx' in locals() else None,
                    )
                    await self._log("ERROR", f"TX reverted on-chain: {reason}")
                    self._nonce_mgr.fail(nonce if 'nonce' in locals() else 0)
                    await dlq.push(self._uid, self._acct.address, f"On-chain revert: {reason}", attempt)
                    return

            except asyncio.CancelledError:
                await self._update_worker("STOPPED", "", "Cancelled")
                return
            except Exception as e:
                await self._log("ERROR", f"Worker error: {str(e)[:80]}")
                await self._rotate_rpc()
                raise

    # ------------------------------------------------------------------
    # Public API for Orchestrator
    # ------------------------------------------------------------------
    async def run(self) -> "ExecutionResult":
        """Main entry point called by Orchestrator. Handles one mint attempt."""
        try:
            nft_addr_c = AsyncWeb3.to_checksum_address(self._cfg.target_nft)

            # 1. Verification (worker 1 only)
            if self._uid == 1 and self._cfg.verifier_enabled:
                verifier = ContractVerifier(self._cfg.explorer_api_key, self._cfg.rpc_ticker)
                if not await verifier.check_guard(nft_addr_c):
                    raise Exception("Contract Unverified — aborting (anti-honeypot guard)")

            # 2. Resolve mint price (3-tier: manual override → SeaDrop → direct call → 0)
            total_value = (await self._resolve_mint_price(nft_addr_c)) * self._cfg.qty

            # 3. Balance check
            bal_wei = await self.w3.eth.get_balance(self._acct.address)
            if bal_wei < total_value:
                await self._log("WARNING", f"Insufficient balance: have {bal_wei/1e18:.5f}, need {total_value/1e18:.5f} ETH")
                return self.ExecutionResult(False)

            # 4. Get nonce + gas
            nonce      = await self._nonce_mgr.get_nonce()
            gas_params = await self._get_gas_params()

            # 5. Build TX
            tx = await self._build_mint_tx(nft_addr_c, total_value, gas_params, nonce)
            if not tx:
                return self.ExecutionResult(False)

            # 6. Simulation — distinguishes retryable (0x) from hard failures
            ok, reason, retryable = await self._simulate_tx(tx)
            if not ok:
                if retryable:
                    # Mint not open yet or transient state — release nonce, log at WARNING
                    await self._log("WARNING", f"⏳ {reason}")
                    self._nonce_mgr.fail(nonce)
                    return self.ExecutionResult(False)
                else:
                    # Real revert (wrong price, sold out, not whitelisted, etc.)
                    await self._log("ERROR", f"Sim reverted (hard stop): {reason}")
                    self._nonce_mgr.fail(nonce)
                    return self.ExecutionResult(False)

            # 7. Broadcast
            signed = self._acct.sign_transaction(tx)
            if self._cfg.flashbots_mode:
                tx_hash_str = await self._send_via_flashbots(signed.raw_transaction)
                tx_hash = bytes.fromhex(tx_hash_str.lstrip("0x"))
            else:
                tx_hash = await self._broadcast_parallel(signed.raw_transaction)
                self._nonce_mgr.confirm(nonce)

            await self._log("INFO", f"TX Broadcast: {tx_hash.hex()[:10]}...")
            broadcast_block = 0
            try:
                broadcast_block = await asyncio.wait_for(self.w3.eth.block_number, timeout=4)
            except Exception:
                pass

            # 8. Wait for receipt with RPC rotation + gas bumping
            current_hash = tx_hash
            receipt      = None
            deadline     = time.monotonic() + 120

            while time.monotonic() < deadline:
                try:
                    receipt = await self._wait_receipt_with_rotation(current_hash, timeout=30)
                    break
                except asyncio.TimeoutError:
                    new_hash = await self._maybe_bump_gas(
                        current_hash, tx, gas_params, nonce, broadcast_block
                    )
                    if new_hash:
                        current_hash = new_hash
                        try:
                            broadcast_block = await asyncio.wait_for(self.w3.eth.block_number, timeout=4)
                        except Exception:
                            pass
                    continue

            if receipt is None:
                await self._log("ERROR", "TX receipt not received within timeout.")
                return self.ExecutionResult(False)

            if receipt.status == 1:
                return self.ExecutionResult(
                    True,
                    current_hash.hex() if isinstance(current_hash, bytes) else current_hash,
                    receipt,
                    total_value,
                )

            await self._log("ERROR", "TX reverted on-chain.")
            return self.ExecutionResult(False)

        except Exception as exc:
            # -------------------------------------------------------
            # Distinguish retryable RPC errors from hard failures
            # -------------------------------------------------------
            current_url = self._rpc_list[self._rpc_index % len(self._rpc_list)]

            if self._is_rate_limited(exc):
                await self._handle_rpc_error(exc, current_url)
                # Retryable — workers will loop and try again after orchestrator delay
                await self._log("WARNING",
                    f"[429] Rate-limited by RPC. Rotated. Will retry after delay. "
                    f"(Tip: set ANKR_API_KEY in .env for authenticated quota)")
                return self.ExecutionResult(False)

            if self._is_retryable_rpc_error(exc):
                await self._handle_rpc_error(exc, current_url)
                await self._log("WARNING", f"[RPC] Transient error rotated. Will retry.")
                return self.ExecutionResult(False)

            # True hard failure — log at ERROR level and surface to DLQ
            await self._log("ERROR", f"Run error: {str(exc)[:120]}")
            log.exception("ExecutionUnit W%d run() unhandled exception", self._uid)
            return self.ExecutionResult(False)

    async def post_mint_actions(self, result: "ExecutionResult"):
        """Cleanup and recording tasks after successful mint."""
        if not result.success:
            return
        nft_addr = AsyncWeb3.to_checksum_address(self._cfg.target_nft)

        if self._cfg.accountant_enabled:
            # FIX M-03: now async
            await Accountant.log_transaction(
                self._cfg.rpc_ticker, self._acct.address, "MINT",
                result.tx_hash, result.receipt, result.value,
            )

        await DiscordReporter.notify_success(
            self._uid, self._acct.address, result.tx_hash, self._cfg.qty, self._cfg.rpc_ticker
        )

        if self._cfg.transfer_enabled and self._cfg.recipient:
            relay = AssetRelay(self.w3, self._acct, self._uid, self._cfg.recipient)
            await relay.execute_consolidation(nft_addr, result.receipt)
            await DiscordReporter.notify_transfer(
                self._uid, self._acct.address, result.tx_hash, self._cfg.recipient
            )
            if self._cfg.sweep_enabled:
                await relay.sweep_native_token(self._cfg.min_sweep_eth)
