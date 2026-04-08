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
from typing import Callable, Any

from web3 import AsyncWeb3, AsyncHTTPProvider
from eth_account import Account

from src.config.settings import ContractSpecs, ConfigurationManager, NETWORKS
from src.utils.core import async_error_handler  # noqa: F401 — available for future use
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
        url      = self._rpc_list[self._rpc_index % len(self._rpc_list)]
        self.w3  = _get_cached_w3(url, self._proxy_url)

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
            await self.w3.eth.call({
                "from":  self._acct.address,
                "to":    tx_data["to"],
                "data":  tx_data.get("data", "0x"),
                "value": tx_data.get("value", 0),
            })
            return True, "", False
        except Exception as exc:
            raw = str(exc)
            reason = self._decode_revert(raw)

            # Empty revert (0x) = contract gave no error string.
            # 99% of the time this means "mint not open yet" or a Solidity
            # require(false) with no message.  Treat as retryable.
            if reason in ("0x", ""):
                return False, "Mint not open yet (empty revert 0x) — will retry", True

            # Real revert with a message = configuration error, wrong price, sold out, etc.
            return False, reason[:200], False

    @staticmethod
    def _decode_revert(raw: str) -> str:
        """Try to extract a human-readable revert message from a web3 exception string."""
        # web3.py wraps it as: ContractLogicError('execution reverted: Reason', '0x...')
        # or as a tuple string: ("execution reverted", "0x08c379a0...")
        import re

        # Try to extract the hex payload
        hex_match = re.search(r"0x[0-9a-fA-F]{8,}", raw)
        if not hex_match:
            # No hex data — check for plain text reason
            text_match = re.search(r"execution reverted:?\s*(.+)", raw, re.IGNORECASE)
            if text_match:
                return text_match.group(1).strip()
            # Truly empty
            return "0x"

        hex_data = hex_match.group(0)
        if hex_data in ("0x", ""):
            return "0x"

        # Standard Error(string) ABI encoding: selector 0x08c379a0
        if hex_data.startswith("0x08c379a0") and len(hex_data) > 10:
            try:
                payload = bytes.fromhex(hex_data[10:])
                # ABI-decoded: offset(32) + length(32) + string
                if len(payload) >= 64:
                    str_len = int.from_bytes(payload[32:64], "big")
                    msg = payload[64:64 + str_len].decode("utf-8", errors="replace")
                    return msg
            except Exception:
                pass

        # Panic(uint256): selector 0x4e487b71
        if hex_data.startswith("0x4e487b71") and len(hex_data) >= 74:
            try:
                code = int(hex_data[2 + 8 + 64 - 64:], 16)
                panic_codes = {
                    1: "assert failed", 17: "arithmetic overflow",
                    18: "division by zero", 32: "array out-of-bounds",
                    33: "invalid enum", 49: "empty storage", 50: "pop empty array",
                    65: "out of memory"
                }
                return f"Panic: {panic_codes.get(code, f'code {code}')}"
            except Exception:
                pass

        return raw[:120]

    # ------------------------------------------------------------------
    # FIX C-06: Receipt polling with RPC rotation
    # ------------------------------------------------------------------
    async def _wait_receipt_with_rotation(self, tx_hash: bytes, timeout: int = 120) -> Any:
        """
        Poll all healthy RPCs for the receipt, rotating on failure.
        Raises asyncio.TimeoutError if not confirmed within `timeout` seconds.
        """
        deadline      = time.monotonic() + timeout
        poll_interval = 2.0
        tx_hex        = tx_hash.hex() if isinstance(tx_hash, bytes) else tx_hash
        if not tx_hex.startswith("0x"):
            tx_hex = "0x" + tx_hex

        while time.monotonic() < deadline:
            rpcs = rpc_health.get_prioritized_rpcs(self._cfg.rpc_ticker)
            for url in rpcs:
                w3_tmp = _get_cached_w3(url, self._proxy_url)   # FIX C-03: use cache
                try:
                    receipt = await asyncio.wait_for(
                        w3_tmp.eth.get_transaction_receipt(tx_hex),
                        timeout=4.0,
                    )
                    if receipt is not None:
                        return receipt
                except Exception:
                    continue
            await asyncio.sleep(poll_interval)

        raise asyncio.TimeoutError(f"Receipt for {tx_hex[:12]}... not found after {timeout}s")

    # ------------------------------------------------------------------
    # FIX M-06: Gas bumping — resubmit if TX is stuck
    # ------------------------------------------------------------------
    async def _maybe_bump_gas(
        self,
        tx_hash: bytes,
        original_tx: dict,
        original_gas_params: dict,
        nonce: int,
        broadcast_block: int,
    ) -> bytes | None:
        """
        Check if TX is stuck after bump_after_blocks blocks.
        If so, resubmit with bumped gas params and the same nonce.
        Returns the new tx_hash if bumped, or None if confirmation seen.
        """
        cfg = self._cfg
        if cfg.bump_after_blocks <= 0:
            return None

        try:
            current_block = await asyncio.wait_for(self.w3.eth.block_number, timeout=4)
        except Exception:
            return None

        if current_block - broadcast_block < cfg.bump_after_blocks:
            return None

        # Check if already confirmed
        try:
            receipt = await asyncio.wait_for(
                self.w3.eth.get_transaction_receipt(tx_hash), timeout=4
            )
            if receipt is not None:
                return None   # Already mined
        except Exception:
            pass

        # Bump params and resubmit with the same nonce (replaces the old TX)
        attempt = max(1, current_block - broadcast_block - cfg.bump_after_blocks + 1)
        bumped  = await gas_oracle.bump_gas_params(original_gas_params, attempt)
        bumped  = self._cap_gas_params(bumped)

        bumped_tx = {**original_tx, **bumped, "nonce": nonce}
        try:
            signed   = self._acct.sign_transaction(bumped_tx)
            new_hash = await self._broadcast_parallel(signed.raw_transaction)
            await self._log("WARNING",
                f"[Gas Bump] Attempt {attempt}: bumped maxFee → "
                f"{bumped.get('maxFeePerGas', 0) / 1e9:.2f} Gwei | new TX: {new_hash.hex()[:10]}..."
            )
            return new_hash
        except Exception as exc:
            await self._log("WARNING", f"[Gas Bump] Resubmit failed: {exc}")
            return None

    # ------------------------------------------------------------------
    # FIX H-09: RPC Race Strategy — wait ALL_COMPLETED then pick first success
    # ------------------------------------------------------------------
    async def _broadcast_parallel(self, signed_raw_tx: bytes) -> bytes:
        rpcs    = rpc_health.get_prioritized_rpcs(self._cfg.rpc_ticker)
        targets = rpcs[:5]
        await self._log("INFO", f"🚀 [Race Strategy] Broadcasting to {len(targets)} nodes simultaneously...")

        async def _single_broadcast(url: str) -> tuple[bytes, str]:
            w3_node = _get_cached_w3(url, self._proxy_url)
            async with rpc_limiter.acquire(url):
                try:
                    tx_hash = await w3_node.eth.send_raw_transaction(signed_raw_tx)
                    if url in rpc_health.stats:
                        rpc_health.stats[url]["score"] = min(1.0, rpc_health.stats[url].get("score", 0.5) + 0.05)
                    rpc_limiter.report_success(url)
                    return tx_hash, url
                except Exception as exc:
                    # Penalise overloaded/rate-limited nodes immediately so they lose priority
                    if ExecutionUnit._is_rate_limited(exc) or ExecutionUnit._is_retryable_rpc_error(exc):
                        rpc_limiter.report_rate_limit(url)
                        if url in rpc_health.stats:
                            rpc_health.stats[url]["score"] = max(0.0, rpc_health.stats[url].get("score", 0.5) - 0.3)
                    raise

        tasks = [asyncio.create_task(_single_broadcast(url)) for url in targets]
        if not tasks:
            raise Exception("No healthy RPCs available for broadcast")

        # FIX H-09: Wait for ALL tasks (with short timeout), then pick first success.
        # Previously waited for FIRST_COMPLETED which cancelled tasks that may have
        # already accepted the TX — causing false failures and nonce re-use.
        done, pending = await asyncio.wait(tasks, timeout=10, return_when=asyncio.ALL_COMPLETED)
        # Cancel any stragglers (shouldn't be any with ALL_COMPLETED+timeout, but be safe)
        for p in pending:
            p.cancel()

        first_exc = None
        for task in done:
            try:
                tx_h, win_url = task.result()
                await self._log("SUCCESS", f"⚡ Node Won: {str(win_url)[:30]}...")
                return tx_h
            except Exception as exc:
                first_exc = exc
                continue

        raise Exception(f"All nodes failed to broadcast: {first_exc}")

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
        sea_addr_c   = AsyncWeb3.to_checksum_address(self._cfg.sea_addr)
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

        # Gas estimation with fallback
        try:
            est_tx = tx.copy()
            est_tx.pop("gas", None)
            est       = await self.w3.eth.estimate_gas(est_tx)
            tx["gas"] = max(21_000, int(est * self._cfg.gas_estimate_multiplier))
        except Exception:
            tx["gas"] = fallback_gas

        return tx

    # ------------------------------------------------------------------
    # Mint price resolution
    # ------------------------------------------------------------------
    async def _resolve_mint_price(self, nft_addr_c: str) -> int:
        """
        Attempt to resolve the mint price in wei using a 3-tier fallback:

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

        # Tier 2: SeaDrop getPublicDrop (works for SEADROP / SEADROP_WL)
        #  — skip in DIRECT/PROXY mode to avoid confusing log noise
        if cfg.mint_mode in ("SEADROP", "SEADROP_WL"):
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

        # Tier 3: common price getter functions on the NFT contract itself
        price_abi = [
            {"inputs": [], "name": "mintPrice",  "outputs": [{"type": "uint256"}], "stateMutability": "view", "type": "function"},
            {"inputs": [], "name": "price",       "outputs": [{"type": "uint256"}], "stateMutability": "view", "type": "function"},
            {"inputs": [], "name": "cost",        "outputs": [{"type": "uint256"}], "stateMutability": "view", "type": "function"},
            {"inputs": [], "name": "PRICE",       "outputs": [{"type": "uint256"}], "stateMutability": "view", "type": "function"},
            {"inputs": [], "name": "mintCost",   "outputs": [{"type": "uint256"}], "stateMutability": "view", "type": "function"},
        ]
        nft_contract = self.w3.eth.contract(address=nft_addr_c, abi=price_abi)
        for fn_name in ("mintPrice", "price", "cost", "PRICE", "mintCost"):
            try:
                fn = getattr(nft_contract.functions, fn_name)
                price_wei = await asyncio.wait_for(fn().call(), timeout=4)
                price_wei = int(price_wei)
                await self._log("INFO", f"[Price] {fn_name}(): {price_wei / 1e18:.6f} ETH")
                return price_wei
            except Exception:
                continue

        # Tier 4: assume free mint
        await self._log("INFO", "[Price] Could not determine mint price — assuming free mint (0 ETH). "
                                 "Set MINT_PRICE_ETH in .env if the contract requires payment.")
        return 0

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
