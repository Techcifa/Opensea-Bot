import asyncio
import json
import logging
import time
import websockets
from eth_utils import keccak
from web3 import AsyncWeb3, AsyncHTTPProvider
from src.config.settings import ConfigurationManager, ContractSpecs

logger = logging.getLogger("MempoolSniper")

class MempoolSniper:
    def __init__(self, cfg: ConfigurationManager, rpc_list: list[str], trigger_callback):
        self._cfg = cfg
        self._rpc_list = rpc_list
        self._trigger_callback = trigger_callback
        self._task: asyncio.Task | None = None
        self._running = False
        self._triggered = False

    def start(self):
        if not self._cfg.mempool_sniping_enabled:
            return
        self._running = True
        self._triggered = False
        self._task = asyncio.create_task(self._run_loop())
        logger.info("Mempool Sniper started.")

    def stop(self):
        self._running = False
        if self._task:
            self._task.cancel()
        logger.info("Mempool Sniper stopped.")

    async def _run_loop(self):
        wss_urls = [url for url in self._rpc_list if url.startswith("wss://") or url.startswith("ws://")]
        
        if not wss_urls:
            logger.warning("No WebSocket (WSS) endpoints found. Falling back to High-Frequency HTTP polling.")
            await self._run_http_polling()
            return

        active_wss = wss_urls[0]
        retry_delay = 2.0
        
        while self._running and not self._triggered:
            try:
                async with websockets.connect(active_wss, open_timeout=5, close_timeout=2) as ws:
                    logger.info(f"Connected to WSS mempool stream: {active_wss[:30]}...")
                    
                    # 1. Detect if Alchemy and use filtered subscription
                    is_alchemy = "alchemy.com" in active_wss
                    if is_alchemy:
                        subscribe_msg = {
                            "jsonrpc": "2.0",
                            "id": 1,
                            "method": "eth_subscribe",
                            "params": [
                                "alchemy_pendingTransactions",
                                {"toAddress": self._cfg.target_nft}
                            ]
                        }
                    else:
                        subscribe_msg = {
                            "jsonrpc": "2.0",
                            "id": 1,
                            "method": "eth_subscribe",
                            "params": ["newPendingTransactions"]
                        }
                        
                    await ws.send(json.dumps(subscribe_msg))
                    resp = await ws.recv()
                    sub_id = json.loads(resp).get("result")
                    logger.info(f"Mempool subscription successful (ID: {sub_id})")
                    
                    # Create a temporary AsyncWeb3 instance for checking transaction details
                    temp_w3 = AsyncWeb3(AsyncWeb3.WebSocketProvider(active_wss))

                    while self._running and not self._triggered:
                        msg = await ws.recv()
                        data = json.loads(msg)
                        
                        if "params" in data and data["params"].get("subscription") == sub_id:
                            tx_data = data["params"]["result"]
                            
                            # If Alchemy, tx_data is the full transaction dict!
                            if is_alchemy and isinstance(tx_data, dict):
                                await self._evaluate_tx(tx_data)
                            # If standard, tx_data is just the tx hash
                            elif isinstance(tx_data, str):
                                # Asynchronously fetch transaction details to avoid blocking
                                asyncio.create_task(self._fetch_and_evaluate_tx(temp_w3, tx_data))
                                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Mempool WSS connection lost: {e}. Reconnecting in {retry_delay}s...")
                await asyncio.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, 60.0)

    async def _fetch_and_evaluate_tx(self, w3: AsyncWeb3, tx_hash: str):
        try:
            tx = await w3.eth.get_transaction(tx_hash)
            if tx:
                await self._evaluate_tx(dict(tx))
        except Exception:
            pass

    async def _evaluate_tx(self, tx: dict):
        if self._triggered:
            return

        to_addr = tx.get("to")
        from_addr = tx.get("from")
        input_data = tx.get("input", "0x")
        
        if not to_addr or not from_addr:
            return

        # Case-insensitive address match
        target_nft = self._cfg.target_nft.lower()
        to_addr = to_addr.lower()
        from_addr = from_addr.lower()
        
        sea_addr = self._cfg.sea_addr.lower()
        input_lc = input_data.lower()
        target_word = target_nft.removeprefix("0x").rjust(64, "0")

        if to_addr != target_nft and not (to_addr == sea_addr and target_word in input_lc):
            return

        # Optional owner restriction
        if self._cfg.contract_owner:
            owner = self._cfg.contract_owner.lower()
            if from_addr != owner:
                return

        # Optional method filter
        if self._cfg.snipe_target_method:
            target_method = self._cfg.snipe_target_method.lower()
            # If target method is specified as method name, convert or check prefix
            # E.g. if developer set flipMintState, check input prefix
            if target_method.startswith("0x"):
                if not input_data.lower().startswith(target_method):
                    return
            else:
                selector = "0x" + keccak(text=f"{target_method}()")[:4].hex()
                if not input_data.lower().startswith(selector):
                    return

        # Trigger!
        self._triggered = True
        logger.info(f"🎯 SNIPED PENDING DEVELOPER TRANSACTION! Hash: {tx.get('hash')}")
        await self._trigger_callback(tx)

    async def _run_http_polling(self):
        """Fallback rapid state polling if WSS is unavailable."""
        # Build rapid contract checker
        temp_w3 = AsyncWeb3(AsyncHTTPProvider(self._rpc_list[0]))
        nft_addr = AsyncWeb3.to_checksum_address(self._cfg.target_nft)
        
        # Try to read SeaDrop public drop state
        sea_contract = temp_w3.eth.contract(
            address=AsyncWeb3.to_checksum_address(self._cfg.sea_addr),
            abi=ContractSpecs.SEA_ABI,
        )
        
        logger.info("High-Frequency HTTP Polling active (interval: 250ms)...")
        while self._running and not self._triggered:
            try:
                drop_data = await sea_contract.functions.getPublicDrop(nft_addr).call()
                start_time = int(drop_data[1])
                current_time = int(time.time())
                
                # If timestamp is reached, trigger
                if start_time <= current_time and start_time > 0:
                    self._triggered = True
                    logger.info("🎯 Snipe Triggered via HTTP State Polling!")
                    await self._trigger_callback(None)
                    break
            except Exception:
                pass
            await asyncio.sleep(0.25)
