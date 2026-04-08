"""
ContractVerifier — checks that a smart contract is verified on Etherscan/Basescan
before allowing any transactions (anti-honeypot guard).

FIX H-02: Changed all exception paths from "fail open" (return True) to
           "fail closed" (return False). Unknown state = unsafe.
"""

import asyncio
import logging
import aiohttp

log = logging.getLogger(__name__)


class ContractVerifier:
    API_MAP = {
        "ETH":  "https://api.etherscan.io/api",
        "BASE": "https://api.basescan.org/api",
        "OP":   "https://api-optimistic.etherscan.io/api",
        "ARB":  "https://api.arbiscan.io/api",
        "POLY": "https://api.polygonscan.com/api",
        "BSC":  "https://api.bscscan.com/api",
        "AVAX": "https://api.snowtrace.io/api",
    }

    def __init__(self, api_key: str, network_ticker: str):
        self.api_key = api_key
        self.network = network_ticker
        self.base_url = self.API_MAP.get(network_ticker)

    async def is_verified(self, contract_address: str) -> bool:
        if not self.base_url:
            log.info("ContractVerifier: unsupported network %s — skipping check", self.network)
            return True   # genuinely unsupported network, not a failure

        if not self.api_key:
            log.warning("ContractVerifier: no EXPLORER_API_KEY — skipping guard (set key to enable)")
            return True   # operator chose to skip, not a failure

        params = {
            "module":  "contract",
            "action":  "getabi",
            "address": contract_address,
            "apikey":  self.api_key,
        }
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    self.base_url, params=params,
                    timeout=aiohttp.ClientTimeout(total=8)
                ) as resp:
                    data = await resp.json()
                    if data.get("status") == "1":
                        return True
                    result_msg = str(data.get("result", "")).lower()
                    if "not verified" in result_msg:
                        log.warning("ContractVerifier: %s is NOT verified on %s", contract_address, self.network)
                        return False
                    # API returned unexpected shape — treat as unsafe
                    log.warning("ContractVerifier: unexpected API response for %s: %s", contract_address, data)
                    return False    # FIX H-02: was return True (fail-open)
        except Exception as exc:
            # FIX H-02: Network/API error → fail CLOSED (was fail open)
            log.error("ContractVerifier: network error checking %s: %s — refusing to proceed", contract_address, exc)
            return False

    async def check_guard(self, contract_address: str) -> bool:
        return await self.is_verified(contract_address)
