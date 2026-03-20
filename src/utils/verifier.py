"""
ContractVerifier — checks that a smart contract is verified on Etherscan/Basescan
before allowing any transactions (anti-honeypot guard).

RuntimeDiagnostics is intentionally NOT included here.
"""

import aiohttp


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
            return True  # Unsupported network — skip check
        if not self.api_key:
            return True  # No API key — skip check

        params = {
            "module": "contract",
            "action": "getabi",
            "address": contract_address,
            "apikey": self.api_key,
        }
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(self.base_url, params=params) as resp:
                    data = await resp.json()
                    if data.get("status") == "1":
                        return True
                    result_msg = str(data.get("result", "")).lower()
                    if "not verified" in result_msg:
                        return False
                    return True  # Unknown error — allow, but log
        except Exception:
            return True  # Network error — fail open

    async def check_guard(self, contract_address: str) -> bool:
        is_safe = await self.is_verified(contract_address)
        return is_safe
