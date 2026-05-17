import aiohttp
import asyncio
import time

class SeaDropWlService:
    _cache: dict[tuple[str, str], tuple[float, dict | None]] = {}
    _locks: dict[tuple[str, str], asyncio.Lock] = {}
    _ttl_seconds = 600

    def __init__(self, api_key: str):
        self._api_key = api_key
        self._base_url = "https://api.opensea.io/api/v2"

    async def fetch_proof(self, contract_address: str, wallet_address: str) -> dict | None:
        if not self._api_key:
            raise ValueError("OpenSea API Key is required for SEADROP_WL mode")

        cache_key = (contract_address.lower(), wallet_address.lower())
        cached = self._cache.get(cache_key)
        now = time.time()
        if cached and now - cached[0] < self._ttl_seconds:
            return cached[1]

        lock = self._locks.setdefault(cache_key, asyncio.Lock())
        async with lock:
            cached = self._cache.get(cache_key)
            now = time.time()
            if cached and now - cached[0] < self._ttl_seconds:
                return cached[1]

            proof = await self._fetch_proof_uncached(contract_address, wallet_address)
            self._cache[cache_key] = (now, proof)
            return proof

    async def prefetch_many(self, contract_address: str, wallet_addresses: list[str], concurrency: int = 8) -> dict[str, dict | None]:
        semaphore = asyncio.Semaphore(concurrency)
        results: dict[str, dict | None] = {}

        async def _one(wallet: str):
            async with semaphore:
                results[wallet] = await self.fetch_proof(contract_address, wallet)

        await asyncio.gather(*(_one(wallet) for wallet in wallet_addresses))
        return results

    @classmethod
    def clear_cache(cls):
        cls._cache.clear()
        cls._locks.clear()

    async def _fetch_proof_uncached(self, contract_address: str, wallet_address: str) -> dict | None:
        url = f"{self._base_url}/drops/{contract_address}/allowlist?wallet={wallet_address}"
        headers = {
            "X-API-KEY": self._api_key,
            "accept": "application/json"
        }

        async with aiohttp.ClientSession() as session:
            try:
                async with session.get(url, headers=headers, timeout=10) as resp:
                    if resp.status in (403, 401):
                        raise Exception("OpenSea API Key invalid or rate limited (403/401)")
                    if resp.status == 404:
                        return None  # Wallet not eligible
                    
                    resp.raise_for_status()
                    data = await resp.json()
                    
                    # Assuming shape is { "merkleProof": [...], "mintParams": {...} }
                    # OpenSea API might wrap it. Let's gracefully extract.
                    proof = data.get("merkleProof") or data.get("proof")
                    params = data.get("mintParams")
                    
                    if proof is None or params is None:
                        raise Exception("Unexpected OS API response shape: missing proof or mintParams")
                        
                    # Tuple structure expected by mintAllowList ABI:
                    # (mintPrice, maxTotalMintableByWallet, startTime, endTime, dropStageIndex, maxTokenSupplyForStage, feeBps, restrictFeeRecipients)
                    mint_tuple = (
                        int(params.get("mintPrice", 0)),
                        int(params.get("maxTotalMintableByWallet", 0)),
                        int(params.get("startTime", 0)),
                        int(params.get("endTime", 0)),
                        int(params.get("dropStageIndex", 0)),
                        int(params.get("maxTokenSupplyForStage", 0)),
                        int(params.get("feeBps", 0)),
                        bool(params.get("restrictFeeRecipients", False))
                    )
                    
                    return {
                        "proof": proof,
                        "mintParams": mint_tuple,
                        "mintPrice": int(params.get("mintPrice", 0))
                    }

            except asyncio.TimeoutError:
                raise Exception("OpenSea API fetch timed out")
