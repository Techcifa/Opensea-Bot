"""
NonceManager — per-wallet nonce tracker that auto-heals gaps and handles reorgs.
"""

import asyncio
from web3 import AsyncWeb3


class NonceManager:
    def __init__(self, w3: AsyncWeb3, address: str, worker_id: int):
        self._w3 = w3
        self._address = address
        self._uid = worker_id
        self._nonce: int | None = None
        self._pending: set[int] = set()
        self._lock = asyncio.Lock()

    async def get_nonce(self) -> int:
        async with self._lock:
            if self._nonce is None:
                await self._sync()
            nonce = self._nonce
            self._pending.add(nonce)
            self._nonce += 1
            return nonce

    def confirm(self, nonce: int):
        """Call after TX is confirmed on-chain."""
        self._pending.discard(nonce)

    def fail(self, nonce: int):
        """Call after TX is known to have failed — free the slot."""
        self._pending.discard(nonce)
        # Roll back local nonce if this was the most recent one
        if self._nonce is not None and nonce < self._nonce:
            # Will force a re-sync on next get_nonce
            self._nonce = None

    async def heal(self):
        """Force re-sync with on-chain nonce (call after suspected reorg)."""
        async with self._lock:
            await self._sync()

    async def _sync(self):
        self._nonce = await self._w3.eth.get_transaction_count(self._address)
        self._pending.clear()
