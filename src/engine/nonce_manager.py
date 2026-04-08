"""
NonceManager — per-wallet nonce tracker that auto-heals gaps and handles reorgs.

FIX C-04: _sync() now has proper error handling that preserves state on failure.
           Uses "pending" block tag so in-flight TXs are counted.
           fail() no longer clears pending set or corrupts state during concurrent use.
"""

import asyncio
import logging
from web3 import AsyncWeb3

log = logging.getLogger(__name__)


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
                await self._sync()   # raises on failure; caller handles
            nonce = self._nonce
            self._pending.add(nonce)
            self._nonce += 1
            return nonce

    def confirm(self, nonce: int):
        """Call after TX is confirmed on-chain."""
        self._pending.discard(nonce)

    def fail(self, nonce: int):
        """
        Call after TX is known to have definitively failed.

        FIX C-04: We no longer wipe self._nonce here. Setting it to None
        would force a _sync() on the next call which clears _pending —
        erasing knowledge of all other in-flight nonces and causing gaps.
        Instead we just discard the single failed slot; the next get_nonce()
        will continue from the local counter (which is ahead of on-chain for
        any still-pending TXs, which is correct).
        """
        self._pending.discard(nonce)

    async def heal(self):
        """
        Force re-sync with on-chain nonce (call after suspected reorg or
        full worker restart when no TXs are in-flight).
        """
        async with self._lock:
            self._pending.clear()   # safe only when called explicitly after stop
            await self._sync()

    async def _sync(self):
        """
        FIX C-04: Wrapped in try/except. On failure:
          - If we have a working local nonce → keep it (don't corrupt).
          - If we've never fetched a nonce → re-raise so the caller aborts.
        Uses "pending" block tag so mempool-accepted but unconfirmed TXs
        are already accounted for, preventing nonce reuse.
        """
        try:
            on_chain = await asyncio.wait_for(
                self._w3.eth.get_transaction_count(self._address, "pending"),
                timeout=6.0,
            )
            # If we have pending local nonces that are higher than on-chain,
            # the on-chain node hasn't seen them yet — keep the local counter.
            local_min = max(self._pending, default=0)
            self._nonce = max(on_chain, local_min)
            log.debug("W%s nonce synced → %d (on-chain pending: %d)", self._uid, self._nonce, on_chain)
        except Exception as exc:
            if self._nonce is not None:
                # Keep the existing local counter — it's better than corrupting state
                log.warning("W%s nonce sync failed (%s), keeping local counter %d", self._uid, exc, self._nonce)
            else:
                # First-ever sync — we have no fallback; let caller decide
                raise RuntimeError(f"W{self._uid} nonce fetch failed: {exc}") from exc
