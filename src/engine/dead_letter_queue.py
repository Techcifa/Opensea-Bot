"""
DeadLetterQueue — stores failed mint jobs for inspection and retry.

FIX H-07: Unified field name 'retry_count' everywhere (was 'attempt' in
           FailedJob but 'retry_count' in the Pydantic DLQJob response model).
"""

import asyncio
import time
from dataclasses import dataclass, field


@dataclass
class FailedJob:
    worker_id: str        # "W1", "W2", etc. (was typed int — now str to match broadcast)
    wallet: str
    error: str
    timestamp: float = field(default_factory=time.time)
    retry_count: int = 1  # FIX H-07: renamed from 'attempt' → 'retry_count'
    retrying: bool = False
    traceback: str = ""


class DeadLetterQueue:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._jobs: list[FailedJob] = []
            cls._instance._lock = asyncio.Lock()
        return cls._instance

    async def push(self, *args, **kwargs):
        """
        Accepts either:
          - push(worker_id, wallet, error, retry_count=1)
          - push({"worker_id": ..., "error": ..., "wallet": ..., ...})
        """
        async with self._lock:
            if args and isinstance(args[0], dict):
                data = args[0]
                job = FailedJob(
                    worker_id=str(data.get("worker_id", "Unknown")),
                    wallet=data.get("wallet", "N/A"),
                    error=data.get("error", "Unknown error")[:400],
                    timestamp=data.get("timestamp", time.time()),
                    retry_count=data.get("retry_count", data.get("attempt", 1)),  # accept old key too
                    traceback=data.get("traceback", ""),
                )
            else:
                worker_id = str(args[0]) if len(args) > 0 else str(kwargs.get("worker_id", 0))
                wallet    = args[1] if len(args) > 1 else kwargs.get("wallet", "N/A")
                error     = args[2] if len(args) > 2 else kwargs.get("error", "Unknown")
                retry_count = args[3] if len(args) > 3 else kwargs.get("retry_count", 1)
                job = FailedJob(worker_id=worker_id, wallet=wallet, error=error, retry_count=retry_count)

            self._jobs.append(job)

    async def get_all(self) -> list[dict]:
        async with self._lock:
            return [
                {
                    "worker_id": j.worker_id,
                    "wallet": j.wallet[:8] + "..." + j.wallet[-4:] if len(j.wallet) > 12 else j.wallet,
                    "error": j.error,
                    "timestamp": j.timestamp,
                    "retry_count": j.retry_count,   # FIX H-07: unified name
                    "retrying": j.retrying,
                    "traceback": j.traceback,
                }
                for j in self._jobs
            ]

    async def clear(self):
        async with self._lock:
            self._jobs.clear()

    async def remove(self, worker_id: str, wallet: str):
        async with self._lock:
            self._jobs = [
                j for j in self._jobs
                if not (j.worker_id == str(worker_id) and j.wallet == wallet)
            ]

    async def size(self) -> int:
        async with self._lock:
            return len(self._jobs)


dlq = DeadLetterQueue()
