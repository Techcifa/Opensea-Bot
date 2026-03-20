"""
DeadLetterQueue — stores failed mint jobs for inspection and retry.
"""

import asyncio
import time
from dataclasses import dataclass, field


@dataclass
class FailedJob:
    worker_id: int
    wallet: str
    error: str
    timestamp: float = field(default_factory=time.time)
    attempt: int = 1
    retrying: bool = False


class DeadLetterQueue:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._jobs: list[FailedJob] = []
            cls._instance._lock = asyncio.Lock()
        return cls._instance

    async def push(self, worker_id: int, wallet: str, error: str, attempt: int = 1):
        async with self._lock:
            self._jobs.append(FailedJob(
                worker_id=worker_id,
                wallet=wallet,
                error=error,
                attempt=attempt,
            ))

    async def get_all(self) -> list[dict]:
        async with self._lock:
            return [
                {
                    "worker_id": j.worker_id,
                    "wallet": j.wallet[:8] + "..." + j.wallet[-4:],
                    "error": j.error,
                    "timestamp": j.timestamp,
                    "attempt": j.attempt,
                    "retrying": j.retrying,
                }
                for j in self._jobs
            ]

    async def clear(self):
        async with self._lock:
            self._jobs.clear()

    async def remove(self, worker_id: int, wallet: str):
        async with self._lock:
            self._jobs = [
                j for j in self._jobs
                if not (j.worker_id == worker_id and j.wallet == wallet)
            ]


dlq = DeadLetterQueue()
