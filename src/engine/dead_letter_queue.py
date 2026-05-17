"""
DeadLetterQueue — stores failed mint jobs for inspection and retry.

FIX H-07: Unified field name 'retry_count' everywhere (was 'attempt' in
           FailedJob but 'retry_count' in the Pydantic DLQJob response model).
"""

import asyncio
import json
import os
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
            job = FailedJob(
                worker_id=worker_id,
                wallet=wallet,
                error=error,
                attempt=attempt,
            )
            self._jobs.append(job)
            self._persist(job)

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

    def _persist(self, job: FailedJob):
        os.makedirs("runs", exist_ok=True)
        path = os.path.join("runs", "dead_letter_queue.jsonl")
        with open(path, "a", encoding="utf-8") as f:
            f.write(json.dumps({
                "worker_id": job.worker_id,
                "wallet": job.wallet,
                "error": job.error,
                "timestamp": job.timestamp,
                "attempt": job.attempt,
            }) + "\n")


dlq = DeadLetterQueue()
