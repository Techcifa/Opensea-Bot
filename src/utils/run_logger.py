"""
Structured JSONL run logger for latency and failure analysis.
"""

import json
import os
import time
from datetime import datetime


class RunLogger:
    @staticmethod
    def log_event(event: str, **fields):
        os.makedirs("runs", exist_ok=True)
        path = os.path.join("runs", f"run_{datetime.utcnow().strftime('%Y%m%d')}.jsonl")
        payload = {
            "ts": time.time(),
            "event": event,
            **fields,
        }
        with open(path, "a", encoding="utf-8") as f:
            f.write(json.dumps(payload, default=str) + "\n")
