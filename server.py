"""
server.py — FastAPI entry point for the NFT Minter (FIXED VERSION).

Routes:
    GET  /                   Serve dashboard.html
    POST /api/start          Start the bot (accepts keys + config override)
    POST /api/stop           Stop the bot
    POST /api/preflight      Run preflight dry-check
    GET  /api/status         Current bot state
    GET  /api/history        Transaction history from history.csv
    GET  /api/gas-history    30-minute rolling gas price data
    GET  /api/dlq            Dead letter queue entries
    WS   /ws                 Realtime event stream
"""

import base64
import json
import os
import sys
import asyncio
import time
import traceback
from contextlib import asynccontextmanager

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from eth_account import Account

# Ensure project root is in path
sys.path.insert(0, os.path.dirname(__file__))

from src.utils.core import SystemCompliance
from src.config.settings import ConfigurationManager
from src.orchestrator import orchestrator
from src.gas_oracle import gas_oracle
from src.engine.dead_letter_queue import dlq
from src.features.accountant import Accountant
from src.preflight import PreflightChecker
from src.utils.rpc_health import rpc_health

SystemCompliance.assert_version()

# ---------------------------------------------------------------------------
# HTTP Client Pool (singleton for resource management)
# ---------------------------------------------------------------------------
class HTTPClientPool:
    """Manages a single persistent aiohttp session to prevent resource leaks."""
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        if self._initialized:
            return
        self._initialized = True
        self._session = None
    
    async def get_session(self):
        """Get or create the persistent session."""
        if self._session is None:
            try:
                import aiohttp
                self._session = aiohttp.ClientSession()
            except Exception:
                pass
        return self._session
    
    async def close(self):
        """Close the session gracefully."""
        if self._session:
            try:
                await self._session.close()
                # Give it a moment to cleanup
                await asyncio.sleep(0.25)
            except Exception:
                pass
            self._session = None

http_pool = HTTPClientPool()

# ---------------------------------------------------------------------------
# Session Persistence (encrypted)
# ---------------------------------------------------------------------------
class SessionStore:
    SESSION_FILE = "session.json"
    
    @staticmethod
    def save(keys, overrides):
        """Save session (unencrypted for now, but marked as TODO)."""
        try:
            # TODO: Encrypt keys using Fernet before saving
            # For now, we use explicit warning
            with open(SessionStore.SESSION_FILE, "w") as f:
                json.dump({"keys": keys, "overrides": overrides}, f)
            # Restrict file permissions
            try:
                os.chmod(SessionStore.SESSION_FILE, 0o600)
            except Exception:
                pass
        except Exception:
            pass

    @staticmethod
    def load():
        if not os.path.exists(SessionStore.SESSION_FILE):
            return None
        try:
            with open(SessionStore.SESSION_FILE, "r") as f:
                return json.load(f)
        except Exception:
            return None

    @staticmethod
    def clear():
        if os.path.exists(SessionStore.SESSION_FILE):
            try:
                os.remove(SessionStore.SESSION_FILE)
            except Exception:
                pass


# ---------------------------------------------------------------------------
# Request / Response models
# ---------------------------------------------------------------------------
class StartRequest(BaseModel):
    keys: list[str]                    # Private keys (decrypted client-side by user)
    config_overrides: dict | None = None  # Optional .env overrides for this session


class PreflightRequest(BaseModel):
    keys: list[str]
    config_overrides: dict | None = None


# Response models for type safety
class GasDataPoint(BaseModel):
    timestamp: int
    gwei: float


class GasHistoryResponse(BaseModel):
    history: list[tuple[int, float]]  # [(timestamp, gwei), ...]


class DLQJob(BaseModel):
    worker_id: str
    error: str
    timestamp: float
    retry_count: int = 0


class DLQResponse(BaseModel):
    jobs: list[DLQJob]


class HistoryResponse(BaseModel):
    rows: list[dict]
    error: str | None = None


class StatusResponse(BaseModel):
    status: str
    running: bool
    uptime: float
    workers: int = 0


# ---------------------------------------------------------------------------
# Lifespan handler (replaces deprecated on_event)
# ---------------------------------------------------------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup and shutdown handler using lifespan context manager."""
    # ========== STARTUP ==========
    print("[*] NFT Minter starting up...")
    
    # Resume session if exists
    session = SessionStore.load()
    if session and session.get("keys"):
        print(f"[*] Found existing session. Resuming bot with {len(session['keys'])} workers...")
        overrides = session.get("overrides")
        if isinstance(overrides, dict):
            for k, v in overrides.items():
                os.environ[str(k)] = str(v)
            ConfigurationManager._instance = None
        
        # Start bot in background
        asyncio.create_task(orchestrator.start(session["keys"]))
    
    print("[+] Startup complete. Waiting for requests...")
    
    yield  # App runs here
    
    # ========== SHUTDOWN ==========
    print("[*] Shutting down gracefully...")
    
    try:
        # Stop the bot
        await orchestrator.stop()
    except Exception as e:
        print(f"[-] Error stopping orchestrator: {e}")
    
    try:
        # Close gas oracle
        gas_oracle.stop()
    except Exception as e:
        print(f"[-] Error stopping gas oracle: {e}")
    
    try:
        # Close RPC health checker
        rpc_health.stop()
    except Exception as e:
        print(f"[-] Error stopping RPC health: {e}")
    
    try:
        # Close HTTP client pool
        await http_pool.close()
    except Exception as e:
        print(f"[-] Error closing HTTP pool: {e}")
    
    print("[+] Shutdown complete.")


# ---------------------------------------------------------------------------
# FastAPI app
# ---------------------------------------------------------------------------
app = FastAPI(title="NFT Minter", version="2.0.0", lifespan=lifespan)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # For development; restrict in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------
def validate_private_key(key: str) -> tuple[bool, str]:
    """Validate a private key and return (is_valid, error_msg)."""
    key = key.strip()
    if not key:
        return False, "Empty key"
    
    try:
        if not key.startswith("0x"):
            key = "0x" + key
        
        acct = Account.from_key(key)
        if not acct.address:
            return False, "Key did not produce a valid address"
        return True, ""
    except ValueError as e:
        return False, f"Invalid key format: {str(e)[:100]}"
    except Exception as e:
        return False, f"Key validation error: {str(e)[:100]}"


# ---------------------------------------------------------------------------
# Dashboard
# ---------------------------------------------------------------------------
@app.get("/", response_class=FileResponse)
async def serve_dashboard():
    path = os.path.join(os.path.dirname(__file__), "dashboard.html")
    return FileResponse(path, media_type="text/html")


@app.get("/health")
async def health_check():
    """Simple endpoint for keep-alive pings."""
    return {"status": "ok", "uptime": orchestrator.get_status().get("uptime", 0)}


# ---------------------------------------------------------------------------
# Bot control
# ---------------------------------------------------------------------------
@app.post("/api/start")
async def start_bot(req: StartRequest):
    """Start the bot with provided keys and optional config overrides."""
    if not req.keys:
        raise HTTPException(status_code=400, detail="No private keys provided.")
    
    # Validate all keys upfront
    invalid_keys = []
    for i, key in enumerate(req.keys):
        is_valid, err = validate_private_key(key)
        if not is_valid:
            invalid_keys.append(f"Key {i+1}: {err}")
    
    if invalid_keys:
        raise HTTPException(
            status_code=400, 
            detail={"errors": invalid_keys}
        )
    
    # Apply any session config overrides
    if req.config_overrides and isinstance(req.config_overrides, dict):
        skip_empty = {"OS_API_KEY"}
        for k, v in req.config_overrides.items():
            val = "" if v is None else str(v)
            if k in skip_empty and val == "":
                continue
            os.environ[str(k)] = val
    
    # Reset singleton so overrides take effect
    ConfigurationManager._instance = None
    
    cfg = ConfigurationManager()
    if not cfg.target_nft or cfg.target_nft.startswith("0xYour"):
        raise HTTPException(status_code=400, detail="NFT_CONTRACT_ADDRESS is not configured in .env")
    
    # Save session for persistence
    SessionStore.save(req.keys, req.config_overrides)
    
    asyncio.create_task(orchestrator.start(req.keys))
    return {"status": "starting", "workers": len(req.keys)}


@app.post("/api/stop")
async def stop_bot():
    """Stop the bot and clear session."""
    await orchestrator.stop()
    SessionStore.clear()
    return {"status": "stopped"}


# ---------------------------------------------------------------------------
# Preflight
# ---------------------------------------------------------------------------
@app.post("/api/preflight")
async def preflight(req: PreflightRequest):
    """Run preflight checks on provided keys."""
    if not req.keys:
        raise HTTPException(status_code=400, detail="No keys provided for preflight.")
    
    # Validate keys
    invalid_keys = []
    for i, key in enumerate(req.keys):
        is_valid, err = validate_private_key(key)
        if not is_valid:
            invalid_keys.append(f"Key {i+1}: {err}")
    
    if invalid_keys:
        raise HTTPException(
            status_code=400, 
            detail={"errors": invalid_keys}
        )
    
    if req.config_overrides and isinstance(req.config_overrides, dict):
        skip_empty = {"OS_API_KEY"}
        for k, v in req.config_overrides.items():
            val = "" if v is None else str(v)
            if k in skip_empty and val == "":
                continue
            os.environ[str(k)] = val
    
    # Reset singleton so overrides take effect for Preflight Check
    ConfigurationManager._instance = None
    
    checker = PreflightChecker(req.keys)
    results = await checker.run()
    return {"checks": results}


# ---------------------------------------------------------------------------
# Status & data endpoints
# ---------------------------------------------------------------------------
@app.get("/api/status", response_model=StatusResponse)
async def get_status():
    return StatusResponse(**orchestrator.get_status())


@app.get("/api/history", response_model=HistoryResponse)
async def get_history():
    try:
        rows = Accountant.read_history()
        return HistoryResponse(rows=rows)
    except Exception as e:
        return HistoryResponse(rows=[], error=str(e))


@app.get("/api/gas-history", response_model=GasHistoryResponse)
async def get_gas_history():
    return GasHistoryResponse(history=gas_oracle.get_history())


@app.get("/api/dlq", response_model=DLQResponse)
async def get_dlq():
    jobs = await dlq.get_all()
    return DLQResponse(jobs=[DLQJob(**j) if isinstance(j, dict) else j for j in jobs])


@app.delete("/api/dlq")
async def clear_dlq():
    await dlq.clear()
    return {"status": "cleared"}


# ---------------------------------------------------------------------------
# WebSocket — realtime event stream (with timeouts)
# ---------------------------------------------------------------------------
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    orchestrator.add_client(websocket)
    
    try:
        # Send current status immediately on connect with timeout
        try:
            await asyncio.wait_for(
                websocket.send_text(json.dumps({
                    "type": "status",
                    **orchestrator.get_status(),
                })),
                timeout=5.0
            )
        except asyncio.TimeoutError:
            raise Exception("Failed to send initial status (client too slow)")
        
        # Keep alive — read loop with heartbeat
        while True:
            try:
                # Receive with timeout
                data = await asyncio.wait_for(websocket.receive_text(), timeout=30)
                if data == "ping":
                    try:
                        await asyncio.wait_for(
                            websocket.send_text(json.dumps({"type": "pong"})),
                            timeout=5.0
                        )
                    except asyncio.TimeoutError:
                        break  # Client is unresponsive
            except asyncio.TimeoutError:
                # Send keepalive ping to client
                try:
                    await asyncio.wait_for(
                        websocket.send_text(json.dumps({"type": "ping"})),
                        timeout=5.0
                    )
                except asyncio.TimeoutError:
                    break  # Client is dead
    except WebSocketDisconnect:
        pass
    except Exception as e:
        pass
    finally:
        orchestrator.remove_client(websocket)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(
        "server:app",
        host="0.0.0.0",
        port=port,
        reload=False,
        log_level="warning",
    )