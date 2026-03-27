"""
server.py — FastAPI entry point for the NFT Minter.

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

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

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
# Session Persistence
# ---------------------------------------------------------------------------
SESSION_FILE = "session.json"

class SessionStore:
    @staticmethod
    def save(keys, overrides):
        try:
            with open(SESSION_FILE, "w") as f:
                json.dump({"keys": keys, "overrides": overrides}, f)
        except Exception:
            pass

    @staticmethod
    def load():
        if not os.path.exists(SESSION_FILE):
            return None
        try:
            with open(SESSION_FILE, "r") as f:
                return json.load(f)
        except Exception:
            return None

    @staticmethod
    def clear():
        if os.path.exists(SESSION_FILE):
            try:
                os.remove(SESSION_FILE)
            except Exception:
                pass

app = FastAPI(title="NFT Minter", version="2.0.0")

@app.on_event("startup")
async def startup_event():
    """Resume session if exists on startup."""
    session = SessionStore.load()
    if session:
        print(f"[*] Found existing session. Resuming bot with {len(session['keys'])} workers...")
        # Re-apply overrides
        overrides = session.get("overrides")
        if isinstance(overrides, dict):
            for k, v in overrides.items():
                os.environ[str(k)] = str(v)
            ConfigurationManager._instance = None
        
        asyncio.create_task(orchestrator.start(session["keys"]))


# ---------------------------------------------------------------------------
# Request / Response models
# ---------------------------------------------------------------------------
class StartRequest(BaseModel):
    keys: list[str]                    # Private keys (decrypted client-side by user)
    config_overrides: dict | None = None  # Optional .env overrides for this session


class PreflightRequest(BaseModel):
    keys: list[str]
    config_overrides: dict | None = None


# ---------------------------------------------------------------------------
# Dashboard
# ---------------------------------------------------------------------------
@app.get("/", response_class=FileResponse)
async def serve_dashboard():
    path = os.path.join(os.path.dirname(__file__), "dashboard.html")
    return FileResponse(path, media_type="text/html")


# ---------------------------------------------------------------------------
# Bot control
# ---------------------------------------------------------------------------
@app.post("/api/start")
async def start_bot(req: StartRequest):
    if not req.keys:
        raise HTTPException(status_code=400, detail="No private keys provided.")

    # Apply any session config overrides (modify env vars in-process)
    if req.config_overrides and isinstance(req.config_overrides, dict):
        skip_empty = {"OS_API_KEY"}  # allow .env to provide secrets unless explicitly set
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
    await orchestrator.stop()
    SessionStore.clear()
    return {"status": "stopped"}


# ---------------------------------------------------------------------------
# Preflight
# ---------------------------------------------------------------------------
@app.post("/api/preflight")
async def preflight(req: PreflightRequest):
    if not req.keys:
        raise HTTPException(status_code=400, detail="No keys provided for preflight.")
        
    if req.config_overrides and isinstance(req.config_overrides, dict):
        skip_empty = {"OS_API_KEY"}  # allow .env to provide secrets unless explicitly set
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
@app.get("/api/status")
async def get_status():
    return orchestrator.get_status()


@app.get("/api/history")
async def get_history():
    try:
        rows = Accountant.read_history()
        return {"rows": rows}
    except Exception as e:
        return {"rows": [], "error": str(e)}


@app.get("/api/gas-history")
async def get_gas_history():
    return {"history": gas_oracle.get_history()}


@app.get("/api/dlq")
async def get_dlq():
    return {"jobs": await dlq.get_all()}


@app.delete("/api/dlq")
async def clear_dlq():
    await dlq.clear()
    return {"status": "cleared"}


# ---------------------------------------------------------------------------
# WebSocket — realtime event stream
# ---------------------------------------------------------------------------
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    orchestrator.add_client(websocket)
    try:
        # Send current status immediately on connect
        await websocket.send_text(json.dumps({
            "type": "status",
            **orchestrator.get_status(),
        }))
        # Keep alive — read loop (client can send pings)
        while True:
            try:
                data = await asyncio.wait_for(websocket.receive_text(), timeout=30)
                # Handle ping/pong
                if data == "ping":
                    await websocket.send_text(json.dumps({"type": "pong"}))
            except asyncio.TimeoutError:
                # Send keepalive ping to client
                await websocket.send_text(json.dumps({"type": "ping"}))
    except WebSocketDisconnect:
        pass
    except Exception:
        pass
    finally:
        orchestrator.remove_client(websocket)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "server:app",
        host="0.0.0.0",
        port=8000,
        reload=False,
        log_level="warning",
    )
