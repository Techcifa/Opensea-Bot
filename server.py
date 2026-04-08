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

FIXES:
  C-01  Session keys are now encrypted with Fernet before writing to disk.
        SESSION_ENCRYPTION_KEY env var required for session persistence.
  C-07  config_overrides uses an explicit allowlist — arbitrary env vars
        (e.g. MASTER_PRIVATE_KEY, PATH) can no longer be injected via API.
  H-07  DLQJob uses 'retry_count' matching the fixed DeadLetterQueue model.
  H-08  All /api/start, /api/stop, /api/preflight endpoints require a Bearer
        token (BOT_API_KEY env var). Token auth is skipped when BOT_API_KEY
        is not set (development mode with a loud warning).
"""

import base64
import hashlib
import json
import os
import sys
import asyncio
import time
import logging
import traceback
from contextlib import asynccontextmanager

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException, Depends, Request
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel
from eth_account import Account

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

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("server")

# ---------------------------------------------------------------------------
# C-07: Explicit allowlist of environment keys that may be overridden via API
# ---------------------------------------------------------------------------
ALLOWED_OVERRIDE_KEYS: set[str] = {
    "NETWORK",
    "NFT_CONTRACT_ADDRESS",
    "MINT_QUANTITY",
    "MINT_MODE",
    "MINT_FUNC_NAME",
    "FORCE_START",
    "GAS_PRICE_GWEI",
    "MAX_GAS_LIMIT",
    "GAS_CEILING_GWEI",
    "GAS_BASE_FEE_MULTIPLIER",
    "PRIORITY_FEE_GWEI",
    "TX_FEE_BUFFER_MULTIPLIER",
    "GAS_ESTIMATE_MULTIPLIER",
    "DIRECT_FALLBACK_GAS_LIMIT",
    "BUMP_AFTER_BLOCKS",
    "MAX_GAS_BUMP_MULTIPLIER",
    "PRE_SIGN_ENABLED",
    "PRE_SIGN_GAS_MULTIPLIER",
    "PRE_SIGN_GAS_LIMIT",
    "FLASHBOTS_MODE",
    "USE_PROXIES",
    "RETRY_DELAY_MIN",
    "RETRY_DELAY_MAX",
    "RETRY_LIMIT",
    "RETRY_DELAY_S",
    "AUTO_FUND_ENABLED",
    "MIN_WORKER_BALANCE",
    "FUNDING_AMOUNT",
    "AUTO_SWEEP_ETH",
    "MIN_ETH_TO_SWEEP",
    "AUTO_TRANSFER_ENABLED",
    "RECIPIENT_ADDRESS",
    "WORKER_PRIORITY",
    "DISCORD_ENABLED",
    "DISCORD_WEBHOOK_URL",
    "ACCOUNTANT_ENABLED",
    "VERIFY_CONTRACT_ENABLED",
    "POST_MINT_LIST_PRICE",
    "RARITY_THRESHOLD",
    "OS_API_KEY",          # OpenSea API key is safe to override (not crypto key)
    "EXPLORER_API_KEY",    # Explorer API key: allows verifier to be enabled at runtime
    "MINT_PRICE_ETH",      # Manual mint price override (wei fallback auto-detection)
    "SKIP_SIMULATION",     # Bypass eth_call simulation (for mints not yet open)
}


def _apply_overrides(overrides: dict | None):
    """Apply validated config overrides to os.environ (C-07 allowlist enforced)."""
    if not overrides or not isinstance(overrides, dict):
        return

    skip_empty = {"OS_API_KEY"}
    rejected   = []

    for k, v in overrides.items():
        if k not in ALLOWED_OVERRIDE_KEYS:
            rejected.append(k)
            continue
        val = "" if v is None else str(v)
        if k in skip_empty and val == "":
            continue
        os.environ[str(k)] = val

    if rejected:
        raise HTTPException(
            status_code=400,
            detail=f"Config keys are not allowed to be overridden: {rejected}",
        )


# ---------------------------------------------------------------------------
# C-01: Encrypted session store
# ---------------------------------------------------------------------------
def _get_fernet():
    """Build a Fernet key from SESSION_ENCRYPTION_KEY env var."""
    try:
        from cryptography.fernet import Fernet
        secret = os.environ.get("SESSION_ENCRYPTION_KEY", "").strip()
        if not secret:
            return None
        key = base64.urlsafe_b64encode(hashlib.sha256(secret.encode()).digest())
        return Fernet(key)
    except ImportError:
        return None


class SessionStore:
    SESSION_FILE = "session.json"   # name kept for compat; content is now binary/encrypted

    @staticmethod
    def save(keys: list[str], overrides: dict | None):
        """FIX C-01: Encrypt session data before writing to disk."""
        fernet = _get_fernet()
        payload = json.dumps({"keys": keys, "overrides": overrides}).encode()
        try:
            if fernet:
                data = fernet.encrypt(payload)
                mode = "wb"
            else:
                log.warning(
                    "[!] SESSION_ENCRYPTION_KEY not set — session stored UNENCRYPTED. "
                    "Set this variable in .env to enable encryption."
                )
                data = payload
                mode = "wb"

            with open(SessionStore.SESSION_FILE, mode) as f:
                f.write(data)
            try:
                os.chmod(SessionStore.SESSION_FILE, 0o600)
            except Exception:
                pass
        except Exception as exc:
            log.error("Session save failed: %s", exc)

    @staticmethod
    def load() -> dict | None:
        if not os.path.exists(SessionStore.SESSION_FILE):
            return None
        try:
            with open(SessionStore.SESSION_FILE, "rb") as f:
                raw = f.read()
            fernet = _get_fernet()
            if fernet:
                try:
                    decrypted = fernet.decrypt(raw)
                    return json.loads(decrypted)
                except Exception:
                    # Could be an old plaintext session file — try raw parse
                    try:
                        return json.loads(raw)
                    except Exception:
                        return None
            else:
                return json.loads(raw)
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
# H-08: API key authentication
# ---------------------------------------------------------------------------
_bearer_scheme = HTTPBearer(auto_error=False)
_BOT_API_KEY   = os.environ.get("BOT_API_KEY", "").strip()

if not _BOT_API_KEY:
    log.warning(
        "[!] BOT_API_KEY is not set — all /api/ control endpoints are publicly accessible. "
        "Set BOT_API_KEY in .env to require authentication."
    )


async def require_api_key(
    request: Request,
    creds: HTTPAuthorizationCredentials | None = Depends(_bearer_scheme),
):
    """
    FIX H-08: Returns 401 if BOT_API_KEY is set and caller did not provide it.
    Skips auth if BOT_API_KEY is unset (development convenience with loud warning).
    """
    if not _BOT_API_KEY:
        return   # Auth disabled — development mode

    if creds is None or creds.credentials != _BOT_API_KEY:
        raise HTTPException(
            status_code=401,
            detail="Invalid or missing API key. Set Authorization: Bearer <BOT_API_KEY>",
            headers={"WWW-Authenticate": "Bearer"},
        )


# ---------------------------------------------------------------------------
# Request / Response models
# ---------------------------------------------------------------------------
class StartRequest(BaseModel):
    keys: list[str]
    config_overrides: dict | None = None


class PreflightRequest(BaseModel):
    keys: list[str]
    config_overrides: dict | None = None


class GasDataPoint(BaseModel):
    timestamp: int
    gwei: float


class GasHistoryResponse(BaseModel):
    history: list[tuple[int, float]]


class DLQJob(BaseModel):
    worker_id: str
    error: str
    timestamp: float
    retry_count: int = 0              # FIX H-07: was mismatched against DLQ 'attempt' field


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
# Lifespan handler
# ---------------------------------------------------------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    log.info("NFT Minter starting up...")

    session = SessionStore.load()
    if session and session.get("keys"):
        log.info("Found existing session. Resuming bot with %d workers...", len(session["keys"]))
        overrides = session.get("overrides")
        if isinstance(overrides, dict):
            try:
                _apply_overrides(overrides)
            except HTTPException:
                log.warning("Session overrides contain disallowed keys — some skipped.")
        ConfigurationManager._instance = None
        asyncio.create_task(orchestrator.start(session["keys"]))

    log.info("Startup complete.")
    yield

    log.info("Shutting down gracefully...")
    try:
        await orchestrator.stop()
    except Exception as exc:
        log.error("Error stopping orchestrator: %s", exc)

    try:
        gas_oracle.stop()
    except Exception as exc:
        log.error("Error stopping gas oracle: %s", exc)

    try:
        rpc_health.stop()
    except Exception as exc:
        log.error("Error stopping RPC health: %s", exc)

    log.info("Shutdown complete.")


# ---------------------------------------------------------------------------
# FastAPI app
# ---------------------------------------------------------------------------
app = FastAPI(title="NFT Minter", version="2.1.0", lifespan=lifespan)

_cors_origins = os.environ.get("CORS_ORIGINS", "*").split(",")
app.add_middleware(
    CORSMiddleware,
    allow_origins=_cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------
def validate_private_key(key: str) -> tuple[bool, str]:
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
    except ValueError as exc:
        return False, f"Invalid key format: {str(exc)[:100]}"
    except Exception as exc:
        return False, f"Key validation error: {str(exc)[:100]}"


def _validate_keys(keys: list[str]) -> list[str]:
    """Validate all keys; raise HTTPException listing failures."""
    invalid = []
    for i, key in enumerate(keys, 1):
        ok, err = validate_private_key(key)
        if not ok:
            invalid.append(f"Key {i}: {err}")
    if invalid:
        raise HTTPException(status_code=400, detail={"errors": invalid})
    return keys


# ---------------------------------------------------------------------------
# Dashboard
# ---------------------------------------------------------------------------
@app.get("/", response_class=FileResponse)
async def serve_dashboard():
    path = os.path.join(os.path.dirname(__file__), "dashboard.html")
    return FileResponse(path, media_type="text/html")


@app.get("/health")
async def health_check():
    return {"status": "ok", "uptime": orchestrator.get_status().get("uptime", 0)}


# ---------------------------------------------------------------------------
# Bot control  (FIX H-08: require_api_key dependency)
# ---------------------------------------------------------------------------
@app.post("/api/start", dependencies=[Depends(require_api_key)])
async def start_bot(req: StartRequest):
    """Start the bot with provided keys and optional config overrides."""
    if not req.keys:
        raise HTTPException(status_code=400, detail="No private keys provided.")

    _validate_keys(req.keys)
    _apply_overrides(req.config_overrides)    # FIX C-07: allowlist enforced
    ConfigurationManager._instance = None

    cfg = ConfigurationManager()
    if not cfg.target_nft or cfg.target_nft.startswith("0xYour"):
        raise HTTPException(status_code=400, detail="NFT_CONTRACT_ADDRESS is not configured in .env")

    SessionStore.save(req.keys, req.config_overrides)   # FIX C-01: encrypted
    asyncio.create_task(orchestrator.start(req.keys))
    return {"status": "starting", "workers": len(req.keys)}


@app.post("/api/stop", dependencies=[Depends(require_api_key)])
async def stop_bot():
    """Stop the bot and clear session."""
    await orchestrator.stop()
    SessionStore.clear()
    return {"status": "stopped"}


# ---------------------------------------------------------------------------
# Preflight (FIX H-08: require_api_key)
# ---------------------------------------------------------------------------
@app.post("/api/preflight", dependencies=[Depends(require_api_key)])
async def preflight(req: PreflightRequest):
    if not req.keys:
        raise HTTPException(status_code=400, detail="No keys provided for preflight.")

    _validate_keys(req.keys)
    _apply_overrides(req.config_overrides)    # FIX C-07
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
    except Exception as exc:
        return HistoryResponse(rows=[], error=str(exc))


@app.get("/api/gas-history", response_model=GasHistoryResponse)
async def get_gas_history():
    return GasHistoryResponse(history=gas_oracle.get_history())


@app.get("/api/dlq", response_model=DLQResponse)
async def get_dlq():
    jobs = await dlq.get_all()
    # FIX H-07: DLQ now returns 'retry_count' matching the Pydantic model
    return DLQResponse(jobs=[DLQJob(**j) for j in jobs])


@app.delete("/api/dlq", dependencies=[Depends(require_api_key)])
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
        try:
            await asyncio.wait_for(
                websocket.send_text(json.dumps({"type": "status", **orchestrator.get_status()})),
                timeout=5.0,
            )
        except asyncio.TimeoutError:
            raise Exception("Failed to send initial status (client too slow)")

        while True:
            try:
                data = await asyncio.wait_for(websocket.receive_text(), timeout=30)
                if data == "ping":
                    try:
                        await asyncio.wait_for(
                            websocket.send_text(json.dumps({"type": "pong"})),
                            timeout=5.0,
                        )
                    except asyncio.TimeoutError:
                        break
            except asyncio.TimeoutError:
                try:
                    await asyncio.wait_for(
                        websocket.send_text(json.dumps({"type": "ping"})),
                        timeout=5.0,
                    )
                except asyncio.TimeoutError:
                    break
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
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(
        "server:app",
        host    = "0.0.0.0",
        port    = port,
        reload  = False,
        workers = 1,          # singleton state requires single process
        log_level = "info",
    )