# NFT Minter Rebuild — Implementation Plan (v2)

Rebuild the legitimate engine of `genoshide/opensea-auto-mint-bot` from scratch — clean architecture, no malware — with all the smart upgrades below. Exposed via a **FastAPI** HTTP + WebSocket server fed into a **bright, colorful, interactive HTML dashboard**.

Target directory: `C:\Users\Dell\Desktop\Opensea Bot\`

---

## Architecture Overview

```
Browser (dashboard.html)
    │  HTTP POST  →  Start / Stop / Configure / Preflight
    │  WebSocket  ←  Worker state, structured logs, gas chart, P&L
    ▼
FastAPI Server (server.py)
    ├── BotOrchestrator          ← manages worker tasks & global state
    │       ├── ExecutionUnit (per wallet)
    │       ├── NonceManager     ← per-wallet nonce healing
    │       ├── MassFunder
    │       ├── AssetRelay
    │       ├── DeadLetterQueue  ← failed jobs, configurable retry
    │       └── GasOracle        ← EIP-1559, shared singleton
    ├── RpcHealthChecker         ← pings RPCs every 30s
    ├── RpcRateLimiter           ← shared per-endpoint call queue
    ├── ConfigStore              ← .env parse + preflight validation
    └── routes/
            ├── POST /api/start
            ├── POST /api/stop
            ├── POST /api/preflight   ← dry-run config check
            ├── GET  /api/status
            ├── GET  /api/history     ← structured P&L data
            ├── GET  /api/gas-history ← 30-min rolling gas data
            └── WS   /ws             ← structured JSON event stream
```

**Tech stack:**
| Layer | Tech |
|---|---|
| Backend | Python 3.10+, FastAPI, uvicorn |
| Realtime | Native FastAPI WebSockets |
| Blockchain | web3.py 7.x async, EIP-1559 |
| Private mempool | Flashbots Relay (optional) |
| HTTP client | aiohttp |
| Frontend | Vanilla HTML + CSS + JS, Chart.js |

---

## Proposed Changes

### 1 · Gas Engine

#### [NEW] `src/gas_oracle.py` — EIP-1559 GasOracle
- Polls every 3 s using `eth.fee_history()` to compute `maxFeePerGas` + `maxPriorityFeePerGas` instead of legacy `gasPrice` — 10–30% fee savings
- Stores a 30-minute rolling window of samples (emitted over WebSocket for the dashboard chart)
- Enforces `GAS_CEILING_GWEI` — if exceeded, workers pause and queue jobs instead of firing blind
- Dynamic gas bumping: if a TX hasn't confirmed in `BUMP_AFTER_BLOCKS`, resubmit with `+12%` priority fee (exponential backoff capped at `MAX_GAS_BUMP_MULTIPLIER`)

#### [NEW] `src/utils/rpc_health.py` — RpcHealthChecker
- Pings each configured RPC endpoint every 30 s via `eth_blockNumber`
- Removes unresponsive nodes from the active rotation automatically
- Logs which RPC each worker is using per call

#### [NEW] `src/utils/rpc_rate_limiter.py` — RpcRateLimiter
- Shared async call queue across all workers per RPC endpoint
- Exponential backoff on HTTP 429s — prevents hammering and hitting rate limits with 50+ wallets

---

### 2 · Execution Engine

#### [NEW] `src/engine/execution.py` — ExecutionUnit (clean rebuild)
All original legitimate features retained, **zero telemetry**:
- Local nonce cache delegated to `NonceManager`
- **TX Simulation**: calls `eth_call` before broadcasting — skips TX and logs revert reason if simulation fails (prevents wasted gas on sold-out collections)
- **Flashbots Mode**: if `FLASHBOTS_MODE=true`, bundles TX through Flashbots Relay (`https://relay.flashbots.net`) instead of public mempool — prevents sandwich attacks
- DIRECT mode (arbitrary `mint_func_name` via `getattr`)
- PROXY mode (MultiMint contract)
- God Mode (pre-signed TX, 5 s before mint time)
- Time Sniper (async sleep to millisecond precision)
- Gas Guardian loop
- Post-success: Accountant, Discord notify, AssetRelay, Dust Sweeper
- **Post-mint: Auto-List** — if `POST_MINT_LIST_PRICE` is set, calls Seaport or OpenSea API to list immediately
- **Trait Sniping**: fetches token metadata post-mint, compares against rarity config, flags below-threshold tokens in dashboard and optionally routes to a dedicated "keep" wallet via AssetRelay
- Failed jobs pushed to `DeadLetterQueue`

#### [NEW] `src/engine/nonce_manager.py` — NonceManager
- Per-wallet pending TX tracking
- Detects nonce gaps and auto-heals by fetching on-chain nonce
- Handles reorgs gracefully

#### [NEW] `src/engine/dead_letter_queue.py` — DeadLetterQueue
- Stores failed mint attempts with error reason + timestamp
- `RETRY_LIMIT` + `RETRY_DELAY_S` config controls reattempt behavior
- DLQ entries visible as a panel in the dashboard

---

### 3 · Config & Orchestration

#### [NEW] `src/config/settings.py`
All original `.env` parsing plus new keys:
```
FLASHBOTS_MODE, GAS_CEILING_GWEI, BUMP_AFTER_BLOCKS, MAX_GAS_BUMP_MULTIPLIER
POST_MINT_LIST_PRICE, RARITY_THRESHOLD
RETRY_LIMIT, RETRY_DELAY_S
WORKER_PRIORITY (comma-sep tiers per key index)
```

#### [NEW] `src/orchestrator.py` — BotOrchestrator
- Holds all `asyncio.Task` refs + `DLQ` + `NonceManager` per wallet
- `start()` runs preflight check first (balance vs. mint cost, contract exists, RPC health)
- `stop()` cancels all tasks gracefully
- Broadcasts structured JSON events over WebSocket to all connected clients
- Worker prioritization — high-priority wallets get first RPC slots and nonce pool access during congestion

#### [NEW] `src/preflight.py` — PreflightChecker
Called on `POST /api/preflight`:
- Verifies NFT contract exists on-chain
- Checks each wallet balance vs. estimated mint cost (price × qty + gas estimate)
- Warns if any wallet would run out of ETH
- Returns a structured checklist surfaced in the dashboard before START

---

### 4 · Security

#### [NEW] `dashboard.html` — Client-Side Key Encryption
- Private keys pasted into a password-protected textarea
- Encrypted client-side with **AES-256-GCM via Web Crypto API** using a user-supplied password
- Server receives only encrypted blobs; decrypts in memory at mint time — keys never touch disk
- Hardware wallet support note: documented in README as a future integration path (Ledger HID)

---

### 5 · Feature Modules (unchanged from v1)

#### [NEW] `src/features/funder.py` — MassFunder *(no changes needed)*
#### [NEW] `src/features/transfer.py` — AssetRelay *(NFT consolidation + dust sweep)*
#### [NEW] `src/features/accountant.py` — Accountant *(appends to `history.csv`)*
#### [NEW] `src/utils/verifier.py` — ContractVerifier only *(RuntimeDiagnostics fully omitted)*
#### [NEW] `src/utils/core.py` — SystemCompliance + async_error_handler
#### [NEW] `src/ui/notifier.py` — DiscordReporter

---

### 6 · Logging

All log calls emit **structured JSON** `{level, timestamp, worker_id, tx_hash, event_type, message}` over the WebSocket stream. Dashboard can filter by level/worker/event type. Log export button in UI downloads the session log as `.json`.

---

### 7 · Frontend Dashboard (`dashboard.html`)

**Design: Bright, colorful, high-energy UI** — vibrant neon accents on white/light-gray surfaces, animated gradient header, playful card hover effects, color-coded status badges, fun micro-animations.

**Panels:**

| Panel | Description |
|---|---|
| **Header** | Animated gradient banner, bot name, live network badge, real-time gas price ticker with color indicator (green/yellow/red) |
| **Gas Sparkline** | Chart.js 30-minute rolling gas price chart — helps spot low-congestion windows |
| **Preflight Checklist** | Visual ✅/⚠️/❌ checklist returned by `/api/preflight` before START |
| **Config Panel** | Collapsible form for all settings — network selector, contract address, qty, mint mode, God Mode toggle, Flashbots toggle, gas ceiling, auto-funder, etc. |
| **Key Vault** | Password-protected encrypted key input with AES-256 client-side encryption indicator |
| **Control Bar** | Big colorful `▶ LAUNCH`, `■ STOP` buttons with animated pulse when running |
| **Workers Grid** | Colorful card per worker — ID, truncated address, balance with sparkle animation on update, priority tier badge, status badge (IDLE/MINTING/SUCCESS/FAILED), **"View TX"** Etherscan link |
| **Dead Letter Queue** | Collapsible panel listing failed jobs with retry controls |
| **Live Log Feed** | Scrolling log with color-coded levels, filter by worker/level, JSON export button |
| **P&L Panel** | Total ETH spent (gas + mint price), estimated NFT floor value (OpenSea/Reservoir API), **net profit/loss** in large bold color (green/red), per-TX breakdown table |
| **History Table** | Full `history.csv` data with sortable columns |

---

### 8 · Project Files

#### [NEW] `requirements.txt`
```
fastapi
uvicorn[standard]
web3==7.12.0
eth-account>=0.8.0
python-dotenv
aiohttp
requests
packaging
websockets
```

#### [NEW] `.env.example` *(all keys with safe defaults and comments)*
#### [NEW] `README.md` *(setup guide: venv → fill .env → `python server.py` → open `http://localhost:8000`)*

---

## Verification Plan

### Server Startup
```powershell
cd "C:\Users\Dell\Desktop\Opensea Bot"
python -m venv venv
.\venv\Scripts\activate
pip install -r requirements.txt
python server.py
```
Expected: uvicorn starts on port 8000, no errors.

### Browser Test
1. Open `http://localhost:8000` — confirm dashboard renders with all panels
2. Run **Preflight** — verify checklist populates
3. Paste a burner wallet key, click **LAUNCH** — confirm WebSocket connects and logs stream
4. Confirm worker card updates in realtime, gas sparkline draws
5. Click **STOP** — confirm graceful shutdown in log

### Malware Check
```powershell
Select-String -Path "C:\Users\Dell\Desktop\Opensea Bot\src\*" -Pattern "RuntimeDiagnostics" -Recurse
Select-String -Path "C:\Users\Dell\Desktop\Opensea Bot\src\*" -Pattern "slack.com" -Recurse
```
Expected: **zero results**.

