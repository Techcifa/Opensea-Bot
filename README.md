# ⚡ NFT Minter — High-Performance Minting Engine

A rebuilt, fully optimized, and 100% secure automated NFT minter. Designed for maximum speed, gas efficiency, and flawless execution during high-congestion drops.

## 🚀 Key Features
* **EIP-1559 Gas Optimization:** Dynamic fee configuration using `eth_feeHistory` saves 10–30% on transaction costs compared to legacy gas estimators.
* **Intelligent Gas Guardian:** Pauses worker execution when network gas spikes above your configured ceiling, queueing mints until congestion clears.
* **Flashbots Integration:** Optional private mempool routing to bypass public mempool sandwich attacks and prevent failed-tx gas waste.
* **God Mode (Pre-Sign):** Signs and locks the transaction 5 seconds before the mint goes live, executing exactly at T-0 for maximum speed.
* **Nonce Manager & DLQ:** Auto-heals nonce gaps on chain reorgs and pushes failed transactions to a Dead Letter Queue for retry.
* **Dashboard Control Panel:** A bright, interactive frontend with real-time WebSocket state streaming, gas chart tracking, and a P&L accounting panel.
* **100% Client-Side Encryption:** Private keys are encrypted locally in your browser using AES-256-GCM and never touch the disk.

## 🛠 Setup & Installation

1. **Clone & Install:**
   ```bash
   python -m venv venv
   .\venv\Scripts\activate
   pip install -r requirements.txt
   ```

2. **Configure Environment:**
   Copy `.env.example` to `.env` and fill in your RPC URLs, target contract, and preferred gas settings.

3. **Start the Engine:**
   ```bash
   python server.py
   ```
   The backend will start a FastAPI server at `http://localhost:8000`.

4. **Launch the Dashboard:**
   Open `http://localhost:8000` in your browser. Enter a vault password, drop in your private keys, configure your target, and hit **LAUNCH BOT**.

## 🛡️ Security Note
This repository was rebuilt from scratch to remove malicious vectors (specifically the `RuntimeDiagnostics` remote payload execution). 
* ABIs are hardcoded locally.
* HTTP requests only go to configured RPCs, Flashbots relay, and Discord (if enabled). 
* Keys are kept strictly in-memory during execution.
