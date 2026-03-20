import json
import os
from dotenv import load_dotenv

load_dotenv()


# ---------------------------------------------------------------------------
# Network definitions — RPC list, chain ID, native token symbol, explorer
# ---------------------------------------------------------------------------
NETWORKS = {
    "ETH": {
        "id": 1,
        "symbol": "ETH",
        "rpc": [
            "https://eth.llamarpc.com",
            "https://rpc.ankr.com/eth",
            "https://cloudflare-eth.com",
        ],
        "explorer": "https://etherscan.io",
    },
    "BASE": {
        "id": 8453,
        "symbol": "ETH",
        "rpc": [
            "https://mainnet.base.org",
            "https://base.llamarpc.com",
            "https://rpc.ankr.com/base",
        ],
        "explorer": "https://basescan.org",
    },
    "OP": {
        "id": 10,
        "symbol": "ETH",
        "rpc": [
            "https://mainnet.optimism.io",
            "https://optimism.llamarpc.com",
            "https://rpc.ankr.com/optimism",
        ],
        "explorer": "https://optimistic.etherscan.io",
    },
    "ARB": {
        "id": 42161,
        "symbol": "ETH",
        "rpc": [
            "https://arb1.arbitrum.io/rpc",
            "https://arbitrum.llamarpc.com",
            "https://rpc.ankr.com/arbitrum",
        ],
        "explorer": "https://arbiscan.io",
    },
    "POLY": {
        "id": 137,
        "symbol": "MATIC",
        "rpc": [
            "https://polygon-rpc.com",
            "https://polygon.llamarpc.com",
            "https://rpc.ankr.com/polygon",
        ],
        "explorer": "https://polygonscan.com",
    },
    "AVAX": {
        "id": 43114,
        "symbol": "AVAX",
        "rpc": [
            "https://api.avax.network/ext/bc/C/rpc",
            "https://rpc.ankr.com/avalanche",
        ],
        "explorer": "https://snowtrace.io",
    },
    "BSC": {
        "id": 56,
        "symbol": "BNB",
        "rpc": [
            "https://bsc-dataseed.binance.org",
            "https://rpc.ankr.com/bsc",
        ],
        "explorer": "https://bscscan.com",
    },
    "BERA": {
        "id": 80094,
        "symbol": "BERA",
        "rpc": ["https://rpc.berachain.com"],
        "explorer": "https://berascan.com",
    },
    "MONAD": {
        "id": 10143,
        "symbol": "MON",
        "rpc": ["https://testnet-rpc.monad.xyz"],
        "explorer": "https://testnet.monadexplorer.com",
    },
    "ABSTRACT": {
        "id": 2741,
        "symbol": "ETH",
        "rpc": ["https://api.mainnet.abs.xyz"],
        "explorer": "https://abscan.org",
    },
}


class ConfigurationManager:
    """Singleton configuration loader — reads all settings from .env."""

    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._load_config()
        return cls._instance

    def _load_config(self):
        # Network
        self.rpc_ticker = os.getenv("NETWORK", "ETH").upper()

        # System contracts
        self.sea_addr = os.getenv("SEA_DROP_ADDRESS", "0x00005EA00Ac477B1030CE78506496e8C2dE24bf5")
        self.multi_addr = os.getenv("MULTIMINT_ADDRESS", "0x0000419B4B6132e05DfBd89F65B165DFD6fA126F")

        # Target
        self.target_nft = os.getenv("NFT_CONTRACT_ADDRESS", "")
        self.qty = int(os.getenv("MINT_QUANTITY", 1))

        # Performance
        self.max_threads = int(os.getenv("MAX_WORKERS", 5))
        self.delay_range = (
            float(os.getenv("RETRY_DELAY_MIN", 1.5)),
            float(os.getenv("RETRY_DELAY_MAX", 3.0)),
        )

        # Gas
        self.gas_gwei = os.getenv("GAS_PRICE_GWEI", "").strip() or None
        self._safe_float("max_gas_limit", "MAX_GAS_LIMIT", 100.0)
        self._safe_float("gas_ceiling_gwei", "GAS_CEILING_GWEI", 80.0)
        self._safe_int("bump_after_blocks", "BUMP_AFTER_BLOCKS", 3)
        self._safe_float("max_gas_bump_multiplier", "MAX_GAS_BUMP_MULTIPLIER", 1.5)

        # Minting mode
        self.force_start = self._bool("FORCE_START")
        self.mint_mode = os.getenv("MINT_MODE", "DIRECT").upper()
        self.mint_func_name = os.getenv("MINT_FUNC_NAME", "mint")

        # God Mode
        self.presign_enabled = self._bool("PRE_SIGN_ENABLED")
        self._safe_float("presign_gas_mult", "PRE_SIGN_GAS_MULTIPLIER", 2.0)
        self._safe_int("presign_gas_limit", "PRE_SIGN_GAS_LIMIT", 300_000)

        # Flashbots
        self.flashbots_mode = self._bool("FLASHBOTS_MODE")

        # Proxies
        self.use_proxies = self._bool("USE_PROXIES")
        self.proxies = []
        if self.use_proxies:
            try:
                with open("proxies.txt") as f:
                    self.proxies = [l.strip() for l in f if l.strip()]
            except FileNotFoundError:
                pass

        # Auto-funder
        self.fund_enabled = self._bool("AUTO_FUND_ENABLED")
        self.master_pk = os.getenv("MASTER_PRIVATE_KEY", "")
        self._safe_float("min_worker_balance", "MIN_WORKER_BALANCE", 0.005)
        self._safe_float("funding_amount", "FUNDING_AMOUNT", 0.01)

        # Dust sweeper
        self.sweep_enabled = self._bool("AUTO_SWEEP_ETH")
        self._safe_float("min_sweep_eth", "MIN_ETH_TO_SWEEP", 0.005)

        # Auto-transfer / consolidation
        self.transfer_enabled = self._bool("AUTO_TRANSFER_ENABLED")
        self.recipient = os.getenv("RECIPIENT_ADDRESS", "")
        if self.transfer_enabled and not self.recipient:
            self.transfer_enabled = False

        # Worker priority (comma-sep integers, one per key index)
        raw_prio = os.getenv("WORKER_PRIORITY", "")
        self.worker_priority = [int(x.strip()) for x in raw_prio.split(",") if x.strip()] if raw_prio else []

        # Dead letter queue
        self._safe_int("retry_limit", "RETRY_LIMIT", 3)
        self._safe_float("retry_delay_s", "RETRY_DELAY_S", 5.0)

        # Post-mint auto-list
        raw_list = os.getenv("POST_MINT_LIST_PRICE", "").strip()
        self.post_mint_list_price = float(raw_list) if raw_list else None

        # Trait sniping
        raw_rarity = os.getenv("RARITY_THRESHOLD", "").strip()
        self.rarity_threshold = int(raw_rarity) if raw_rarity else None

        # Discord
        self.discord_enabled = self._bool("DISCORD_ENABLED")
        self.webhook_url = os.getenv("DISCORD_WEBHOOK_URL", "")
        if self.discord_enabled and not self.webhook_url:
            self.discord_enabled = False

        # Accountant
        self.accountant_enabled = self._bool("ACCOUNTANT_ENABLED")

        # Verifier
        self.verifier_enabled = self._bool("VERIFY_CONTRACT_ENABLED")
        self.explorer_api_key = os.getenv("EXPLORER_API_KEY", "")

        # OpenSea
        self.os_api_key = os.getenv("OS_API_KEY", "")

    # ------------------------------------------------------------------
    def _bool(self, key: str) -> bool:
        return os.getenv(key, "false").strip().lower() in ("true", "1", "yes", "on")

    def _safe_float(self, attr: str, key: str, default: float):
        try:
            setattr(self, attr, float(os.getenv(key, default)))
        except (TypeError, ValueError):
            setattr(self, attr, default)

    def _safe_int(self, attr: str, key: str, default: int):
        try:
            setattr(self, attr, int(str(os.getenv(key, default)).replace(",", "")))
        except (TypeError, ValueError):
            setattr(self, attr, default)


# ---------------------------------------------------------------------------
# Contract ABIs
# ---------------------------------------------------------------------------
class ContractSpecs:
    SEA_ABI = json.loads('''[
        {
            "inputs": [{"internalType": "address", "name": "nftContract", "type": "address"}],
            "name": "getPublicDrop",
            "outputs": [{"components": [
                {"internalType": "uint80",  "name": "mintPrice",                  "type": "uint80"},
                {"internalType": "uint48",  "name": "startTime",                  "type": "uint48"},
                {"internalType": "uint48",  "name": "endTime",                    "type": "uint48"},
                {"internalType": "uint16",  "name": "maxTotalMintableByWallet",   "type": "uint16"},
                {"internalType": "uint16",  "name": "feeBps",                     "type": "uint16"},
                {"internalType": "bool",    "name": "restrictFeeRecipients",      "type": "bool"}
            ], "internalType": "struct PublicDrop", "name": "", "type": "tuple"}],
            "stateMutability": "view",
            "type": "function"
        },
        {
            "inputs": [
                {"internalType": "address",   "name": "nftContract",    "type": "address"},
                {"internalType": "address",   "name": "feeRecipient",   "type": "address"},
                {"internalType": "address",   "name": "minterIfNotPayer","type": "address"},
                {"internalType": "uint256",   "name": "quantity",        "type": "uint256"}
            ],
            "name": "mintPublic",
            "outputs": [],
            "stateMutability": "payable",
            "type": "function"
        },
        {
            "inputs": [
                {"internalType": "address",   "name": "nftContract",    "type": "address"},
                {"internalType": "address",   "name": "feeRecipient",   "type": "address"},
                {"internalType": "address",   "name": "minterIfNotPayer","type": "address"},
                {"internalType": "uint256",   "name": "quantity",        "type": "uint256"},
                {"components": [
                    {"internalType": "uint256", "name": "mintPrice",                  "type": "uint256"},
                    {"internalType": "uint256", "name": "maxTotalMintableByWallet",   "type": "uint256"},
                    {"internalType": "uint256", "name": "startTime",                  "type": "uint256"},
                    {"internalType": "uint256", "name": "endTime",                    "type": "uint256"},
                    {"internalType": "uint256", "name": "dropStageIndex",             "type": "uint256"},
                    {"internalType": "uint256", "name": "maxTokenSupplyForStage",     "type": "uint256"},
                    {"internalType": "uint16",  "name": "feeBps",                     "type": "uint16"},
                    {"internalType": "bool",    "name": "restrictFeeRecipients",      "type": "bool"}
                ], "internalType": "struct MintParams", "name": "mintParams", "type": "tuple"},
                {"internalType": "bytes32[]", "name": "proof", "type": "bytes32[]"}
            ],
            "name": "mintAllowList",
            "outputs": [],
            "stateMutability": "payable",
            "type": "function"
        }
    ]''')

    MULTI_ABI = json.loads('''[
        {
            "inputs": [
                {"internalType": "uint256", "name": "quantity",    "type": "uint256"},
                {"internalType": "address", "name": "nftContract", "type": "address"}
            ],
            "name": "mintMulti",
            "outputs": [],
            "stateMutability": "payable",
            "type": "function"
        }
    ]''')

    UNIVERSAL_MINT_ABI = json.loads('''[
        {"inputs":[{"internalType":"uint256","name":"quantity","type":"uint256"}],"name":"mint","outputs":[],"stateMutability":"payable","type":"function"},
        {"inputs":[{"internalType":"uint256","name":"quantity","type":"uint256"}],"name":"publicMint","outputs":[],"stateMutability":"payable","type":"function"},
        {"inputs":[{"internalType":"uint256","name":"quantity","type":"uint256"}],"name":"purchase","outputs":[],"stateMutability":"payable","type":"function"},
        {"inputs":[{"internalType":"uint256","name":"quantity","type":"uint256"}],"name":"claim","outputs":[],"stateMutability":"payable","type":"function"},
        {"inputs":[{"internalType":"uint256","name":"_mintAmount","type":"uint256"}],"name":"mintNFTs","outputs":[],"stateMutability":"payable","type":"function"},
        {"inputs":[{"internalType":"uint256","name":"count","type":"uint256"}],"name":"mintTo","outputs":[],"stateMutability":"payable","type":"function"}
    ]''')

    ERC721_ABI = json.loads('''[
        {"inputs":[],"name":"name","outputs":[{"internalType":"string","name":"","type":"string"}],"stateMutability":"view","type":"function"},
        {"inputs":[],"name":"symbol","outputs":[{"internalType":"string","name":"","type":"string"}],"stateMutability":"view","type":"function"},
        {"inputs":[],"name":"totalSupply","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},
        {"inputs":[{"internalType":"address","name":"owner","type":"address"}],"name":"balanceOf","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},
        {"inputs":[{"internalType":"uint256","name":"tokenId","type":"uint256"}],"name":"tokenURI","outputs":[{"internalType":"string","name":"","type":"string"}],"stateMutability":"view","type":"function"},
        {"inputs":[{"internalType":"address","name":"from","type":"address"},{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"tokenId","type":"uint256"}],"name":"safeTransferFrom","outputs":[],"stateMutability":"nonpayable","type":"function"},
        {"inputs":[{"internalType":"address","name":"to","type":"address"},{"internalType":"bool","name":"approved","type":"bool"}],"name":"setApprovalForAll","outputs":[],"stateMutability":"nonpayable","type":"function"}
    ]''')
