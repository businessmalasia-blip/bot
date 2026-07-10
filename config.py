import os
from dotenv import load_dotenv

load_dotenv()

HELIUS_API_KEY = os.environ["HELIUS_API_KEY"]
HELIUS_RPC = f"https://mainnet.helius-rpc.com/?api-key={HELIUS_API_KEY}"
HELIUS_WSS = f"wss://mainnet.helius-rpc.com/?api-key={HELIUS_API_KEY}"

TELEGRAM_BOT_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
TELEGRAM_CHAT_ID = int(os.environ["TELEGRAM_CHAT_ID"])

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

PUMP_PROGRAM = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"

SOL_PRICE_INTERVAL = float(os.getenv("SOL_PRICE_INTERVAL", "2"))
MCAP_THRESHOLD = float(os.getenv("MCAP_THRESHOLD", "8000"))
# Alert fires at the first crossing of this mark (dynamic window, no upper bound)
MCAP_ALERT_LOW = float(os.getenv("MCAP_ALERT_LOW", "10000"))
MCAP_WAIT_TIMEOUT = float(os.getenv("MCAP_WAIT_TIMEOUT", "300"))

CONCENTRATION_MAX_SINGLE = float(os.getenv("CONCENTRATION_MAX_SINGLE", "3.5"))
CONCENTRATION_MAX_TOP10 = float(os.getenv("CONCENTRATION_MAX_TOP10", "22.0"))

HUMAN_MIN_PERCENT = float(os.getenv("HUMAN_MIN_PERCENT", "50"))
UNKNOWN_MAX_PERCENT = float(os.getenv("UNKNOWN_MAX_PERCENT", "30"))

# Cache TTLs for wallet-history lookups. A wallet that has history keeps it
# forever, so a long HUMAN TTL is safe and lets the cache accumulate a real
# base of known-alive wallets (and saves Helius credits on re-checks).
# UNKNOWN wallets may become active any minute — keep that TTL short.
HUMAN_CACHE_TTL = int(os.getenv("HUMAN_CACHE_TTL", str(7 * 86400)))
UNKNOWN_CACHE_TTL = int(os.getenv("UNKNOWN_CACHE_TTL", "1800"))

HELIUS_RPS = int(os.getenv("HELIUS_RPS", "5"))
HELIUS_DELAY = 1.0 / HELIUS_RPS

# Velocity: minimum unique buyers within the last VELOCITY_WINDOW seconds
VELOCITY_MIN_BUYERS = int(os.getenv("VELOCITY_MIN_BUYERS", "5"))
VELOCITY_WINDOW = int(os.getenv("VELOCITY_WINDOW", "60"))

# Bundle detection: max transactions allowed in the creation slot (+1 slot).
# The creation tx itself counts, and it usually contains the dev's own buy;
# a couple of same-slot snipers are common even on legit launches.
BUNDLE_MAX_CREATION_TXS = int(os.getenv("BUNDLE_MAX_CREATION_TXS", "5"))

# Socials: informational by default; set to 1 to reject tokens without any socials
SOCIALS_REQUIRED = os.getenv("SOCIALS_REQUIRED", "0") == "1"

# Stats journal
DB_PATH = os.getenv("DB_PATH", "screener_stats.db")

# Don't re-analyze the same mint more often than this (seconds). Every buy
# above the threshold re-triggers analysis otherwise, burning credits.
ANALYZED_TTL = int(os.getenv("ANALYZED_TTL", "21600"))

# WebSocket mode:
#   1 = transactionSubscribe (Enhanced WS, Helius Developer plan and above)
#   0 = logsSubscribe + getTransaction fetch (works on the free tier)
USE_ENHANCED_WS = os.getenv("USE_ENHANCED_WS", "0") == "1"
# Free-tier budget for fetching buy transactions, requests per second.
# 1M credits/month ~= 0.35 rps average; leave headroom for the analysis calls.
TX_FETCH_RPS = float(os.getenv("TX_FETCH_RPS", "0.25"))
