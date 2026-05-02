from dataclasses import dataclass
import os
from dotenv import load_dotenv

load_dotenv()


@dataclass
class Settings:

    # =========================
    # API URLs
    # =========================
    BINANCE_BASE_URL: str = os.getenv("BINANCE_BASE_URL", "https://api.binance.com")
    COINBASE_BASE_URL: str = os.getenv("COINBASE_BASE_URL", "https://api.exchange.coinbase.com")
    COINAPI_BASE_URL: str = os.getenv("COINAPI_BASE_URL", "https://rest.coinapi.io")

    # =========================
    # API KEYS
    # =========================
    COINAPI_KEY: str = os.getenv("COINAPI_KEY", "")

    # =========================
    # SYMBOLS
    # =========================
    SYMBOL_BINANCE: str = os.getenv("SYMBOL_BINANCE", "BTCUSDT")
    SYMBOL_COINBASE: str = os.getenv("SYMBOL_COINBASE", "BTC-USD")
    SYMBOL_COINAPI: str = os.getenv("SYMBOL_COINAPI", "BITSTAMP_SPOT_BTC_USD")

    # =========================
    # TIMEFRAME
    # =========================
    INTERVAL: str = os.getenv("INTERVAL", "1m")
    COINBASE_GRANULARITY: int = int(os.getenv("COINBASE_GRANULARITY", "60"))
    COINAPI_PERIOD_ID: str = os.getenv("COINAPI_PERIOD_ID", "1MIN")

    # =========================
    # EXTRACTION WINDOW
    # =========================
    DEFAULT_DAYS: int = int(os.getenv("DEFAULT_DAYS", "7"))

    # =========================
    # NETWORK / ROBUSTNESS
    # =========================
    TIMEOUT: int = int(os.getenv("TIMEOUT", "30"))
    RETRY_COUNT: int = int(os.getenv("RETRY_COUNT", "3"))
    RETRY_SLEEP_SECONDS: float = float(os.getenv("RETRY_SLEEP_SECONDS", "2"))

    # =========================
    # DATA VALIDATION
    # =========================
    VALIDATION_MIN_RATIO: float = float(os.getenv("VALIDATION_MIN_RATIO", "0.95"))

    # =========================
    # GCP
    # =========================
    GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID")
    GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")
    GCS_PREFIX = os.getenv("GCS_PREFIX")

settings = Settings()