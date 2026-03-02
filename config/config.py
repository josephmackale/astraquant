# File: config.py

import os

class Config:
    """
    Central configuration for the real Deriv execution engine.
    All sensitive values MUST come from environment variables.
    """

    # -------------------------------
    # REAL DERIV CONNECTION SETTINGS
    # -------------------------------
    DERIV_APP_ID = os.getenv("DERIV_APP_ID", "1089")  
    # 1089 is a safe public app_id for testing. Replace in production.

    DERIV_WS_URL = (
        f"wss://ws.derivws.com/websockets/v3?app_id={DERIV_APP_ID}"
    )

    # Trading token (demo or real). Must be passed by env.
    DERIV_API_TOKEN = os.getenv("DERIV_API_TOKEN", None)
    if not DERIV_API_TOKEN:
        print("⚠️ WARNING: DERIV_API_TOKEN is not set — real trading will fail.")

    # -------------------------------
    # ENGINE RUNTIME CONFIG
    # -------------------------------
    TICK_BUFFER = int(os.getenv("TICK_BUFFER", "200"))
    MAX_CONCURRENT_TRADES = int(os.getenv("MAX_CONCURRENT_TRADES", "2"))

    # -------------------------------
    # RISK SETTINGS
    # -------------------------------
    BASE_RISK_PCT = float(os.getenv("BASE_RISK_PCT", "0.01"))  # 1%
    MIN_STAKE = float(os.getenv("MIN_STAKE", "0.35"))          # Deriv minimum
    MAX_STAKE = float(os.getenv("MAX_STAKE", "2000"))

    # -------------------------------
    # ARC (Adaptive Risk Control)
    # -------------------------------
    ARC_ENABLED = os.getenv("ARC_ENABLED", "false").lower() == "true"
    ARC_MAX_MULTIPLIER = float(os.getenv("ARC_MAX_MULTIPLIER", "4.0"))

    # -------------------------------
    # ENGINE BEHAVIOR
    # -------------------------------
    LOOP_INTERVAL_MS = int(os.getenv("LOOP_INTERVAL_MS", "0"))  # event-driven
