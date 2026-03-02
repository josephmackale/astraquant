# File: engine/config.py
#
# CTRADER-only config (Deriv removed).
# Keep this file boring + explicit for stability in production containers.

import os
from dataclasses import dataclass


def _env(key: str, default: str | None = None) -> str | None:
    val = os.getenv(key)
    return val if (val is not None and val != "") else default


def _env_required(key: str) -> str:
    val = _env(key)
    if val is None:
        raise RuntimeError(f"Missing required env var: {key}")
    return val


def _env_int(key: str, default: int) -> int:
    v = _env(key)
    if v is None:
        return default
    try:
        return int(v)
    except Exception:
        raise RuntimeError(f"Invalid int for {key}: {v!r}")


def _env_float(key: str, default: float) -> float:
    v = _env(key)
    if v is None:
        return default
    try:
        return float(v)
    except Exception:
        raise RuntimeError(f"Invalid float for {key}: {v!r}")


def _env_bool(key: str, default: bool) -> bool:
    v = _env(key)
    if v is None:
        return default
    return str(v).strip().lower() in ("1", "true", "yes", "y", "on")


@dataclass(frozen=True)
class Settings:
    # ============================
    # CORE (CTRADER ONLY)
    # ============================
    BROKER: str
    CTRADER_ENV: str
    CTRADER_ACCOUNT_ID: int
    CTRADER_SYMBOL: str

    # Internal tick WS feed (ws service)
    CORE_WS_URL: str

    # ============================
    # MODE FLAGS
    # ============================
    OBSERVE_ONLY: bool

    # ============================
    # SERVICE URLS
    # ============================
    AI_URL: str
    RISK_URL: str

    # ============================
    # LOG PATHS
    # ============================
    LEARN_JSONL_PATH: str

    # ============================
    # SESSION GUARDS (safety rails)
    # ============================
    SESSION_GUARD_LOSS_STREAK: int
    SESSION_GUARD_LOSS_PAUSE_MIN: int
    SESSION_GUARD_DD_USD: float
    SESSION_GUARD_DD_PAUSE_MIN: int


def load_settings(require_ws_url: bool = True) -> Settings:
    broker = (_env("BROKER", "CTRADER") or "CTRADER").upper()
    if broker != "CTRADER":
        raise RuntimeError(
            f"BROKER must be CTRADER (Deriv removed). Got: {broker!r}"
        )

    ctrader_env = (_env("CTRADER_ENV", "demo") or "demo").lower()
    if ctrader_env not in ("demo", "live"):
        raise RuntimeError(f"CTRADER_ENV must be demo or live. Got: {ctrader_env!r}")

    account_id = _env_int("CTRADER_ACCOUNT_ID", 0)
    if account_id <= 0:
        raise RuntimeError("CTRADER_ACCOUNT_ID must be set to a valid positive integer.")

    symbol = _env_required("CTRADER_SYMBOL")

    core_ws_url = _env("CORE_WS_URL", "")
    if require_ws_url and not core_ws_url:
        raise RuntimeError("CORE_WS_URL is required (internal ticks WS feed).")

    return Settings(
        BROKER=broker,
        CTRADER_ENV=ctrader_env,
        CTRADER_ACCOUNT_ID=account_id,
        CTRADER_SYMBOL=symbol,
        CORE_WS_URL=core_ws_url or "",
        OBSERVE_ONLY=_env_bool("OBSERVE_ONLY", True),
        AI_URL=_env("AI_URL", "http://ai:8002") or "http://ai:8002",
        RISK_URL=_env("RISK_URL", "http://risk:8004") or "http://risk:8004",
        LEARN_JSONL_PATH=_env("LEARN_JSONL_PATH", "/opt/astraquant/logs/learn.jsonl")
        or "/opt/astraquant/logs/learn.jsonl",
        SESSION_GUARD_LOSS_STREAK=_env_int("SESSION_GUARD_LOSS_STREAK", 6),
        SESSION_GUARD_LOSS_PAUSE_MIN=_env_int("SESSION_GUARD_LOSS_PAUSE_MIN", 1),
        SESSION_GUARD_DD_USD=_env_float("SESSION_GUARD_DD_USD", 10.0),
        SESSION_GUARD_DD_PAUSE_MIN=_env_int("SESSION_GUARD_DD_PAUSE_MIN", 2),
    )
