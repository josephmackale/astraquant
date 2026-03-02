from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from typing import Optional, Dict, Any
import math
import statistics
import uvicorn

app = FastAPI(title="AstraQuant AI Assess Service (v2 - Balanced)", version="2.1")

# -----------------------------
# Request / Response Models
# -----------------------------
class AssessRequest(BaseModel):
    price: float
    volatility: float
    market: str
    recent_ticks: Optional[list] = Field(default_factory=list)  # list of recent price floats (newest last)
    recent_volume: Optional[list] = Field(default_factory=list) # optional volumes aligned with ticks
    session: Optional[str] = None  # e.g. "asia", "europe", "america"
    account_state: Optional[Dict[str, Any]] = None  # optional metadata (balance, recent_winrate, etc.)

class AssessResponse(BaseModel):
    # core decision fields required by the engine
    enter: bool
    direction: Optional[str] = None       # "CALL" / "PUT" or None
    confidence: float = 0.0               # 0.0 - 1.0 normalized confidence

    # existing diagnostics (kept for observability)
    score: float  # 0.0 - 1.0 normalized confidence (internal)
    trend: Optional[str] = None  # "UP" or "DOWN" or None
    features: Dict[str, Any] = Field(default_factory=dict)  # diagnostic features
    reason: Optional[str] = None
    interval_s: Optional[float] = None  # suggested wait interval in seconds

# -----------------------------
# Utility / Feature functions
# -----------------------------
def safe_mean(arr):
    if not arr:
        return 0.0
    try:
        return statistics.mean(arr)
    except Exception:
        return sum(arr) / len(arr) if arr else 0.0

def simple_sma(ticks, window=5):
    if not ticks or len(ticks) < 1:
        return None
    window = min(window, len(ticks))
    return safe_mean(ticks[-window:])

def returns(ticks):
    if not ticks or len(ticks) < 2:
        return []
    out = []
    for i in range(1, len(ticks)):
        prev = ticks[i-1]
        cur = ticks[i]
        if prev == 0:
            out.append(0.0)
        else:
            out.append((cur - prev) / prev)
    return out

def momentum(ticks, short=3, long=8):
    if len(ticks) < 2:
        return 0.0
    s = simple_sma(ticks, short) or ticks[-1]
    l = simple_sma(ticks, long) or ticks[-1]
    denom = max(abs(l), 1e-9)
    return (s - l) / denom

def momentum_angle(ticks):
    r = returns(ticks[-10:]) if len(ticks) >= 2 else []
    if not r:
        return 0.0
    slope = safe_mean(r)
    return math.degrees(math.atan(slope))

def volatility_profile(ticks):
    if not ticks or len(ticks) < 2:
        return 0.0
    r = returns(ticks)
    if not r:
        return 0.0
    try:
        return statistics.pstdev(r)
    except Exception:
        m = safe_mean(r)
        var = sum((x - m) ** 2 for x in r) / len(r)
        return math.sqrt(var)

def volatility_compression(ticks, lookback=20):
    if len(ticks) < 5:
        return 0.0
    v_short = volatility_profile(ticks[-5:])
    v_long = volatility_profile(ticks[-lookback:]) if len(ticks) >= lookback else volatility_profile(ticks)
    if v_long == 0:
        return 0.0
    return (v_long - v_short) / v_long

def anti_chop_detector(ticks, window=12, thresh=0.0025):
    if len(ticks) < window:
        return False
    segment = ticks[-window:]
    highs = max(segment)
    lows = min(segment)
    if lows <= 0:
        return False
    rng = (highs - lows) / lows
    return rng < thresh

def trend_stability(ticks, lookback=20):
    r = returns(ticks[-lookback:]) if len(ticks) >= 2 else []
    if not r:
        return 1.0
    signs = [1 if x > 0 else (-1 if x < 0 else 0) for x in r]
    prev = None
    changes = 0
    effective = 0
    for s in signs:
        if s == 0:
            continue
        effective += 1
        if prev is None:
            prev = s
            continue
        if s != prev:
            changes += 1
            prev = s
    if effective <= 1:
        return 1.0
    stability = 1.0 - (changes / effective)
    return max(0.0, min(1.0, stability))

def orderflow_synthetic(recent_volumes, ticks):
    if not recent_volumes or not ticks:
        return 0.0
    if len(recent_volumes) != len(ticks):
        n = min(len(recent_volumes), len(ticks))
        vols = recent_volumes[-n:]
        tks = ticks[-n:]
    else:
        vols = recent_volumes
        tks = ticks
    if len(tks) < 2 or sum(vols) == 0:
        return 0.0
    rs = [tks[i] - tks[i-1] for i in range(1, len(tks))]
    vs = vols[1:]
    if not vs:
        return 0.0
    signed = sum(r * v for r, v in zip(rs, vs))
    norm = sum(vs)
    raw = signed / (norm + 1e-9)
    return max(-1.0, min(1.0, math.tanh(raw * 5.0)))

def session_bias(session_str):
    if not session_str:
        return 0.0
    s = session_str.lower()
    if "asia" in s:
        return 0.0
    if "europe" in s:
        return 0.01
    if "america" in s or "us" in s:
        return 0.02
    return 0.0

# -----------------------------
# Scoring function (Balanced)
# -----------------------------
def compute_score(features):
    w = {
        "momentum": 3.0,
        "momentum_angle": 0.6,
        "vol_comp": 1.2,
        "orderflow": 2.0,
        "volatility": -0.8,
        "anti_chop": -2.5,
        "trend_stability": 1.5,
        "session_bias": 0.6,
    }

    raw = 0.0
    raw += w["momentum"] * features.get("momentum", 0.0)
    raw += w["momentum_angle"] * (features.get("momentum_angle", 0.0) / 10.0)
    raw += w["vol_comp"] * features.get("volatility_compression", 0.0)
    raw += w["orderflow"] * features.get("orderflow", 0.0)
    raw += w["volatility"] * features.get("volatility", 0.0)
    raw += w["anti_chop"] * (1.0 if features.get("anti_chop") else 0.0)
    raw += w["trend_stability"] * features.get("trend_stability", 1.0)
    raw += w["session_bias"] * features.get("session_bias", 0.0)

    scaled = math.tanh(raw)
    score = (scaled + 1.0) / 2.0
    return max(0.0, min(1.0, score))

# -----------------------------
# Interval mapping (AI -> trading interval)
# -----------------------------
def compute_interval_from_features(features, score,
                                   min_interval=7.5,
                                   max_interval=30.0,
                                   bias_factor=1.0):
    base = max_interval - score * (max_interval - min_interval)
    vol = features.get("volatility", 0.0)
    vol_multiplier = 1.0 + min(max(vol * 5.0, 0.0), 2.0)
    stability = features.get("trend_stability", 1.0)
    stability_multiplier = 1.0 - (0.25 * (stability - 0.5))
    state = features.get("market_state", None) or ""
    state = state.upper()
    state_mut = 1.0
    if state == "CHOP":
        state_mut = 1.8
    elif state == "LOW_VOL":
        state_mut = 1.5
    elif state == "SPIKE":
        state_mut = 1.6
    elif state == "TRENDING":
        state_mut = 0.9
    oflow = features.get("orderflow", 0.0)
    oflow_adjust = 1.0 - min(max(oflow * 0.5, -0.2), 0.2)
    interval = base * vol_multiplier * stability_multiplier * state_mut * oflow_adjust * bias_factor
    interval = max(min_interval, min(max_interval, float(interval)))
    return round(interval, 2)

# -----------------------------
# Market State Classifier
# -----------------------------
def classify_market_state(features):
    vol = features.get("volatility", 0.0)
    vol_comp = features.get("volatility_compression", 0.0)
    anti_chop = features.get("anti_chop", False)
    stability = features.get("trend_stability", 1.0)

    if vol < 1e-6:
        return "LOW_VOL"
    if anti_chop:
        return "CHOP"
    if vol_comp < -0.6 and vol > 0.005:
        return "SPIKE"
    if stability >= 0.6 and (abs(features.get("momentum", 0.0)) > 0.0008 or vol_comp > 0.1):
        return "TRENDING"
    return "NEUTRAL"

# -----------------------------
# Main assess endpoint (v2 Balanced) - PATCHED to include direction & confidence
# -----------------------------
@app.post("/v1/assess", response_model=AssessResponse)
async def assess(req: AssessRequest):
    """
    Simple 3-tick scalper:
    - If last 3 ticks are strictly up    -> enter CALL
    - If last 3 ticks are strictly down  -> enter PUT
    - Otherwise                          -> no trade
    """

    ticks = req.recent_ticks or []
    vols = req.recent_volume or []

    if not ticks and req.price is not None:
        ticks = [req.price]

    ticks = [float(x) for x in ticks] if ticks else []
    vols = [float(x) for x in vols] if vols else []

    if not ticks:
        return AssessResponse(
            enter=False,
            direction=None,
            confidence=0.0,
            score=0.0,
            trend=None,
            features={"market_state": "NO_DATA"},
            reason="no_ticks",
            interval_s=7.5,
        )

    # 3-tick pattern detection
    def detect_momentum(prices):
        if len(prices) < 3:
            return None
        a, b, c = prices[-3], prices[-2], prices[-1]
        if a < b < c:
            return "UP"
        if a > b > c:
            return "DOWN"
        return None

    momentum_dir = detect_momentum(ticks)

    # simple volatility estimate
    if len(ticks) >= 2:
        window = ticks[-20:]
        mean = sum(window) / len(window)
        var = sum((p - mean) ** 2 for p in window) / len(window)
        vol_est = math.sqrt(var) if var > 0 else 0.0
    else:
        vol_est = 0.0

    last_price = ticks[-1]
    delta = ticks[-1] - ticks[-2] if len(ticks) >= 2 else 0.0

    # Decide entry
    if momentum_dir == "UP":
        enter = True
        trend = "UP"
        direction = "CALL"
        reason = "three_tick_momentum"
        market_state = "ENTRY"
        score = 0.9
        confidence = 0.9

    elif momentum_dir == "DOWN":
        enter = True
        trend = "DOWN"
        direction = "PUT"
        reason = "three_tick_momentum"
        market_state = "ENTRY"
        score = 0.9
        confidence = 0.9

    else:
        enter = False
        trend = None
        direction = None
        reason = "no_three_tick_pattern"
        market_state = "FLAT"
        score = 0.3
        confidence = 0.3

    # Interval suggestion
    interval_s = 7.5

    # Diagnostic features for engine logs
    features = {
        "last_price": last_price,
        "delta": delta,
        "volatility": vol_est,
        "prices_len": len(ticks),
        "momentum_dir": momentum_dir,
        "three_tick_pattern": momentum_dir is not None,
        "market_state": market_state,
        "interval_s": interval_s,
    }

    return AssessResponse(
        enter=enter,
        direction=direction,
        confidence=round(confidence, 3),
        score=round(score, 3),
        trend=trend,
        features=features,
        reason=reason,
        interval_s=interval_s,
    )

# -----------------------------
# HEALTH ENDPOINT (required by docker)
# -----------------------------
@app.get("/health")
async def health():
    return {"status": "ok", "mode": "ai_v2_balanced", "version": "2.1"}

# -----------------------------
# Quick runner for local dev
# -----------------------------
if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8092)
