import numpy as np
import logging

logger = logging.getLogger("ai-service")


class SimpleAIModelV2:
    """
    Safe and flexible model:
    - Ignores unknown features
    - Fills missing features with defaults
    - Never raises errors
    - Always returns a probability in [0, 1]
    """

    # Expected consistent input features for prediction
    REQUIRED_FEATURES = {
        "price_change": 0.0,
        "volatility": 0.1,
        "time_of_day": 720,
        "tick_direction": 0
    }

    def __init__(self):
        logger.info("SimpleAIModelV2 initialized.")
        self.is_trained = False

    # ---------------------------------------------------------
    # PREDICT
    # ---------------------------------------------------------
    def predict(self, features: dict) -> float:
        safe = {}

        # Fill in required features, ignore unknown features
        for key, default in self.REQUIRED_FEATURES.items():
            safe[key] = float(features.get(key, default))

        # Example simple scoring logic (same as v1)
        score = (
            (safe["price_change"] * 5) +
            (safe["tick_direction"] * 0.1) +
            (safe["volatility"] * 0.3) -
            abs(720 - safe["time_of_day"]) * 0.0002
        )

        # Normalize
        prob = 1 / (1 + np.exp(-score))

        return float(max(0.0, min(1.0, prob)))

    # ---------------------------------------------------------
    # TRAIN
    # ---------------------------------------------------------
    def train(self, df):
        """
        Optional — keeps pipeline compatible.
        Training does nothing for now.
        """
        logger.info(f"Training received {len(df)} rows, training skipped.")
        self.is_trained = True

class AIIntelligenceV1:
    """
    PATCH 1 – Trend State Engine + Volatility Normalization + Pre-Filter AI Score
    Safe drop-in module. Does not interfere with existing classes.
    """

    def __init__(self):
        self.prev_price = None
        self.trend_state = "UNKNOWN"
        self.reliability = 50  # Adaptive memory of signal quality

    def analyze_tick(self, price: float, volatility: float):
        # First tick
        if self.prev_price is None:
            self.prev_price = price
            return {"trend": "INIT", "score": 0}

        # ---- 1. Normalized delta ----
        delta = price - self.prev_price
        nd = delta / (volatility + 1e-6)

        # ---- 2. Trend Classification ----
        if nd > 2:
            trend = "UP_STRONG"
        elif nd > 1:
            trend = "UP_MEDIUM"
        elif nd > 0.3:
            trend = "UP_WEAK"
        elif nd < -2:
            trend = "DOWN_STRONG"
        elif nd < -1:
            trend = "DOWN_MEDIUM"
        elif nd < -0.3:
            trend = "DOWN_WEAK"
        else:
            trend = "RANGE"

        self.trend_state = trend

        # ---- 3. Confidence Score ----
        base = 50
        if "STRONG" in trend:
            base += 30
        elif "MEDIUM" in trend:
            base += 15
        elif "WEAK" in trend:
            base += 5
        else:
            base -= 10

        # Reliability memory influence
        score = max(0, min(100, base + (self.reliability - 50) * 0.4))

        # Smooth reliability update
        self.reliability = (self.reliability * 0.9) + (base * 0.1)

        self.prev_price = price

        return {
            "trend": trend,
            "normalized_delta": nd,
            "score": score
        }
class AIIntervalMapper:
    """
    PATCH 2 – Converts intelligence scores into dynamic entry intervals.
    Attach to your AI handler or call directly from engine.
    """

    def __init__(self):
        pass

    def compute_interval(self, score: float, trend: str, volatility: float):
        """
        Returns recommended interval (seconds) for next trade.
        """

        # 1 — Trend base interval
        if "STRONG" in trend:
            base = 2.0
        elif "MEDIUM" in trend:
            base = 3.5
        elif "WEAK" in trend:
            base = 5.0
        else:  # RANGE / CHOP
            base = 7.0

        # 2 — Score adjustment
        # Score 0 → slowest   (x1.5)
        # Score 100 → fastest (x0.5)
        score_factor = 1.5 - (score / 100)  # 0.5 → 1.5

        # 3 — Volatility smoothing
        # If volatility is high, slow down slightly
        vol_factor = 1.0 + min(0.30, volatility * 0.5)

        interval = base * score_factor * vol_factor

        # Clamp interval into safe limits (1.5s – 10s)
        interval = min(max(interval, 1.5), 10.0)

        return interval

    def analyze_and_map(self, ai_patch1, price: float, volatility: float):
        """
        Convenience wrapper:
        - Use Patch1 intelligence
        - Generate interval
        - Return full AI response
        """

        result = ai_patch1.analyze_tick(price, volatility)

        trend = result["trend"]
        score = result["score"]

        interval = self.compute_interval(score, trend, volatility)

        result["interval"] = interval
        return result
