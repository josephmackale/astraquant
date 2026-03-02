# engine/ws_sync.py
import math
import time
from collections import deque
from typing import Dict, Any, Deque

# sliding window in seconds and max samples
WINDOW_SECONDS = 60
MAX_SAMPLES = 120  # keep a bit more

class TickAggregator:
    def __init__(self):
        self.ticks: Deque[Dict[str, Any]] = deque()
        self.last_price = None

    def on_tick(self, price: float, ts: float = None) -> Dict[str, Any]:
        """
        Call this for every tick. Returns aggregated stats:
          - volatility: estimated stddev of returns in decimal
          - recent_return: last price change %
        """
        if ts is None:
            ts = time.time()
        self.ticks.append({"ts": ts, "price": float(price)})
        self.last_price = float(price)

        # prune old
        cutoff = ts - WINDOW_SECONDS
        while self.ticks and self.ticks[0]["ts"] < cutoff:
            self.ticks.popleft()

        # compute log returns
        prices = [p["price"] for p in self.ticks]
        if len(prices) < 2:
            return {"volatility": 0.0, "recent_return": 0.0, "price": price}

        returns = []
        for i in range(1, len(prices)):
            # use simple returns
            r = (prices[i] - prices[i-1]) / max(1e-8, prices[i-1])
            returns.append(r)
        # volatility = stddev of returns * sqrt(samples) -> approximate annualization not needed
        mean = sum(returns) / len(returns)
        var = sum((x - mean) ** 2 for x in returns) / max(1, len(returns) - 1)
        vol = math.sqrt(var) if var >= 0 else 0.0

        # scale to typical decimal volatility: clamp to 0..1 in practice
        volatility = min(1.0, vol)

        recent_return = returns[-1] if returns else 0.0

        return {"volatility": volatility, "recent_return": recent_return, "price": price}
