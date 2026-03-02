from collections import deque
from dataclasses import dataclass
from typing import Deque


@dataclass
class OrderFlowState:
    """
    Simple container for orderflow metrics.
    - pressure:   -1.0 (strong down) → +1.0 (strong up)
    - shock:      True if last move is an abnormal spike
    - avg_delta:  average price change in the window
    - last_delta: last price change
    """
    pressure: float
    shock: bool
    avg_delta: float
    last_delta: float


class OrderFlowEngine:
    """
    Synthetic OrderFlow v1 for Deriv indices (no real L2 book).
    Uses only ticks to build a "pressure" and "shock" signal.

    Idea:
      - track last N deltas (price differences)
      - pressure = (up-move volume - down-move volume) / total_abs_moves
      - shock    = last_delta is a big outlier vs recent deltas
    """

    def __init__(self, window: int = 30, shock_zscore: float = 3.0):
        self.window: int = window
        self.shock_zscore: float = shock_zscore
        self.deltas: Deque[float] = deque(maxlen=window)
        self.last_price: float | None = None

    def update(self, price: float) -> OrderFlowState:
        """
        Update with a new price tick and return current state.
        """
        if self.last_price is not None:
            delta = price - self.last_price
            self.deltas.append(delta)
        self.last_price = price

        # not enough history yet
        if len(self.deltas) < 3:
            return OrderFlowState(
                pressure=0.0,
                shock=False,
                avg_delta=0.0,
                last_delta=0.0,
            )

        # compute pressure
        up_volume = sum(d for d in self.deltas if d > 0)
        down_volume = -sum(d for d in self.deltas if d < 0)
        total = up_volume + down_volume

        if total <= 0:
            pressure = 0.0
        else:
            pressure = (up_volume - down_volume) / total  # -1..+1

        # stats for shock detection
        avg = sum(self.deltas) / len(self.deltas)
        var = sum((d - avg) ** 2 for d in self.deltas) / len(self.deltas)
        std = var ** 0.5
        last_delta = self.deltas[-1]

        shock = False
        if std > 0:
            # how many std devs away is last_delta?
            z = abs(last_delta - avg) / std
            shock = z >= self.shock_zscore

        return OrderFlowState(
            pressure=pressure,
            shock=shock,
            avg_delta=avg,
            last_delta=last_delta,
        )
