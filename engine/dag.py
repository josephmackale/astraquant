# engine/dag.py
# ASTRAQUANT V1 — Decision Agreement Gate (DAG) v1.1
# Purpose: Reduce over-participation by requiring early "prove-then-commit" agreement.
# - Strategy remains unchanged. This gate sits between decisions and execution/logging.
# - Produces dag_events.jsonl for post-analysis.
#
# DAG v1.1 rules:
# 1) Cooldown gate: block if opposite-direction signal happened within cooldown_seconds.
# 2) Pending (prove window): when enter_long/enter_short arrives, create PENDING signal.
# 3) During pending:
#    - cancel if opposite enter_* arrives (opposite_signal)
#    - cancel if adverse excursion reaches max_adverse_ticks (early_adverse)
#    - cancel if prove window expires (expired)
#    - commit if favorable excursion reaches prove_ticks (commit)
#
# Notes:
# - Works in OBSERVE-FIRST mode: controller decides what "commit" means (log-only or execute).
# - Uses mid price for prove checks (controller passes price=mid).
# - tick_size defaults to 0.01 for XAUUSD as per your logs; adjust via env if needed.

from __future__ import annotations

import json
import os
import time
import uuid
from dataclasses import dataclass
from typing import Any, Dict, Literal, Optional

Direction = Literal["long", "short"]
Decision = Literal["enter_long", "enter_short", "skip"]
BlockReason = Literal["cooldown", "opposite_signal", "early_adverse", "expired", "pending_exists"]


def _env_int(name: str, default: int) -> int:
    v = os.getenv(name)
    if v is None or v == "":
        return default
    try:
        return int(v)
    except Exception:
        return default


def _env_float(name: str, default: float) -> float:
    v = os.getenv(name)
    if v is None or v == "":
        return default
    try:
        return float(v)
    except Exception:
        return default


@dataclass
class DagConfig:
    # Core parameters
    cooldown_seconds: int = _env_int("DAG_COOLDOWN_SECONDS", 20)
    prove_window_seconds: int = _env_int("DAG_PROVE_WINDOW_SECONDS", 8)
    prove_ticks: int = _env_int("DAG_PROVE_TICKS", 3)
    max_adverse_ticks: int = _env_int("DAG_MAX_ADVERSE_TICKS", 2)

    # Market microstructure
    tick_size: float = _env_float("DAG_TICK_SIZE", 0.01)

    # Logging
    log_path: str = os.getenv(
        "DAG_LOG_PATH",
        "/opt/astraquant/logs/engine/v1/dag_events.jsonl"
    )


@dataclass
class PendingSignal:
    signal_id: str
    direction: Direction
    signal_price: float
    signal_ts: float           # epoch seconds
    expires_at: float          # epoch seconds
    max_favorable_ticks: int = 0
    max_adverse_ticks: int = 0
    status: Literal["PENDING", "BLOCKED", "COMMITTED"] = "PENDING"
    block_reason: Optional[BlockReason] = None


class DagGate:
    def __init__(self, cfg: Optional[DagConfig] = None):
        self.cfg = cfg or DagConfig()
        self.pending: Optional[PendingSignal] = None

        # Track last time we saw a raw signal per direction (epoch seconds)
        self.last_signal_ts_by_dir: Dict[Direction, float] = {"long": -1e18, "short": -1e18}

        self._ensure_log_dir()

    # --------------------------
    # Internals
    # --------------------------
    def _ensure_log_dir(self) -> None:
        d = os.path.dirname(self.cfg.log_path)
        if d and not os.path.exists(d):
            os.makedirs(d, exist_ok=True)

    def _log(self, payload: Dict[str, Any]) -> None:
        payload.setdefault("ts", time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime()))
        with open(self.cfg.log_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(payload, ensure_ascii=False) + "\n")

    def _decision_to_dir(self, decision: str) -> Optional[Direction]:
        if decision == "enter_long":
            return "long"
        if decision == "enter_short":
            return "short"
        return None

    def _opposite(self, d: Direction) -> Direction:
        return "short" if d == "long" else "long"

    def _signed_ticks_in_favor(self, direction: Direction, current_price: float, signal_price: float) -> int:
        """
        Returns ticks in FAVOR of direction.
          long : (current - signal) / tick_size
          short: (signal - current) / tick_size
        Rounded to nearest int.
        """
        raw = (current_price - signal_price) if direction == "long" else (signal_price - current_price)
        return int(round(raw / self.cfg.tick_size))

    # --------------------------
    # Public API
    # --------------------------
    def on_decision(self, decision: str, price: float, now_ts: float) -> Dict[str, Any]:
        """
        Called when the strategy emits a decision.
        Returns:
          {"action":"none"}
          {"action":"pending_created","signal_id":...}
          {"action":"blocked","signal_id":...,"reason":...}
        """
        direction = self._decision_to_dir(decision)
        if direction is None:
            return {"action": "none"}

        opp = self._opposite(direction)

        # Cooldown: if opposite signal occurred too recently, block.
        last_opp_ts = self.last_signal_ts_by_dir[opp]
        opp_age = now_ts - last_opp_ts
        if opp_age < self.cfg.cooldown_seconds:
            signal_id = uuid.uuid4().hex
            self._log({
                "type": "dag_blocked",
                "signal_id": signal_id,
                "direction": direction,
                "price": price,
                "reason": "cooldown",
                "cooldown_seconds": self.cfg.cooldown_seconds,
                "last_opposite_signal_age_s": round(opp_age, 3),
            })
            self.last_signal_ts_by_dir[direction] = now_ts
            return {"action": "blocked", "signal_id": signal_id, "reason": "cooldown"}

        # If already pending, do not overwrite (prevents spam). Block new signal.
        if self.pending and self.pending.status == "PENDING":
            signal_id = uuid.uuid4().hex
            self._log({
                "type": "dag_blocked",
                "signal_id": signal_id,
                "direction": direction,
                "price": price,
                "reason": "pending_exists",
                "pending_signal_id": self.pending.signal_id,
            })
            self.last_signal_ts_by_dir[direction] = now_ts
            return {"action": "blocked", "signal_id": signal_id, "reason": "pending_exists"}

        # Create pending signal
        signal_id = uuid.uuid4().hex
        self.pending = PendingSignal(
            signal_id=signal_id,
            direction=direction,
            signal_price=price,
            signal_ts=now_ts,
            expires_at=now_ts + float(self.cfg.prove_window_seconds),
        )
        self.last_signal_ts_by_dir[direction] = now_ts

        self._log({
            "type": "dag_signal",
            "signal_id": signal_id,
            "direction": direction,
            "price": price,
            "prove_window_seconds": self.cfg.prove_window_seconds,
            "prove_ticks": self.cfg.prove_ticks,
            "max_adverse_ticks": self.cfg.max_adverse_ticks,
            "tick_size": self.cfg.tick_size,
        })
        return {"action": "pending_created", "signal_id": signal_id}

    def on_tick(self, price: float, now_ts: float, latest_decision: Optional[str] = None) -> Dict[str, Any]:
        """
        Called on every tick.
        Returns:
          {"action":"none"}
          {"action":"blocked","signal_id":...,"reason":...}
          {"action":"commit","signal_id":...,"direction":...,"commit_price":...}
        """
        if not self.pending or self.pending.status != "PENDING":
            return {"action": "none"}

        p = self.pending

        # Expired
        if now_ts > p.expires_at:
            p.status = "BLOCKED"
            p.block_reason = "expired"
            self._log({
                "type": "dag_blocked",
                "signal_id": p.signal_id,
                "direction": p.direction,
                "reason": "expired",
                "max_favorable": p.max_favorable_ticks,
                "max_adverse": p.max_adverse_ticks,
            })
            return {"action": "blocked", "signal_id": p.signal_id, "reason": "expired"}

        # Opposite signal during pending -> cancel
        if latest_decision in ("enter_long", "enter_short"):
            inc_dir = self._decision_to_dir(latest_decision)
            if inc_dir and inc_dir != p.direction:
                p.status = "BLOCKED"
                p.block_reason = "opposite_signal"
                self._log({
                    "type": "dag_blocked",
                    "signal_id": p.signal_id,
                    "direction": p.direction,
                    "reason": "opposite_signal",
                    "max_favorable": p.max_favorable_ticks,
                    "max_adverse": p.max_adverse_ticks,
                })
                return {"action": "blocked", "signal_id": p.signal_id, "reason": "opposite_signal"}

        # Update excursions
        favorable = self._signed_ticks_in_favor(p.direction, price, p.signal_price)
        adverse = -favorable

        if favorable > p.max_favorable_ticks:
            p.max_favorable_ticks = favorable
        if adverse > p.max_adverse_ticks:
            p.max_adverse_ticks = adverse

        # Early adverse cancellation
        if p.max_adverse_ticks >= self.cfg.max_adverse_ticks:
            p.status = "BLOCKED"
            p.block_reason = "early_adverse"
            self._log({
                "type": "dag_blocked",
                "signal_id": p.signal_id,
                "direction": p.direction,
                "reason": "early_adverse",
                "max_favorable": p.max_favorable_ticks,
                "max_adverse": p.max_adverse_ticks,
            })
            return {"action": "blocked", "signal_id": p.signal_id, "reason": "early_adverse"}

        # Commit if proved
        if p.max_favorable_ticks >= self.cfg.prove_ticks:
            p.status = "COMMITTED"
            self._log({
                "type": "dag_committed",
                "signal_id": p.signal_id,
                "direction": p.direction,
                "commit_price": price,
                "ticks_to_prove": p.max_favorable_ticks,
                "time_to_prove_ms": int((now_ts - p.signal_ts) * 1000),
                "max_adverse": p.max_adverse_ticks,
            })
            return {
                "action": "commit",
                "signal_id": p.signal_id,
                "direction": p.direction,
                "commit_price": price,
            }

        return {"action": "none"}
