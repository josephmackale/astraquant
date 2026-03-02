# engine/logger_v1.py
from __future__ import annotations

import json
import os
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


@dataclass(frozen=True)
class V1LogPaths:
    base_dir: Path

    @property
    def ticks(self) -> Path: return self.base_dir / "ticks.jsonl"

    @property
    def orderflow(self) -> Path: return self.base_dir / "orderflow.jsonl"

    @property
    def state(self) -> Path: return self.base_dir / "state.jsonl"

    @property
    def decisions(self) -> Path: return self.base_dir / "decisions.jsonl"

    @property
    def trades(self) -> Path: return self.base_dir / "trades.jsonl"

    @property
    def health(self) -> Path: return self.base_dir / "health.jsonl"


class V1Logger:
    """
    Append-only JSONL logger for AstraQuant V1.
    - Thread-safe
    - Flushes each write (you can tail -f live)
    - Schema-light but consistent
    """

    def __init__(self, base_dir: Optional[str] = None):
        base = base_dir or os.getenv("AQ_LOG_DIR", "/app/logs/v1")
        self.paths = V1LogPaths(Path(base))
        self.paths.base_dir.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()

    def _write(self, path: Path, payload: Dict[str, Any]) -> None:
        payload.setdefault("ts", _utc_now_iso())
        line = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
        with self._lock:
            with path.open("a", encoding="utf-8") as f:
                f.write(line + "\n")
                f.flush()

    # ---- Event helpers (your core layers) ----

    def tick(self, symbol: str, bid: float | None, ask: float | None, mid: float | None, spread: float | None,
             source: str = "ctrader", session: str = "off", extra: Optional[Dict[str, Any]] = None) -> None:
        self._write(self.paths.ticks, {
            "type": "tick",
            "symbol": symbol,
            "bid": bid,
            "ask": ask,
            "mid": mid,
            "spread": spread,
            "source": source,
            "session": session,
            **(extra or {}),
        })

    def orderflow_event(self, symbol: str, event_type: str, price_from: float | None = None, price_to: float | None = None,
                        duration_ms: int | None = None, tick_count: int | None = None,
                        strength_score: float | None = None, context: Optional[Dict[str, Any]] = None) -> None:
        self._write(self.paths.orderflow, {
            "type": "orderflow_event",
            "symbol": symbol,
            "event_type": event_type,
            "price_from": price_from,
            "price_to": price_to,
            "duration_ms": duration_ms,
            "tick_count": tick_count,
            "strength_score": strength_score,
            "context": context or {},
        })

    def state(self, symbol: str, state: str, reason: str = "", state_data: Optional[Dict[str, Any]] = None) -> None:
        self._write(self.paths.state, {
            "type": "engine_state",
            "symbol": symbol,
            "state": state,
            "reason": reason,
            "state_data": state_data or {},
        })

    def decision(self, symbol: str, decision: str, confidence: float | None = None, reason: str = "",
                 context: Optional[Dict[str, Any]] = None) -> None:
        self._write(self.paths.decisions, {
            "type": "trade_decision",
            "symbol": symbol,
            "decision": decision,  # enter_long/enter_short/skip/exit/hold
            "confidence": confidence,
            "reason": reason,
            "context": context or {},
        })

    def trade(self, symbol: str, direction: str, entry_price: float, sl: float | None, tp: float | None,
              trade_id: str | None = None, extra: Optional[Dict[str, Any]] = None) -> None:
        self._write(self.paths.trades, {
            "type": "trade_open",
            "symbol": symbol,
            "trade_id": trade_id,
            "direction": direction,
            "entry_price": entry_price,
            "sl": sl,
            "tp": tp,
            **(extra or {}),
        })

    def trade_close(self, symbol: str, trade_id: str | None, exit_price: float, result: str,
                    pnl: float | None = None, exit_reason: str = "", extra: Optional[Dict[str, Any]] = None) -> None:
        self._write(self.paths.trades, {
            "type": "trade_close",
            "symbol": symbol,
            "trade_id": trade_id,
            "exit_price": exit_price,
            "result": result,  # win/loss/breakeven
            "pnl": pnl,
            "exit_reason": exit_reason,
            **(extra or {}),
        })

    def health(self, status: str = "ok", cpu: float | None = None, mem: float | None = None,
               tick_lag_ms: int | None = None, warnings: Optional[Dict[str, Any]] = None) -> None:
        self._write(self.paths.health, {
            "type": "engine_health",
            "status": status,  # ok/degraded/halted
            "cpu": cpu,
            "mem": mem,
            "tick_lag_ms": tick_lag_ms,
            "warnings": warnings or {},
        })
