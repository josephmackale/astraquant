# engine/controller.py
# ASTRAQUANT V1 — CTRADER ONLY — OBSERVE-FIRST CONTROLLER (DROP-IN)
# ✅ Fixes already present in your version:
# - Implements PA-only exit manager inside on_tick (tick-driven).
# - Hygiene logic indentation so EXECUTION only happens on hygiene PASS.
# - Ensures cooldown + state resets happen correctly (observe_only still logs decisions).
#
# ✅ NEW (surgical additions only):
# - Adds EXIT_MODE switch:
#     EXIT_MODE=pa_manager  -> keeps your current behavior-based exit manager
#     EXIT_MODE=baseline    -> deterministic TP/SL/timeout (continuation-delivery) + trades.jsonl
# - Adds impulse/acceptance bounds tracking to compute baseline TP/SL from price action context
# - Adds minimal trades.jsonl lifecycle output (authoritative close events)
#
# ✅ Step 1 LIVENESS/WATCHDOG:
# - Heartbeat (every HEARTBEAT_EVERY_SEC)
# - Detect "no ticks" (warn + optional stream restart)
#
# ✅ FIX INCLUDED (critical):
# - now_ms is defined BEFORE using it for last_tick_ms

import asyncio
import os
import time
import logging

from collections import deque
from datetime import datetime, timezone

from engine.orderflow import OrderFlowEngine
from engine.ws_server import broadcast
from engine.ctrader.spotware_client import SpotwarePriceClient

# V1 JSONL Logger
from engine.logger_v1 import V1Logger

# ✅ DAG (Decision Agreement Gate)
from engine.dag import DagGate, DagConfig

logger = logging.getLogger("engine-controller")
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))


# --------------------------
# V1 PA thresholds (minimal, tune later)
# --------------------------
ACCEPTANCE_TICKS_MIN = 3
DECISION_COOLDOWN_MS = 60_000

# --------------------------
# ✅ EXECUTION HYGIENE (post-DAG, pre-decision log)
# --------------------------
HYGIENE_CONFIRM_FAVOR_TICKS = 0.5   # must print +0.5 ticks in favor to pass
HYGIENE_MAX_ADVERSE_TICKS   = 1.2   # cancel if -1.2 ticks against before confirm
HYGIENE_WINDOW_TICKS        = 100   # resolve within next 100 ticks

# ==========================
# LIVENESS / WATCHDOG (Step 1)
# ==========================
HEARTBEAT_EVERY_SEC = int(os.getenv("HEARTBEAT_EVERY_SEC", "30"))
NO_TICKS_WARN_SEC = int(os.getenv("NO_TICKS_WARN_SEC", "20"))
NO_TICKS_RESTART_SEC = int(os.getenv("NO_TICKS_RESTART_SEC", "60"))  # set 0 to disable auto-restart

# ==========================
# EXIT MODE SWITCH (NEW)
# ==========================
# "pa_manager"  -> current behavior-based exit manager (your existing code)
# "baseline"    -> deterministic TP/SL/timeout for truthful lifecycle + analytics
EXIT_MODE = os.getenv("EXIT_MODE", "baseline").strip().lower()

# Baseline (V1) exit configuration
EXIT_TIMEOUT_MS = int(os.getenv("EXIT_TIMEOUT_MS", "900000"))              # 15 minutes
EXIT_MIN_BUFFER_TICKS = float(os.getenv("EXIT_MIN_BUFFER_TICKS", "0.02"))  # add to SL beyond acceptance bound
TRADES_PATH = os.getenv("TRADES_PATH", "/opt/astraquant/logs/engine/v1/trades.jsonl")

# ==========================
# PA-ONLY EXIT (your existing manager; keep as-is)
# ==========================
EXIT_NO_EXT_TICKS = int(os.getenv("EXIT_NO_EXT_TICKS", "12"))                # loss of progress
EXIT_TRAIL_ARM_MFE = float(os.getenv("EXIT_TRAIL_ARM_MFE", "1.8"))           # arm trailing after MFE
EXIT_TRAIL_GIVEBACK = float(os.getenv("EXIT_TRAIL_GIVEBACK", "0.8"))         # giveback threshold
EXIT_OPP_INIT_TICKS = float(os.getenv("EXIT_OPP_INIT_TICKS", "1.2"))         # opposite initiative
EXIT_PRESSURE_FLIP_TICKS = int(os.getenv("EXIT_PRESSURE_FLIP_TICKS", "8"))   # optional
EXIT_PRESSURE_FLIP_THRESH = float(os.getenv("EXIT_PRESSURE_FLIP_THRESH", "0.25"))

DEFAULT_FIXED_STAKE = float(os.getenv("FIXED_STAKE", "0.10"))


def _utc_now_ms() -> int:
    return int(time.time() * 1000)


class Controller:
    """
    V1 Controller (cTrader / Spotware Open API)
    - OBSERVE_ONLY=1 (default): logs + broadcasts only, NO execution
    - Robust reconnect loop for VPS continuous run
    - Logs ticks, orderflow events, state, health, decisions
    """

    def __init__(self):

        # --------------------------
        # LIVENESS (Step 1)
        # --------------------------
        self.last_tick_ms = 0
        self.last_heartbeat_ms = 0
        self._watchdog_task = None

        # --------------------------
        # ENV
        # --------------------------
        self.market = os.getenv("CTRADER_SYMBOL")
        if not self.market:
            raise RuntimeError("Missing CTRADER_SYMBOL")

        self.observe_only = os.getenv("OBSERVE_ONLY", "1").lower() in ("1", "true", "yes")

        # --------------------------
        # STATE
        # --------------------------
        self.running = True
        self.tick_count = 0

        self.last_price = None
        self.last_bid = None
        self.last_ask = None
        self.last_mid = None
        self.last_spread = None

        # Buffers for quick local diagnostics (optional)
        self.recent_ticks = deque(maxlen=600)

        # Orderflow (behavioral engine)
        self.orderflow = OrderFlowEngine(window=30, shock_zscore=3.0)
        self.last_of_pressure = 0.0
        self.last_of_shock = False

        # Cooldown
        self.last_decision_ms = 0

        # --------------------------
        # PA STATE (V1 minimal)
        # --------------------------
        self.pa_state = "waiting_impulse"
        self.impulse_dir = None          # "up" | "down"
        self.impulse_price = None
        self.acceptance_start_ms = None

        # ✅ NEW: bounds for baseline TP/SL
        self.impulse_high = None
        self.impulse_low = None
        self.acceptance_high = None
        self.acceptance_low = None

        # Logger (JSONL)
        self.v1log = V1Logger()

        # --------------------------
        # ✅ DAG STATE
        # --------------------------
        self.dag = DagGate(DagConfig())  # writes dag_events.jsonl internally
        self._last_raw_decision = None   # latest raw strategy decision (for opposite-signal cancel)

        # --------------------------
        # ✅ HYGIENE STATE
        # --------------------------
        self._hygiene_pending = None
        # structure:
        # {
        #   "decision": "enter_long"/"enter_short",
        #   "dir": +1/-1,
        #   "entry_price": float,
        #   "ticks_seen": int,
        #   "best_favor": float,
        #   "worst_adverse": float,
        #   "signal_id": str|None,
        #   "commit_price": float|None,
        #   "ctx": dict,  # ✅ NEW: impulse/acceptance bounds snapshot
        # }

        # --------------------------
        # ✅ PA-only position state (single position for V1)
        # --------------------------
        self._open_pos = None

        # Spotware client
        self.spotware = SpotwarePriceClient(self)

        # Execution manager is created lazily (only if needed)
        self.exec_mgr = None

        # ✅ NEW: exit mode + trades path
        self.exit_mode = EXIT_MODE
        self.trades_path = TRADES_PATH

        # Initial state log
        logger.info("🧠 Controller initialized")
        logger.info("🔧 SYMBOL=%s OBSERVE_ONLY=%s EXIT_MODE=%s", self.market, self.observe_only, self.exit_mode)
        self.v1log.state(self.market, "idle", reason="boot", state_data={"observe_only": self.observe_only, "exit_mode": self.exit_mode})

    # --------------------------
    # Helpers
    # --------------------------
    def _reset_pa(self):
        self.pa_state = "waiting_impulse"
        self.impulse_dir = None
        self.impulse_price = None
        self.acceptance_start_ms = None
        # ✅ reset bounds too
        self.impulse_high = None
        self.impulse_low = None
        self.acceptance_high = None
        self.acceptance_low = None

    def _clear_hygiene(self):
        self._hygiene_pending = None

    def _start_hygiene(self, decision: str, mid: float, signal_id=None, commit_price=None, ctx=None):
        self._hygiene_pending = {
            "decision": decision,
            "dir": 1 if decision == "enter_long" else -1,
            "entry_price": float(mid),
            "ticks_seen": 0,
            "best_favor": 0.0,
            "worst_adverse": 0.0,
            "signal_id": signal_id,
            "commit_price": commit_price,
            "ctx": ctx or {},
        }

    async def _ensure_exec_mgr(self):
        if self.exec_mgr is None:
            from engine.execution import ExecutionManager
            self.exec_mgr = ExecutionManager(controller=self)

    def _append_jsonl(self, path: str, obj: dict):
        try:
            os.makedirs(os.path.dirname(path), exist_ok=True)
            import json
            with open(path, "a", encoding="utf-8") as f:
                f.write(json.dumps(obj, separators=(",", ":"), ensure_ascii=False) + "\n")
        except Exception:
            logger.exception("Failed writing jsonl: %s", path)

    def _register_open_pos(
        self,
        *,
        side: str,
        mid: float,
        now_ms: int,
        vol: float,
        signal_id=None,
        exec_result=None,
        tp_price=None,
        sl_price=None,
        ctx=None,
    ):
        if self._open_pos is not None:
            return  # already tracking a position

        order_id = None
        position_id = None
        raw = None

        if isinstance(exec_result, dict):
            order_id = exec_result.get("order_id")
            raw = exec_result.get("raw")
            if isinstance(raw, dict):
                position_id = raw.get("position_id") or raw.get("positionId") or raw.get("positionID")

        self._open_pos = {
            "side": side,                         # "buy" / "sell"
            "dir": 1 if side == "buy" else -1,     # signed direction
            "entry_mid": float(mid),
            "volume": float(vol),
            "opened_ms": int(now_ms),
            "ticks": 0,
            "mfe": 0.0,   # retained for pa_manager mode
            "mae": 0.0,   # retained for pa_manager mode
            "ticks_since_progress": 0,
            "last_progress_mid": float(mid),
            "flip_ticks": 0,
            "signal_id": signal_id,
            "order_id": order_id,
            "position_id": position_id,
            # ✅ NEW baseline fields
            "tp_price": tp_price,
            "sl_price": sl_price,
            "timeout_ms": int(now_ms + EXIT_TIMEOUT_MS),
            "ctx": ctx or {},
        }

        self.v1log.state(
            self.market,
            "position_opened",
            reason="registered_for_exit_manager",
            state_data={
                "side": side,
                "entry_mid": mid,
                "volume": vol,
                "order_id": order_id,
                "position_id": position_id,
                "signal_id": signal_id,
                "exit_mode": self.exit_mode,
                "tp_price": tp_price,
                "sl_price": sl_price,
                "timeout_ms": int(now_ms + EXIT_TIMEOUT_MS),
            },
        )

    async def _manage_open_pos(self, *, bid: float, ask: float, mid: float, spread: float, lag_ms, now_ms: int) -> bool:
        """
        Returns True if it handled an open position this tick (including exiting).
        If True, caller should NOT process entries this tick.
        """
        if self._open_pos is None:
            return False

        # ==========================================================
        # ✅ BASELINE MODE (V1): deterministic TP/SL/timeout
        # ==========================================================
        if self.exit_mode == "baseline":
            pos = self._open_pos
            side = pos["side"]
            px = bid if side == "buy" else ask  # realizable

            tp = pos.get("tp_price")
            sl = pos.get("sl_price")
            timeout_ms = pos.get("timeout_ms")

            exit_reason = None

            # 1) SL
            if sl is not None:
                if (side == "buy" and px <= sl) or (side == "sell" and px >= sl):
                    exit_reason = "sl"

            # 2) TP
            if exit_reason is None and tp is not None:
                if (side == "buy" and px >= tp) or (side == "sell" and px <= tp):
                    exit_reason = "tp"

            # 3) Timeout
            if exit_reason is None and timeout_ms is not None and now_ms >= timeout_ms:
                exit_reason = "timeout"

            if exit_reason is None:
                return True  # block entries while trade is open

            # Log exit decision
            self.v1log.decision(
                symbol=self.market,
                decision="exit",
                reason=f"baseline_{exit_reason}",
                context={
                    "price": mid,
                    "bid": bid,
                    "ask": ask,
                    "entry_mid": pos.get("entry_mid"),
                    "tp_price": tp,
                    "sl_price": sl,
                    "ticks_in_trade": pos.get("ticks"),
                    "pressure": float(self.last_of_pressure),
                    "order_id": pos.get("order_id"),
                    "position_id": pos.get("position_id"),
                    "signal_id": pos.get("signal_id"),
                },
            )

            # Authoritative trade lifecycle record (always write)
            self._append_jsonl(
                self.trades_path,
                {
                    "ts_open_ms": pos.get("opened_ms"),
                    "ts_close_ms": now_ms,
                    "trade_id": f"v1-{pos.get('signal_id') or 'na'}",
                    "signal_id": pos.get("signal_id"),
                    "side": side,
                    "entry_mid": pos.get("entry_mid"),
                    "exit_price": float(px),
                    "tp_price": tp,
                    "sl_price": sl,
                    "exit_reason": exit_reason,
                    "exit_mode": "baseline",
                },
            )

            # If observe_only, don't send any close. Just clear local state.
            if self.observe_only:
                self.v1log.state(
                    self.market,
                    "exit_attempt",
                    reason=f"baseline_{exit_reason}",
                    state_data={"close_ok": False, "close_err": "observe_only", "signal_id": pos.get("signal_id")},
                )
                self._open_pos = None
                self.last_decision_ms = now_ms
                return True

            # CLOSE ATTEMPT (best-effort) — reuse your existing close patterns
            close_ok = False
            close_err = None
            try:
                if hasattr(self.spotware, "close_position") and pos.get("position_id") is not None:
                    await self.spotware.close_position(position_id=pos["position_id"])
                    close_ok = True
                elif hasattr(self.spotware, "close_trade") and pos.get("order_id") is not None:
                    await self.spotware.close_trade(order_id=pos["order_id"])
                    close_ok = True
                else:
                    await self._ensure_exec_mgr()
                    opposite_side = "sell" if side == "buy" else "buy"
                    resp = await self.exec_mgr.place_trade(
                        market=self.market,
                        side=opposite_side,
                        stake=pos["volume"],
                        duration_sec=0,
                        spread=spread,
                        lag_ms=lag_ms,
                        client_tag=f"aq:exit:{pos.get('signal_id') or 'na'}",
                    )
                    close_ok = bool(resp.get("ok"))
            except Exception as e:
                close_err = str(e)

            self.v1log.state(
                self.market,
                "exit_attempt",
                reason=f"baseline_{exit_reason}",
                state_data={
                    "close_ok": close_ok,
                    "close_err": close_err,
                    "entry_mid": pos.get("entry_mid"),
                    "tp_price": tp,
                    "sl_price": sl,
                    "exit_price": float(px),
                    "pressure": float(self.last_of_pressure),
                    "order_id": pos.get("order_id"),
                    "position_id": pos.get("position_id"),
                    "signal_id": pos.get("signal_id"),
                },
            )

            self._open_pos = None
            self.last_decision_ms = now_ms
            return True

        # ==========================================================
        # EXISTING MODE: behavior-based PA manager (unchanged below)
        # ==========================================================
        pos = self._open_pos
        pos["ticks"] += 1

        # Favorable / adverse excursion from entry
        move_signed = (mid - pos["entry_mid"]) * pos["dir"]
        if move_signed > pos["mfe"]:
            pos["mfe"] = move_signed
            pos["ticks_since_progress"] = 0
            pos["last_progress_mid"] = mid
        else:
            pos["ticks_since_progress"] += 1

        # Track worst adverse too
        if move_signed < pos["mae"]:
            pos["mae"] = move_signed

        giveback = pos["mfe"] - move_signed
        opp_move = -move_signed  # positive if moving against the trade

        # pressure flip sustained
        adverse_pressure = (-pos["dir"]) * float(self.last_of_pressure)
        if adverse_pressure > EXIT_PRESSURE_FLIP_THRESH:
            pos["flip_ticks"] += 1
        else:
            pos["flip_ticks"] = 0

        exit_reason = None

        # (1) Loss of progress
        if pos["ticks_since_progress"] >= EXIT_NO_EXT_TICKS:
            exit_reason = f"exit_no_extension:{EXIT_NO_EXT_TICKS}"

        # (2) Giveback after extension (trail)
        if exit_reason is None and pos["mfe"] >= EXIT_TRAIL_ARM_MFE and giveback >= EXIT_TRAIL_GIVEBACK:
            exit_reason = f"exit_giveback:mfe>={EXIT_TRAIL_ARM_MFE}_gb>={EXIT_TRAIL_GIVEBACK}"

        # (3) Opposite initiative
        if exit_reason is None and opp_move >= EXIT_OPP_INIT_TICKS:
            exit_reason = f"exit_opposite_initiative:opp>={EXIT_OPP_INIT_TICKS}"

        # (4) Pressure flip sustained
        if exit_reason is None and pos["flip_ticks"] >= EXIT_PRESSURE_FLIP_TICKS:
            exit_reason = f"exit_pressure_flip:{EXIT_PRESSURE_FLIP_TICKS}"

        if exit_reason is None:
            # Still in position; do not allow entries this tick
            return True

        # Log exit decision (PA-only)
        self.v1log.decision(
            symbol=self.market,
            decision="exit",
            reason=exit_reason,
            context={
                "price": mid,
                "entry_mid": pos["entry_mid"],
                "side": pos["side"],
                "dir": pos["dir"],
                "mfe": pos["mfe"],
                "mae": pos["mae"],
                "giveback": giveback,
                "ticks_in_trade": pos["ticks"],
                "ticks_since_progress": pos["ticks_since_progress"],
                "pressure": float(self.last_of_pressure),
                "order_id": pos.get("order_id"),
                "position_id": pos.get("position_id"),
                "signal_id": pos.get("signal_id"),
            },
        )

        # If observe_only, we don't send any close. Just clear local state.
        if self.observe_only:
            self.v1log.state(
                self.market,
                "exit_attempt",
                reason=exit_reason,
                state_data={"close_ok": False, "close_err": "observe_only", "signal_id": pos.get("signal_id")},
            )
            self._open_pos = None
            self.last_decision_ms = now_ms
            return True

        # CLOSE ATTEMPT (best-effort)
        close_ok = False
        close_err = None

        try:
            # Preferred: broker has a close_position/close_trade method
            if hasattr(self.spotware, "close_position") and pos.get("position_id") is not None:
                await self.spotware.close_position(position_id=pos["position_id"])
                close_ok = True
            elif hasattr(self.spotware, "close_trade") and pos.get("order_id") is not None:
                await self.spotware.close_trade(order_id=pos["order_id"])
                close_ok = True
            else:
                # Fallback: flatten by sending opposite market order (netting accounts)
                await self._ensure_exec_mgr()
                opposite_side = "sell" if pos["side"] == "buy" else "buy"
                resp = await self.exec_mgr.place_trade(
                    market=self.market,
                    side=opposite_side,
                    stake=pos["volume"],
                    duration_sec=0,
                    spread=spread,
                    lag_ms=lag_ms,
                    client_tag=f"aq:exit:{pos.get('signal_id') or 'na'}",
                )
                close_ok = bool(resp.get("ok"))
        except Exception as e:
            close_err = str(e)

        self.v1log.state(
            self.market,
            "exit_attempt",
            reason=exit_reason,
            state_data={
                "close_ok": close_ok,
                "close_err": close_err,
                "entry_mid": pos["entry_mid"],
                "mfe": pos["mfe"],
                "mae": pos["mae"],
                "giveback": giveback,
                "ticks_in_trade": pos["ticks"],
                "pressure": float(self.last_of_pressure),
                "order_id": pos.get("order_id"),
                "position_id": pos.get("position_id"),
                "signal_id": pos.get("signal_id"),
            },
        )

        self._open_pos = None
        self.last_decision_ms = now_ms
        return True

    # --------------------------
    # Tick handler
    # --------------------------
    async def on_tick(self, tick: dict):
        if not isinstance(tick, dict):
            return

        sym = tick.get("symbol")
        if sym != self.market:
            return

        try:
            bid = float(tick.get("bid"))
            ask = float(tick.get("ask"))
            price = float(tick.get("price"))
        except Exception:
            return

        mid = (bid + ask) / 2.0
        spread = ask - bid

        # ✅ FIX: define now_ms BEFORE using it
        now_ms = _utc_now_ms()
        now_ts = now_ms / 1000.0  # ✅ DAG uses seconds

        # ✅ Liveness marker (uses now_ms safely)
        self.last_tick_ms = now_ms

        self.tick_count += 1
        self.last_price = price
        self.last_bid = bid
        self.last_ask = ask
        self.last_mid = mid
        self.last_spread = spread

        # Tick timestamp normalization
        lag_ms = None
        raw_ts = tick.get("ts")
        try:
            if isinstance(raw_ts, (int, float)):
                tick_ms = int(raw_ts * 1000) if raw_ts < 10_000_000_000 else int(raw_ts)
                lag_ms = max(0, now_ms - tick_ms)
        except Exception:
            lag_ms = None

        self.recent_ticks.append({"t": datetime.now(timezone.utc), "mid": mid})

        # --------------------------
        # LOG: TICK
        # --------------------------
        self.v1log.tick(
            symbol=self.market,
            bid=bid,
            ask=ask,
            mid=mid,
            spread=spread,
            source="ctrader",
            session="off",
            extra={"tick_lag_ms": lag_ms, "tick_n": self.tick_count},
        )

        # --------------------------
        # Orderflow update
        # --------------------------
        prev_shock = self.last_of_shock
        of = self.orderflow.update(mid)
        self.last_of_pressure = of.pressure
        self.last_of_shock = of.shock

        if (not prev_shock) and self.last_of_shock:
            self.v1log.orderflow_event(
                symbol=self.market,
                event_type="volatility_spike",
                price_from=None,
                price_to=mid,
                duration_ms=None,
                tick_count=None,
                strength_score=float(self.last_of_pressure),
                context={"shock": True, "pressure": float(self.last_of_pressure), "spread": spread},
            )

        # --------------------------
        # HEALTH
        # --------------------------
        if (lag_ms is not None and lag_ms > 1500) or (spread is not None and spread > 1.5):
            self.v1log.health(
                status="degraded",
                tick_lag_ms=lag_ms,
                warnings={"spread": spread, "pressure": float(self.last_of_pressure), "shock": bool(self.last_of_shock)},
            )

        # --------------------------
        # BROADCAST
        # --------------------------
        await broadcast(
            {
                "symbol": self.market,
                "price": price,
                "mid": mid,
                "bid": bid,
                "ask": ask,
                "spread": spread,
                "pressure": float(self.last_of_pressure),
                "shock": bool(self.last_of_shock),
                "ts": time.time(),
            }
        )

        # ==========================================================
        # ✅ 0) EXIT MANAGER — manage open trade FIRST
        # ==========================================================
        handled_pos = await self._manage_open_pos(bid=bid, ask=ask, mid=mid, spread=spread, lag_ms=lag_ms, now_ms=now_ms)
        if handled_pos:
            return

        # ==========================================================
        # ✅ 1) HYGIENE (tick-driven): resolve committed signals BEFORE logging "enter_*"
        # ==========================================================
        if self._hygiene_pending is not None:
            hp = self._hygiene_pending
            hp["ticks_seen"] += 1

            move = (mid - hp["entry_price"]) * hp["dir"]
            if move > hp["best_favor"]:
                hp["best_favor"] = move
            if move < hp["worst_adverse"]:
                hp["worst_adverse"] = move

            # ✅ PASS
            if hp["best_favor"] >= HYGIENE_CONFIRM_FAVOR_TICKS:
                # 1) Log confirmed signal
                self.v1log.decision(
                    symbol=self.market,
                    decision=hp["decision"],
                    confidence=min(1.0, abs(self.last_of_pressure) / 3.0),
                    reason="hygiene_pass_after_dag_commit",
                    context={
                        "price": mid,
                        "pressure": float(self.last_of_pressure),
                        "spread": spread,
                        "ticks": self.tick_count,
                        "dag_used": True,
                        "dag_signal_id": hp.get("signal_id"),
                        "dag_commit_price": hp.get("commit_price"),
                        "hygiene": {
                            "ticks_seen": hp["ticks_seen"],
                            "best_favor": hp["best_favor"],
                            "worst_adverse": hp["worst_adverse"],
                            "confirm_favor": HYGIENE_CONFIRM_FAVOR_TICKS,
                            "max_adverse": HYGIENE_MAX_ADVERSE_TICKS,
                            "window": HYGIENE_WINDOW_TICKS,
                        },
                        "pa_ctx": hp.get("ctx") or {},
                    },
                )

                # 2) EXECUTE (ONLY HERE) after hygiene pass
                # NOTE: Spotware execution responses may contain protobuf objects that are not JSON serializable.
                # We must make 'raw' safe before writing to JSONL.
                def _safe_jsonable(x):
                    if x is None:
                        return None
                    try:
                        import json as _json
                        _json.dumps(x)
                        return x
                    except Exception:
                        try:
                            return str(x)
                        except Exception:
                            return "<unserializable>"

                exec_result = None
                side = "buy" if hp["decision"] == "enter_long" else "sell"
                vol = DEFAULT_FIXED_STAKE
                # Always log an attempt so state trail never "goes dark" before execution_result/execution_error.
                self.v1log.state(
                    self.market,
                    "execution_attempt",
                    reason="about_to_execute" if not self.observe_only else "observe_only",
                    state_data={
                        "decision": hp["decision"],
                        "side": side,
                        "price": mid,
                        "spread": spread,
                        "lag_ms": lag_ms,
                        "signal_id": hp.get("signal_id"),
                    },
                )

                if not self.observe_only:
                    try:
                        await self._ensure_exec_mgr()
                        exec_result = await self.exec_mgr.place_trade(
                            market=self.market,
                            side=side,
                            stake=vol,
                            duration_sec=0,
                            spread=spread,
                            lag_ms=lag_ms,
                            client_tag=f"aq:{hp.get('signal_id') or 'na'}",
                        )

                        # log execution outcome
                        self.v1log.state(
                            self.market,
                            "execution_result",
                            reason=exec_result.get("reason", "unknown"),
                            state_data={
                                "ok": exec_result.get("ok", False),
                                "decision": hp["decision"],
                                "side": side,
                                "price": mid,
                                "spread": spread,
                                "lag_ms": lag_ms,
                                "signal_id": hp.get("signal_id"),
                                "order_id": exec_result.get("order_id"),
                                "raw": _safe_jsonable(exec_result.get("raw")),
                            },
                        )

                    except Exception as e:
                        self.v1log.state(
                            self.market,
                            "execution_error",
                            reason="exception",
                            state_data={
                                "error": str(e),
                                "decision": hp["decision"],
                                "side": side,
                                "price": mid,
                                "spread": spread,
                                "lag_ms": lag_ms,
                                "signal_id": hp.get("signal_id"),
                            },
                        )
                        exec_result = {"ok": False, "reason": "exception"}

                # ✅ Register open position for exit manager:
                # - live: only if broker execution ok
                # - observe_only: simulate local position so we can still generate trades.jsonl in baseline mode
                should_register = False
                if self.observe_only:
                    should_register = True
                elif isinstance(exec_result, dict) and exec_result.get("ok"):
                    should_register = True

                if should_register:
                    ctx = hp.get("ctx") or {}
                    ih = ctx.get("impulse_high")
                    il = ctx.get("impulse_low")
                    ah = ctx.get("acceptance_high")
                    al = ctx.get("acceptance_low")

                    tp_price = None
                    sl_price = None

                    # TP = 1x impulse range projection (pure PA delivery)
                    try:
                        if ih is not None and il is not None and float(ih) > float(il):
                            impulse_range = float(ih) - float(il)
                            tp_price = (mid + impulse_range) if side == "buy" else (mid - impulse_range)
                    except Exception:
                        tp_price = None

                    # SL = acceptance invalidation bound + buffer
                    try:
                        buffer = max(float(spread), EXIT_MIN_BUFFER_TICKS)
                        if al is not None and ah is not None:
                            sl_price = (float(al) - buffer) if side == "buy" else (float(ah) + buffer)
                    except Exception:
                        sl_price = None

                    self._register_open_pos(
                        side=side,
                        mid=mid,
                        now_ms=now_ms,
                        vol=vol,
                        signal_id=hp.get("signal_id"),
                        exec_result=(exec_result if isinstance(exec_result, dict) else None),
                        tp_price=tp_price,
                        sl_price=sl_price,
                        ctx=ctx,
                    )

                # cooldown + reset after resolved
                self.last_decision_ms = now_ms
                self._reset_pa()
                self._last_raw_decision = None
                self._clear_hygiene()
                return

            # ❌ CANCEL: adverse before confirm
            if hp["worst_adverse"] <= -HYGIENE_MAX_ADVERSE_TICKS:
                self.v1log.decision(
                    symbol=self.market,
                    decision="skip",
                    reason="hygiene_cancel_adverse_after_dag_commit",
                    context={
                        "price": mid,
                        "pressure": float(self.last_of_pressure),
                        "spread": spread,
                        "ticks": self.tick_count,
                        "dag_used": True,
                        "dag_signal_id": hp.get("signal_id"),
                        "dag_commit_price": hp.get("commit_price"),
                        "hygiene": {
                            "ticks_seen": hp["ticks_seen"],
                            "best_favor": hp["best_favor"],
                            "worst_adverse": hp["worst_adverse"],
                            "confirm_favor": HYGIENE_CONFIRM_FAVOR_TICKS,
                            "max_adverse": HYGIENE_MAX_ADVERSE_TICKS,
                            "window": HYGIENE_WINDOW_TICKS,
                        },
                        "pa_ctx": hp.get("ctx") or {},
                    },
                )
                self.last_decision_ms = now_ms
                self._reset_pa()
                self._last_raw_decision = None
                self._clear_hygiene()
                return

            # ❌ CANCEL: timeout
            if hp["ticks_seen"] >= HYGIENE_WINDOW_TICKS:
                self.v1log.decision(
                    symbol=self.market,
                    decision="skip",
                    reason="hygiene_cancel_timeout_after_dag_commit",
                    context={
                        "price": mid,
                        "pressure": float(self.last_of_pressure),
                        "spread": spread,
                        "ticks": self.tick_count,
                        "dag_used": True,
                        "dag_signal_id": hp.get("signal_id"),
                        "dag_commit_price": hp.get("commit_price"),
                        "hygiene": {
                            "ticks_seen": hp["ticks_seen"],
                            "best_favor": hp["best_favor"],
                            "worst_adverse": hp["worst_adverse"],
                            "confirm_favor": HYGIENE_CONFIRM_FAVOR_TICKS,
                            "max_adverse": HYGIENE_MAX_ADVERSE_TICKS,
                            "window": HYGIENE_WINDOW_TICKS,
                        },
                        "pa_ctx": hp.get("ctx") or {},
                    },
                )
                self.last_decision_ms = now_ms
                self._reset_pa()
                self._last_raw_decision = None
                self._clear_hygiene()
                return

            # If hygiene unresolved, do NOT allow new DAG/PA signals yet.
            return

        # ==========================================================
        # ✅ 2) DAG HOOK #1 (tick-driven): update pending & commit if proved
        # ==========================================================
        dag_tick_action = self.dag.on_tick(
            price=mid,
            now_ts=now_ts,
            latest_decision=self._last_raw_decision,
        )

        if dag_tick_action.get("action") == "commit":
            direction = dag_tick_action.get("direction")
            decision = "enter_long" if direction == "long" else "enter_short"

            # Start hygiene tracking (post-commit, pre-log) with PA context snapshot
            self._start_hygiene(
                decision=decision,
                mid=mid,
                signal_id=dag_tick_action.get("signal_id"),
                commit_price=dag_tick_action.get("commit_price"),
                ctx={
                    "impulse_high": self.impulse_high,
                    "impulse_low": self.impulse_low,
                    "acceptance_high": self.acceptance_high,
                    "acceptance_low": self.acceptance_low,
                    "impulse_dir": self.impulse_dir,
                },
            )

            self.v1log.state(
                self.market,
                "hygiene_pending",
                reason="post_dag_commit_confirm_or_cancel",
                state_data={
                    "dir": "long" if decision == "enter_long" else "short",
                    "signal_id": dag_tick_action.get("signal_id"),
                    "price": mid,
                },
            )

            # Prevent PA spam while hygiene pending
            self._reset_pa()
            return

        # --------------------------
        # PA STATE MACHINE → DECISIONS (V1)
        # --------------------------
        if now_ms - self.last_decision_ms < DECISION_COOLDOWN_MS:
            return

        # 1) Impulse detection
        if self.pa_state == "waiting_impulse":
            if self.last_of_shock:
                self.pa_state = "impulse"
                self.impulse_dir = "up" if self.last_of_pressure >= 0 else "down"
                self.impulse_price = mid
                # ✅ start impulse bounds
                self.impulse_high = mid
                self.impulse_low = mid

                self.v1log.state(
                    self.market,
                    "impulse_detected",
                    reason=f"pressure={self.last_of_pressure:.2f}",
                    state_data={"dir": self.impulse_dir, "price": mid},
                )
            return

        # 2) Acceptance
        if self.pa_state == "impulse":
            # ✅ keep expanding impulse bounds while in impulse
            if self.impulse_high is None or mid > self.impulse_high:
                self.impulse_high = mid
            if self.impulse_low is None or mid < self.impulse_low:
                self.impulse_low = mid

            if not self.last_of_shock:
                self.pa_state = "acceptance"
                self.acceptance_start_ms = now_ms
                # ✅ start acceptance bounds
                self.acceptance_high = mid
                self.acceptance_low = mid

                self.v1log.state(
                    self.market,
                    "acceptance",
                    reason="shock_absorbed",
                    state_data={"dir": self.impulse_dir},
                )
            return

        # 3) Continuation → DECISION
        if self.pa_state == "acceptance":
            ticks_in_acceptance = int((now_ms - self.acceptance_start_ms) / 100)

            # ✅ expand acceptance bounds during acceptance
            if self.acceptance_high is None or mid > self.acceptance_high:
                self.acceptance_high = mid
            if self.acceptance_low is None or mid < self.acceptance_low:
                self.acceptance_low = mid

            continuation = (
                (self.impulse_dir == "up" and self.last_of_pressure > 0) or
                (self.impulse_dir == "down" and self.last_of_pressure < 0)
            )

            # PASS: Acceptance matured AND continuation confirmed → raw decision
            if ticks_in_acceptance >= ACCEPTANCE_TICKS_MIN and continuation:
                decision = "enter_long" if self.impulse_dir == "up" else "enter_short"
                self._last_raw_decision = decision

                # DAG HOOK #2 (decision-driven)
                dag_action = self.dag.on_decision(
                    decision=decision,
                    price=mid,
                    now_ts=now_ts,
                )

                if dag_action.get("action") == "blocked":
                    self.v1log.decision(
                        symbol=self.market,
                        decision="skip",
                        reason=f"dag_blocked:{dag_action.get('reason')}",
                        context={
                            "price": mid,
                            "pressure": float(self.last_of_pressure),
                            "spread": spread,
                            "ticks": self.tick_count,
                            "dag_used": True,
                            "dag_block_reason": dag_action.get("reason"),
                            "dag_signal_id": dag_action.get("signal_id"),
                        },
                    )
                    self.last_decision_ms = now_ms
                    self._reset_pa()
                    self._last_raw_decision = None
                    return

                if dag_action.get("action") == "pending_created":
                    self.v1log.state(
                        self.market,
                        "dag_pending",
                        reason="prove_then_commit",
                        state_data={
                            "dir": "long" if decision == "enter_long" else "short",
                            "signal_id": dag_action.get("signal_id"),
                            "price": mid,
                        },
                    )
                    # No cooldown here; cooldown occurs after commit+resolve (hygiene) or block/expire.
                    self._reset_pa()
                    return

                # If DAG returns something unexpected, fail safe to reset PA
                self._reset_pa()
                return

            # FAIL: Acceptance matured but continuation not confirmed
            if ticks_in_acceptance >= ACCEPTANCE_TICKS_MIN and not continuation:
                self._last_raw_decision = "skip"
                self.v1log.decision(
                    symbol=self.market,
                    decision="skip",
                    reason="acceptance_failed",
                    context={
                        "price": mid,
                        "pressure": float(self.last_of_pressure),
                        "spread": spread,
                    },
                )
                self.last_decision_ms = now_ms
                self._reset_pa()
                self._last_raw_decision = None
                return

        if self.tick_count % 10 == 0:
            logger.info(
                "[TICK] #%d mid=%.5f spread=%.5f ofP=%.2f shock=%s lag_ms=%s",
                self.tick_count,
                mid,
                spread,
                float(self.last_of_pressure),
                bool(self.last_of_shock),
                str(lag_ms),
            )

    async def _watchdog_loop(self):
        """
        Step 1 watchdog:
        - emits a heartbeat every HEARTBEAT_EVERY_SEC
        - detects "no ticks" situations
        - optionally restarts the Spotware stream if ticks stall
        """
        while self.running:
            await asyncio.sleep(1)
            now_ms = _utc_now_ms()

            # Heartbeat
            if (self.last_heartbeat_ms == 0) or (now_ms - self.last_heartbeat_ms >= HEARTBEAT_EVERY_SEC * 1000):
                self.last_heartbeat_ms = now_ms

                age_ms = None if not self.last_tick_ms else (now_ms - self.last_tick_ms)

                self.v1log.health(
                    status="ok",
                    tick_lag_ms=None,
                    warnings={
                        "heartbeat": True,
                        "tick_age_ms": age_ms,
                        "tick_count": self.tick_count,
                        "open_pos": bool(self._open_pos is not None),
                        "hygiene_pending": bool(self._hygiene_pending is not None),
                        "observe_only": bool(self.observe_only),
                        "exit_mode": str(self.exit_mode),
                    },
                )

            # No-ticks warning
            if self.last_tick_ms:
                tick_age_sec = (now_ms - self.last_tick_ms) / 1000.0

                if tick_age_sec >= NO_TICKS_WARN_SEC:
                    self.v1log.health(
                        status="degraded",
                        tick_lag_ms=None,
                        warnings={
                            "no_ticks_warn": True,
                            "tick_age_sec": tick_age_sec,
                            "tick_count": self.tick_count,
                        },
                    )

                # Optional auto-restart
                if NO_TICKS_RESTART_SEC > 0 and tick_age_sec >= NO_TICKS_RESTART_SEC:
                    self.v1log.health(
                        status="degraded",
                        tick_lag_ms=None,
                        warnings={
                            "no_ticks_restart": True,
                            "tick_age_sec": tick_age_sec,
                        },
                    )
                    try:
                        # Best-effort stop to force reconnect loop in trade_loop
                        await self.spotware.stop()
                    except Exception:
                        pass

                    # Reset marker so we don't spam restarts
                    self.last_tick_ms = now_ms

    # --------------------------
    # Main loop
    # --------------------------
    async def trade_loop(self):
        logger.info("🚀 trade_loop starting (Spotware Open API)")
        self.v1log.state(self.market, "waiting_impulse", reason="start_stream")

        # Start watchdog (Step 1)
        if self._watchdog_task is None or self._watchdog_task.done():
            self._watchdog_task = asyncio.create_task(self._watchdog_loop())

        backoff = 1
        while self.running:
            try:
                await self.spotware.start()
                logger.warning("⚠️ Spotware client returned; reconnecting…")
            except Exception as e:
                logger.exception("❌ Spotware client error: %s", e)

            if not self.running:
                break

            self.v1log.health(status="degraded", warnings={"spotware_reconnect": True, "backoff_sec": backoff})
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30)

        logger.info("🛑 trade_loop stopped")

    async def stop(self):
        self.running = False
        self.v1log.state(self.market, "session_paused", reason="stop_called")
        try:
            await self.spotware.stop()
        except Exception:
            pass
