# engine/execution.py
# ASTRAQUANT V1 — CTRADER ONLY — DEMO EXECUTION MANAGER (DROP-IN)
#
# FIX INCLUDED:
# - Prevents permanent "max_concurrent_trades_reached" deadlock by syncing
#   open-trade count from the broker (Spotware client) before enforcing limits.
# - Keeps the SAME public interface: ExecutionManager.start/stop/place_trade/subscribe_ticks
#
# Key idea:
# - Old code incremented _open_trades on send, but never decremented unless
#   notify_trade_closed() was called (which currently isn't wired).
# - This file now attempts to reconcile open positions from the broker client
#   before blocking, so demo trading can run continuously.

from __future__ import annotations

import asyncio
import logging
import os
import time
from typing import Any, Callable, Dict, Optional, Tuple, List

logger = logging.getLogger("executor")
logger.setLevel(os.getenv("LOG_LEVEL", "INFO"))

# Hard enable switch (default OFF)
EXECUTION_ENABLED = os.getenv("EXECUTION_ENABLED", "0").lower() in ("1", "true", "yes")

# Safety / limits (demo defaults)
MAX_CONCURRENT_TRADES = int(os.getenv("MAX_CONCURRENT_TRADES", "1"))
MIN_SECS_BETWEEN_ENTRIES = float(os.getenv("MIN_SECS_BETWEEN_ENTRIES", "15"))

# Execution-quality blocks
EXEC_MAX_SPREAD = float(os.getenv("EXEC_MAX_SPREAD", "1.5"))
EXEC_MAX_LAG_MS = int(os.getenv("EXEC_MAX_LAG_MS", "1500"))

# Size (demo)
FIXED_STAKE = float(os.getenv("FIXED_STAKE", "0.10"))

# How often we try to sync from broker (ms)
SYNC_OPEN_TRADES_EVERY_MS = int(float(os.getenv("SYNC_OPEN_TRADES_EVERY_SEC", "2.0")) * 1000)


def _now_ms() -> int:
    return int(time.time() * 1000)


class ExecutionManager:
    """
    CTRADER-only ExecutionManager (Demo Execution Drop-in)
    - start/stop kept for compatibility (no tick streaming here)
    - place_trade places a market order IF enabled and gates pass
    - NEW: sync open trades from broker to avoid stale concurrency lock
    """

    def __init__(
        self,
        api_token: Optional[str] = None,
        controller: Optional[object] = None,
        tick_symbol: Optional[str] = None,
        simulate: bool = False,
        **kwargs,
    ):
        self.api_token = api_token
        self.controller = controller
        self.tick_symbol = (tick_symbol or os.getenv("CTRADER_SYMBOL") or "").strip().upper() or None

        # Optional explicit broker client (Spotware client instance)
        self.broker = kwargs.get("broker", None)

        self.connected: bool = False
        self._running_task: Optional[asyncio.Task] = None

        # callbacks (kept for compatibility)
        self.on_tick_callback: Optional[Callable[[dict], Any]] = None
        self.on_trade_opened: Optional[Callable[[dict], Any]] = None
        self.on_trade_closed: Optional[Callable[[dict], Any]] = None
        self.on_contract_update: Optional[Callable[[dict], Any]] = None

        # Internal execution state
        self._open_trades: int = 0
        self._last_entry_ms: int = 0
        self._last_sync_ms: int = 0

        logger.info(
            "ExecutionManager initialized. symbol=%s EXECUTION_ENABLED=%s max_concurrent=%s min_entry_gap_sec=%s",
            self.tick_symbol,
            EXECUTION_ENABLED,
            MAX_CONCURRENT_TRADES,
            MIN_SECS_BETWEEN_ENTRIES,
        )

    async def start(self):
        """
        Compatibility. Marks manager as connected and starts idle loop.
        Also performs an initial open-trades sync (best-effort).
        """
        if self._running_task and not self._running_task.done():
            return
        self.connected = True
        logger.info("[EXECUTOR] start() — ready. (No tick streaming handled here.)")

        # Best-effort initial sync so we don't start in a stale locked state
        try:
            await self._sync_open_trades(force=True)
        except Exception:
            logger.exception("[EXECUTOR] initial open-trades sync failed (continuing)")

        self._running_task = asyncio.create_task(self._idle_loop())

    async def stop(self):
        """
        Stop the idle loop.
        """
        if self._running_task:
            self._running_task.cancel()
            try:
                await self._running_task
            except asyncio.CancelledError:
                pass
        self.connected = False
        logger.info("[EXECUTOR] stopped.")

    async def _idle_loop(self):
        """
        Keep task alive so anything awaiting start() doesn't break.
        """
        try:
            while True:
                await asyncio.sleep(60)
        except asyncio.CancelledError:
            return

    def heartbeat_supported(self) -> bool:
        return False

    # -------------------------
    # Broker helpers
    # -------------------------
    def _get_broker(self) -> Optional[Any]:
        """
        Prefer explicit broker, else controller.spotware
        """
        if self.broker is not None:
            return self.broker
        if self.controller is None:
            return None
        return getattr(self.controller, "spotware", None)

    def _extract_positions_from_broker(self, broker: Any) -> Optional[List[Any]]:
        """
        Best-effort extraction of open positions from a Spotware client.

        Returns:
          - list of positions if we can infer it
          - None if we cannot determine positions safely
        """
        # Common attributes we might have
        candidate_attrs = (
            "open_positions",
            "positions",
            "_open_positions",
            "_positions",
            "positions_cache",
            "_positions_cache",
        )

        for attr in candidate_attrs:
            if hasattr(broker, attr):
                try:
                    val = getattr(broker, attr)
                    if val is None:
                        continue
                    if isinstance(val, list):
                        return val
                    if isinstance(val, dict):
                        return list(val.values())
                except Exception:
                    continue

        return None

    async def _try_fetch_positions_via_method(self, broker: Any) -> Optional[List[Any]]:
        """
        Best-effort fetch via common async methods, if implemented by broker client.
        """
        candidate_methods = (
            "get_open_positions",
            "fetch_open_positions",
            "list_open_positions",
            "get_positions",
            "fetch_positions",
            "list_positions",
        )

        for name in candidate_methods:
            if hasattr(broker, name) and callable(getattr(broker, name)):
                fn = getattr(broker, name)
                try:
                    res = fn()
                    if asyncio.iscoroutine(res):
                        res = await res
                    if isinstance(res, list):
                        return res
                    if isinstance(res, dict):
                        return list(res.values())
                except Exception:
                    continue

        return None

    async def _sync_open_trades(self, *, force: bool = False) -> None:
        """
        Sync open trades count from broker (best-effort).

        We only overwrite _open_trades when we have a meaningful positions snapshot.
        If we cannot determine positions, we keep internal counter to preserve safety.
        """
        now = _now_ms()
        if not force and (now - self._last_sync_ms) < SYNC_OPEN_TRADES_EVERY_MS:
            return

        broker = self._get_broker()
        if broker is None:
            return

        positions = self._extract_positions_from_broker(broker)
        if positions is None:
            positions = await self._try_fetch_positions_via_method(broker)

        if positions is None:
            # Can't safely determine; keep internal counter
            self._last_sync_ms = now
            return

        prev = self._open_trades
        self._open_trades = len(positions)
        self._last_sync_ms = now

        if prev != self._open_trades:
            logger.info("[EXECUTOR] synced open trades from broker: %s → %s", prev, self._open_trades)

    # -------------------------
    # Safety gate
    # -------------------------
    async def _can_enter(self, *, spread: Optional[float], lag_ms: Optional[int]) -> Tuple[bool, str]:
        if not EXECUTION_ENABLED:
            return False, "execution_disabled"

        # Reconcile open trades first (prevents stale lock)
        try:
            await self._sync_open_trades()
        except Exception:
            # If sync fails, we fall back to internal counter (still safe)
            logger.debug("[EXECUTOR] open-trades sync failed; using internal counter", exc_info=True)

        now = _now_ms()

        if self._open_trades >= MAX_CONCURRENT_TRADES:
            return False, "max_concurrent_trades_reached"

        if (now - self._last_entry_ms) < int(MIN_SECS_BETWEEN_ENTRIES * 1000):
            return False, "entry_rate_limited"

        if spread is not None and spread > EXEC_MAX_SPREAD:
            return False, "spread_too_wide"

        if lag_ms is not None and lag_ms > EXEC_MAX_LAG_MS:
            return False, "tick_lag_too_high"

        return True, "ok"

    # -------------------------
    # Trade placement
    # -------------------------
    async def place_trade(
        self,
        market: str,
        side: str,
        stake: float = 0.0,
        duration_sec: int = 0,
        *,
        spread: Optional[float] = None,
        lag_ms: Optional[int] = None,
        client_tag: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Place a DEMO market order (BUY/SELL).

        Returns dict:
          { ok: bool, reason: str, order_id?: str, raw?: any }
        """
        market = (market or "").strip().upper()
        side = (side or "").strip().lower()
        volume = float(stake if stake and stake > 0 else FIXED_STAKE)

        if side not in ("buy", "sell"):
            return {"ok": False, "reason": "invalid_side"}

        ok, reason = await self._can_enter(spread=spread, lag_ms=lag_ms)
        if not ok:
            logger.warning("[EXECUTOR] blocked: %s market=%s side=%s vol=%s open_trades=%s/%s",
                           reason, market, side, volume, self._open_trades, MAX_CONCURRENT_TRADES)
            return {
                "ok": False,
                "reason": reason,
                "market": market,
                "side": side,
                "volume": volume,
                "open_trades": self._open_trades,
                "max_concurrent": MAX_CONCURRENT_TRADES,
            }

        broker = self._get_broker()
        if broker is None:
            return {"ok": False, "reason": "missing_broker_client", "market": market, "side": side, "volume": volume}

        # Call the broker order method (adapt to your Spotware client)
        try:
            if hasattr(broker, "place_market_order"):
                resp = await broker.place_market_order(symbol=market, side=side, volume=volume, client_tag=client_tag)
            elif hasattr(broker, "send_market_order"):
                resp = await broker.send_market_order(symbol=market, side=side, volume=volume, client_tag=client_tag)
            else:
                return {
                    "ok": False,
                    "reason": "broker_missing_order_method",
                    "market": market,
                    "side": side,
                    "volume": volume,
                }
        except Exception as e:
            logger.exception("[EXECUTOR] broker order failed: %s", str(e))
            return {
                "ok": False,
                "reason": "broker_exception",
                "error": str(e),
                "market": market,
                "side": side,
                "volume": volume,
            }

        # Mark entry bookkeeping
        # NOTE: we keep internal bookkeeping, but the next _sync_open_trades() will reconcile anyway.
        self._open_trades += 1
        self._last_entry_ms = _now_ms()

        order_id = None
        if isinstance(resp, dict):
            order_id = resp.get("order_id") or resp.get("id") or resp.get("client_order_id")

        result = {
            "ok": True,
            "reason": "order_sent",
            "order_id": order_id,
            "raw": resp,
            "market": market,
            "side": side,
            "volume": volume,
        }
        logger.info("[EXECUTOR] sent: market=%s side=%s vol=%s order_id=%s", market, side, volume, str(order_id))

        # Optional callback
        if self.on_trade_opened:
            try:
                await maybe_await(self.on_trade_opened, result)
            except Exception:
                logger.exception("[EXECUTOR] on_trade_opened callback failed")

        return result

    # -------------------------
    # Optional lifecycle hooks (for later wiring)
    # -------------------------
    def notify_trade_closed(self, payload: Optional[Dict[str, Any]] = None):
        """
        Call this when you implement position tracking and detect a close.
        """
        self._open_trades = max(0, self._open_trades - 1)

        if self.on_trade_closed:
            asyncio.create_task(maybe_await(self.on_trade_closed, payload or {"ok": True, "reason": "trade_closed"}))

    async def subscribe_ticks(self, symbol: str):
        """
        No-op. Tick streaming is handled by SpotwarePriceClient → Controller.on_tick.
        """
        logger.info("[EXECUTOR] subscribe_ticks(%s) ignored (ticks handled elsewhere).", symbol)

    async def unsubscribe_ticks(self, symbol: str):
        """
        No-op.
        """
        logger.info("[EXECUTOR] unsubscribe_ticks(%s) ignored.", symbol)


async def maybe_await(fn, *args, **kwargs):
    res = fn(*args, **kwargs)
    if asyncio.iscoroutine(res):
        return await res
    return res
