# engine/engine.py
import asyncio
import logging
import json
import time
from datetime import datetime, timedelta
from typing import Optional, Tuple, Any, Dict

import httpx

from engine.ws_client import WSClient





from config import Config
from engine.execution import ExecutionManager


logger = logging.getLogger("engine.core")
logging.basicConfig(level=logging.INFO)


class StrategyEngine:
    """
    Rebuilt StrategyEngine — drop-in replacement for engine/engine.py

    Responsibilities:
      - receive websocket ticks (ws_client)
      - keep tick buffer for short-window price action & volatility
      - call AI service for trend/score/interval
      - apply AI entry filter
      - call risk service for stake & permission
      - place orders using ExecutionManager
      - smoothing and safety for intervals & stake
    """

    def __init__(self, cfg: Config):
        self.cfg = cfg

        # API endpoints from config
        self.api_gateway = cfg.API_GATEWAY
        self.risk_url = f"{self.api_gateway}{cfg.RISK_ASSESS_PATH}"
        # Prefer explicit AI URL from config if present, else compose
        self.ai_base = getattr(cfg, "AI_URL", None) or f"{self.api_gateway}{getattr(cfg, 'AI_PATH', '/ai')}"
        # Websocket URL and execution manager
        self.ws = WSClient(cfg.WS_URL)
        self.execution = ExecutionManager(simulate=cfg.SIMULATE_EXECUTION)

        # runtime flags
        self.running = False

        # Attach callback for incoming ticks
        self.ws.on_tick = self.on_tick

        # ----- Strategy / runtime state -----
        self.market = getattr(cfg, "DEFAULT_MARKET", "R_50")
        self.account_id = getattr(cfg, "ACCOUNT_ID", "engine-live")
        self.balance = float(getattr(cfg, "INITIAL_BALANCE", 1000.0))
        self.recent_winrate = float(getattr(cfg, "INITIAL_WINRATE", 0.5))
        self.current_minute_exposure = 0.0
        self.mode = getattr(cfg, "MODE", "normal")
        self.arc_mode = False

        # tick buffer and volatility
        self.recent_ticks = []  # list of {"t": datetime, "price": float}
        self.tick_window_seconds = int(getattr(cfg, "TICK_WINDOW_SECONDS", 30))
        self.last_volatility = float(getattr(cfg, "INITIAL_VOLATILITY", 0.12))

        # intervals
        self.min_interval = float(getattr(cfg, "MIN_INTERVAL", 0.5))
        self.max_interval = float(getattr(cfg, "MAX_INTERVAL", 10.0))
        self.current_interval = float(getattr(cfg, "DEFAULT_INTERVAL", 3.0))
        self.ai_interval: Optional[float] = None
        self.next_trade_time: Optional[datetime] = None

        # AI latest outputs (kept from last analyze)
        self.last_ai_trend: Optional[str] = None
        self.last_ai_score: Optional[float] = None
        self.last_ai_delta: Optional[float] = None

        # smoothing and stake
        self.min_pct = float(getattr(cfg, "MIN_PCT", 0.01))
        self.max_pct = float(getattr(cfg, "MAX_PCT", 0.03))
        self.base_pct = float(getattr(cfg, "BASE_PCT", 0.02))
        self.smoothed_pct = self.base_pct
        self.min_stake_amount = float(getattr(cfg, "MIN_STAKE_AMOUNT", 0.1))

        # drawdown / session
        self.peak_balance = self.balance
        self.session_start = datetime.utcnow()
        self.session_profit = 0.0
        self.session_profit_target = float(getattr(cfg, "SESSION_TARGET", 50.0))
        self.max_drawdown = float(getattr(cfg, "MAX_DRAWDOWN", 0.20))
        self.current_drawdown = 0.0
        self.max_drawdown_seen = 0.0

        # misc
        logger.info("StrategyEngine initialized.")

    # -----------------------
    # Lifecycle
    # -----------------------
    async def start(self):
        logger.info("[ENGINE] starting")
        self.running = True
        try:
            await self.ws.connect()
            logger.info("[ENGINE] connected to WS")
        except Exception as e:
            logger.error(f"[ENGINE] failed connecting to WS: {e}")

    async def stop(self):
        logger.info("[ENGINE] stopping")
        self.running = False
        try:
            await self.ws.close()
        except Exception:
            pass

    # -----------------------
    # AI integration
    # -----------------------
    async def ai_analyze_tick(self, price: float, volatility: float) -> Optional[dict]:
        """
        Calls the AI service endpoint to get {trend, normalized_delta, score, interval}
        Returns parsed dict or None
        """
        try:
            url = f"{self.ai_base.rstrip('/')}/analyze_tick"
            payload = {"price": float(price), "volatility": float(volatility)}
            timeout_seconds = float(getattr(self.cfg, "AI_REQUEST_TIMEOUT", 2.0))

            async with httpx.AsyncClient(timeout=timeout_seconds) as client:
                r = await client.post(url, json=payload)
                text = r.text
                if r.status_code != 200:
                    logger.debug(f"[AI] non-200 {r.status_code}: {text}")
                    return None
                try:
                    data = r.json()
                except Exception:
                    logger.debug("[AI] failed to parse JSON response")
                    return None

            if data.get("status") != "ok":
                logger.debug(f"[AI] status not ok: {data}")
                return None

            return data
        except Exception as e:
            logger.debug(f"[AI] request failed: {e}")
            return None

    def update_ai_interval(self, interval: Any):
        """
        Accepts AI-proposed interval, clamps & smooths into self.ai_interval
        """
        try:
            if interval is None:
                return
            iv = float(interval)
            iv = max(self.min_interval, min(self.max_interval, iv))
            smoothing = 0.3
            self.ai_interval = smoothing * iv + (1 - smoothing) * (self.ai_interval or self.current_interval)
        except Exception:
            logger.debug("Failed to parse ai interval, ignoring.")

    def compute_adaptive_interval(self) -> float:
        """
        Produces the next interval (seconds) combining AI suggestion and volatility.
        """
        base = self.ai_interval if self.ai_interval is not None else self.current_interval
        vol = float(self.last_volatility) if self.last_volatility is not None else 0.12
        # slightly slow down when volatility increases
        vol_factor = 1.0 + (vol * 1.5)
        scaled = base * vol_factor
        final = max(self.min_interval, min(self.max_interval, scaled))
        smoothing = 0.2
        self.current_interval = smoothing * final + (1 - smoothing) * self.current_interval
        return self.current_interval

    def scale_interval(self, ai_interval: Optional[float], volatility: float) -> float:
        """
        Extra helper used by on_tick: scale AI interval with volatility and clamp.
        """
        try:
            iv = float(ai_interval) if ai_interval is not None else self.current_interval
            # volatility increases interval by up to 30%
            vol_factor = 1.0 + min(0.30, volatility * 0.5)
            iv = iv * vol_factor
            iv = max(self.min_interval, min(self.max_interval, iv))
            return iv
        except Exception:
            return float(self.current_interval)

    # -----------------------
    # AI Entry Filter (PATCH 6)
    # -----------------------
    async def ai_allows_entry(self) -> bool:
        """
        Final AI confirmation filter before placing trade.
        Uses latest AI outputs (self.last_ai_trend, self.last_ai_score, self.last_ai_delta)
        """
        trend = self.last_ai_trend
        score = self.last_ai_score
        delta = self.last_ai_delta
        vol = float(self.last_volatility or 0.1)

        # Block if AI hasn't produced any data yet (safe default)
        if trend is None or score is None or delta is None:
            logger.info("[AI FILTER] Missing AI data → BLOCK entry.")
            return False

        if score < 0.40:
            logger.info(f"[AI FILTER] score too low ({score}) → BLOCK entry.")
            return False

        if trend in ("RANGE", "RANGE/CHOP", "sideways"):
            logger.info("[AI FILTER] trend indicates range/sideways → BLOCK entry.")
            return False

        # Momentum must roughly align with trend
        if trend.startswith("UP") and delta < 0:
            logger.info("[AI FILTER] delta contradicts UP trend → BLOCK entry.")
            return False
        if trend.startswith("DOWN") and delta > 0:
            logger.info("[AI FILTER] delta contradicts DOWN trend → BLOCK entry.")
            return False

        # High-volatility safety
        if vol > 0.65 and score < 0.70:
            logger.info("[AI FILTER] high vol + weak score → BLOCK entry.")
            return False

        logger.info(f"[AI FILTER] OK trend={trend} score={score:.2f} delta={delta:.4f} vol={vol:.3f} → ALLOW")
        return True

    # -----------------------
    # Risk decision helper
    # -----------------------
    async def request_risk_decision(self) -> Dict[str, Any]:
        """
        Calls the configured risk service to request permission & stake.
        Returns decision dict {allow_trade: bool, stake: float, reason: str}
        """
        try:
            payload = {
                "account": {
                    "account_id": self.account_id,
                    "balance": float(self.balance),
                    "recent_winrate": float(self.recent_winrate),
                    "current_minute_exposure": float(self.current_minute_exposure),
                    "volatility": float(self.last_volatility),
                    "mode": self.mode,
                    "arc_mode": bool(self.arc_mode),
                }
            }
            timeout_seconds = float(getattr(self.cfg, "RISK_TIMEOUT", 2.0))
            async with httpx.AsyncClient(timeout=timeout_seconds) as client:
                r = await client.post(self.risk_url, json=payload)
                text = r.text
                if r.status_code != 200:
                    logger.debug(f"[RISK] non-200 {r.status_code}: {text}")
                    return {"allow_trade": False, "stake": 0.0, "reason": "risk-non200"}

                try:
                    data = r.json()
                except Exception:
                    logger.debug("[RISK] failed to parse JSON response")
                    return {"allow_trade": False, "stake": 0.0, "reason": "parse-fail"}

            decision = data.get("data") if isinstance(data, dict) and "data" in data else data
            return decision
        except Exception as e:
            logger.error(f"[RISK] request failed: {e}")
            # safe fallback
            return {"allow_trade": True, "stake": max(self.min_stake_amount, round(self.balance * self.min_pct, 2)), "reason": "fallback"}

    # -----------------------
    # Tick handler (main)
    # -----------------------
    async def on_tick(self, tick: dict):
        """
        Called for each incoming tick.
        tick example: {'timestamp': 169..., 'price': 1234.5}
        """
        try:
            # lightweight validation
            if not isinstance(tick, dict):
                return

            price = tick.get("price")
            symbol = tick.get("symbol", self.market)
            ts = datetime.utcnow()

            # 1) append tick into buffer
            if price is not None:
                try:
                    self.recent_ticks.append({"t": ts, "price": float(price)})
                except Exception:
                    pass

            # trim
            cutoff = ts - timedelta(seconds=self.tick_window_seconds)
            while self.recent_ticks and self.recent_ticks[0]["t"] < cutoff:
                self.recent_ticks.pop(0)

            # 2) optionally update volatility from tick
            vol = tick.get("volatility") or tick.get("vol")
            if vol is not None:
                try:
                    self.last_volatility = max(0.0, min(1.0, float(vol)))
                except Exception:
                    logger.debug("Invalid volatility on tick, ignoring.")

            # log
            logger.info(f"[TICK] {symbol} price={price} vol={self.last_volatility}")

            # 3) call AI analyze endpoint (non-blocking to risk/exec flow)
            ai_resp = await self.ai_analyze_tick(price, self.last_volatility)
            if ai_resp:
                # expected: {status:'ok', trend:..., normalized_delta:..., score:..., interval:...}
                self.last_ai_trend = ai_resp.get("trend")
                self.last_ai_delta = ai_resp.get("normalized_delta")
                self.last_ai_score = float(ai_resp.get("score")) if ai_resp.get("score") is not None else None
                ai_interval = ai_resp.get("interval")
                # update engine AI interval store
                self.update_ai_interval(ai_interval)
                logger.info(f"[AI] trend={self.last_ai_trend} score={self.last_ai_score} interval={ai_interval}")
            else:
                logger.debug("[AI] no response for this tick")

            # 4) Early exit if AI filter blocks entry
            allowed_by_ai = await self.ai_allows_entry()
            if not allowed_by_ai:
                logger.info("[ENGINE] AI blocked this tick from trading")
                return

            # 5) Enforce cooldown: do not trade until next_trade_time
            now = datetime.utcnow()
            if self.next_trade_time is not None and now < self.next_trade_time:
                # nothing to do this tick
                return

            # compute next wait and set cooldown
            next_iv = self.compute_adaptive_interval()
            wait_s = float(next_iv)
            self.next_trade_time = now + timedelta(seconds=wait_s)
            logger.info(f"[ENGINE] AI accepted → next trade in {wait_s:.2f}s")

            # 6) Ask Risk about permission & stake
            decision = await self.request_risk_decision()
            logger.info(f"[RISK] decision={decision}")
            if not decision or not decision.get("allow_trade", False):
                logger.info("[ENGINE] Risk denied trade")
                return

            stake = float(decision.get("stake", max(self.min_stake_amount, round(self.balance * self.min_pct, 2))))

            # 7) Execute order
            try:
                order = await self.execution.execute_order(account_id=self.account_id, stake=stake)
                logger.info(f"[EXEC] order placed: {order.get('order_id')} stake={stake}")
            except Exception as e:
                logger.exception(f"[EXEC] execution failed: {e}")
                return

            # 8) Post-trade bookkeeping
            try:
                # update exposure
                if self.balance > 0:
                    self.current_minute_exposure += (stake / max(self.balance, 1.0))
                # note: actual balance update occurs on trade-closed callback from ExecutionManager
            except Exception:
                pass

        except Exception as outer_e:
            logger.exception(f"[ENGINE] on_tick error: {outer_e}")

    # -----------------------
    # Utility helpers
    # -----------------------
    def compute_drawdown(self) -> float:
        try:
            if not self.peak_balance or self.peak_balance <= 0:
                return 0.0
            dd = (self.peak_balance - self.balance) / max(self.peak_balance, 1.0)
            dd = max(0.0, dd)
            self.current_drawdown = dd
            if dd > self.max_drawdown_seen:
                self.max_drawdown_seen = dd
            return dd
        except Exception:
            return 0.0

    def calc_adaptive_stake(self) -> float:
        """
        Recreate a conservative adaptive stake based on
        winrate, volatility and drawdown.
        """
        balance = max(self.balance, 1.0)
        try:
            # winrate estimate
            winrate = float(getattr(self, "recent_winrate", 0.5))
        except Exception:
            winrate = 0.5

        pct = self.base_pct
        try:
            if self.last_volatility > 0.25:
                pct *= 0.7
            elif self.last_volatility < 0.08:
                pct *= 1.15
        except Exception:
            pass

        dd = self.compute_drawdown()
        if dd > 0.05:
            pct *= 0.8
        if dd > 0.10:
            pct *= 0.6

        if self.arc_mode:
            pct = max(pct, float(getattr(self.cfg, "ARC_MIN_PCT", 0.005)))

        pct *= (0.9 + winrate)
        pct *= (1 + min(getattr(self, "loss_streak", 0), 3) * 0.05)

        pct = max(self.min_pct, min(pct, self.max_pct))
        self.smoothed_pct = (self.smoothed_pct * 0.6) + (pct * 0.4)

        final = max(round(balance * self.smoothed_pct, 2), round(self.min_stake_amount, 2))
        return final

    # -----------------------
    # Heartbeat / helpers
    # -----------------------
    async def send_heartbeat(self):
        try:
            await self.ws.send({"type": "heartbeat", "from": "engine"})
        except Exception as e:
            logger.debug(f"[HEARTBEAT] failed: {e}")
