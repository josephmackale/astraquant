# File: risk/risk.py
from typing import Optional, Dict, Any, Tuple
import math

class RiskEngine:
    """
    Unified Risk Engine (safe, non-compounding).

    Usage:
      engine = RiskEngine(debug=False)
      decision = engine.assess_trade(account_dict)
      # decision contains keys: allow_trade, stake, stake_pct, reason, risk_score, dynamic_stoploss_pct, dynamic_stoploss_value, new_exposure, surplus_mode

    Design notes:
      - stake_pct is calculated from base_pct + volatility influence, then gently scaled by balance / recent_winrate / confidence.
      - There is NO feedback from previous stake into base_stake.  This prevents compounding drift.
      - ARC mode halves stake_pct before clamping.
    """

    def __init__(
        self,
        base_pct: float = 0.004,            # baseline 0.4% of balance
        volatility_factor: float = 0.005,   # how much volatility affects stake_pct (vol * volatility_factor)
        winrate_scale: float = 0.8,         # scales how much recent_winrate affects stake_pct (0..1)
        balance_buckets: Optional[Dict[str, float]] = None,  # multipliers by balance tiers
        exposure_limit: float = 0.35,       # per-minute exposure limit (0..1)
        max_stake_pct: float = 0.05,        # maximum stake percent (5%)
        min_stake_pct: float = 0.0005,      # minimum stake percent (0.05%)
        debug: bool = False
    ):
        self.base_pct = float(base_pct)
        self.volatility_factor = float(volatility_factor)
        self.winrate_scale = float(winrate_scale)
        self.exposure_limit = float(exposure_limit)
        self.max_stake_pct = float(max_stake_pct)
        self.min_stake_pct = float(min_stake_pct)
        self.debug = bool(debug)

        # default balance multipliers (safe, non-compounding influence)
        # Keys are upper bounds for balance tier
        # value is multiplier applied to stake_pct base after vol/winrate adjustments
        self.balance_buckets = balance_buckets or {
            200.0: 0.6,
            1000.0: 0.85,
            5000.0: 1.0,
            float("inf"): 1.2
        }

    # -------------------------
    # Dynamic stop-loss (improved from risk_logic.dynamic_stop_loss)
    # -------------------------
    def compute_dynamic_stoploss_pct(self, balance: float, volatility: float, performance_factor: float = 1.0) -> float:
        """
        Returns stoploss as a decimal pct of balance (e.g., 0.06 == 6%).
        Reasonable defaults that adapt by volatility, performance, and balance tiers.
        """
        base = 0.015  # baseline 1.5%
        # volatility scaling: slightly greater sensitivity but clamped
        vol_scale = max(0.5, min(3.0, 1.0 + float(volatility) * 2.0))
        # performance factor (1.0 = neutral). small effect only.
        perf_scale = max(0.7, min(1.5, 1.0 + (float(performance_factor) - 1.0) * 0.5))

        # balance bucket multiplier
        b = float(balance)
        for bound, mult in self.balance_buckets.items():
            if b <= float(bound):
                bal_scale = float(mult)
                break

        sl_fraction = base * vol_scale * perf_scale * bal_scale
        # clamp to safe window: not lower than 0.5% and not more than 10%
        return max(0.005, min(0.10, sl_fraction))

    # -------------------------
    # Helper: choose balance multiplier
    # -------------------------
    def _balance_multiplier(self, balance: float) -> float:
        b = float(balance)
        for bound, mult in self.balance_buckets.items():
            if b <= float(bound):
                return float(mult)
        return 1.0

    # -------------------------
    # Stake sizing (unified and safe)
    # -------------------------
    def compute_stake_pct(
        self,
        balance: float,
        volatility: float,
        recent_winrate: Optional[float] = None,
        confidence: Optional[float] = None,
        arc_mode: bool = False
    ) -> float:
        """
        Returns precise stake_pct (decimal). No rounding; caller computes stake amount.
        Formula (non-compounding):
          - base_pct + volatility * volatility_factor
          - scale by winrate: multiplier in [0.6 .. 1.4] depending on recent_winrate (0..1)
          - scale by confidence: multiplier in [0.8 .. 1.6] if provided
          - scale by balance_bucket multiplier
          - ARC mode halves the resulting pct (conservative)
          - final clamp to [min_stake_pct, max_stake_pct]
        """
        # Defensive casts
        b = float(balance)
        vol = float(volatility)
        stake_pct = float(self.base_pct) + vol * float(self.volatility_factor)

        # apply recent_winrate scaling: map 0..1 to [0.6..1.4] with center 1.0 at 0.5
        if recent_winrate is not None:
            rw = max(0.0, min(1.0, float(recent_winrate)))
            # remap: 0 -> 0.6, 0.5 -> 1.0, 1.0 -> 1.4
            win_mult = 0.6 + (rw * (1.4 - 0.6))
            stake_pct *= (1.0 + (win_mult - 1.0) * float(self.winrate_scale))
        # else: if unknown, don't modify stake_pct

        # confidence scaling (optional)
        if confidence is not None:
            c = max(0.0, min(1.0, float(confidence)))
            conf_mult = 0.8 + 0.8 * c  # maps 0..1 -> 0.8..1.6
            stake_pct *= conf_mult

        # balance bucket multiplier
        stake_pct *= self._balance_multiplier(b)

        # ARC mode reduces risk by half
        if arc_mode:
            stake_pct *= 0.5

        # clamp to safe bounds
        stake_pct = max(float(self.min_stake_pct), min(float(self.max_stake_pct), stake_pct))
        return float(stake_pct)

    # -------------------------
    # Risk score (unchanged idea, tuned)
    # -------------------------
    def compute_risk_score(self, volatility: float, recent_winrate: float, current_minute_exposure: float, arc_mode: bool=False) -> float:
        """
        risk_score = volatility*50 + (1 - recent_winrate)*30 + exposure*20 + arc_penalty
        Score typically 0..100+, higher means riskier.
        """
        vol_component = float(volatility) * 50.0
        winrate_component = (1.0 - float(recent_winrate)) * 30.0
        exposure_component = float(current_minute_exposure) * 20.0
        arc_penalty = 10.0 if arc_mode else 0.0

        score = vol_component + winrate_component + exposure_component + arc_penalty
        return round(score, 2)

    # -------------------------
    # Primary assess_trade
    # -------------------------
    def assess_trade(self, account: Dict[str, Any]) -> Dict[str, Any]:
        """
        account keys:
          - account_id (str)
          - balance (float)
          - recent_winrate (float)
          - current_minute_exposure (float)  # 0..1
          - volatility (float)                # decimal, e.g., 0.12
          - mode (str)
          - arc_mode (bool)
        optional:
          - session_profit (float)
          - target (float)
          - surplus_mode (bool)
          - confidence (float 0..1)
          - recent_performance_factor (float) # for stoploss scaling
        """
        # sanitize inputs
        balance = float(account.get("balance", 0.0))
        recent_winrate = float(account.get("recent_winrate", 0.5))
        current_minute_exposure = float(account.get("current_minute_exposure", 0.0))
        volatility = float(account.get("volatility", 0.0))
        mode = account.get("mode", "normal")
        arc_mode = bool(account.get("arc_mode", False))
        confidence = account.get("confidence", None)

        # surplus/target detection
        raw_session_profit = account.get("session_profit")
        session_profit = float(raw_session_profit) if raw_session_profit is not None else 0.0
        target = account.get("target", None)
        surplus_mode = bool(account.get("surplus_mode", False))
        if target is not None:
            try:
                target_val = float(target)
                if session_profit >= target_val:
                    surplus_mode = True
            except Exception:
                pass

        # quick mode-based blocks
        if mode == "locked":
            return {
                "allow_trade": False,
                "stake": 0.0,
                "stake_pct": 0.0,
                "reason": "account_locked"
            }

        # compute risk score
        risk_score = self.compute_risk_score(volatility, recent_winrate, current_minute_exposure, arc_mode=arc_mode)

        # exposure check
        if current_minute_exposure > self.exposure_limit:
            return {
                "allow_trade": False,
                "stake": 0.0,
                "stake_pct": 0.0,
                "reason": "exposure_limit_reached",
                "risk_score": risk_score
            }

        # risk threshold
        if risk_score > 80.0:
            return {
                "allow_trade": False,
                "stake": 0.0,
                "stake_pct": 0.0,
                "reason": "risk_score_too_high",
                "risk_score": risk_score
            }

        # compute stake_pct (precise) and stake amount
        stake_pct = self.compute_stake_pct(
            balance=balance,
            volatility=volatility,
            recent_winrate=recent_winrate,
            confidence=confidence,
            arc_mode=arc_mode
        )

        stake_amount = round(balance * stake_pct, 2)  # money rounded to cents

        # stoploss
        perf_factor = float(account.get("recent_performance_factor", 1.0))
        stoploss_pct = self.compute_dynamic_stoploss_pct(balance, volatility, perf_factor)
        stoploss_value = round(balance * stoploss_pct, 2)

        # estimate new exposure (naive): exposure increases by stake/balance (clamped)
        new_exposure = min(1.0, current_minute_exposure + (stake_amount / max(1.0, balance)))

        result = {
            "allow_trade": True,
            "stake": stake_amount,
            "stake_pct": round(stake_pct, 6),   # precise pct for logging (6 decimal places)
            "reason": "ok",
            "risk_score": risk_score,
            "dynamic_stoploss_pct": round(stoploss_pct, 4),
            "dynamic_stoploss_value": stoploss_value,
            "new_exposure": round(new_exposure, 4),
            "surplus_mode": surplus_mode
        }

        if self.debug:
            result["_debug"] = {
                "inputs": {
                    "balance": balance,
                    "volatility": volatility,
                    "recent_winrate": recent_winrate,
                    "confidence": confidence,
                    "current_minute_exposure": current_minute_exposure,
                    "mode": mode,
                    "arc_mode": arc_mode,
                    "session_profit": session_profit,
                    "target": target,
                    "recent_performance_factor": perf_factor
                },
                "computed": {
                    "stake_pct_precise": stake_pct,
                    "stake_amount_rounded": stake_amount,
                    "stoploss_pct": stoploss_pct,
                    "risk_score": risk_score,
                    "balance_multiplier": self._balance_multiplier(balance)
                }
            }

        return result


# -------------------------
# Quick local test harness (run this file directly to sanity-check)
# -------------------------
if __name__ == "__main__":
    engine = RiskEngine(debug=True)
    example = {
        "account_id": "test",
        "balance": 100.0,
        "recent_winrate": 0.6,
        "current_minute_exposure": 0.0,
        "volatility": 0.12,
        "mode": "normal",
        "arc_mode": False,
        "confidence": None,
        "session_profit": 0.0,
        "target": None,
        "recent_performance_factor": 1.0
    }

    for i in range(6):
        out = engine.assess_trade(example)
        print(f"cycle {i+1}: stake={out['stake']} stake_pct={out['stake_pct']} risk_score={out['risk_score']} new_exposure={out['new_exposure']}")
