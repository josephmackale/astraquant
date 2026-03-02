# engine/engine_integration.py
import asyncio
import logging
from typing import Dict, Any

from engine.risk_client import RiskClient

LOG = logging.getLogger("engine")
risk_client = RiskClient()

# Example in-controller state (you will integrate with your actual controller state)
class EngineController:
    def __init__(self):
        self.balance = 100.0
        self.current_minute_exposure = 0.0  # 0..1
        self.account_id = "engine-main"

    async def request_risk_and_maybe_trade(self, extra_inputs: Dict[str, Any]):
        account_payload = {
            "account_id": self.account_id,
            "balance": self.balance,
            "recent_winrate": extra_inputs.get("recent_winrate", 0.5),
            "current_minute_exposure": self.current_minute_exposure,
            "volatility": extra_inputs.get("volatility", 0.12),
            "mode": extra_inputs.get("mode", "normal"),
            "arc_mode": extra_inputs.get("arc_mode", False),
            "session_profit": extra_inputs.get("session_profit", 0.0),
            "target": extra_inputs.get("target"),
            "surplus_mode": extra_inputs.get("surplus_mode", False),
            "confidence": extra_inputs.get("confidence"),
            "recent_performance_factor": extra_inputs.get("recent_performance_factor", 1.0)
        }

        try:
            resp = await risk_client.assess(account_payload)
        except Exception as e:
            LOG.error("Risk call failed, skipping trade: %s", e)
            return {"allowed": False, "reason": "risk_unavailable"}

        # handle response
        allow_trade = bool(resp.get("allow_trade", False))
        if not allow_trade:
            LOG.info("Risk denied trade: reason=%s risk_score=%s", resp.get("reason"), resp.get("risk_score"))
            return {"allowed": False, "reason": resp.get("reason"), "meta": resp}

        # apply stake & exposure
        stake = float(resp["stake"])
        stake_pct = float(resp.get("stake_pct", round(stake / max(1.0, self.balance), 6)))
        new_exposure = float(resp.get("new_exposure", self.current_minute_exposure + (stake / max(1.0, self.balance))))

        # commit exposure update (engine-level)
        self.current_minute_exposure = new_exposure

        LOG.info("RISK OK: stake=%.2f stake_pct=%s new_exposure=%.4f", stake, stake_pct, new_exposure)

        # return instruction to executor
        return {
            "allowed": True,
            "stake": stake,
            "stake_pct": stake_pct,
            "stoploss_value": resp.get("dynamic_stoploss_value"),
            "stoploss_pct": resp.get("dynamic_stoploss_pct"),
            "risk_score": resp.get("risk_score"),
            "meta": resp
        }

    async def shutdown(self):
        await risk_client.close()

# Example usage inside an async loop
# controller = EngineController()
# result = await controller.request_risk_and_maybe_trade({...})
