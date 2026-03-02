# engine/system_test.py
import asyncio
import random
import logging
from engine.engine_integration import EngineController
from engine.ws_sync import TickAggregator
from engine.ai_blender import smooth_confidence, safe_confidence_for_risk

LOG = logging.getLogger("system_test")
logging.basicConfig(level=logging.INFO)

async def run_simulation():
    controller = EngineController()
    agg = TickAggregator()
    prev_conf = None

    # simulate N ticks, 1 per second
    for i in range(30):
        price = 1000.0 + random.uniform(-10, 10) * (1 + i*0.01)
        stats = agg.on_tick(price)
        volatility = stats["volatility"]
        # mock AI confidence (flaky)
        raw_conf = max(0.0, min(1.0, 0.5 + random.uniform(-0.4, 0.4)))
        prev_conf = smooth_confidence(prev_conf, raw_conf)
        safe_conf = safe_confidence_for_risk(prev_conf)

        extra_inputs = {
            "recent_winrate": 0.6,
            "volatility": volatility,
            "mode": "normal",
            "arc_mode": False,
            "session_profit": 0.0,
            "target": 160,
            "confidence": safe_conf,
            "recent_performance_factor": 1.0
        }

        res = await controller.request_risk_and_maybe_trade(extra_inputs)
        LOG.info("tick %d price=%.2f vol=%.6f -> res=%s", i+1, price, volatility, res)
        await asyncio.sleep(1.0)

    await controller.shutdown()

if __name__ == "__main__":
    asyncio.run(run_simulation())
