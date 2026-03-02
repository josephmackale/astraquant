import os
import asyncio
from redis_ticks import publish_loop_stub

if __name__ == "__main__":
    symbol = os.getenv("CTRADER_SYMBOL", "XAUUSD")
    start_price = float(os.getenv("STUB_START_PRICE", "2050.0"))
    step = float(os.getenv("STUB_STEP", "0.1"))
    interval = float(os.getenv("STUB_INTERVAL_SEC", "1.0"))

    asyncio.run(
        publish_loop_stub(
            symbol=symbol,
            start_price=start_price,
            step=step,
            interval_sec=interval,
        )
    )
