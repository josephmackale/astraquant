import os
import json
import time
import asyncio
from typing import Any, Dict, Optional

try:
    import redis.asyncio as redis  # redis-py >= 4.2
except Exception:
    redis = None


REDIS_URL = os.getenv("REDIS_URL", "redis://aq_redis:6379/0")
TICK_CHANNEL_PREFIX = os.getenv("TICK_CHANNEL_PREFIX", "ticks")


def channel_for(symbol: str) -> str:
    s = (symbol or "").strip() or "XAUUSD"
    return f"{TICK_CHANNEL_PREFIX}:{s}"


def now_ts() -> int:
    return int(time.time())


class RedisTickPublisher:
    def __init__(self, redis_url: str = REDIS_URL):
        if redis is None:
            raise RuntimeError(
                "redis.asyncio not available. Add `redis>=4.2.0` to engine/requirements.txt and rebuild."
            )
        self.redis_url = redis_url
        self._r: Optional["redis.Redis"] = None

    async def connect(self) -> None:
        self._r = redis.from_url(self.redis_url, decode_responses=False)
        await self._r.ping()

    async def close(self) -> None:
        if self._r is not None:
            await self._r.close()
            self._r = None

    async def publish_tick(self, symbol: str, tick: Dict[str, Any]) -> int:
        if self._r is None:
            raise RuntimeError("Redis publisher not connected. Call connect() first.")

        payload = dict(tick)
        payload.setdefault("symbol", symbol)
        payload.setdefault("ts", now_ts())

        chan = channel_for(symbol)
        msg = json.dumps(payload, separators=(",", ":"), ensure_ascii=False)
        return await self._r.publish(chan, msg)


async def publish_loop_stub(
    symbol: str = "XAUUSD",
    start_price: float = 2050.0,
    step: float = 0.1,
    interval_sec: float = 1.0,
) -> None:
    pub = RedisTickPublisher()
    await pub.connect()
    try:
        i = 0
        while True:
            price = round(start_price + i * step, 5)
            subs = await pub.publish_tick(symbol, {"price": price})
            print(f"[engine] published {symbol} price={price} subs={subs}")
            i += 1
            await asyncio.sleep(interval_sec)
    finally:
        await pub.close()
