# File: ws/app.py
#
# Redis -> WebSocket forwarder (stable, minimal, CTRADER-only pipeline).
# WS endpoint:
#   ws://<host>:8003/ws?symbol=XAUUSD
#
# Redis channel:
#   ticks:XAUUSD   (TICK_CHANNEL_PREFIX defaults to "ticks")

import asyncio
import logging
import os
from typing import Optional

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse

import redis.asyncio as redis

logger = logging.getLogger("aq_ws")
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))

app = FastAPI()

REDIS_URL = os.getenv("REDIS_URL", "redis://aq_redis:6379/0")
TICK_CHANNEL_PREFIX = os.getenv("TICK_CHANNEL_PREFIX", "ticks")

_redis: Optional[redis.Redis] = None


def channel_for(symbol: str) -> str:
    return f"{TICK_CHANNEL_PREFIX}:{symbol}"


@app.on_event("startup")
async def startup():
    global _redis
    logger.info("[aq_ws] Starting FastAPI WebSocket server...")
    _redis = redis.from_url(REDIS_URL, decode_responses=True)

    await _redis.ping()
    logger.info(f"[aq_ws] Redis connected: {REDIS_URL}")
    logger.info("[aq_ws] Ready. Forwarding Redis messages from ticks:<symbol> to WS clients.")
    logger.info("[aq_ws] Tick publisher disabled. Expecting ticks from Engine via Redis.")


@app.on_event("shutdown")
async def shutdown():
    global _redis
    if _redis is not None:
        try:
            await _redis.close()
        finally:
            _redis = None


@app.get("/")
async def root():
    return JSONResponse({"ok": True, "service": "aq_ws", "redis": REDIS_URL})


@app.websocket("/ws")
async def ws_ticks(websocket: WebSocket, symbol: str = ""):
    symbol = (symbol or "").strip().upper()
    if not symbol:
        await websocket.close(code=1008)
        return

    await websocket.accept()
    logger.info(f"[aq_ws] Client connected, symbol={symbol}")

    if _redis is None:
        logger.error("[aq_ws] Redis not initialized")
        await websocket.close(code=1011)
        return

    ch = channel_for(symbol)
    pubsub = _redis.pubsub()
    await pubsub.subscribe(ch)
    logger.info(f"[aq_ws] Subscribed to {ch}")

    async def pump_redis_to_ws():
        """
        Correct redis.asyncio pattern: async for msg in pubsub.listen()
        (pubsub.get_message() is not available in this environment)
        """
        try:
            async for msg in pubsub.listen():
                if msg is None:
                    continue

                # Ignore subscribe/unsubscribe notifications
                if msg.get("type") != "message":
                    continue

                data = msg.get("data")
                if data is None:
                    continue

                # PROOF LOG: shows every forwarded message
                logger.info(f"[aq_ws] → WS send {symbol}: {str(data)[:200]}")

                await websocket.send_text(str(data))
        except WebSocketDisconnect:
            pass
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.warning(f"[aq_ws] pump_redis_to_ws error symbol={symbol}: {e}")

    async def pump_ws_rx():
        # consume inbound messages (keepalive)
        try:
            while True:
                await websocket.receive_text()
        except WebSocketDisconnect:
            pass
        except asyncio.CancelledError:
            pass
        except Exception:
            pass

    t1 = asyncio.create_task(pump_redis_to_ws())
    t2 = asyncio.create_task(pump_ws_rx())

    try:
        await asyncio.wait({t1, t2}, return_when=asyncio.FIRST_COMPLETED)
    finally:
        for t in (t1, t2):
            t.cancel()

        try:
            await pubsub.unsubscribe(ch)
        except Exception:
            pass
        try:
            await pubsub.close()
        except Exception:
            pass

        logger.info(f"[aq_ws] Client disconnected, symbol={symbol}")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "app:app",
        host="0.0.0.0",
        port=8003,
        log_level=os.getenv("LOG_LEVEL", "info").lower(),
    )
