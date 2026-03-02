import os
import json
import time
import asyncio
import logging
from typing import Dict, Set, Optional

import websockets
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Query
from fastapi.responses import JSONResponse
from routers.ctrader_oauth import router as ctrader_router

# ============================================================
# CONFIG
# ============================================================

APP_NAME = "aq_api"
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

CORE_WS_URL = os.getenv("CORE_WS_URL")  # e.g. ws://aq_ws:8003/ws?symbol=XAUUSD
if not CORE_WS_URL:
    raise RuntimeError("CORE_WS_URL is not set")

API_KEY = os.getenv("API_KEY", "")  # used for upstream authorize message

STREAM_IDLE_TTL = 30        # seconds with no clients before stopping upstream
RECONNECT_DELAY = 1.0       # seconds
PING_INTERVAL = 20          # upstream ping interval

# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s [%(levelname)s] [api] %(message)s",
)
logger = logging.getLogger(APP_NAME)

# ============================================================
# APP
# ============================================================

app = FastAPI(title=APP_NAME)
app.include_router(ctrader_router)

# ============================================================
# STREAM HUB
# ============================================================

class StreamState:
    def __init__(self, symbol: str):
        self.symbol = symbol
        self.clients: Set[WebSocket] = set()
        self.task: Optional[asyncio.Task] = None
        self.last_tick_ts: float = 0.0
        self.last_client_ts: float = 0.0
        self.last_error: Optional[str] = None
        self.upstream_connected: bool = False


class PriceStreamHub:
    def __init__(self):
        self._lock = asyncio.Lock()
        self._streams: Dict[str, StreamState] = {}

    async def add_client(self, symbol: str, ws: WebSocket):
        async with self._lock:
            st = self._streams.get(symbol)
            if not st:
                st = StreamState(symbol)
                self._streams[symbol] = st
                logger.info(f"creating stream for {symbol}")

            st.clients.add(ws)
            st.last_client_ts = time.time()

            if st.task is None or st.task.done():
                st.task = asyncio.create_task(self._run_upstream(st))

    async def remove_client(self, symbol: str, ws: WebSocket):
        async with self._lock:
            st = self._streams.get(symbol)
            if not st:
                return
            st.clients.discard(ws)
            st.last_client_ts = time.time()

    async def broadcast(self, st: StreamState, payload: dict):
        if not st.clients:
            return

        msg = json.dumps(payload, separators=(",", ":"), ensure_ascii=False)
        dead = set()

        for ws in list(st.clients):
            try:
                await ws.send_text(msg)
            except Exception:
                dead.add(ws)

        if dead:
            async with self._lock:
                for ws in dead:
                    st.clients.discard(ws)

    def _build_upstream_url(self, symbol: str) -> str:
        base_url = CORE_WS_URL.split("?")[0]
        return f"{base_url}?symbol={symbol}"

    async def _authorize_upstream(self, upstream):
        """
        Match engine WSClient behavior:
        send {"authorize": <token>} if API_KEY is set.
        """
        if not API_KEY:
            return
        try:
            await upstream.send(json.dumps({"authorize": API_KEY}))
            logger.info("[upstream] authorize sent")
        except Exception as e:
            logger.warning(f"[upstream] authorize send failed: {e}")

    async def _send_subscribe(self, upstream, symbol: str):
        """
        Optional subscribe handshake. Harmless if ignored.
        """
        for msg in (
            {"type": "subscribe", "symbol": symbol},
            {"action": "subscribe", "symbol": symbol},
            {"symbol": symbol},
        ):
            try:
                await upstream.send(json.dumps(msg))
            except Exception:
                pass

    async def _run_upstream(self, st: StreamState):
        symbol = st.symbol
        url = self._build_upstream_url(symbol)

        logger.info(f"upstream start {symbol} → {url}")

        while True:
            # stop if idle
            now = time.time()
            async with self._lock:
                if not st.clients and st.last_client_ts and (now - st.last_client_ts) > STREAM_IDLE_TTL:
                    logger.info(f"stream idle, stopping {symbol}")
                    self._streams.pop(symbol, None)
                    return

            try:
                async with websockets.connect(
                    url,
                    ping_interval=PING_INTERVAL,
                    ping_timeout=PING_INTERVAL,
                    close_timeout=5,
                    max_queue=1000,
                ) as upstream:

                    st.upstream_connected = True
                    st.last_error = None
                    logger.info(f"upstream connected {symbol}")

                    # IMPORTANT: authorize like engine WSClient
                    await self._authorize_upstream(upstream)

                    # optional subscribe messages
                    await self._send_subscribe(upstream, symbol)

                    async for msg in upstream:
                        # msg should be JSON; still guard
                        try:
                            data = json.loads(msg)
                        except Exception:
                            # forward raw
                            data = {"symbol": symbol, "raw": msg, "ts": time.time()}

                        st.last_tick_ts = time.time()
                        await self.broadcast(st, data)

            except Exception as e:
                st.upstream_connected = False
                st.last_error = str(e)
                logger.warning(f"upstream error {symbol}: {e}")
                await asyncio.sleep(RECONNECT_DELAY)

    async def status(self):
        now = time.time()
        async with self._lock:
            return {
                sym: {
                    "clients": len(st.clients),
                    "upstream_connected": st.upstream_connected,
                    "last_tick_age_sec": (now - st.last_tick_ts) if st.last_tick_ts else None,
                    "last_error": st.last_error,
                }
                for sym, st in self._streams.items()
            }


hub = PriceStreamHub()

# ============================================================
# ROUTES
# ============================================================

@app.get("/health")
async def health():
    return {"ok": True, "service": APP_NAME}


@app.get("/stream/status")
async def stream_status():
    return JSONResponse(await hub.status())


@app.websocket("/ws/prices")
async def ws_prices(
    ws: WebSocket,
    symbol: str = Query(..., description="Trading symbol, e.g. XAUUSD"),
):
    await ws.accept()
    await ws.send_text(json.dumps({"type": "hello", "symbol": symbol, "ts": time.time()}))

    await hub.add_client(symbol, ws)

    try:
        # keep alive; no client messages required
        while True:
            await asyncio.sleep(60)
    except WebSocketDisconnect:
        pass
    finally:
        await hub.remove_client(symbol, ws)
        try:
            await ws.close()
        except Exception:
            pass
