# engine/main.py  (DROP-IN, CTRADER-ONLY, WITH WS STREAM)
import asyncio
import os
import logging

from contextlib import asynccontextmanager

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
import uvicorn

from engine.controller import Controller
from engine.ws_server import register, unregister

logger = logging.getLogger("engine-main")
logging.basicConfig(level=logging.INFO)


# ---------------------------------------------------------
# GLOBALS
# ---------------------------------------------------------
controller: Controller | None = None
engine_task: asyncio.Task | None = None


# ---------------------------------------------------------
# VALIDATE ENV (STRICT)
# ---------------------------------------------------------
def validate_env() -> None:
    broker = (os.getenv("BROKER") or "CTRADER").upper()
    symbol = os.getenv("CTRADER_SYMBOL") or os.getenv("SYMBOL")
    ws_url = os.getenv("CORE_WS_URL")

    if broker != "CTRADER":
        raise RuntimeError(f"BROKER must be CTRADER (got {broker})")

    if not symbol:
        raise RuntimeError("Missing CTRADER_SYMBOL (or SYMBOL)")

    if not ws_url:
        raise RuntimeError("Missing CORE_WS_URL (WS tick feed required)")

    logger.info("🔧 ENV OK → BROKER=CTRADER SYMBOL=%s WS=%s", symbol, ws_url)


# ---------------------------------------------------------
# LIFESPAN — START / STOP ENGINE
# ---------------------------------------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    global controller, engine_task

    logger.info("🚀 ENGINE BOOT (CTRADER-ONLY)")

    try:
        validate_env()
    except Exception as e:
        logger.critical("❌ Engine config error: %s", e)
        controller = None
        yield
        return

    try:
        controller = Controller()
        app.state.controller = controller
        logger.info("🧠 Controller instantiated.")
    except Exception as e:
        logger.critical("❌ Controller init failed: %s", e)
        controller = None
        yield
        return

    try:
        engine_task = asyncio.create_task(controller.trade_loop())
        logger.info("🎛️ trade_loop started.")
    except Exception as e:
        logger.critical("❌ Failed to start trade_loop: %s", e)
        controller = None
        engine_task = None
        yield
        return

    yield

    # -----------------------
    # SHUTDOWN (CLEAN)
    # -----------------------
    logger.info("🛑 ENGINE SHUTDOWN SIGNAL")

    try:
        if controller:
            await controller.stop()
    except Exception as e:
        logger.warning("Controller stop() failed: %s", e)

    if engine_task:
        engine_task.cancel()
        try:
            await engine_task
        except asyncio.CancelledError:
            logger.info("🔌 trade_loop cancelled cleanly.")
        except Exception as e:
            logger.warning("trade_loop shutdown error: %s", e)

    controller = None
    engine_task = None


# ---------------------------------------------------------
# FASTAPI APP
# ---------------------------------------------------------
app = FastAPI(
    title="AstraQuant Engine (CTRADER)",
    version="1.0.0",
    description="CTRADER-only execution engine for AstraQuant",
    lifespan=lifespan,
)


# ---------------------------------------------------------
# HEALTH
# ---------------------------------------------------------
@app.get("/")
async def home():
    c = getattr(app.state, "controller", None)
    return {
        "engine": "running",
        "broker": "CTRADER",
        "controller": "active" if c else "missing",
        "observe_only": os.getenv("OBSERVE_ONLY", "1"),
        "status": "ok",
    }


# ---------------------------------------------------------
# DASHBOARD
# ---------------------------------------------------------
@app.get("/v1/dashboard")
async def dashboard():
    c = getattr(app.state, "controller", None)
    if not c:
        return {"error": "controller not initialized"}

    return {
        "symbol": c.market,
        "observe_only": c.observe_only,
        "last_price": c.last_price,
        "spread": c.last_spread,
        "volatility": c.last_volatility,
        "orderflow": {
            "pressure": c.last_of_pressure,
            "shock": c.last_of_shock,
        },
        "pa": c.pa_stats,
        "ai": {
            "score": c.last_ai_score,
            "confidence": c.last_ai_conf,
            "trend": c.last_ai_trend,
        },
        "balance": c.current_balance,
        "drawdown": c.current_drawdown,
        "loss_streak": c.loss_streak,
        "win_streak": c.win_streak,
        "running": c.running,
    }


# ---------------------------------------------------------
# ENGINE WS — PRICE STREAM (FOR GATEWAY & CLIENTS)
# ---------------------------------------------------------
@app.websocket("/ws")
async def engine_ws(ws: WebSocket):
    await register(ws)
    try:
        while True:
            await asyncio.sleep(60)
    except WebSocketDisconnect:
        pass
    finally:
        await unregister(ws)


# ---------------------------------------------------------
# LOCAL DEV ENTRYPOINT
# ---------------------------------------------------------
def start():
    logger.info("Starting Uvicorn on :8001")
    uvicorn.run("engine.main:app", host="0.0.0.0", port=8001, reload=False)


if __name__ == "__main__":
    start()
