# engine/client.py
import asyncio
import json
import logging
from typing import Any, Optional, Callable

import websockets

logger = logging.getLogger(__name__)


class TickWSClient:
    """
    Resilient WebSocket client for AstraQuant ticks.

    - connect(): starts a background loop that keeps the socket open
    - close(): stops loop + closes socket
    - Calls on_tick(tick) for each received message (parsed JSON if possible)
    - Supports BOTH sync and async on_tick callbacks
    """

    def __init__(
        self,
        url: str,
        on_tick: Optional[Callable[[Any], Any]] = None,
        reconnect_delay: float = 1.0,
        max_reconnect_delay: float = 15.0,
        ping_interval: float = 20.0,
        ping_timeout: float = 20.0,
    ):
        self.url = url
        self.on_tick = on_tick

        self.reconnect_delay = reconnect_delay
        self.max_reconnect_delay = max_reconnect_delay
        self.ping_interval = ping_interval
        self.ping_timeout = ping_timeout

        self._stop = asyncio.Event()
        self._task: Optional[asyncio.Task] = None
        self._conn = None

    async def connect(self):
        """Start the background receive loop (safe to call multiple times)."""
        if self._task and not self._task.done():
            logger.info("[WS] connect() called but loop already running")
            return

        self._stop.clear()
        self._task = asyncio.create_task(self._run())
        logger.info(f"[WS] connect() scheduled url={self.url}")

    async def _run(self):
        delay = self.reconnect_delay

        while not self._stop.is_set():
            try:
                logger.info(f"[WS] connecting… url={self.url}")
                self._conn = await websockets.connect(
                    self.url,
                    ping_interval=self.ping_interval,
                    ping_timeout=self.ping_timeout,
                    close_timeout=5,
                    max_queue=1000,
                )
                logger.info("[WS] connected ✅")
                delay = self.reconnect_delay  # reset backoff after success

                while not self._stop.is_set():
                    msg = await self._conn.recv()

                    # 🔎 prove we are receiving data
                    try:
                        logger.info(f"[WS RX] {str(msg)[:180]}")
                    except Exception:
                        pass

                    tick: Any = msg
                    try:
                        if isinstance(msg, bytes):
                            msg = msg.decode("utf-8", errors="ignore")
                        if isinstance(msg, str):
                            tick = json.loads(msg)
                    except Exception:
                        tick = msg  # raw fallback

                    # ✅ Support sync + async callbacks
                    if self.on_tick:
                        try:
                            r = self.on_tick(tick)
                            if asyncio.iscoroutine(r):
                                asyncio.create_task(r)
                        except Exception as e:
                            logger.exception(f"[WS] on_tick failed: {e}")

            except asyncio.CancelledError:
                break
            except Exception as e:
                if self._stop.is_set():
                    break
                logger.warning(f"[WS] error: {e} (reconnecting in {delay:.1f}s)")
                await asyncio.sleep(delay)
                delay = min(delay * 2, self.max_reconnect_delay)
            finally:
                try:
                    if self._conn is not None:
                        await self._conn.close()
                except Exception:
                    pass
                self._conn = None

        logger.info("[WS] stopped")

    async def close(self):
        """Stop the loop and close the connection."""
        self._stop.set()

        if self._task:
            self._task.cancel()
            try:
                await self._task
            except Exception:
                pass
        self._task = None

        try:
            if self._conn is not None:
                await self._conn.close()
        except Exception:
            pass
        self._conn = None


def attach_ws_to_controller(controller, url: str) -> TickWSClient:
    """
    Convenience hook:
    - Calls controller.on_tick(tick) if present (supports async)
    - Also keeps controller.last_tick/last_price updated as a fallback
    """

    def _on_tick(tick: Any):
        # Always keep raw last_tick for visibility/debug
        try:
            controller.last_tick = tick
        except Exception:
            pass

        # Optional: store last price if tick has it
        try:
            if isinstance(tick, dict):
                if "price" in tick:
                    controller.last_price = tick.get("price")
                elif "tick" in tick and isinstance(tick["tick"], dict):
                    controller.last_price = tick["tick"].get("price")
        except Exception:
            pass

        # ✅ If controller has on_tick, call it (can be async)
        try:
            fn = getattr(controller, "on_tick", None)
            if fn:
                return fn(tick)  # may return coroutine; caller schedules it
        except Exception:
            logger.exception("[CTL] controller.on_tick failed")

        # Fallback visibility
        try:
            logger.info(f"[CTL TICK] {str(tick)[:160]}")
        except Exception:
            pass

    return TickWSClient(url=url, on_tick=_on_tick)
