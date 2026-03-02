import json
import asyncio
from fastapi import WebSocket

_clients: set[WebSocket] = set()
_lock = asyncio.Lock()

async def register(ws: WebSocket):
    await ws.accept()
    async with _lock:
        _clients.add(ws)

async def unregister(ws: WebSocket):
    async with _lock:
        _clients.discard(ws)

async def broadcast(payload: dict):
    if not _clients:
        return

    msg = json.dumps(payload, separators=(",", ":"), ensure_ascii=False)
    dead = []

    async with _lock:
        for ws in list(_clients):
            try:
                await ws.send_text(msg)
            except Exception:
                dead.append(ws)

        for ws in dead:
            _clients.discard(ws)
