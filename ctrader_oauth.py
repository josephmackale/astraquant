import json
import os
import secrets
import time
from pathlib import Path
from urllib.parse import urlencode

import httpx
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import RedirectResponse, JSONResponse

router = APIRouter(prefix="/ctrader", tags=["ctrader"])

AUTH_URL = os.getenv("CTRADER_AUTH_URL", "https://connect.spotware.com/apps/auth").strip()
TOKEN_URL = os.getenv("CTRADER_TOKEN_URL", "https://connect.spotware.com/apps/token").strip()

TOKENS_DIR = Path(os.getenv("CTRADER_TOKENS_DIR", "/opt/astraquant/config/ctrader"))
TOKENS_FILE = TOKENS_DIR / "tokens.json"

REFRESH_EARLY_SECONDS = int(os.getenv("CTRADER_REFRESH_EARLY_SECONDS", str(24 * 3600)))
HTTP_TIMEOUT = float(os.getenv("CTRADER_HTTP_TIMEOUT", "25"))


def _env(name: str) -> str:
    v = (os.getenv(name) or "").strip()
    if not v:
        raise HTTPException(status_code=500, detail=f"Missing env var: {name}")
    return v


def _load_tokens() -> dict:
    if not TOKENS_FILE.exists():
        return {}
    try:
        return json.loads(TOKENS_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _save_tokens(tokens: dict) -> None:
    TOKENS_DIR.mkdir(parents=True, exist_ok=True)
    TOKENS_FILE.write_text(json.dumps(tokens, indent=2, sort_keys=True), encoding="utf-8")


def _ensure_expires_at(tokens: dict) -> dict:
    if isinstance(tokens.get("expires_at"), int):
        return tokens
    expires_in = tokens.get("expires_in")
    if isinstance(expires_in, (int, float)):
        tokens["expires_at"] = int(time.time()) + int(expires_in) - 60
    return tokens


def _seconds_left(tokens: dict) -> int | None:
    tokens = _ensure_expires_at(tokens)
    ea = tokens.get("expires_at")
    if not isinstance(ea, int):
        return None
    return ea - int(time.time())


def _expiring_soon(tokens: dict) -> bool:
    left = _seconds_left(tokens)
    if left is None:
        return True
    return left <= REFRESH_EARLY_SECONDS


async def _refresh_with_refresh_token(tokens: dict) -> dict:
    client_id = _env("CTRADER_CLIENT_ID")
    client_secret = _env("CTRADER_CLIENT_SECRET")

    refresh_token = tokens.get("refresh_token")
    if not refresh_token:
        raise HTTPException(status_code=400, detail="No refresh_token present. Re-login via /ctrader/login")

    data = {
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
        "client_id": client_id,
        "client_secret": client_secret,
    }

    async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
        r = await client.post(TOKEN_URL, data=data)

    if r.status_code >= 400:
        raise HTTPException(status_code=502, detail=f"Token refresh failed: {r.status_code} {r.text}")

    new_tokens = r.json()
    merged = dict(tokens)
    merged.update(new_tokens)

    if "refresh_token" not in new_tokens:
        merged["refresh_token"] = refresh_token

    merged = _ensure_expires_at(merged)
    _save_tokens(merged)
    return merged


@router.get("/login")
def login():
    client_id = _env("CTRADER_CLIENT_ID")
    redirect_uri = _env("CTRADER_REDIRECT_URI")

    state = secrets.token_urlsafe(24)
    scope = (os.getenv("CTRADER_SCOPE") or "trading").strip()

    qs = urlencode(
        {
            "client_id": client_id,
            "redirect_uri": redirect_uri,
            "response_type": "code",
            "state": state,
            "scope": scope,
        }
    )
    return RedirectResponse(f"{AUTH_URL}?{qs}", status_code=302)


@router.get("/callback")
async def callback(code: str | None = None, error: str | None = None, state: str | None = None):
    client_id = _env("CTRADER_CLIENT_ID")
    client_secret = _env("CTRADER_CLIENT_SECRET")
    redirect_uri = _env("CTRADER_REDIRECT_URI")

    if error:
        raise HTTPException(status_code=400, detail=f"OAuth error: {error}")
    if not code:
        raise HTTPException(status_code=400, detail="Missing ?code=")

    data = {
        "grant_type": "authorization_code",
        "code": code,
        "redirect_uri": redirect_uri,
        "client_id": client_id,
        "client_secret": client_secret,
    }

    async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
        r = await client.post(TOKEN_URL, data=data)

    if r.status_code >= 400:
        raise HTTPException(status_code=502, detail=f"Token exchange failed: {r.status_code} {r.text}")

    tokens = _ensure_expires_at(r.json())
    _save_tokens(tokens)

    return {
        "ok": True,
        "saved_to": str(TOKENS_FILE),
        "has_access_token": bool(tokens.get("access_token")),
        "has_refresh_token": bool(tokens.get("refresh_token")),
        "expires_at": tokens.get("expires_at"),
    }


@router.get("/status")
def status():
    tokens = _load_tokens()
    return {
        "ok": True,
        "path": str(TOKENS_FILE),
        "tokens_present": TOKENS_FILE.exists(),
        "has_access_token": bool(tokens.get("access_token")) if tokens else False,
        "has_refresh_token": bool(tokens.get("refresh_token")) if tokens else False,
        "expires_at": tokens.get("expires_at"),
        "seconds_left": _seconds_left(tokens),
        "expiring_soon": _expiring_soon(tokens),
    }


@router.post("/refresh")
async def refresh():
    tokens = _load_tokens()
    if not tokens:
        raise HTTPException(status_code=400, detail="No tokens found. Login via /ctrader/login first.")
    tokens = await _refresh_with_refresh_token(tokens)
    return {
        "ok": True,
        "expires_at": tokens.get("expires_at"),
    }


@router.get("/token")
async def token(reveal: int = Query(0)):
    tokens = _load_tokens()
    if not tokens:
        raise HTTPException(status_code=400, detail="No tokens found. Login via /ctrader/login first.")

    if _expiring_soon(tokens):
        tokens = await _refresh_with_refresh_token(tokens)

    resp = {
        "ok": True,
        "expires_at": tokens.get("expires_at"),
        "seconds_left": _seconds_left(tokens),
    }
    if reveal == 1:
        resp["access_token"] = tokens.get("access_token")
    return resp
