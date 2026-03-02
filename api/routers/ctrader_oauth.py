import json
import os
import secrets
import time
from pathlib import Path
from urllib.parse import urlencode

import httpx
from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import RedirectResponse, JSONResponse

router = APIRouter(prefix="/ctrader", tags=["ctrader"])

AUTH_URL = os.getenv("CTRADER_AUTH_URL", "https://connect.spotware.com/apps/auth").strip()
TOKEN_URL = os.getenv("CTRADER_TOKEN_URL", "https://connect.spotware.com/apps/token").strip()

TOKENS_DIR = Path(os.getenv("CTRADER_TOKENS_DIR", "/opt/astraquant/config/ctrader"))
TOKENS_FILE = TOKENS_DIR / "tokens.json"

REFRESH_EARLY_SECONDS = int(os.getenv("CTRADER_REFRESH_EARLY_SECONDS", str(24 * 3600)))
HTTP_TIMEOUT = float(os.getenv("CTRADER_HTTP_TIMEOUT", "25"))

# OAuth state cookie settings (replaces oauth_state.txt file)
STATE_COOKIE = os.getenv("CTRADER_STATE_COOKIE", "ctrader_oauth_state").strip()
STATE_TTL_SECONDS = int(os.getenv("CTRADER_STATE_TTL_SECONDS", "600"))  # 10 minutes
STATE_COOKIE_SECURE = (os.getenv("CTRADER_STATE_COOKIE_SECURE", "0").strip().lower() in ("1", "true", "yes"))
STATE_COOKIE_SAMESITE = (os.getenv("CTRADER_STATE_COOKIE_SAMESITE", "lax").strip().lower())
if STATE_COOKIE_SAMESITE not in ("lax", "strict", "none"):
    STATE_COOKIE_SAMESITE = "lax"


def _env(name: str) -> str:
    v = (os.getenv(name) or "").strip()
    if not v:
        raise HTTPException(status_code=500, detail=f"Missing env var: {name}")
    return v


def _oauth_client_id() -> str:
    """
    Spotware OAuth requires the FULL client id string (with underscore suffix).
    Example: 19659_BKRMuBH5ZDW98dZVFbpqOhTcIYiL4diZG0NEKF06M1ruw7UVG9
    Back-compat: allow CTRADER_CLIENT_ID if present.
    """
    return (os.getenv("CTRADER_OAUTH_CLIENT_ID") or os.getenv("CTRADER_CLIENT_ID") or "").strip() or _env("CTRADER_OAUTH_CLIENT_ID")


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
    """
    Guarantee expires_at unix timestamp exists when expires_in/expiresIn is provided.
    """
    if isinstance(tokens.get("expires_at"), int):
        return tokens

    expires_in = tokens.get("expires_in")
    if expires_in is None:
        expires_in = tokens.get("expiresIn")

    if isinstance(expires_in, (int, float)):
        tokens["expires_at"] = int(time.time()) + int(expires_in) - 60  # 60s buffer
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


def _normalize_and_dual_write(tokens: dict) -> dict:
    """
    Spotware OAuth returns: access_token / refresh_token / token_type / expires_in
    Your engine historically used: accessToken / refreshToken / tokenType / expiresIn
    We'll store BOTH sets to prevent breaking either side.
    """
    at = tokens.get("access_token") or tokens.get("accessToken")
    rt = tokens.get("refresh_token") or tokens.get("refreshToken")
    tt = tokens.get("token_type") or tokens.get("tokenType")
    ei = tokens.get("expires_in") if tokens.get("expires_in") is not None else tokens.get("expiresIn")

    if at:
        tokens["access_token"] = at
        tokens["accessToken"] = at
    if rt:
        tokens["refresh_token"] = rt
        tokens["refreshToken"] = rt
    if tt:
        tokens["token_type"] = tt
        tokens["tokenType"] = tt
    if ei is not None:
        tokens["expires_in"] = ei
        tokens["expiresIn"] = ei

    tokens = _ensure_expires_at(tokens)
    return tokens


def _raise_if_oauth_error(payload: dict):
    """
    Spotware errors sometimes come back as:
      { "errorCode": "...", "description": "..." }
    or OAuth style:
      { "error": "...", "error_description": "..." }
    Never save these into tokens.json.
    """
    if payload.get("errorCode") or payload.get("error"):
        raise HTTPException(status_code=400, detail=payload)


async def _refresh_with_refresh_token(tokens: dict) -> dict:
    """
    Refresh and persist tokens, returning merged tokens.
    """
    client_id = _oauth_client_id()
    client_secret = _env("CTRADER_CLIENT_SECRET")

    refresh_token = tokens.get("refresh_token") or tokens.get("refreshToken")
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
    _raise_if_oauth_error(new_tokens)

    merged = dict(tokens)
    merged.update(new_tokens)

    # Provider may not return a new refresh token
    if not (merged.get("refresh_token") or merged.get("refreshToken")):
        merged["refresh_token"] = refresh_token
        merged["refreshToken"] = refresh_token

    merged = _normalize_and_dual_write(merged)
    _save_tokens(merged)
    return merged


@router.get("/login")
def login():
    client_id = _oauth_client_id()
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

    resp = RedirectResponse(f"{AUTH_URL}?{qs}", status_code=302)
    # Store state in cookie so callback can validate reliably (no container-file race)
    resp.set_cookie(
        key=STATE_COOKIE,
        value=state,
        max_age=STATE_TTL_SECONDS,
        httponly=True,
        samesite=STATE_COOKIE_SAMESITE,
        secure=STATE_COOKIE_SECURE,
        path="/",
    )
    return resp


@router.get("/callback")
async def callback(request: Request, code: str | None = None, error: str | None = None, state: str | None = None):
    client_id = _oauth_client_id()
    client_secret = _env("CTRADER_CLIENT_SECRET")
    redirect_uri = _env("CTRADER_REDIRECT_URI")

    if error:
        raise HTTPException(status_code=400, detail=f"OAuth error: {error}")
    if not code:
        raise HTTPException(status_code=400, detail="Missing ?code=")

    cookie_state = (request.cookies.get(STATE_COOKIE) or "").strip()
    query_state = (state or "").strip()

    if not cookie_state:
        raise HTTPException(status_code=400, detail="OAuth state missing. Re-login via /ctrader/login")

    if not query_state:
        raise HTTPException(status_code=400, detail="Missing ?state=. Re-login via /ctrader/login")

    if query_state != cookie_state:
        raise HTTPException(status_code=400, detail="OAuth state mismatch. Re-login via /ctrader/login")

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

    tokens = r.json()
    _raise_if_oauth_error(tokens)

    tokens = _normalize_and_dual_write(tokens)
    _save_tokens(tokens)

    resp = JSONResponse(
        {
            "ok": True,
            "saved_to": str(TOKENS_FILE),
            "has_access_token": bool(tokens.get("access_token") or tokens.get("accessToken")),
            "has_refresh_token": bool(tokens.get("refresh_token") or tokens.get("refreshToken")),
            "expires_in": tokens.get("expires_in") or tokens.get("expiresIn"),
            "expires_at": tokens.get("expires_at"),
            "state_received": True,
        }
    )
    # state is single-use
    resp.delete_cookie(key=STATE_COOKIE, path="/")
    return resp


@router.get("/status")
def status():
    tokens = _load_tokens()
    left = _seconds_left(tokens) if tokens else None

    return {
        "ok": True,
        "path": str(TOKENS_FILE),
        "tokens_present": TOKENS_FILE.exists(),
        "has_access_token": bool(tokens.get("access_token") or tokens.get("accessToken")) if tokens else False,
        "has_refresh_token": bool(tokens.get("refresh_token") or tokens.get("refreshToken")) if tokens else False,
        "expires_at": tokens.get("expires_at") if tokens else None,
        "seconds_left": left,
        "expiring_soon": _expiring_soon(tokens) if tokens else True,
    }


@router.post("/refresh")
async def refresh():
    tokens = _load_tokens()
    if not tokens:
        raise HTTPException(status_code=400, detail="No tokens found. Login via /ctrader/login first.")
    merged = await _refresh_with_refresh_token(tokens)

    return JSONResponse(
        {
            "ok": True,
            "saved_to": str(TOKENS_FILE),
            "has_access_token": bool(merged.get("access_token") or merged.get("accessToken")),
            "has_refresh_token": bool(merged.get("refresh_token") or merged.get("refreshToken")),
            "expires_in": merged.get("expires_in") or merged.get("expiresIn"),
            "expires_at": merged.get("expires_at"),
        }
    )


@router.get("/token")
async def token(reveal: int = Query(default=0, description="Set reveal=1 to include access_token")):
    """
    Returns token metadata; auto-refreshes if expiring soon.
    By default does NOT reveal access_token (safer).
    """
    tokens = _load_tokens()
    if not tokens:
        raise HTTPException(status_code=400, detail="No tokens found. Login via /ctrader/login first.")

    if _expiring_soon(tokens):
        tokens = await _refresh_with_refresh_token(tokens)
    else:
        tokens = _normalize_and_dual_write(tokens)

    resp = {
        "ok": True,
        "expires_at": tokens.get("expires_at"),
        "seconds_left": _seconds_left(tokens),
        "expiring_soon": _expiring_soon(tokens),
    }
    if reveal == 1:
        resp["access_token"] = tokens.get("access_token") or tokens.get("accessToken")
    return resp
