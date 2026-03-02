import asyncio
import logging
import os
import re
import time
from typing import Optional, Tuple, Dict, Any

import websockets

from engine.feeds.spotware_pb import OpenApiMessages_pb2 as OA
from engine.feeds.spotware_pb import OpenApiCommonMessages_pb2 as CM
from engine.feeds.spotware_pb import OpenApiModelMessages_pb2 as OAM

logger = logging.getLogger("ctrader-spotware")
logger.setLevel(logging.INFO)

# ---------------- ENUM HELPERS ----------------


def _camel_to_upper_snake(name: str) -> str:
    s1 = re.sub(r"(.)([A-Z][a-z]+)", r"\1_\2", name)
    s2 = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", s1)
    return s2.upper()


def _payload_type_value(enum_name: str) -> int:
    return int(OAM.ProtoOAPayloadType.Value(enum_name))


def _payload_type_for(msg) -> int:
    name = msg.DESCRIPTOR.name

    # HARD FIX: enum name mismatch in Spotware protos
    if name == "ProtoOAGetAccountListByAccessTokenReq":
        return _payload_type_value("PROTO_OA_GET_ACCOUNTS_BY_ACCESS_TOKEN_REQ")

    enum_name = _camel_to_upper_snake(name)
    return _payload_type_value(enum_name)


def _norm(s: str) -> str:
    return (s or "").strip().upper()


def _pt(enum_name: str) -> Optional[int]:
    """
    Return payload type value if enum exists, else None.
    (Helps when protos differ slightly across versions.)
    """
    try:
        return _payload_type_value(enum_name)
    except Exception:
        return None


# ---------------- ERRORS ----------------


class SpotwareAPIError(RuntimeError):
    def __init__(self, error_code: str, description: str):
        super().__init__(f"{error_code}: {description}")
        self.error_code = (error_code or "").strip()
        self.description = (description or "").strip()


# ---------------- CLIENT ----------------


class SpotwarePriceClient:
    """
    Spotware (cTrader Open API) price streamer + minimal trading bridge (V1).

    Precision goals:
      - ONE websocket reader loop only (prevents message stealing / race conditions)
      - Stream ticks to controller.on_tick
      - Trade methods wait for broker events via an internal inbox queue
      - Hedging-safe exit: close by positionId (not flatten-by-opposite)

    Env:
      - CTRADER_WS_URL
      - CTRADER_OAUTH_CLIENT_ID or CTRADER_CLIENT_ID
      - CTRADER_CLIENT_SECRET
      - CTRADER_SYMBOL
      - CTRADER_ACCOUNT_ID (optional; will pick first if absent)
      - CTRADER_VOLUME_MULTIPLIER (optional; default 100000)
    """

    TOKENS_PATH = "/opt/astraquant/config/ctrader/tokens.json"
    OAUTH_TOKEN_URL = "https://connect.spotware.com/apps/token"

    def __init__(self, controller):
        self.controller = controller
        self.ws: Optional[websockets.WebSocketClientProtocol] = None
        self.running = True

        self.ws_url = os.getenv("CTRADER_WS_URL")
        self.client_id = os.getenv("CTRADER_OAUTH_CLIENT_ID") or os.getenv("CTRADER_CLIENT_ID")
        self.client_secret = os.getenv("CTRADER_CLIENT_SECRET")
        self.symbol = os.getenv("CTRADER_SYMBOL")

        # ✅ Load BOTH access + refresh (supports both key styles)
        self.access_token, self.refresh_token = self._load_tokens()

        # Optional overrides / execution helpers
        self.account_id_override = (os.getenv("CTRADER_ACCOUNT_ID") or "").strip() or None
        self.volume_multiplier = int(os.getenv("CTRADER_VOLUME_MULTIPLIER", "100000"))

        # computed during handshake
        self._digits: int = 5
        self._scale: int = 10 ** self._digits

        # resolved during handshake
        self.account_id: Optional[int] = None
        self.symbol_id: Optional[int] = None

        # Internal inbox for non-spot events (execution, errors, etc.)
        self._inbox: "asyncio.Queue[CM.ProtoMessage]" = asyncio.Queue(maxsize=5000)

        if not all([self.ws_url, self.client_id, self.client_secret, self.symbol, self.access_token]):
            raise RuntimeError("Missing Spotware config")

    # ---------------- TOKEN IO + REFRESH ----------------

    def _load_tokens(self) -> Tuple[str, str]:
        import json

        with open(self.TOKENS_PATH, "r") as f:
            cfg = json.load(f)

        access = cfg.get("accessToken") or cfg.get("access_token")
        refresh = cfg.get("refreshToken") or cfg.get("refresh_token")

        if not access:
            raise RuntimeError("No access token in tokens.json (expected access_token or accessToken)")
        if not refresh:
            raise RuntimeError("No refresh token in tokens.json (expected refresh_token or refreshToken)")

        return str(access), str(refresh)

    def _write_tokens(self, *, access: str, refresh: str, expires_in: Optional[int] = None, token_type: str = "bearer") -> None:
        import json

        cfg: Dict[str, Any] = {
            "accessToken": access,
            "access_token": access,
            "refreshToken": refresh,
            "refresh_token": refresh,
            "tokenType": token_type,
            "token_type": token_type,
        }
        if expires_in is not None:
            cfg["expiresIn"] = int(expires_in)
            cfg["expires_in"] = int(expires_in)

        with open(self.TOKENS_PATH, "w") as f:
            json.dump(cfg, f, indent=2)

    def _refresh_tokens(self) -> None:
        """
        Refresh access token using refresh token + client credentials.
        Updates tokens.json and updates in-memory tokens.
        """
        import json
        import urllib.parse
        import urllib.request
        import urllib.error

        if not self.client_id or not self.client_secret:
            raise RuntimeError("Missing client_id/client_secret for token refresh")

        logger.warning("🔁 Refreshing Spotware OAuth token (CH_ACCESS_TOKEN_INVALID recovery)")

        form = urllib.parse.urlencode(
            {
                "grant_type": "refresh_token",
                "refresh_token": self.refresh_token,
                "client_id": self.client_id,
                "client_secret": self.client_secret,
            }
        ).encode("utf-8")

        req = urllib.request.Request(
            self.OAUTH_TOKEN_URL,
            data=form,
            method="POST",
            headers={
                "Accept": "application/json",
                "Content-Type": "application/x-www-form-urlencoded",
            },
        )

        try:
            with urllib.request.urlopen(req, timeout=30) as r:
                raw = r.read().decode("utf-8")
                data = json.loads(raw) if raw else {}
        except urllib.error.HTTPError as e:
            body = ""
            try:
                body = e.read().decode("utf-8")
            except Exception:
                pass
            raise RuntimeError(f"Token refresh failed (HTTP {e.code}): {body or e.reason}") from e
        except Exception as e:
            raise RuntimeError(f"Token refresh failed (network/parse): {e}") from e

        new_access = data.get("accessToken") or data.get("access_token")
        new_refresh = data.get("refreshToken") or data.get("refresh_token") or self.refresh_token
        expires_in = data.get("expiresIn") or data.get("expires_in")
        token_type = data.get("tokenType") or data.get("token_type") or "bearer"

        if not new_access:
            raise RuntimeError(f"Token refresh failed: {data}")

        self._write_tokens(
            access=str(new_access),
            refresh=str(new_refresh),
            expires_in=expires_in,
            token_type=str(token_type),
        )

        self.access_token = str(new_access)
        self.refresh_token = str(new_refresh)

        logger.info("✅ Spotware token refreshed and persisted")


    async def _send(self, msg):
        if self.ws is None:
            raise RuntimeError("Websocket not connected")
        env = CM.ProtoMessage(
            payloadType=_payload_type_for(msg),
            payload=msg.SerializeToString(),
        )
        await self.ws.send(env.SerializeToString())

    async def _recv_ws(self) -> CM.ProtoMessage:
        """
        Read directly from websocket.
        IMPORTANT: only the start() loop should call this once streaming begins.
        """
        if self.ws is None:
            raise RuntimeError("Websocket not connected")
        raw = await self.ws.recv()
        if isinstance(raw, str):
            raw = raw.encode()
        env = CM.ProtoMessage()
        env.ParseFromString(raw)
        return env

    async def _wait_for_ws(self, enum_name: str) -> CM.ProtoMessage:
        """
        Handshake-only wait: reads from websocket directly.
        Safe because handshake occurs before streaming loop starts.
        """
        want = _payload_type_value(enum_name)
        err_pt = _payload_type_value("PROTO_OA_ERROR_RES")

        while True:
            env = await self._recv_ws()
            pt = int(env.payloadType)
            logger.info("📩 RX payloadType=%s", pt)

            if pt == want:
                return env

            if pt == err_pt:
                err = OA.ProtoOAErrorRes()
                err.ParseFromString(env.payload)
                raise SpotwareAPIError(getattr(err, "errorCode", ""), getattr(err, "description", ""))

    async def _wait_for_inbox(self, want_pts: "set[int]") -> CM.ProtoMessage:
        """
        Streaming-safe wait: consumes from inbox queue (NOT websocket).
        """
        err_pt = _payload_type_value("PROTO_OA_ERROR_RES")

        while True:
            env = await self._inbox.get()
            pt = int(env.payloadType)

            if pt == err_pt:
                err = OA.ProtoOAErrorRes()
                err.ParseFromString(env.payload)
                raise SpotwareAPIError(getattr(err, "errorCode", ""), getattr(err, "description", ""))

            if pt in want_pts:
                return env
            # else ignore (other non-spot events)

    # ---------------- SYMBOL RESOLUTION ----------------

    async def _resolve_symbol(self, account_id: int) -> Tuple[int, int, str]:
        """
        Returns: (symbol_id, digits, resolved_symbol_name)

        Patch behavior:
          - Dumps all XAU* candidates once to logs
          - Picks symbol by EXACT match ONLY
        """
        await self._send(
            OA.ProtoOASymbolsListReq(
                ctidTraderAccountId=account_id,
                includeArchivedSymbols=False,
            )
        )

        want = _norm(self.symbol)

        while True:
            env = await self._recv_ws()
            pt = int(env.payloadType)

            if pt == _payload_type_value("PROTO_OA_SYMBOLS_LIST_RES"):
                res = OA.ProtoOASymbolsListRes()
                res.ParseFromString(env.payload)

                # One-time dump of XAU-related instruments
                if not hasattr(self, "_xau_dumped"):
                    self._xau_dumped = True
                    matches = []
                    for s in res.symbol:
                        nm = (getattr(s, "symbolName", "") or "")
                        if "XAU" in nm.upper():
                            matches.append(
                                {
                                    "name": nm,
                                    "id": int(getattr(s, "symbolId", 0) or 0),
                                    "digits": int(getattr(s, "digits", 0) or 0),
                                    "pipPosition": int(getattr(s, "pipPosition", 0) or 0),
                                    "desc": getattr(s, "description", "") or "",
                                }
                            )
                    logger.info("XAU candidates: %s", matches)

                # EXACT match ONLY
                pick = None
                for s in res.symbol:
                    name = _norm(getattr(s, "symbolName", ""))
                    if name == want:
                        pick = s
                        break

                if pick is None:
                    sample = [getattr(s, "symbolName", "") for s in res.symbol[:50]]
                    raise RuntimeError(f"Exact CTRADER_SYMBOL '{self.symbol}' not found. Sample symbols: {sample}")

                logger.info(
                    "SYMBOL PICKED name=%s id=%s digits=%s pipPos=%s desc=%s",
                    getattr(pick, "symbolName", None),
                    getattr(pick, "symbolId", None),
                    getattr(pick, "digits", None),
                    getattr(pick, "pipPosition", None),
                    getattr(pick, "description", None),
                )

                sid = int(getattr(pick, "symbolId"))
                digits = int(getattr(pick, "digits", 5))
                resolved_name = getattr(pick, "symbolName", self.symbol)
                return sid, digits, resolved_name

            if pt == _payload_type_value("PROTO_OA_ERROR_RES"):
                err = OA.ProtoOAErrorRes()
                err.ParseFromString(env.payload)
                raise SpotwareAPIError(getattr(err, "errorCode", ""), getattr(err, "description", ""))

    # ---------------- HANDSHAKE + STREAM ----------------

    async def start(self):
        logger.info("🔌 Connecting to Spotware %s", self.ws_url)
        self.ws = await websockets.connect(self.ws_url, max_size=None)

        # 1️⃣ Application auth
        await self._send(
            OA.ProtoOAApplicationAuthReq(
                clientId=self.client_id,
                clientSecret=self.client_secret,
            )
        )
        await self._wait_for_ws("PROTO_OA_APPLICATION_AUTH_RES")
        logger.info("✅ ApplicationAuth OK")

        # 2️⃣ Fetch accounts (auto-refresh once on invalid token)
        for attempt in (1, 2):
            try:
                await self._send(OA.ProtoOAGetAccountListByAccessTokenReq(accessToken=self.access_token))
                env = await self._wait_for_ws("PROTO_OA_GET_ACCOUNTS_BY_ACCESS_TOKEN_RES")
                break
            except SpotwareAPIError as e:
                if e.error_code == "CH_ACCESS_TOKEN_INVALID" and attempt == 1:
                    logger.warning("⚠️ CH_ACCESS_TOKEN_INVALID on GetAccounts — refreshing and retrying")
                    self._refresh_tokens()
                    continue
                raise

        res = OA.ProtoOAGetAccountListByAccessTokenRes()
        res.ParseFromString(env.payload)

        accounts = [int(a.ctidTraderAccountId) for a in res.ctidTraderAccount]
        logger.info("✅ Accounts: %s", accounts)

        if not accounts:
            raise RuntimeError("No trading accounts available")

        # pick account deterministically if override present
        account_id: Optional[int] = None
        if self.account_id_override:
            try:
                wanted = int(self.account_id_override)
                if wanted in accounts:
                    account_id = wanted
                    logger.info("✅ Using CTRADER_ACCOUNT_ID override: %s", wanted)
                else:
                    logger.warning(
                        "⚠️ CTRADER_ACCOUNT_ID=%s not in available accounts=%s. Using first account.",
                        wanted,
                        accounts,
                    )
            except Exception:
                logger.warning("⚠️ Invalid CTRADER_ACCOUNT_ID=%s. Using first account.", self.account_id_override)

        if account_id is None:
            account_id = accounts[0]

        self.account_id = account_id
        # mirror to controller for other modules
        try:
            self.controller.account_id = account_id
        except Exception:
            pass

        # 3️⃣ Account auth (also auto-refresh once if token rotated between steps)
        for attempt in (1, 2):
            try:
                await self._send(
                    OA.ProtoOAAccountAuthReq(
                        ctidTraderAccountId=account_id,
                        accessToken=self.access_token,
                    )
                )
                await self._wait_for_ws("PROTO_OA_ACCOUNT_AUTH_RES")
                break
            except SpotwareAPIError as e:
                if e.error_code == "CH_ACCESS_TOKEN_INVALID" and attempt == 1:
                    logger.warning("⚠️ CH_ACCESS_TOKEN_INVALID on AccountAuth — refreshing and retrying")
                    self._refresh_tokens()
                    continue
                raise

        logger.info("✅ AccountAuth OK (%s)", account_id)

        # 4️⃣ Resolve symbol (id + digits) + subscribe
        symbol_id, digits, resolved_name = await self._resolve_symbol(account_id)
        self.symbol_id = symbol_id
        try:
            self.controller.symbol_id = symbol_id
        except Exception:
            pass

        self._digits = int(digits)
        self._scale = 10 ** int(digits)

        logger.info("🎯 Resolved %s → symbolId=%s digits=%s (%s)", self.symbol, symbol_id, digits, resolved_name)

        await self._send(
            OA.ProtoOASubscribeSpotsReq(
                ctidTraderAccountId=account_id,
                symbolId=[symbol_id],
            )
        )
        await self._wait_for_ws("PROTO_OA_SUBSCRIBE_SPOTS_RES")
        logger.info("📈 Spot stream live")

        # ---------------- STREAM (single ws reader) ----------------
        spot_pt = _payload_type_value("PROTO_OA_SPOT_EVENT")

        while self.running:
            env = await self._recv_ws()
            pt = int(env.payloadType)

            if pt == spot_pt:
                evt = OA.ProtoOASpotEvent()
                evt.ParseFromString(env.payload)

                # digits-aware scaling
                bid = float(evt.bid) / float(self._scale)
                ask = float(evt.ask) / float(self._scale)

                # sanity guards
                if bid <= 0 or ask <= 0 or ask < bid:
                    continue

                spread = ask - bid
                if spread > 50:
                    logger.warning("⚠️ Weird spread=%s bid=%s ask=%s digits=%s", spread, bid, ask, self._digits)
                    continue

                if not hasattr(self, "_dbg_once"):
                    self._dbg_once = True
                    logger.info(
                        "DBG tick rawBid=%s rawAsk=%s digits=%s scale=%s bid=%s ask=%s price=%s spread=%s",
                        evt.bid,
                        evt.ask,
                        self._digits,
                        self._scale,
                        bid,
                        ask,
                        (bid + ask) / 2.0,
                        spread,
                    )

                await self.controller.on_tick(
                    {
                        "symbol": self.symbol,
                        "bid": bid,
                        "ask": ask,
                        "spread": spread,
                        "price": (bid + ask) / 2.0,
                        "ts": time.time(),
                    }
                )
                continue

            # non-spot events go to inbox for order/close waiters
            try:
                self._inbox.put_nowait(env)
            except asyncio.QueueFull:
                # If inbox overflows, something is wrong; drop oldest by draining one.
                try:
                    _ = self._inbox.get_nowait()
                except Exception:
                    pass
                try:
                    self._inbox.put_nowait(env)
                except Exception:
                    pass

    # ---------------- TRADING (ENTRY) ----------------

    async def place_market_order(self, *, symbol: str, side: str, volume: float, client_tag: Optional[str] = None) -> Dict[str, Any]:
        """
        Minimal MARKET order sender for ExecutionManager compatibility.

        Hedging-safe:
          - opens a position (does NOT flatten)
        """
        if self.ws is None:
            raise RuntimeError("Spotware websocket not connected")

        if self.account_id is None or self.symbol_id is None:
            raise RuntimeError("Spotware not ready: missing account_id/symbol_id (handshake not completed)")

        side = (side or "").strip().lower()
        if side not in ("buy", "sell"):
            raise ValueError(f"Invalid side: {side}")

        trade_side = OAM.ProtoOATradeSide.BUY if side == "buy" else OAM.ProtoOATradeSide.SELL

        # Map "stake/volume" to broker volume units. Multiplier default 100000, configurable.
        volume_units = int(float(volume) * float(self.volume_multiplier))
        if volume_units <= 0:
            raise ValueError(
                f"Invalid volume={volume} -> volume_units={volume_units} (multiplier={self.volume_multiplier})"
            )

        msg = OA.ProtoOANewOrderReq(
            ctidTraderAccountId=int(self.account_id),
            symbolId=int(self.symbol_id),
            orderType=OAM.ProtoOAOrderType.MARKET,
            tradeSide=trade_side,
            volume=int(volume_units),
            label=client_tag or "astraquant_v1",
        )

        await self._send(msg)

        want = set()
        exec_pt = _pt("PROTO_OA_EXECUTION_EVENT")
        if exec_pt is not None:
            want.add(exec_pt)

        env = await self._wait_for_inbox(want_pts=want)

        res = OA.ProtoOAExecutionEvent()
        res.ParseFromString(env.payload)

        return {
            "order_id": getattr(res, "orderId", None),
            "position_id": getattr(res, "positionId", None),
            "raw": res,
        }

    # ---------------- TRADING (EXIT / HEDGING-SAFE CLOSE) ----------------

    async def close_position(self, *, position_id: int, volume: Optional[float] = None, client_tag: Optional[str] = None) -> Dict[str, Any]:
        """
        Hedging-safe close: closes by positionId.

        If volume is None -> full close.
        If volume is provided -> partial close (if broker supports it).
        """
        if self.ws is None:
            raise RuntimeError("Spotware websocket not connected")

        if self.account_id is None:
            raise RuntimeError("Spotware not ready: missing account_id")

        # Proto presence guard (some proto sets rename this)
        CloseReqCls = getattr(OA, "ProtoOAClosePositionReq", None)
        if CloseReqCls is None:
            raise RuntimeError("Spotware protos missing ProtoOAClosePositionReq")

        kwargs: Dict[str, Any] = {
            "ctidTraderAccountId": int(self.account_id),
            "positionId": int(position_id),
        }

        if volume is not None:
            vol_units = int(float(volume) * float(self.volume_multiplier))
            if vol_units <= 0:
                raise ValueError(f"Invalid close volume={volume} -> units={vol_units}")
            kwargs["volume"] = int(vol_units)

        if client_tag:
            # Some brokers accept label; ignore if proto doesn't have it
            try:
                kwargs["label"] = client_tag
            except Exception:
                pass

        msg = CloseReqCls(**kwargs)
        await self._send(msg)

        # We accept either EXECUTION_EVENT or CLOSE_POSITION_RES (proto variants)
        want = set()
        exec_pt = _pt("PROTO_OA_EXECUTION_EVENT")
        if exec_pt is not None:
            want.add(exec_pt)

        close_res_pt = _pt("PROTO_OA_CLOSE_POSITION_RES")
        if close_res_pt is not None:
            want.add(close_res_pt)

        env = await self._wait_for_inbox(want_pts=want)

        pt = int(env.payloadType)
        if close_res_pt is not None and pt == close_res_pt:
            # If your protos include this response, parse it.
            CloseResCls = getattr(OA, "ProtoOAClosePositionRes", None)
            if CloseResCls is None:
                return {"ok": True, "reason": "close_position_res_received", "position_id": int(position_id), "raw": env.payload}

            res = CloseResCls()
            res.ParseFromString(env.payload)
            return {"ok": True, "reason": "close_position_res", "position_id": int(position_id), "raw": res}

        # Default: execution event
        res = OA.ProtoOAExecutionEvent()
        res.ParseFromString(env.payload)
        return {
            "ok": True,
            "reason": "execution_event",
            "order_id": getattr(res, "orderId", None),
            "position_id": getattr(res, "positionId", None) or int(position_id),
            "raw": res,
        }
