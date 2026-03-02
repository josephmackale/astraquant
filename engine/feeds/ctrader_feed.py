# File: engine/feeds/ctrader_feed.py
# Drop-in cTrader Spotware Open API tick stream (protobuf envelope)
#
# Robust against proto version differences:
# - Auto-detect payload-type enum by scanning generated modules
# - Auto-detect message class names for GetAccounts by AccessToken
# - Auto-detect AccountAuthReq required id field (accountId vs ctidTraderAccountId)
# - Build protobuf messages via descriptor-aware assignment (avoids constructor TypeError)
# - Coerces per-field types based on proto descriptors
# - Converts websocket text frames to bytes
# - Prints full traceback on reconnect errors

import asyncio
import json
import os
import time
import traceback
from pathlib import Path
from typing import Any, AsyncIterator, Dict, Tuple

import websockets
from google.protobuf.descriptor import FieldDescriptor
from google.protobuf.message import DecodeError, Message

from engine.feeds.spotware_pb import OpenApiCommonMessages_pb2 as C
from engine.feeds.spotware_pb import OpenApiCommonModelMessages_pb2 as CM
from engine.feeds.spotware_pb import OpenApiMessages_pb2 as M
from engine.feeds.spotware_pb import OpenApiModelMessages_pb2 as MM


# ----------------------------
# CONFIG (env)
# ----------------------------
CTRADER_WS_URL = os.getenv("CTRADER_WS_URL", "wss://demo.ctraderapi.com:5035")
TOKENS_FILE = Path("/opt/astraquant/config/ctrader/tokens.json")

# Prefer OAuth-style naming but accept your older envs too
CLIENT_ID = (os.getenv("CTRADER_OAUTH_CLIENT_ID") or os.getenv("CTRADER_CLIENT_ID") or "").strip()
CLIENT_SECRET = (os.getenv("CTRADER_OAUTH_CLIENT_SECRET") or os.getenv("CTRADER_CLIENT_SECRET") or "").strip()

ACCOUNT_ID_RAW = (os.getenv("CTRADER_ACCOUNT_ID") or "").strip()
SYMBOL_NAME = (os.getenv("CTRADER_SYMBOL") or "XAUUSD").strip()

RECONNECT_DELAY = 2.0


# ----------------------------
# Helpers
# ----------------------------
def _load_tokens() -> Dict[str, Any]:
    if not TOKENS_FILE.exists():
        raise RuntimeError(f"Missing tokens file: {TOKENS_FILE}")
    return json.loads(TOKENS_FILE.read_text(encoding="utf-8"))


def _as_bytes(raw: Any) -> bytes:
    if isinstance(raw, bytes):
        return raw
    if isinstance(raw, str):
        return raw.encode("utf-8", errors="ignore")
    return bytes(raw)


def _is_enum_wrapper(obj: Any) -> bool:
    return hasattr(obj, "items") and callable(getattr(obj, "items"))


def _find_payload_enum() -> Tuple[Any, str, Dict[str, int]]:
    """
    Scan generated proto modules for the payload enum used in ProtoMessage.payloadType.
    Picks the enum with strong OA indicators.
    """
    mods = [(C, "C"), (CM, "CM"), (M, "M"), (MM, "MM")]
    candidates = []

    def score(items: Dict[str, int]) -> int:
        keys = list(items.keys())
        s = 0
        hints = [
            "APPLICATION_AUTH",
            "ACCOUNT_AUTH",
            "SYMBOLS_LIST",
            "SUBSCRIBE_SPOTS",
            "SPOT_EVENT",
            "OA_",
            "PROTO_OA_",
        ]
        for h in hints:
            if any(h in k for k in keys):
                s += 10
        if len(keys) > 150:
            s += 5
        if len(keys) > 300:
            s += 5
        return s

    for mod, modname in mods:
        for name, obj in vars(mod).items():
            if not _is_enum_wrapper(obj):
                continue
            try:
                items = dict(obj.items())
            except Exception:
                continue
            candidates.append((score(items), f"{modname}.{name}", obj, items))

    candidates.sort(key=lambda x: x[0], reverse=True)

    for sc, fullname, enum_obj, items in candidates:
        if sc >= 10:
            return enum_obj, fullname, items

    top = [(sc, fullname, list(items.keys())[:15]) for sc, fullname, _, items in candidates[:8]]
    raise RuntimeError(
        "Could not auto-detect Open API payload enum. "
        f"Top candidates (score, name, sample keys): {top}"
    )


PAYLOAD_ENUM, PAYLOAD_ENUM_NAME, PAYLOAD_ITEMS = _find_payload_enum()
print(f"[ctrader_feed] using payload enum: {PAYLOAD_ENUM_NAME} (keys={len(PAYLOAD_ITEMS)})")


def _pt(*names: str) -> int:
    for n in names:
        if n in PAYLOAD_ITEMS:
            return PAYLOAD_ITEMS[n]
    raise RuntimeError(
        f"Missing payloadType in {PAYLOAD_ENUM_NAME}: tried {list(names)}. "
        f"Available sample: {list(PAYLOAD_ITEMS.keys())[:120]}"
    )


def _msg(*names: str):
    """
    Return first existing message class in OpenApiMessages_pb2.
    """
    for n in names:
        if hasattr(M, n):
            return getattr(M, n)
    # include some hints
    sample = [x for x in dir(M) if x.startswith("ProtoOA")][:80]
    raise RuntimeError(f"Missing message class: tried {list(names)}. Sample ProtoOA*: {sample}")


def _coerce_for_field(field: FieldDescriptor, value: Any) -> Any:
    """
    Coerce python values to match protobuf field types.
    """
    if value is None:
        return value

    # repeated fields: expect list-like
    if getattr(field, "label", None) == FieldDescriptor.LABEL_REPEATED:
        if not isinstance(value, (list, tuple)):
            value = [value]
        coerced_list = []

        class _Scalar:
            type = field.type
            enum_type = getattr(field, "enum_type", None)

        for v in value:
            coerced_list.append(_coerce_for_field(_Scalar, v))
        return coerced_list

    t = getattr(field, "type", None)

    if t in (
        FieldDescriptor.TYPE_INT32,
        FieldDescriptor.TYPE_INT64,
        FieldDescriptor.TYPE_UINT32,
        FieldDescriptor.TYPE_UINT64,
        FieldDescriptor.TYPE_SINT32,
        FieldDescriptor.TYPE_SINT64,
        FieldDescriptor.TYPE_FIXED32,
        FieldDescriptor.TYPE_FIXED64,
        FieldDescriptor.TYPE_SFIXED32,
        FieldDescriptor.TYPE_SFIXED64,
    ):
        if isinstance(value, (bytes, bytearray)):
            value = value.decode("utf-8", errors="ignore")
        return int(value)

    if t == FieldDescriptor.TYPE_BOOL:
        if isinstance(value, str):
            return value.strip().lower() in ("1", "true", "yes", "y", "on")
        return bool(value)

    if t == FieldDescriptor.TYPE_STRING:
        if isinstance(value, (bytes, bytearray)):
            return value.decode("utf-8", errors="ignore")
        return str(value)

    if t == FieldDescriptor.TYPE_BYTES:
        if isinstance(value, (bytes, bytearray)):
            return bytes(value)
        return str(value).encode("utf-8")

    if t in (FieldDescriptor.TYPE_FLOAT, FieldDescriptor.TYPE_DOUBLE):
        if isinstance(value, (bytes, bytearray)):
            value = value.decode("utf-8", errors="ignore")
        return float(value)

    if t == FieldDescriptor.TYPE_ENUM:
        if isinstance(value, int):
            return value
        if isinstance(value, str):
            name = value.strip()
            enum_type = getattr(field, "enum_type", None)
            if enum_type and name in enum_type.values_by_name:
                return enum_type.values_by_name[name].number
        return int(value)

    # message fields: pass through (caller should supply message instance)
    return value


def _make_msg(msg_cls, **kwargs) -> Message:
    """
    Construct protobuf message safely by assigning fields with descriptor-aware coercion.
    Unknown fields are ignored (keeps this drop-in resilient).
    """
    msg = msg_cls()
    for k, v in kwargs.items():
        if v is None:
            continue
        f = msg_cls.DESCRIPTOR.fields_by_name.get(k)
        if not f:
            continue
        v2 = _coerce_for_field(f, v)
        if f.label == FieldDescriptor.LABEL_REPEATED:
            getattr(msg, k).extend(v2)
        else:
            setattr(msg, k, v2)
    return msg


# ----------------------------
# Payload types (robust variants)
# ----------------------------
PT_APP_AUTH_REQ = _pt("PROTO_OA_APPLICATION_AUTH_REQ", "OA_APPLICATION_AUTH_REQ", "APPLICATION_AUTH_REQ")
PT_ACC_AUTH_REQ = _pt("PROTO_OA_ACCOUNT_AUTH_REQ", "OA_ACCOUNT_AUTH_REQ", "ACCOUNT_AUTH_REQ")

PT_SYMBOLS_LIST_REQ = _pt("PROTO_OA_SYMBOLS_LIST_REQ", "OA_SYMBOLS_LIST_REQ", "SYMBOLS_LIST_REQ")
PT_SYMBOLS_LIST_RES = _pt("PROTO_OA_SYMBOLS_LIST_RES", "OA_SYMBOLS_LIST_RES", "SYMBOLS_LIST_RES")

PT_SUB_SPOTS_REQ = _pt("PROTO_OA_SUBSCRIBE_SPOTS_REQ", "OA_SUBSCRIBE_SPOTS_REQ", "SUBSCRIBE_SPOTS_REQ")
PT_SPOT_EVENT = _pt("PROTO_OA_SPOT_EVENT", "OA_SPOT_EVENT", "SPOT_EVENT")

# Error payload can vary
PT_ERROR_RES = None
for variants in [
    ("PROTO_OA_ERROR_RES", "OA_ERROR_RES", "ERROR_RES"),
    ("ERROR_RES",),
]:
    try:
        PT_ERROR_RES = _pt(*variants)
        break
    except Exception:
        pass
if PT_ERROR_RES is None:
    raise RuntimeError(f"Could not find ERROR_RES payload type in {PAYLOAD_ENUM_NAME}")

PT_APP_AUTH_RES = _pt("PROTO_OA_APPLICATION_AUTH_RES", "OA_APPLICATION_AUTH_RES", "APPLICATION_AUTH_RES")
PT_ACC_AUTH_RES = _pt("PROTO_OA_ACCOUNT_AUTH_RES", "OA_ACCOUNT_AUTH_RES", "ACCOUNT_AUTH_RES")

# Get accounts (payload types vary by proto version)
PT_GET_ACCOUNTS_REQ = None
PT_GET_ACCOUNTS_RES = None
for variants in [
    ("PROTO_OA_GET_ACCOUNTS_BY_ACCESS_TOKEN_REQ", "PROTO_OA_GET_ACCOUNT_LIST_BY_ACCESS_TOKEN_REQ", "OA_GET_ACCOUNTS_BY_ACCESS_TOKEN_REQ"),
    ("PROTO_OA_GET_ACCOUNT_LIST_BY_ACCESS_TOKEN_REQ",),
]:
    try:
        PT_GET_ACCOUNTS_REQ = _pt(*variants)
        break
    except Exception:
        pass

for variants in [
    ("PROTO_OA_GET_ACCOUNTS_BY_ACCESS_TOKEN_RES", "PROTO_OA_GET_ACCOUNT_LIST_BY_ACCESS_TOKEN_RES", "OA_GET_ACCOUNTS_BY_ACCESS_TOKEN_RES"),
    ("PROTO_OA_GET_ACCOUNT_LIST_BY_ACCESS_TOKEN_RES",),
]:
    try:
        PT_GET_ACCOUNTS_RES = _pt(*variants)
        break
    except Exception:
        pass

if PT_GET_ACCOUNTS_REQ is None or PT_GET_ACCOUNTS_RES is None:
    raise RuntimeError("Could not find GET_ACCOUNTS_BY_ACCESS_TOKEN payload types in payload enum")

# Message classes for get accounts (also vary)
GET_ACCOUNTS_REQ_CLS = _msg(
    "ProtoOAGetAccountListByAccessTokenReq",
    "ProtoOAGetAccountsByAccessTokenReq",
)
GET_ACCOUNTS_RES_CLS = _msg(
    "ProtoOAGetAccountListByAccessTokenRes",
    "ProtoOAGetAccountsByAccessTokenRes",
)

# Symbols + spots message classes (stable)
SYMBOLS_LIST_REQ_CLS = _msg("ProtoOASymbolsListReq")
SYMBOLS_LIST_RES_CLS = _msg("ProtoOASymbolsListRes")
SUB_SPOTS_REQ_CLS = _msg("ProtoOASubscribeSpotsReq")
SPOT_EVENT_CLS = _msg("ProtoOASpotEvent")

# Auth message classes
APP_AUTH_REQ_CLS = _msg("ProtoOAApplicationAuthReq")
ACC_AUTH_REQ_CLS = _msg("ProtoOAAccountAuthReq")

# Error response message class (OA error)
OA_ERROR_RES_CLS = None
for name in ("ProtoOAErrorRes",):
    if hasattr(M, name):
        OA_ERROR_RES_CLS = getattr(M, name)
        break


async def _wait_for_payload(ws, expected_type: int):
    while True:
        env = await _recv_env(ws)

        if env.payloadType == PT_ERROR_RES:
            raise RuntimeError(_decode_error_payload(env.payload))

        if env.payloadType == expected_type:
            return env


async def _resolve_ctid_trader_account_id(ws, access_token: str) -> int:
    """
    Your proto returns:
      ProtoOAGetAccountListByAccessTokenRes.ctidTraderAccount[] with items that contain ctidTraderAccountId.
    So CTRADER_ACCOUNT_ID must be the CTID trader account id.
    """
    req = _make_msg(
        M.ProtoOAGetAccountListByAccessTokenReq,
        accessToken=access_token,
    )
    await _send(ws, PT_GET_ACCOUNTS_REQ, req)

    target = int(ACCOUNT_ID_RAW)

    while True:
        env = await _recv_env(ws)

        if env.payloadType == PT_ERROR_RES:
            raise RuntimeError(_decode_error_payload(env.payload))

        if env.payloadType != PT_GET_ACCOUNTS_RES:
            continue

        res = M.ProtoOAGetAccountListByAccessTokenRes.FromString(env.payload)

        # Your repeated list field is "ctidTraderAccount"
        items = getattr(res, "ctidTraderAccount", None)
        if items is None:
            # fallback: find first repeated message field
            items = None
            for f in res.DESCRIPTOR.fields:
                if f.label == FieldDescriptor.LABEL_REPEATED and f.type == FieldDescriptor.TYPE_MESSAGE:
                    items = getattr(res, f.name)
                    break
            if items is None:
                raise RuntimeError("GetAccountListByAccessTokenRes has no repeated account list field")

        ids = []
        for acc in items:
            ctid = int(getattr(acc, "ctidTraderAccountId", 0) or 0)
            if ctid:
                ids.append(ctid)
            if ctid == target:
                return ctid

        # If env is wrong but only one account exists, auto-pick it
        if len(ids) == 1:
            print(f"[ctrader_feed] ⚠️ CTRADER_ACCOUNT_ID={ACCOUNT_ID_RAW} not found; auto-using {ids[0]}")
            return ids[0]

        raise RuntimeError(
            f"CTID Trader Account ID {ACCOUNT_ID_RAW} not found in access-token account list (available={ids})"
        )


# ----------------------------
# ProtoMessage I/O
# ----------------------------
async def _send(ws, payload_type: int, msg: Message) -> None:
    env = C.ProtoMessage(payloadType=payload_type, payload=msg.SerializeToString())
    await ws.send(env.SerializeToString())


async def _recv_env(ws) -> C.ProtoMessage:
    raw = await ws.recv()
    data = _as_bytes(raw)
    return C.ProtoMessage.FromString(data)


def _decode_error_payload(payload: bytes) -> str:
    # Try OA error first, then common error
    if OA_ERROR_RES_CLS is not None:
        try:
            err = OA_ERROR_RES_CLS.FromString(payload)
            return f"OA_ERROR_RES: {err}"
        except Exception:
            pass
    try:
        err = C.ProtoErrorRes.FromString(payload)
        return f"ERROR_RES: {err}"
    except Exception:
        pass
    return "ERROR_RES: (unable to decode error payload)"


# ----------------------------
# Protocol steps
# ----------------------------
def _account_auth_id_field() -> str:
    """
    Detect which id field AccountAuthReq expects.
    Common variants:
      - ctidTraderAccountId
      - accountId
    """
    fields = [f.name for f in ACC_AUTH_REQ_CLS.DESCRIPTOR.fields]
    if "ctidTraderAccountId" in fields:
        return "ctidTraderAccountId"
    if "accountId" in fields:
        return "accountId"
    # fallback: choose first int64-ish field that contains 'id'
    for f in ACC_AUTH_REQ_CLS.DESCRIPTOR.fields:
        if "id" in f.name.lower() and f.type in (FieldDescriptor.TYPE_INT32, FieldDescriptor.TYPE_INT64, FieldDescriptor.TYPE_UINT32, FieldDescriptor.TYPE_UINT64):
            return f.name
    raise RuntimeError(f"Could not determine AccountAuthReq id field. Fields: {fields}")


async def _auth(ws, access_token: str) -> None:
    if not CLIENT_ID:
        raise RuntimeError("CTRADER_CLIENT_ID (or CTRADER_OAUTH_CLIENT_ID) is missing")
    if not CLIENT_SECRET:
        raise RuntimeError("CTRADER_CLIENT_SECRET (or CTRADER_OAUTH_CLIENT_SECRET) is missing")
    if not ACCOUNT_ID_RAW:
        raise RuntimeError("CTRADER_ACCOUNT_ID is missing")

    # 1) Application auth
    app_req = _make_msg(
        APP_AUTH_REQ_CLS,
        clientId=CLIENT_ID,
        clientSecret=CLIENT_SECRET,
    )
    await _send(ws, PT_APP_AUTH_REQ, app_req)

    # Wait for application auth confirmation
    await _wait_for_payload(ws, PT_APP_AUTH_RES)

    # 2) Resolve CTID trader account id (after app auth)
    ctid_trader_account_id = await _resolve_ctid_trader_account_id(ws, access_token)

    # 3) Account auth
    id_field = _account_auth_id_field()

    kwargs = {
        "accessToken": access_token,
    }
    # Put the correct id field in
    if id_field == "ctidTraderAccountId":
        kwargs[id_field] = ctid_trader_account_id
    else:
        # typically accountId
        kwargs[id_field] = ACCOUNT_ID_RAW

    acc_req = _make_msg(ACC_AUTH_REQ_CLS, **kwargs)
    await _send(ws, PT_ACC_AUTH_REQ, acc_req)

    # Wait for account auth confirmation
    await _wait_for_payload(ws, PT_ACC_AUTH_RES)


async def _resolve_symbol_id(ws) -> int:
    await _send(ws, PT_SYMBOLS_LIST_REQ, _make_msg(SYMBOLS_LIST_REQ_CLS))

    while True:
        env = await _recv_env(ws)

        if env.payloadType == PT_ERROR_RES:
            raise RuntimeError(_decode_error_payload(env.payload))

        if env.payloadType != PT_SYMBOLS_LIST_RES:
            continue

        res = SYMBOLS_LIST_RES_CLS.FromString(env.payload)

        # list might be "symbol" but keep it flexible
        sym_list = None
        for cand in ("symbol", "symbols"):
            if hasattr(res, cand):
                sym_list = getattr(res, cand)
                break
        if sym_list is None:
            # find repeated message list
            for f in res.DESCRIPTOR.fields:
                if f.label == FieldDescriptor.LABEL_REPEATED and f.type == FieldDescriptor.TYPE_MESSAGE:
                    sym_list = getattr(res, f.name)
                    break

        if sym_list is None:
            raise RuntimeError("SymbolsListRes has no repeated symbol list field")

        for s in sym_list:
            name = getattr(s, "symbolName", None) or getattr(s, "name", None) or ""
            if str(name) == SYMBOL_NAME:
                sid = getattr(s, "symbolId", None) or getattr(s, "id", None)
                return int(sid)

        raise RuntimeError(f"Symbol not found in symbols list: {SYMBOL_NAME}")


# ----------------------------
# Public async generator
# ----------------------------
async def ctrader_ticks() -> AsyncIterator[Dict[str, Any]]:
    tokens = _load_tokens()
    access_token = tokens.get("access_token") or tokens.get("accessToken")
    if not access_token:
        raise RuntimeError("No access token in tokens.json (expected access_token or accessToken)")

    while True:
        try:
            async with websockets.connect(
                CTRADER_WS_URL,
                ping_interval=20,
                ping_timeout=20,
                close_timeout=5,
                max_queue=2048,
            ) as ws:
                await _auth(ws, access_token)

                symbol_id = await _resolve_symbol_id(ws)

                sub_req = _make_msg(SUB_SPOTS_REQ_CLS, symbolId=[symbol_id])
                await _send(ws, PT_SUB_SPOTS_REQ, sub_req)

                while True:
                    env = await _recv_env(ws)

                    if env.payloadType == PT_ERROR_RES:
                        raise RuntimeError(_decode_error_payload(env.payload))

                    if env.payloadType != PT_SPOT_EVENT:
                        continue

                    try:
                        ev = SPOT_EVENT_CLS.FromString(env.payload)
                    except DecodeError:
                        continue

                    bid = getattr(ev, "bid", 0) or 0
                    ask = getattr(ev, "ask", 0) or 0
                    if bid <= 0 or ask <= 0:
                        continue

                    price = (float(bid) + float(ask)) / 2.0
                    yield {"symbol": SYMBOL_NAME, "price": price, "ts": time.time()}

        except Exception as e:
            print(f"[ctrader_feed] error: {repr(e)} — reconnecting...")
            print(traceback.format_exc())
            await asyncio.sleep(RECONNECT_DELAY)


# ----------------------------
# CLI run (optional)
# ----------------------------
if __name__ == "__main__":
    async def _main():
        n = 0
        async for t in ctrader_ticks():
            print(t)
            n += 1
            if n >= 50:
                break

    asyncio.run(_main())
