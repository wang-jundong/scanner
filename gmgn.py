"""GMGN Solana client (Cloudflare-aware via curl_cffi)."""

from __future__ import annotations

import base64
import json
import os
import threading
import time
import uuid
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from curl_cffi import requests


BASE = "https://gmgn.ai"
OPENAPI_BASE = "https://openapi.gmgn.ai"
DEFAULT_HEADERS = {
    "Accept": "application/json",
    "Referer": "https://gmgn.ai/",
    "Origin": "https://gmgn.ai",
}

# Optional Bearer token for /pf/api profit_stat + holdings (UI winrate for any wallet)
_ACCESS_TOKEN: str | None = None
_DEVICE_ID: str | None = None
_CLIENT_ID: str | None = None

# Official GMGN OpenAPI key (X-APIKEY) — no JWT expiry; preferred for enrich
_API_KEY: str | None = None

# Free plan leaky bucket: rate/capacity = 5/5 (Weight 5 ≈ 5 weight/sec).
# Plus=20/20, Pro=50/50. Stay slightly under to avoid RATE_LIMIT_BANNED.
_OPENAPI_LOCK = threading.Lock()
_OPENAPI_WEIGHT_RATE = 4.5  # weight units refilled per second
_OPENAPI_WEIGHT_CAP = 5.0
_OPENAPI_WEIGHT_TOKENS = 5.0
_OPENAPI_WEIGHT_UPDATED = 0.0
_OPENAPI_BANNED_UNTIL = 0.0

# Route weight (from GMGN Free/Plus plan docs). Unknown paths default to 3.
_OPENAPI_ROUTE_WEIGHT = {
    "/v1/user/wallet_activity": 3,
    "/v1/user/wallet_stats": 3,
    "/v1/user/wallet_profits": 3,
    "/v1/user/wallet_holdings": 2,
    "/v1/user/created_tokens": 2,
    "/v1/user/info": 2,
    "/v1/user/wallet_token_balance": 2,
    "/v1/market/rank": 2,
    "/v1/market/token_top_traders": 2,
}

# Serialize common_stat — CF 403s hard when many workers hit it at once
_COMMON_STAT_LOCK = threading.Lock()
_COMMON_STAT_LAST = 0.0
_COMMON_STAT_MIN_INTERVAL = 0.4


class GmgnError(RuntimeError):
    pass


class GmgnAuthError(GmgnError):
    """Bearer token missing, expired, or rejected (HTTP 401/403)."""


class GmgnRateLimitError(GmgnError):
    """Cloudflare / HTTP 429–403 rate limit (JWT still valid)."""


def _decode_jwt_payload(token: str) -> dict[str, Any]:
    try:
        part = token.split(".")[1]
        pad = "=" * ((4 - len(part) % 4) % 4)
        data = json.loads(base64.urlsafe_b64decode(part + pad))
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def set_access_token(token: str | None) -> None:
    """Set GMGN web access token (Bearer). Empty/None clears it."""
    global _ACCESS_TOKEN, _DEVICE_ID, _CLIENT_ID
    if token:
        token = token.strip()
        if token.lower().startswith("bearer "):
            token = token[7:].strip()
    _ACCESS_TOKEN = token or None
    _DEVICE_ID = None
    _CLIENT_ID = None
    if _ACCESS_TOKEN:
        claims = _decode_jwt_payload(_ACCESS_TOKEN)
        nested = claims.get("data") if isinstance(claims.get("data"), dict) else {}
        _DEVICE_ID = nested.get("device_id") or claims.get("device_id")
        _CLIENT_ID = nested.get("client_id") or claims.get("client_id")


def get_access_token() -> str | None:
    return _ACCESS_TOKEN


def set_api_key(key: str | None) -> None:
    global _API_KEY
    if key:
        key = key.strip()
        if key.lower().startswith("x-apikey:"):
            key = key.split(":", 1)[1].strip()
    _API_KEY = key or None


def get_api_key() -> str | None:
    return _API_KEY


def has_gmgn_auth() -> bool:
    """True if OpenAPI key or usable Bearer is available for wallet stats."""
    return bool(_API_KEY) or _use_bearer()


def _use_bearer() -> bool:
    """True when a *fresh* JWT + device/client claims are ready.

    Expired JWTs must not be sent — they cause HTTP 401 and abort hub/prescreen.
    """
    if not (_ACCESS_TOKEN and _DEVICE_ID and _CLIENT_ID):
        return False
    left = token_seconds_left(_ACCESS_TOKEN)
    return left is not None and left >= 60


def token_seconds_left(token: str | None = None) -> float | None:
    """JWT exp - now (UTC seconds). None if exp claim missing/unreadable.

    When only an OpenAPI key is configured (no JWT), returns a large positive
    value so callers that gate on TTL do not pause for a Bearer paste.
    """
    raw = token if token is not None else _ACCESS_TOKEN
    if not raw:
        if _API_KEY and token is None:
            return 86400.0 * 365
        return None
    claims = _decode_jwt_payload(raw)
    exp = claims.get("exp")
    if exp is None:
        return None
    try:
        return float(exp) - time.time()
    except (TypeError, ValueError):
        return None


def load_api_key(
    *,
    config_key: str | None = None,
    key_file: str | Path | None = None,
) -> str | None:
    """
    Resolve OpenAPI key from:
      1) config gmgn.api_key
      2) GMGN_API_KEY env
      3) key file (default: .gmgn_api_key)
    """
    if config_key and str(config_key).strip():
        set_api_key(str(config_key))
        return _API_KEY
    env = os.environ.get("GMGN_API_KEY")
    if env and env.strip():
        set_api_key(env)
        return _API_KEY
    path = Path(key_file) if key_file else Path(__file__).resolve().parent / ".gmgn_api_key"
    if path.is_file():
        text = path.read_text().strip()
        if text:
            set_api_key(text.splitlines()[0].strip())
            return _API_KEY
    set_api_key(None)
    return None


def load_access_token(
    *,
    config_token: str | None = None,
    token_file: str | Path | None = None,
) -> str | None:
    """
    Resolve access token from (highest priority first):
      1) explicit config value
      2) GMGN_ACCESS_TOKEN env
      3) token file (default: .gmgn_token next to this package)
    """
    if config_token and str(config_token).strip():
        set_access_token(str(config_token))
        return _ACCESS_TOKEN
    env = os.environ.get("GMGN_ACCESS_TOKEN")
    if env and env.strip():
        set_access_token(env)
        return _ACCESS_TOKEN
    path = Path(token_file) if token_file else Path(__file__).resolve().parent / ".gmgn_token"
    if path.is_file():
        text = path.read_text().strip()
        if text:
            set_access_token(text.splitlines()[0].strip())
            return _ACCESS_TOKEN
    set_access_token(None)
    return None


def _headers(*, auth: bool = False) -> dict[str, str]:
    headers = dict(DEFAULT_HEADERS)
    if auth:
        if not _ACCESS_TOKEN:
            raise GmgnError("GMGN access token required (set gmgn.access_token / GMGN_ACCESS_TOKEN / .gmgn_token)")
        headers["Authorization"] = f"Bearer {_ACCESS_TOKEN}"
    return headers


def _with_auth_params(path: str) -> str:
    """GMGN /pf/api requires device_id+client_id from the JWT or returns 401 param error."""
    if not (_DEVICE_ID and _CLIENT_ID):
        raise GmgnError(
            "GMGN token missing device_id/client_id claims — copy a fresh Authorization Bearer from DevTools"
        )
    parts = urlsplit(path)
    q = dict(parse_qsl(parts.query, keep_blank_values=True))
    q.setdefault("device_id", str(_DEVICE_ID))
    q.setdefault("client_id", str(_CLIENT_ID))
    return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(q), parts.fragment))


def _request_json(
    path: str,
    *,
    retries: int = 3,
    auth: bool = False,
    method: str = "GET",
    data: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if auth:
        path = _with_auth_params(path)
    url = BASE + path
    last_err: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            if method.upper() == "GET":
                resp = requests.get(
                    url,
                    impersonate="chrome131",
                    headers=_headers(auth=auth),
                    timeout=30,
                )
            else:
                resp = requests.post(
                    url,
                    impersonate="chrome131",
                    headers=_headers(auth=auth),
                    json=data or {},
                    timeout=30,
                )
            if resp.status_code in (429, 500, 502, 503, 504):
                if attempt < retries:
                    time.sleep(min(2**attempt + 1, 20))
                    continue
                raise GmgnRateLimitError(f"HTTP {resp.status_code} for {path} (rate-limit)")
            # Auth failures: 401 always; 403 only when JWT is dead/near-dead or on
            # /pf/api (Bearer-gated). Healthy-TTL 403s are often Cloudflare/rate-limit.
            left = token_seconds_left()
            if auth and resp.status_code == 401:
                raise GmgnAuthError(f"HTTP 401 for {path} (auth/token issue)")
            if auth and resp.status_code == 403:
                # Healthy JWT + 403 is usually CF/rate-limit (incl. /pf/api).
                # Only treat as hard auth when the token is dead/near-dead.
                dead = left is None or left < 60
                if dead:
                    raise GmgnAuthError(f"HTTP 403 for {path} (auth/token issue)")
                if attempt < retries:
                    time.sleep(min(2**attempt + 2, 25))
                    continue
                raise GmgnRateLimitError(
                    f"HTTP 403 for {path} (rate-limit/CF; TTL≈{left})"
                )
            if auth and resp.status_code == 400:
                if left is not None and left < 30:
                    raise GmgnAuthError(f"HTTP 400 for {path} (token expired/near-expiry)")
            if resp.status_code in (401, 403):
                if resp.status_code == 403 and attempt < retries:
                    time.sleep(min(2**attempt + 2, 25))
                    continue
                if resp.status_code == 403:
                    raise GmgnRateLimitError(f"HTTP 403 for {path} (rate-limit/CF)")
                raise GmgnError(f"HTTP {resp.status_code} for {path}")
            if resp.status_code != 200:
                raise GmgnError(f"HTTP {resp.status_code} for {path}")
            if not resp.text or not resp.text.strip().startswith(("{", "[")):
                raise GmgnError(f"Non-JSON response for {path}")
            payload = resp.json()
            code = payload.get("code")
            if code not in (0, None, "0"):
                raise GmgnError(
                    f"GMGN error on {path}: {payload.get('msg') or payload.get('message') or payload}"
                )
            return payload
        except GmgnAuthError:
            raise
        except Exception as exc:  # noqa: BLE001
            last_err = exc
            if attempt < retries:
                time.sleep(2**attempt)
                continue
            raise GmgnError(f"Failed {path}: {exc}") from exc
    raise GmgnError(f"Failed {path}: {last_err}")


def configure_openapi_rate_limit(
    *,
    rate: float = 4.5,
    capacity: float = 5.0,
) -> None:
    """Tune leaky bucket to plan (Free 5/5, Plus 20/20, Pro 50/50)."""
    global _OPENAPI_WEIGHT_RATE, _OPENAPI_WEIGHT_CAP, _OPENAPI_WEIGHT_TOKENS
    _OPENAPI_WEIGHT_RATE = max(0.5, float(rate))
    _OPENAPI_WEIGHT_CAP = max(1.0, float(capacity))
    _OPENAPI_WEIGHT_TOKENS = _OPENAPI_WEIGHT_CAP


def _openapi_route_weight(path: str) -> float:
    base = path.split("?")[0]
    return float(_OPENAPI_ROUTE_WEIGHT.get(base, 3))


def _openapi_refill_locked(now: float) -> None:
    global _OPENAPI_WEIGHT_TOKENS, _OPENAPI_WEIGHT_UPDATED
    if _OPENAPI_WEIGHT_UPDATED <= 0:
        _OPENAPI_WEIGHT_UPDATED = now
        return
    elapsed = max(0.0, now - _OPENAPI_WEIGHT_UPDATED)
    _OPENAPI_WEIGHT_TOKENS = min(
        _OPENAPI_WEIGHT_CAP,
        _OPENAPI_WEIGHT_TOKENS + elapsed * _OPENAPI_WEIGHT_RATE,
    )
    _OPENAPI_WEIGHT_UPDATED = now


def _openapi_acquire_weight(path: str) -> None:
    """Block until the leaky bucket can afford this route's weight."""
    global _OPENAPI_WEIGHT_TOKENS, _OPENAPI_BANNED_UNTIL
    cost = _openapi_route_weight(path)
    while True:
        with _OPENAPI_LOCK:
            now = time.time()
            if now < _OPENAPI_BANNED_UNTIL:
                wait = _OPENAPI_BANNED_UNTIL - now
            else:
                _openapi_refill_locked(now)
                if _OPENAPI_WEIGHT_TOKENS >= cost:
                    _OPENAPI_WEIGHT_TOKENS -= cost
                    return
                need = cost - _OPENAPI_WEIGHT_TOKENS
                wait = need / max(_OPENAPI_WEIGHT_RATE, 0.1)
        time.sleep(min(max(wait, 0.05), 30.0))


def _openapi_note_rate_limit(resp: Any, payload: dict[str, Any] | None = None) -> float:
    """Record ban/reset; return seconds to sleep. Do not spam during ban."""
    global _OPENAPI_BANNED_UNTIL, _OPENAPI_WEIGHT_TOKENS
    now = time.time()
    reset_at: float | None = None
    if payload and payload.get("reset_at") not in (None, ""):
        try:
            reset_at = float(payload["reset_at"])
        except (TypeError, ValueError):
            reset_at = None
    if reset_at is None:
        hdr = None
        try:
            hdr = resp.headers.get("X-RateLimit-Reset") or resp.headers.get(
                "x-ratelimit-reset"
            )
        except Exception:
            hdr = None
        if hdr:
            try:
                reset_at = float(hdr)
            except (TypeError, ValueError):
                reset_at = None
    # Default short cool-down if no reset hint (avoid extending ban by spamming)
    sleep_s = 8.0
    if reset_at is not None and reset_at > now:
        sleep_s = min(reset_at - now + 0.5, 300.0)
        _OPENAPI_BANNED_UNTIL = max(_OPENAPI_BANNED_UNTIL, reset_at + 0.5)
    else:
        _OPENAPI_BANNED_UNTIL = max(_OPENAPI_BANNED_UNTIL, now + sleep_s)
    with _OPENAPI_LOCK:
        _OPENAPI_WEIGHT_TOKENS = 0.0
    return sleep_s


def _openapi_request_json(
    path: str,
    *,
    params: dict[str, Any] | None = None,
    retries: int = 4,
) -> dict[str, Any]:
    """Official OpenAPI (X-APIKEY). Fresh client_id per attempt — replays are rejected.

    Uses a Free-plan leaky bucket (Weight 5 ≈ 5 weight/sec). wallet_stats /
    wallet_activity cost 3 each → ~1.5 calls/sec sustained. On 429, wait for
    reset_at / X-RateLimit-Reset instead of hammering (which extends the ban).
    """
    if not _API_KEY:
        raise GmgnAuthError("GMGN API key required (set GMGN_API_KEY / .gmgn_api_key)")
    last_err: Exception | None = None
    for attempt in range(1, retries + 1):
        q = dict(params or {})
        q["timestamp"] = int(time.time())
        q["client_id"] = str(uuid.uuid4())
        parts = urlsplit(path)
        existing = dict(parse_qsl(parts.query, keep_blank_values=True))
        existing.update({k: str(v) for k, v in q.items() if v is not None})
        url = OPENAPI_BASE + urlunsplit(
            ("", "", parts.path, urlencode(existing), "")
        )
        headers = {
            "Accept": "application/json",
            "X-APIKEY": _API_KEY,
        }
        try:
            _openapi_acquire_weight(parts.path)
            resp = requests.get(
                url, impersonate="chrome131", headers=headers, timeout=45
            )
            if resp.status_code in (429, 500, 502, 503, 504):
                payload = None
                try:
                    if resp.text and resp.text.strip().startswith("{"):
                        payload = resp.json()
                except Exception:
                    payload = None
                sleep_s = _openapi_note_rate_limit(resp, payload)
                err = (payload or {}).get("error") if payload else ""
                if attempt < retries and resp.status_code == 429:
                    # Spamming during ban extends it — wait full reset once
                    time.sleep(sleep_s)
                    continue
                raise GmgnRateLimitError(
                    f"HTTP {resp.status_code} for {path}"
                    + (f" ({err})" if err else "")
                )
            if resp.status_code in (401, 403):
                body = (resp.text or "")[:300]
                if "PLAN_UPGRADE_REQUIRED" in body:
                    raise GmgnAuthError(
                        f"HTTP {resp.status_code} for {path}: plan cannot call this "
                        f"route — upgrade at https://gmgn.ai/ai?tab=paid_plans"
                    )
                if "REPLAY" in body.upper() and attempt < retries:
                    time.sleep(0.5)
                    continue
                if resp.status_code == 403 and attempt < retries:
                    time.sleep(min(3 * attempt, 20))
                    continue
                raise GmgnAuthError(f"HTTP {resp.status_code} for {path}: {body}")
            if resp.status_code != 200:
                raise GmgnError(
                    f"HTTP {resp.status_code} for {path}: {(resp.text or '')[:200]}"
                )
            if not resp.text or not resp.text.strip().startswith(("{", "[")):
                raise GmgnError(f"Non-JSON response for {path}")
            payload = resp.json()
            code = payload.get("code")
            if code not in (0, None, "0"):
                err = str(payload.get("error") or "")
                msg = str(payload.get("message") or "")
                if ("REPLAY" in err.upper() or "REPLAY" in msg.upper()) and attempt < retries:
                    time.sleep(0.5)
                    continue
                if (
                    "RATE" in err.upper()
                    or "429" in msg
                    or "TOO_MANY" in err.upper()
                ) and attempt < retries:
                    sleep_s = _openapi_note_rate_limit(resp, payload)
                    time.sleep(sleep_s)
                    continue
                raise GmgnError(
                    f"GMGN OpenAPI error on {path}: "
                    f"{payload.get('message') or payload.get('msg') or payload}"
                )
            return payload
        except (GmgnAuthError, GmgnRateLimitError):
            raise
        except Exception as exc:  # noqa: BLE001
            last_err = exc
            if attempt < retries:
                time.sleep(2**attempt)
                continue
            raise GmgnError(f"Failed OpenAPI {path}: {exc}") from exc
    raise GmgnError(f"Failed OpenAPI {path}: {last_err}")


def fetch_wallet_stats_openapi(
    address: str,
    *,
    chain: str = "sol",
    period: str = "7d",
) -> dict[str, Any]:
    """
    Official wallet stats: winrate, pnl, buys/sells, tags, created_token_count.

    Prefer this over browser Bearer profit_stat when an API key is configured.
    """
    payload = _openapi_request_json(
        "/v1/user/wallet_stats",
        params={
            "chain": chain,
            "wallet_address": address,
            "period": period,
        },
    )
    data = payload.get("data") or {}
    return data if isinstance(data, dict) else {}


def _extract_rank(payload: dict[str, Any]) -> list[dict[str, Any]]:
    data = payload.get("data") or {}
    for key in ("rank", "wallets", "list"):
        value = data.get(key)
        if isinstance(value, list):
            return value
    if isinstance(data, list):
        return data
    return []


def fetch_wallet_rank(
    *,
    chain: str = "sol",
    period: str = "7d",
    tag: str = "smart_degen",
    orderby: str = "pnl_7d",
    direction: str = "desc",
    limit: int = 100,
    min_winrate_7d: float | None = None,
    min_pnl_7d: float | None = None,
) -> list[dict[str, Any]]:
    path = (
        f"/defi/quotation/v1/rank/{chain}/wallets/{period}"
        f"?tag={tag}&orderby={orderby}&direction={direction}&limit={limit}"
    )
    if min_winrate_7d is not None:
        path += f"&min_winrate_7d={min_winrate_7d}"
    if min_pnl_7d is not None:
        path += f"&min_pnl_7d={min_pnl_7d}"
    return _extract_rank(_request_json(path, auth=_use_bearer()))


def collect_wallets(
    *,
    chain: str,
    period: str,
    tags: Iterable[str],
    orderbys: Iterable[str],
    delay_sec: float = 0.35,
    min_winrate_7d: float | None = None,
    min_pnl_7d: float | None = None,
) -> dict[str, dict[str, Any]]:
    """Fetch multiple leaderboard slices and de-dupe by wallet address.

    Prefer rows that include GMGN's authoritative ``winrate_7d`` (UI winrate).
    """
    wallets: dict[str, dict[str, Any]] = {}
    tag_list = list(tags)
    order_list = list(orderbys)
    total = max(len(tag_list) * len(order_list), 1)
    done = 0

    for tag in tag_list:
        for orderby in order_list:
            done += 1
            try:
                rows = fetch_wallet_rank(
                    chain=chain,
                    period=period,
                    tag=tag,
                    orderby=orderby,
                    min_winrate_7d=min_winrate_7d,
                    min_pnl_7d=min_pnl_7d,
                )
            except GmgnError as exc:
                print(f"[leaderboard {done}/{total}] tag={tag} orderby={orderby} FAIL {exc}")
                rows = []
            print(f"[leaderboard {done}/{total}] tag={tag} orderby={orderby} n={len(rows)}")
            for row in rows:
                addr = row.get("wallet_address") or row.get("address")
                if not addr:
                    continue
                prev = wallets.get(addr)
                if prev is None or _row_score(row) >= _row_score(prev):
                    wallets[addr] = row
            if delay_sec > 0:
                time.sleep(delay_sec)

    return wallets


def _row_score(row: dict[str, Any]) -> float:
    """Prefer rows that carry real winrate_7d, then activity/profit."""
    try:
        has_wr = 1e12 if row.get("winrate_7d") not in (None, "") else 0.0
        return (
            has_wr
            + float(row.get("txs_7d") or 0)
            + float(row.get("realized_profit_7d") or 0)
        )
    except (TypeError, ValueError):
        return 0.0


def fetch_wallet_detail(address: str, *, chain: str = "sol") -> dict[str, Any]:
    """walletNew-style pnl/txs. Prefer OpenAPI wallet_stats when API key is set."""
    if _API_KEY:
        stats = fetch_wallet_stats_openapi(address, chain=chain, period="7d")
        common = stats.get("common") if isinstance(stats.get("common"), dict) else {}
        return {
            "pnl_7d": stats.get("realized_profit_pnl") or stats.get("pnl"),
            "buy_7d": stats.get("buy"),
            "sell_7d": stats.get("sell"),
            "realized_profit_7d": stats.get("realized_profit"),
            "tags": common.get("tags") or [],
            "name": common.get("name") or "",
            "twitter_username": common.get("twitter_username"),
            "last_active_timestamp": stats.get("last_timestamp"),
            "created_token_count": common.get("created_token_count"),
            "_openapi_stats": stats,
        }
    path = f"/defi/quotation/v1/smartmoney/{chain}/walletNew/{address}?period=7d"
    data = _request_json(path, auth=_use_bearer()).get("data") or {}
    return data if isinstance(data, dict) else {}


def fetch_wallet_common_stat(address: str, *, chain: str = "sol") -> dict[str, Any]:
    """Tags + created_token_count (best minter signal). Prefer Bearer when available."""
    global _COMMON_STAT_LAST
    path = f"/api/v1/wallet_common_stat/{chain}/{address}"
    # One-at-a-time + spacing — this endpoint CF-blocks under parallel enrich.
    with _COMMON_STAT_LOCK:
        gap = time.time() - _COMMON_STAT_LAST
        if gap < _COMMON_STAT_MIN_INTERVAL:
            time.sleep(_COMMON_STAT_MIN_INTERVAL - gap)
        try:
            data = _request_json(path, auth=_use_bearer(), retries=5).get("data") or {}
        finally:
            _COMMON_STAT_LAST = time.time()
    return data if isinstance(data, dict) else {}


def fetch_wallet_profit_stat(
    address: str,
    *,
    chain: str = "sol",
    period: str = "7d",
) -> dict[str, Any]:
    """
    Authenticated UI profit/winrate stats.

    Requires access token. This is how GMGN serves winrate for wallets that are
    not on the public leaderboard (e.g. Grapes.b5).
    """
    path = f"/pf/api/v1/wallet/{chain}/{address}/profit_stat/{period}"
    data = _request_json(path, auth=True).get("data") or {}
    return data if isinstance(data, dict) else {}


def fetch_wallet_holdings(
    address: str,
    *,
    chain: str = "sol",
    limit: int = 50,
    orderby: str = "last_active_timestamp",
    direction: str = "desc",
) -> list[dict[str, Any]]:
    """Authenticated holdings list (used by GMGN address page)."""
    path = (
        f"/pf/api/v1/wallet/{chain}/{address}/holdings"
        f"?limit={limit}&orderby={orderby}&direction={direction}"
    )
    data = _request_json(path, auth=True).get("data") or {}
    rows = data.get("holdings") or data.get("list") or []
    return rows if isinstance(rows, list) else []


def resolve_winrate(
    address: str,
    *,
    chain: str = "sol",
    period: str = "7d",
    leaderboard_row: dict[str, Any] | None = None,
    detail: dict[str, Any] | None = None,
    openapi_stats: dict[str, Any] | None = None,
) -> tuple[float | None, str]:
    """
    Resolve GMGN UI winrate with priority:
      1) leaderboard winrate_7d
      2) OpenAPI wallet_stats (API key)
      3) authenticated profit_stat.winrate (Bearer)
      4) walletNew.winrate (usually null)
    """
    if leaderboard_row and leaderboard_row.get("winrate_7d") not in (None, ""):
        return float(leaderboard_row["winrate_7d"]), "leaderboard"

    stats = openapi_stats
    if stats is None and _API_KEY:
        try:
            stats = fetch_wallet_stats_openapi(address, chain=chain, period=period)
        except GmgnAuthError:
            raise
        except GmgnError:
            stats = None
    if stats:
        pnl_stat = stats.get("pnl_stat") if isinstance(stats.get("pnl_stat"), dict) else {}
        for src in (pnl_stat, stats):
            for key in ("winrate", "winrate_7d", f"winrate_{period}"):
                if src.get(key) not in (None, ""):
                    return float(src[key]), "openapi"

    if _use_bearer():
        try:
            profit = fetch_wallet_profit_stat(address, chain=chain, period=period)
            for key in ("winrate", "winrate_7d", f"winrate_{period}"):
                if profit.get(key) not in (None, ""):
                    return float(profit[key]), "profit_stat"
            # Some payloads nest under pnl_detail
            nested = profit.get("pnl_detail") or {}
            if isinstance(nested, dict):
                for key in ("winrate", "winrate_7d"):
                    if nested.get(key) not in (None, ""):
                        return float(nested[key]), "profit_stat"
        except GmgnAuthError:
            raise
        except GmgnError:
            pass

    detail = detail or {}
    for key in ("winrate_7d", "winrate"):
        if detail.get(key) not in (None, ""):
            return float(detail[key]), "walletNew"
    return None, "missing"


def fetch_trending_tokens(
    *,
    chain: str = "sol",
    timeframes: list[str] | None = None,
    orderbys: list[str] | None = None,
    limit: int = 40,
    delay_sec: float = 0.25,
) -> list[dict[str, Any]]:
    timeframes = timeframes or ["5m", "1h", "6h", "24h"]
    orderbys = orderbys or ["volume", "swaps"]
    # OpenAPI interval names
    tf_map = {"5m": "5m", "1h": "1h", "6h": "6h", "24h": "24h", "1m": "1m"}
    seen: dict[str, dict[str, Any]] = {}
    for tf in timeframes:
        for orderby in orderbys:
            if _API_KEY:
                try:
                    payload = _openapi_request_json(
                        "/v1/market/rank",
                        params={
                            "chain": chain,
                            "interval": tf_map.get(tf, tf),
                            "order_by": orderby,
                            "direction": "desc",
                            "limit": limit,
                        },
                    )
                except GmgnError as exc:
                    print(f"[tokens] openapi {tf}/{orderby} FAIL {exc}")
                    continue
                data = payload.get("data") or {}
                # Sometimes nested {code,data:{rank}}
                if isinstance(data, dict) and "rank" not in data and isinstance(
                    data.get("data"), dict
                ):
                    data = data["data"]
                rank = (data.get("rank") if isinstance(data, dict) else None) or []
            else:
                path = (
                    f"/defi/quotation/v1/rank/{chain}/swaps/{tf}"
                    f"?orderby={orderby}&direction=desc&limit={limit}"
                )
                try:
                    payload = _request_json(path, auth=_use_bearer())
                except GmgnAuthError:
                    raise
                except GmgnError as exc:
                    print(f"[tokens] {tf}/{orderby} FAIL {exc}")
                    continue
                rank = (payload.get("data") or {}).get("rank") or []
            print(f"[tokens] {tf}/{orderby} n={len(rank)}")
            for row in rank:
                mint = row.get("address")
                if mint and mint not in seen:
                    seen[mint] = row
            if delay_sec > 0:
                time.sleep(delay_sec)
    return list(seen.values())


def fetch_token_trades(
    mint: str,
    *,
    chain: str = "sol",
    limit: int = 50,
) -> list[dict[str, Any]]:
    path = f"/vas/api/v1/token_trades/{chain}/{mint}?limit={limit}"
    data = _request_json(path, auth=_use_bearer()).get("data") or {}
    rows = data.get("history") or data.get("list") or []
    return rows if isinstance(rows, list) else []


def fetch_token_traders(
    mint: str,
    *,
    chain: str = "sol",
    limit: int = 50,
) -> list[dict[str, Any]]:
    """Top traders on a mint. Prefer OpenAPI when API key is set."""
    if _API_KEY:
        payload = _openapi_request_json(
            "/v1/market/token_top_traders",
            params={"chain": chain, "address": mint, "limit": limit},
        )
        data = payload.get("data") or {}
        rows = data.get("list") or []
        return rows if isinstance(rows, list) else []
    path = f"/vas/api/v1/token_traders/{chain}/{mint}?limit={limit}"
    data = _request_json(path, auth=_use_bearer()).get("data") or {}
    rows = data.get("list") or []
    return rows if isinstance(rows, list) else []


# mint -> open_ts (None = looked up, missing). Shared across sniper checks in one run.
_OPEN_TS_CACHE: dict[str, int | None] = {}
_OPEN_TS_CACHE_PATH: Path | None = None


def configure_open_ts_cache(path: str | Path | None) -> None:
    """Load/persist open_timestamp disk cache (survives JWT cycles in one machine)."""
    global _OPEN_TS_CACHE_PATH
    _OPEN_TS_CACHE_PATH = Path(path) if path else None
    if not _OPEN_TS_CACHE_PATH or not _OPEN_TS_CACHE_PATH.is_file():
        return
    try:
        raw = json.loads(_OPEN_TS_CACHE_PATH.read_text())
        if isinstance(raw, dict):
            for k, v in raw.items():
                if v is None:
                    _OPEN_TS_CACHE[str(k)] = None
                else:
                    try:
                        _OPEN_TS_CACHE[str(k)] = int(v)
                    except (TypeError, ValueError):
                        continue
    except Exception:
        pass


def _persist_open_ts_cache() -> None:
    if not _OPEN_TS_CACHE_PATH:
        return
    try:
        _OPEN_TS_CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
        _OPEN_TS_CACHE_PATH.write_text(json.dumps(_OPEN_TS_CACHE))
    except Exception:
        pass


def fetch_token_open_timestamp(mint: str, *, chain: str = "sol") -> int | None:
    """
    Token market-open time (Unix seconds). Snipers buy within seconds of this.
    Tries a few GMGN paths used by the web UI. Results are cached in-memory (+ disk).
    """
    if mint in _OPEN_TS_CACHE:
        return _OPEN_TS_CACHE[mint]
    paths = (
        f"/api/v1/token_info/{chain}/{mint}",
        f"/defi/quotation/v1/tokens/{chain}/{mint}",
        f"/vas/api/v1/token_info/{chain}/{mint}",
    )
    found: int | None = None
    for path in paths:
        try:
            data = _request_json(path, auth=_use_bearer()).get("data") or {}
        except GmgnAuthError:
            raise
        except GmgnError:
            continue
        if not isinstance(data, dict):
            continue
        for key in ("open_timestamp", "open_time", "creation_timestamp", "created_timestamp"):
            val = data.get(key)
            if val in (None, "", 0, "0"):
                for nest_key in ("pool", "stat", "token"):
                    nest = data.get(nest_key)
                    if isinstance(nest, dict) and nest.get(key) not in (None, "", 0, "0"):
                        val = nest.get(key)
                        break
            if val not in (None, "", 0, "0"):
                try:
                    found = int(float(val))
                    break
                except (TypeError, ValueError):
                    continue
        if found is not None:
            break
    _OPEN_TS_CACHE[mint] = found
    if len(_OPEN_TS_CACHE) % 25 == 0:
        _persist_open_ts_cache()
    return found
