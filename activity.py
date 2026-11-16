"""GMGN wallet activity — detect token launches (real minter signal)."""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any

from gmgn import (
    GmgnAuthError,
    GmgnError,
    _openapi_request_json,
    _request_json,
    _use_bearer,
    fetch_token_open_timestamp,
    get_api_key,
)


@dataclass
class ActivitySummary:
    address: str
    activities_scanned: int = 0
    buy: int = 0
    sell: int = 0
    launch: int = 0
    claim_fee: int = 0
    other: int = 0
    launched_tokens: list[str] = field(default_factory=list)
    launched_symbols: list[str] = field(default_factory=list)

    @property
    def is_minter(self) -> bool:
        return self.launch > 0


def fetch_wallet_activity(
    address: str,
    *,
    chain: str = "sol",
    limit: int = 50,
    max_pages: int = 4,
    delay_sec: float = 0.25,
) -> list[dict[str, Any]]:
    """Paginate recent wallet activity (buy/sell/launch/...)."""
    activities: list[dict[str, Any]] = []
    cursor: str | None = None

    for _ in range(max(max_pages, 1)):
        try:
            if get_api_key():
                params: dict[str, Any] = {
                    "chain": chain,
                    "wallet_address": address,
                    "limit": limit,
                }
                if cursor:
                    params["cursor"] = cursor
                    params["page_token"] = cursor
                payload = _openapi_request_json(
                    "/v1/user/wallet_activity", params=params
                )
            else:
                path = (
                    f"/vas/api/v1/wallet_activity/{chain}"
                    f"?wallet={address}&limit={limit}"
                )
                if cursor:
                    path += f"&cursor={cursor}"
                payload = _request_json(path, auth=_use_bearer())
        except GmgnError as exc:
            raise GmgnError(f"Activity fetch failed for {address}: {exc}") from exc

        data = payload.get("data") or {}
        page = data.get("activities") or []
        if not isinstance(page, list) or not page:
            break
        activities.extend(page)

        nxt = data.get("next") or data.get("next_page_token")
        if not nxt or nxt == cursor:
            break
        cursor = str(nxt)
        if delay_sec > 0:
            time.sleep(delay_sec)

    return activities


def summarize_activity(
    address: str,
    activities: list[dict[str, Any]],
) -> ActivitySummary:
    summary = ActivitySummary(address=address, activities_scanned=len(activities))
    seen_mints: set[str] = set()

    for row in activities:
        event = str(row.get("event_type") or row.get("type") or "").lower()
        if event == "buy":
            summary.buy += 1
        elif event == "sell":
            summary.sell += 1
        elif event == "launch":
            summary.launch += 1
            token = row.get("token") or {}
            mint = token.get("address")
            symbol = token.get("symbol")
            if mint and mint not in seen_mints:
                seen_mints.add(str(mint))
                summary.launched_tokens.append(str(mint))
                if symbol:
                    summary.launched_symbols.append(str(symbol))
        elif event == "claim_fee":
            summary.claim_fee += 1
        else:
            summary.other += 1

    return summary


def check_minter(
    address: str,
    *,
    chain: str = "sol",
    max_pages: int = 4,
    delay_sec: float = 0.25,
) -> ActivitySummary:
    activities = fetch_wallet_activity(
        address,
        chain=chain,
        max_pages=max_pages,
        delay_sec=delay_sec,
    )
    return summarize_activity(address, activities)


def _activity_buy_ts(row: dict[str, Any]) -> int | None:
    for key in ("timestamp", "block_timestamp", "event_timestamp", "time", "created_at"):
        val = row.get(key)
        if val in (None, ""):
            continue
        try:
            ts = int(float(val))
        except (TypeError, ValueError):
            continue
        # ms → s
        if ts > 10_000_000_000:
            ts //= 1000
        return ts
    return None


def estimate_snipe_ratio(
    activities: list[dict[str, Any]],
    *,
    chain: str = "sol",
    max_seconds_after_open: int = 3,
    sample_buys: int = 40,
    delay_sec: float = 0.05,
) -> tuple[float | None, int, int]:
    """
    Fraction of recent buys that land within max_seconds_after_open of token open.

    Snipers buy right after the pool opens / immediately after the dev buy —
    not merely wallets with skewed buy/sell counts.

    Returns (ratio_or_None, snipe_buys, scored_buys).
    """
    buys: list[tuple[str, int]] = []
    for row in activities:
        et = str(row.get("event_type") or row.get("type") or "").lower()
        if et != "buy":
            continue
        token = row.get("token") or {}
        mint = token.get("address") if isinstance(token, dict) else None
        ts = _activity_buy_ts(row)
        if not mint or ts is None:
            continue
        buys.append((str(mint), ts))
        if len(buys) >= max(sample_buys, 1):
            break

    if len(buys) < 5:
        return None, 0, len(buys)

    open_cache: dict[str, int | None] = {}
    snipes = 0
    scored = 0
    for mint, buy_ts in buys:
        if mint not in open_cache:
            try:
                open_cache[mint] = fetch_token_open_timestamp(mint, chain=chain)
            except GmgnAuthError:
                raise
            except GmgnError:
                open_cache[mint] = None
            if delay_sec > 0:
                time.sleep(delay_sec)
        open_ts = open_cache[mint]
        if open_ts is None:
            continue
        scored += 1
        delta = buy_ts - open_ts
        if 0 <= delta <= max_seconds_after_open:
            snipes += 1

    if scored < 5:
        return None, snipes, scored
    return snipes / scored, snipes, scored


def estimate_winrate_from_activity(activities: list[dict[str, Any]]) -> float | None:
    """
    Approximate GMGN winrate from activity.

    Prefer per-token realized PnL using sell proceeds vs buy_cost_usd.
    A token counts as a win when sum(sell - basis) > 0.
    """
    from collections import defaultdict

    by_token: dict[str, dict[str, float]] = defaultdict(
        lambda: {"pnl": 0.0, "sells": 0.0, "buys": 0.0}
    )
    for row in activities:
        token = (row.get("token") or {}).get("address")
        if not token:
            continue
        et = str(row.get("event_type") or "").lower()
        if et == "sell" and row.get("buy_cost_usd") not in (None, ""):
            proceeds = float(row.get("cost_usd") or 0)
            basis = float(row.get("buy_cost_usd") or 0)
            by_token[token]["pnl"] += proceeds - basis
            by_token[token]["sells"] += 1
        elif et == "buy":
            by_token[token]["buys"] += 1

    scored = [d for d in by_token.values() if d["sells"] > 0]
    if len(scored) < 5:
        return None
    wins = sum(1 for d in scored if d["pnl"] > 0)
    return wins / len(scored)
