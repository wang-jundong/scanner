"""Discover trader wallets from trending trades + seed co-traders."""

from __future__ import annotations

import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable

from activity import fetch_wallet_activity
from gmgn import (
    fetch_token_traders,
    fetch_trending_tokens,
    GmgnAuthError,
    GmgnError,
    _request_json,
    _use_bearer,
)

# Grapes.b5-like wallets often carry axiom/arbitrager, not only smart_degen
DEFAULT_PREFER_TAGS = {
    "smart_degen",
    "pump_smart",
    "renowned",
    "kol",
    "axiom",
    "arbitrager",
}
DEFAULT_BOT_TAGS = {
    "sandwich_bot",
    "snipe_bot",
    "sniper",
    "bundler",
    "wash_trader",
    "rat_trader",
    "top_dev",
}


@dataclass
class TradeSighting:
    address: str
    tokens: set[str] = field(default_factory=set)
    buy_events: int = 0
    sell_events: int = 0
    realized_profit: float = 0.0
    tags: set[str] = field(default_factory=set)
    sources: set[str] = field(default_factory=set)


def fetch_token_trades_pages(
    mint: str,
    *,
    chain: str = "sol",
    limit: int = 50,
    max_pages: int = 3,
    delay_sec: float = 0.15,
) -> list[dict]:
    """Paginate recent trades so we see more makers than the first page."""
    rows: list[dict] = []
    cursor: str | None = None
    for _ in range(max(max_pages, 1)):
        path = f"/vas/api/v1/token_trades/{chain}/{mint}?limit={limit}"
        if cursor:
            path += f"&cursor={cursor}"
        try:
            payload = _request_json(path, auth=_use_bearer())
        except GmgnError:
            break
        data = payload.get("data") or {}
        page = data.get("history") or data.get("list") or []
        if not isinstance(page, list) or not page:
            break
        rows.extend(page)
        nxt = data.get("next")
        if not nxt or nxt == cursor:
            break
        cursor = str(nxt)
        if delay_sec > 0:
            time.sleep(delay_sec)
    return rows


def discover_from_token_activity(
    *,
    chain: str = "sol",
    timeframes: list[str] | None = None,
    orderbys: list[str] | None = None,
    tokens_limit: int = 40,
    trades_per_token: int = 50,
    trade_pages: int = 3,
    delay_sec: float = 0.25,
) -> tuple[dict[str, TradeSighting], set[str]]:
    """
    Pull recent makers from trending token trades only.

    token_traders is intentionally skipped: it ranks big holders / whales and
    buries normal smart_degen traders like Grapes.b5.
    """
    tokens = fetch_trending_tokens(
        chain=chain,
        timeframes=timeframes,
        orderbys=orderbys,
        limit=tokens_limit,
        delay_sec=delay_sec,
    )
    creators = {str(t["creator"]) for t in tokens if t.get("creator")}
    print(f"[discover] unique tokens={len(tokens)} creators={len(creators)}")

    sightings: dict[str, TradeSighting] = {}

    for idx, token in enumerate(tokens, start=1):
        mint = str(token.get("address") or "")
        symbol = str(token.get("symbol") or mint[:8])
        if not mint:
            continue

        try:
            trades = fetch_token_trades_pages(
                mint,
                chain=chain,
                limit=trades_per_token,
                max_pages=trade_pages,
                delay_sec=min(delay_sec, 0.15),
            )
        except GmgnError as exc:
            print(f"  [{idx}/{len(tokens)}] trades {symbol} FAIL {exc}")
            trades = []

        for row in trades:
            maker = row.get("maker")
            if not maker or maker in creators:
                continue
            hit = sightings.get(maker)
            if hit is None:
                hit = TradeSighting(address=str(maker))
                sightings[maker] = hit
            hit.tokens.add(mint)
            hit.sources.add("trades")
            event = str(row.get("event") or "").lower()
            if event == "buy":
                hit.buy_events += 1
            elif event == "sell":
                hit.sell_events += 1
            hit.realized_profit += float(row.get("realized_profit") or 0)
            for tag in row.get("maker_tags") or []:
                hit.tags.add(str(tag))

        print(
            f"  [{idx}/{len(tokens)}] {symbol} "
            f"trades={len(trades)} wallets={len(sightings)}"
        )
        if delay_sec > 0:
            time.sleep(delay_sec)

    print(f"[discover] candidate wallets={len(sightings)}")
    return sightings, creators


def _ingest_trade_row(
    sightings: dict[str, TradeSighting],
    row: dict,
    *,
    mint: str,
    creators: set[str],
    source: str,
) -> None:
    maker = row.get("maker") or row.get("address") or row.get("account_address")
    if not maker or maker in creators:
        return
    hit = sightings.get(maker)
    if hit is None:
        hit = TradeSighting(address=str(maker))
        sightings[maker] = hit
    hit.tokens.add(mint)
    hit.sources.add(source)
    event = str(row.get("event") or row.get("event_type") or "").lower()
    if event == "buy":
        hit.buy_events += 1
    elif event == "sell":
        hit.sell_events += 1
    elif source == "traders":
        # token_traders rows are holders; treat buy_tx as activity signal
        buys = int(float(row.get("buy_tx_count_cur") or 0))
        sells = int(float(row.get("sell_tx_count_cur") or 0))
        hit.buy_events += max(buys, 1)
        hit.sell_events += sells
    hit.realized_profit += float(
        row.get("realized_profit") or row.get("profit") or 0
    )
    for tag in row.get("maker_tags") or row.get("tags") or []:
        hit.tags.add(str(tag))


def discover_from_seeds(
    seeds: list[str],
    *,
    chain: str = "sol",
    activity_pages: int = 6,
    tokens_per_seed: int = 25,
    trades_per_token: int = 50,
    trade_pages: int = 3,
    include_traders: bool = True,
    traders_limit: int = 40,
    delay_sec: float = 0.2,
) -> dict[str, TradeSighting]:
    """
    Find Grapes-like wallets by expanding from example seeds:
    seed activity tokens → co-traders on those same tokens.
    """
    sightings: dict[str, TradeSighting] = {}
    seed_set = {s.strip() for s in seeds if s and s.strip()}
    if not seed_set:
        return sightings

    # Always keep seeds themselves as candidates (they define the target profile)
    for seed in seed_set:
        sightings[seed] = TradeSighting(
            address=seed,
            buy_events=1,
            tags={"seed"},
            sources={"seed"},
        )

    token_scores: dict[str, int] = {}
    for seed in seed_set:
        try:
            acts = fetch_wallet_activity(
                seed,
                chain=chain,
                max_pages=activity_pages,
                delay_sec=min(delay_sec, 0.15),
            )
        except GmgnError as exc:
            print(f"[seed] activity FAIL {seed[:12]}... {exc}")
            continue
        for row in acts:
            mint = (row.get("token") or {}).get("address")
            if not mint:
                continue
            token_scores[str(mint)] = token_scores.get(str(mint), 0) + 1
        print(f"[seed] {seed[:12]}... activity={len(acts)} tokens={len(token_scores)}")
        if delay_sec > 0:
            time.sleep(delay_sec)

    ranked_tokens = sorted(token_scores, key=token_scores.get, reverse=True)
    ranked_tokens = ranked_tokens[: max(tokens_per_seed * len(seed_set), 1)]
    print(f"[seed] expanding via {len(ranked_tokens)} tokens traded by seeds")

    for idx, mint in enumerate(ranked_tokens, start=1):
        try:
            trades = fetch_token_trades_pages(
                mint,
                chain=chain,
                limit=trades_per_token,
                max_pages=trade_pages,
                delay_sec=min(delay_sec, 0.15),
            )
        except GmgnError as exc:
            print(f"  [{idx}/{len(ranked_tokens)}] trades FAIL {mint[:8]} {exc}")
            trades = []

        for row in trades:
            _ingest_trade_row(
                sightings, row, mint=mint, creators=set(), source="seed_trades"
            )

        traders_n = 0
        if include_traders:
            try:
                traders = fetch_token_traders(mint, chain=chain, limit=traders_limit)
                traders_n = len(traders)
                for row in traders:
                    _ingest_trade_row(
                        sightings,
                        row,
                        mint=mint,
                        creators=set(),
                        source="seed_traders",
                    )
            except GmgnError:
                traders_n = 0

        print(
            f"  [{idx}/{len(ranked_tokens)}] {mint[:8]}... "
            f"trades={len(trades)} traders={traders_n} wallets={len(sightings)}"
        )
        if delay_sec > 0:
            time.sleep(delay_sec)

    print(f"[seed] candidate wallets={len(sightings)}")
    return sightings


def discover_via_hub_expansion(
    *,
    chain: str = "sol",
    timeframes: list[str] | None = None,
    orderbys: list[str] | None = None,
    tokens_limit: int = 30,
    prefer_tags: list[str] | None = None,
    hops: int = 2,
    hubs_per_hop: int = 50,
    activity_pages: int = 2,
    expand_tokens: int = 80,
    traders_limit: int = 80,
    delay_sec: float = 0.1,
    trader_workers: int = 8,
    activity_workers: int = 12,
    max_long_tail: int = 500,
    checkpoint_path: str | Path | None = None,
    ensure_token: Callable[[], None] | None = None,
) -> dict[str, TradeSighting]:
    """
    Find Grapes-like wallets that never appear on trending trade pages.

    Flow (no seed wallet required):
      trending tokens → prefer-tagged traders (hubs)
      → hubs' recent activity tokens (mid-cap / non-trending)
      → token_traders on those tokens (this is where Grapes.b5 shows up)
      → optional 2nd hop for more coverage

    Hop2+ scans nearly the full long-tail token pool (capped), not just top score,
    because Grapes-like mints often have low hub co-occurrence scores.

    checkpoint_path: resume across JWT cycles (hop1/activity/hop2 progress).
    ensure_token: called before batches; should block until Bearer is fresh.
    """
    prefer = {t.lower() for t in (prefer_tags or list(DEFAULT_PREFER_TAGS))}
    ck_path = Path(checkpoint_path) if checkpoint_path else None

    sightings: dict[str, TradeSighting] = {}
    seed_set: set[str] = set()
    token_pool: dict[str, int] = {}
    hubs: set[str] = set()
    scanned_mints: set[str] = set()
    phase = "start"
    activity_idx = 0
    hub_list: list[str] = []

    def _pulse() -> None:
        if ensure_token:
            ensure_token()

    def _save_ck(current_phase: str, *, activity_at: int = 0) -> None:
        if not ck_path:
            return
        payload = {
            "saved_at": time.time(),
            "phase": current_phase,
            "activity_idx": activity_at,
            "seed_set": sorted(seed_set),
            "token_pool": token_pool,
            "hubs": sorted(hubs),
            "hub_list": hub_list,
            "scanned_mints": sorted(scanned_mints),
            "sightings": sightings_to_json(sightings),
        }
        ck_path.parent.mkdir(parents=True, exist_ok=True)
        ck_path.write_text(json.dumps(payload))
        print(
            f"[hub] checkpoint {current_phase} wallets={len(sightings)} "
            f"tokens={len(token_pool)} → {ck_path}"
        )

    if ck_path and ck_path.is_file():
        try:
            ck = json.loads(ck_path.read_text())
            sightings = sightings_from_json(ck.get("sightings") or [])
            seed_set = set(ck.get("seed_set") or [])
            token_pool = {str(k): int(v) for k, v in (ck.get("token_pool") or {}).items()}
            hubs = set(ck.get("hubs") or [])
            hub_list = list(ck.get("hub_list") or [])
            scanned_mints = set(ck.get("scanned_mints") or [])
            phase = str(ck.get("phase") or "start")
            activity_idx = int(ck.get("activity_idx") or 0)
            if sightings and phase not in ("start", "done"):
                print(
                    f"[hub] resume checkpoint phase={phase} "
                    f"wallets={len(sightings)} activity_idx={activity_idx} "
                    f"scanned={len(scanned_mints)}"
                )
            else:
                phase = "start"
        except Exception as exc:  # noqa: BLE001
            print(f"[hub] checkpoint unreadable: {exc}")
            phase = "start"

    def rank_hub_addrs(addrs: set[str] | list[str]) -> list[str]:
        """Prefer-tagged hubs; niche (2-25 tokens) before mega hubs."""
        bots = {t.lower() for t in DEFAULT_BOT_TAGS}
        niche: list[tuple[int, int, int, str]] = []
        wide: list[tuple[int, int, int, str]] = []
        empty: list[tuple[int, int, int, str]] = []
        for addr in addrs:
            s = sightings.get(addr)
            if s is None:
                empty.append((0, 0, 0, addr))
                continue
            tags_l = {t.lower() for t in s.tags}
            if tags_l & bots:
                continue
            pref_n = len(tags_l & prefer)
            ntok = len(s.tokens)
            row = (pref_n, ntok, s.buy_events, addr)
            if 2 <= ntok <= 25:
                niche.append(row)
            else:
                wide.append(row)
        niche.sort(key=lambda x: (-x[0], -x[1], -x[2], x[3]))
        wide.sort(key=lambda x: (-x[0], -x[1], -x[2], x[3]))
        return [a for *_, a in niche + wide + empty]

    def fetch_traders_rows(mint: str) -> tuple[str, list[dict], list[dict]]:
        traders: list[dict] = []
        trades: list[dict] = []
        try:
            traders = fetch_token_traders(mint, chain=chain, limit=traders_limit)
        except GmgnAuthError:
            raise
        except GmgnError:
            traders = []
        # Recent makers catch active axiom wallets that fall off top-trader boards
        try:
            trades = fetch_token_trades_pages(
                mint, chain=chain, limit=50, max_pages=2, delay_sec=0
            )
        except GmgnAuthError:
            raise
        except GmgnError:
            trades = []
        return mint, traders, trades

    def merge_trader_rows(mint: str, traders: list[dict], source: str) -> set[str]:
        hubs_found: set[str] = set()
        for row in traders:
            addr = row.get("address") or row.get("maker") or row.get("account_address")
            if not addr:
                continue
            tags = {str(x) for x in (row.get("tags") or row.get("maker_tags") or [])}
            tags_l = {t.lower() for t in tags}
            hit = sightings.get(str(addr))
            if hit is None:
                hit = TradeSighting(address=str(addr))
                sightings[str(addr)] = hit
            hit.tokens.add(mint)
            hit.sources.add(source)
            hit.tags |= tags
            buys = int(float(row.get("buy_tx_count_cur") or row.get("buy") or 0))
            sells = int(float(row.get("sell_tx_count_cur") or row.get("sell") or 0))
            hit.buy_events += max(buys, 1)
            hit.sell_events += sells
            hit.realized_profit += float(row.get("realized_profit") or row.get("profit") or 0)
            if tags_l & prefer:
                hubs_found.add(str(addr))
        return hubs_found

    def merge_trade_rows(mint: str, trades: list[dict], source: str) -> set[str]:
        hubs_found: set[str] = set()
        for row in trades:
            addr = row.get("maker") or row.get("address") or row.get("account_address")
            if not addr:
                continue
            tags = {str(x) for x in (row.get("maker_tags") or row.get("tags") or [])}
            tags_l = {t.lower() for t in tags}
            hit = sightings.get(str(addr))
            if hit is None:
                hit = TradeSighting(address=str(addr))
                sightings[str(addr)] = hit
            hit.tokens.add(mint)
            hit.sources.add(source)
            hit.tags |= tags
            event = str(row.get("event") or row.get("event_type") or "").lower()
            if event == "buy":
                hit.buy_events += 1
            elif event == "sell":
                hit.sell_events += 1
            else:
                hit.buy_events += 1
            hit.realized_profit += float(row.get("realized_profit") or 0)
            if tags_l & prefer:
                hubs_found.add(str(addr))
        return hubs_found

    def rank_tokens_for_hop(hop: int) -> list[str]:
        if hop <= 1:
            return sorted(seed_set)
        long_tail = [m for m in token_pool if m not in seed_set]
        long_tail.sort(key=lambda m: (token_pool[m], m))  # rarest first
        n = max_long_tail
        if len(long_tail) <= n:
            return long_tail
        # Stratified: rare niches + mid band + hotter co-trade mints
        n_rare = n // 2
        n_mid = (3 * n) // 10
        n_hi = n - n_rare - n_mid
        rare = long_tail[:n_rare]
        mid_start = max(0, len(long_tail) // 2 - n_mid // 2)
        mid = long_tail[mid_start : mid_start + n_mid]
        hi = long_tail[-n_hi:] if n_hi else []
        out: list[str] = []
        seen: set[str] = set()
        for m in rare + mid + hi:
            if m not in seen:
                seen.add(m)
                out.append(m)
        return out

    def scan_traders(hop: int) -> None:
        nonlocal hubs
        ranked = [m for m in rank_tokens_for_hop(hop) if m not in scanned_mints]
        # expand_tokens raises the hop2+ mint budget when larger than max_long_tail
        budget = max(max_long_tail, expand_tokens) if hop >= 2 else len(ranked)
        ranked = ranked[:budget]
        print(
            f"[hub] hop={hop} scanning traders+trades on {len(ranked)} tokens "
            f"(pool={len(token_pool)} long_tail={sum(1 for m in token_pool if m not in seed_set)}) "
            f"workers={trader_workers}"
        )
        if not ranked:
            return
        _pulse()
        new_hubs: set[str] = set()
        source = f"hub_traders_h{hop}"
        done = 0
        try:
            with ThreadPoolExecutor(max_workers=max(trader_workers, 1)) as pool:
                futs = [pool.submit(fetch_traders_rows, mint) for mint in ranked]
                for fut in as_completed(futs):
                    mint, traders, trades = fut.result()
                    scanned_mints.add(mint)
                    new_hubs |= merge_trader_rows(mint, traders, source)
                    if trades:
                        new_hubs |= merge_trade_rows(mint, trades, source)
                    done += 1
                    if done % 25 == 0 or done == len(ranked):
                        print(
                            f"  [hub hop{hop}] {done}/{len(ranked)} "
                            f"hubs={len(hubs|new_hubs)} wallets={len(sightings)}"
                        )
                    if done % 100 == 0:
                        _save_ck(f"hop{hop}_traders")
                        _pulse()
        except GmgnAuthError:
            hubs |= new_hubs
            _save_ck(f"hop{hop}_traders")
            raise
        hubs |= new_hubs
        print(f"[hub] hop={hop} hubs={len(hubs)} candidate_wallets={len(sightings)}")
        _save_ck(f"hop{hop}_traders_done")

    def read_hub_activity(hubs_to_read: list[str], *, resume_idx: int = 0) -> None:
        nonlocal hub_list, activity_idx
        hub_list = list(hubs_to_read)
        activity_idx = resume_idx
        todo = list(range(activity_idx, len(hub_list)))
        print(
            f"[hub] reading activity for {len(hub_list)} hubs "
            f"(resume idx={activity_idx}, workers={activity_workers})"
        )

        def _fetch_hub_activity(idx: int) -> tuple[int, list[dict]]:
            addr = hub_list[idx]
            try:
                acts = fetch_wallet_activity(
                    addr,
                    chain=chain,
                    max_pages=activity_pages,
                    delay_sec=0,
                )
                return idx, acts
            except GmgnAuthError:
                raise
            except GmgnError:
                return idx, []

        done_idxs: set[int] = set(range(activity_idx))
        frontier = activity_idx
        _pulse()
        try:
            with ThreadPoolExecutor(max_workers=max(activity_workers, 1)) as pool:
                futs = {pool.submit(_fetch_hub_activity, i): i for i in todo}
                finished = 0
                for fut in as_completed(futs):
                    try:
                        idx, acts = fut.result()
                    except GmgnAuthError:
                        _save_ck("activity", activity_at=frontier)
                        raise
                    for row in acts:
                        mint = (row.get("token") or {}).get("address")
                        if mint:
                            token_pool[str(mint)] = token_pool.get(str(mint), 0) + 1
                    done_idxs.add(idx)
                    finished += 1
                    while frontier < len(hub_list) and frontier in done_idxs:
                        frontier += 1
                    if finished % 20 == 0 or finished == len(todo):
                        print(
                            f"  [hub activity] {finished}/{len(todo)} "
                            f"frontier={frontier} tokens={len(token_pool)}"
                        )
                    if frontier > activity_idx and frontier % 10 == 0:
                        _save_ck("activity", activity_at=frontier)
                        _pulse()
        except GmgnAuthError:
            _save_ck("activity", activity_at=frontier)
            raise

    # --- phase machine ---
    if phase == "start":
        _pulse()
        seed_tokens = fetch_trending_tokens(
            chain=chain,
            timeframes=timeframes or ["5m", "1h", "6h"],
            orderbys=orderbys or ["volume", "swaps"],
            limit=tokens_limit,
            delay_sec=delay_sec,
        )
        seed_set = {str(t["address"]) for t in seed_tokens if t.get("address")}
        token_pool = {m: 1 for m in seed_set}
        print(f"[hub] seed trending tokens={len(token_pool)}")
        scan_traders(1)
        phase = "activity"
        activity_idx = 0
        hub_list = rank_hub_addrs(hubs)[:hubs_per_hop]
        _save_ck("activity", activity_at=0)

    used_activity_hubs: set[str] = set(hub_list)

    if phase in ("activity", "hop1_traders", "hop1_traders_done"):
        if phase.startswith("hop1"):
            phase = "activity"
            activity_idx = 0
            hub_list = rank_hub_addrs(hubs)[:hubs_per_hop]
        if not hub_list:
            hub_list = rank_hub_addrs(hubs)[:hubs_per_hop]
        used_activity_hubs |= set(hub_list)
        read_hub_activity(hub_list, resume_idx=activity_idx)
        phase = "hop2"
        _save_ck("hop2")

    if phase in ("hop2", "hop2_traders") and hops >= 2:
        scan_traders(2)
        phase = "hop3" if hops >= 3 else "done"
        _save_ck(phase)

    # Hop3+: re-hub from prefer-tagged discoveries, pull THEIR niche tokens
    # (Grapes-like wallets sit on mints that hop1 trending hubs never touch)
    for hop_n in range(3, hops + 1):
        if phase == "done":
            break
        # Resume: skip completed hops
        if phase not in (
            f"hop{hop_n}",
            f"hop{hop_n}_traders",
            f"hop{hop_n}_activity",
            f"hop{hop_n}_traders_done",
        ):
            # If checkpoint says we're past this hop, continue
            try:
                cur = int("".join(ch for ch in phase if ch.isdigit()) or "0")
            except ValueError:
                cur = 0
            if cur > hop_n or (phase == "done"):
                continue
        print(f"[hub] hop={hop_n} expanding from prefer hubs' activity")
        next_hubs = [
            a for a in rank_hub_addrs(hubs) if a not in used_activity_hubs
        ][:hubs_per_hop]
        if not next_hubs:
            print(f"[hub] hop={hop_n} no new hubs — stop")
            phase = "done"
            break
        used_activity_hubs |= set(next_hubs)
        activity_idx = 0
        hub_list = next_hubs
        _save_ck(f"hop{hop_n}_activity", activity_at=0)
        read_hub_activity(next_hubs, resume_idx=0)
        scan_traders(hop_n)
        phase = "done" if hop_n >= hops else f"hop{hop_n + 1}"
        _save_ck(phase)

    phase = "done"

    if ck_path and ck_path.is_file():
        try:
            ck_path.unlink()
            print(f"[hub] checkpoint cleared ({ck_path})")
        except OSError:
            pass

    print(f"[hub] done candidates={len(sightings)} tokens_seen={len(token_pool)}")
    return sightings


def extend_hub_sightings(
    sightings: dict[str, TradeSighting],
    *,
    chain: str = "sol",
    prefer_tags: list[str] | None = None,
    hubs_per_hop: int = 120,
    activity_pages: int = 4,
    traders_limit: int = 100,
    max_long_tail: int = 4000,
    trader_workers: int = 24,
    activity_workers: int = 12,
    hop_label: int = 3,
    already_used_hubs: set[str] | None = None,
    ensure_token: Callable[[], None] | None = None,
) -> dict[str, TradeSighting]:
    """
    One extra hop on an existing hub cache: top prefer wallets → their activity
    tokens → token_traders + recent trades.

    Use when a hop2 cache missed Grapes-like niches that co-traders of hop2 hubs touch.
    """
    prefer = {t.lower() for t in (prefer_tags or list(DEFAULT_PREFER_TAGS))}
    used = set(already_used_hubs or [])

    def _pulse() -> None:
        if ensure_token:
            ensure_token()

    bots = {t.lower() for t in DEFAULT_BOT_TAGS}

    def rank_hub_addrs() -> list[str]:
        """
        Mid-niche prefer hubs bridge to Grapes-like wallets better than
        mega multi-token hubs (different token neighborhood).
        Mix: niche (2-25 tokens) first, then larger.
        """
        niche: list[tuple[int, int, int, str]] = []
        wide: list[tuple[int, int, int, str]] = []
        for addr, s in sightings.items():
            tags_l = {t.lower() for t in s.tags}
            if tags_l & bots:
                continue
            if not (tags_l & prefer):
                continue
            if addr in used:
                continue
            ntok = len(s.tokens)
            score = (len(tags_l & prefer), ntok, s.buy_events, addr)
            if 2 <= ntok <= 25:
                niche.append(score)
            elif ntok >= 1:
                wide.append(score)
        niche.sort(key=lambda x: (-x[0], -x[1], -x[2], x[3]))
        wide.sort(key=lambda x: (-x[0], -x[1], -x[2], x[3]))
        # 70% niche bridges + 30% wide coverage
        n_niche = max(1, int(hubs_per_hop * 0.7))
        picked = niche[:n_niche]
        picked += wide[: max(0, hubs_per_hop - len(picked))]
        if len(picked) < hubs_per_hop:
            picked += niche[n_niche : n_niche + (hubs_per_hop - len(picked))]
        return [a for *_, a in picked]

    hubs = rank_hub_addrs()
    if not hubs:
        print("[hub-extend] no unused prefer hubs", flush=True)
        return sightings

    print(
        f"[hub-extend] hop={hop_label} activity on {len(hubs)} prefer hubs "
        f"(pages={activity_pages})",
        flush=True,
    )
    token_pool: dict[str, int] = {}
    _pulse()

    act_fail = 0

    def _fetch_act(addr: str) -> tuple[str, list[dict], str | None]:
        try:
            return (
                addr,
                fetch_wallet_activity(
                    addr, chain=chain, max_pages=activity_pages, delay_sec=0
                ),
                None,
            )
        except GmgnAuthError as exc:
            return addr, [], f"auth:{exc}"
        except GmgnError as exc:
            return addr, [], str(exc)[:120]

    with ThreadPoolExecutor(max_workers=max(activity_workers, 1)) as pool:
        futs = [pool.submit(_fetch_act, a) for a in hubs]
        done = 0
        for fut in as_completed(futs):
            _addr, acts, err = fut.result()
            if err:
                act_fail += 1
                if act_fail <= 3 or act_fail % 25 == 0:
                    print(
                        f"  [hub-extend activity] FAIL {act_fail}: {err}",
                        flush=True,
                    )
            for row in acts:
                mint = (row.get("token") or {}).get("address")
                if mint:
                    token_pool[str(mint)] = token_pool.get(str(mint), 0) + 1
            done += 1
            if done % 20 == 0 or done == len(hubs):
                print(
                    f"  [hub-extend activity] {done}/{len(hubs)} "
                    f"tokens={len(token_pool)} fails={act_fail}",
                    flush=True,
                )
            if done % 30 == 0:
                _pulse()
    if act_fail:
        print(
            f"[hub-extend] activity failures={act_fail}/{len(hubs)} "
            f"(often OpenAPI 429 — lower activity_workers)",
            flush=True,
        )
    if not token_pool:
        print(
            "[hub-extend] no tokens from activity — skip mint scan "
            "(retry later / lower concurrency)",
            flush=True,
        )
        return sightings

    # Prefer rare + mid + hot
    long_tail = sorted(token_pool.keys(), key=lambda m: (token_pool[m], m))
    n = max_long_tail
    if len(long_tail) > n:
        n_rare = n // 2
        n_mid = (3 * n) // 10
        n_hi = n - n_rare - n_mid
        mid_start = max(0, len(long_tail) // 2 - n_mid // 2)
        pick = long_tail[:n_rare] + long_tail[mid_start : mid_start + n_mid] + long_tail[-n_hi:]
        seen: set[str] = set()
        ranked = []
        for m in pick:
            if m not in seen:
                seen.add(m)
                ranked.append(m)
    else:
        ranked = long_tail

    source = f"hub_traders_h{hop_label}"
    print(
        f"[hub-extend] scanning {len(ranked)} mints workers={trader_workers}",
        flush=True,
    )
    _pulse()

    def _fetch_mint(mint: str) -> tuple[str, list[dict], list[dict]]:
        traders: list[dict] = []
        trades: list[dict] = []
        try:
            traders = fetch_token_traders(mint, chain=chain, limit=traders_limit)
        except GmgnAuthError:
            # One mint auth blip must not kill the whole hop3 extend
            traders = []
        except GmgnError:
            pass
        try:
            trades = fetch_token_trades_pages(
                mint, chain=chain, limit=50, max_pages=2, delay_sec=0
            )
        except (GmgnAuthError, GmgnError):
            pass
        return mint, traders, trades

    before = len(sightings)
    done = 0
    with ThreadPoolExecutor(max_workers=max(trader_workers, 1)) as pool:
        futs = [pool.submit(_fetch_mint, m) for m in ranked]
        for fut in as_completed(futs):
            mint, traders, trades = fut.result()
            for row in traders:
                addr = row.get("address") or row.get("maker") or row.get("account_address")
                if not addr:
                    continue
                tags = {str(x) for x in (row.get("tags") or row.get("maker_tags") or [])}
                hit = sightings.get(str(addr))
                if hit is None:
                    hit = TradeSighting(address=str(addr))
                    sightings[str(addr)] = hit
                hit.tokens.add(mint)
                hit.sources.add(source)
                hit.tags |= tags
                hit.buy_events += max(
                    int(float(row.get("buy_tx_count_cur") or row.get("buy") or 0)), 1
                )
            for row in trades:
                addr = row.get("maker") or row.get("address")
                if not addr:
                    continue
                tags = {str(x) for x in (row.get("maker_tags") or row.get("tags") or [])}
                hit = sightings.get(str(addr))
                if hit is None:
                    hit = TradeSighting(address=str(addr))
                    sightings[str(addr)] = hit
                hit.tokens.add(mint)
                hit.sources.add(source)
                hit.tags |= tags
                event = str(row.get("event") or "").lower()
                if event == "sell":
                    hit.sell_events += 1
                else:
                    hit.buy_events += 1
            done += 1
            if done % 50 == 0 or done == len(ranked):
                print(
                    f"  [hub-extend scan] {done}/{len(ranked)} "
                    f"wallets={len(sightings)} (+{len(sightings)-before})",
                    flush=True,
                )
            if done % 100 == 0:
                _pulse()

    print(
        f"[hub-extend] done wallets={len(sightings)} "
        f"(+{len(sightings) - before}) tokens={len(token_pool)}",
        flush=True,
    )
    return sightings


def sightings_to_json(sightings: dict[str, TradeSighting]) -> list[dict]:
    rows = []
    for s in sightings.values():
        rows.append(
            {
                "address": s.address,
                "tokens": sorted(s.tokens),
                "buy_events": s.buy_events,
                "sell_events": s.sell_events,
                "realized_profit": s.realized_profit,
                "tags": sorted(s.tags),
                "sources": sorted(s.sources),
            }
        )
    return rows


def sightings_from_json(rows: list[dict]) -> dict[str, TradeSighting]:
    out: dict[str, TradeSighting] = {}
    for row in rows:
        addr = str(row.get("address") or "")
        if not addr:
            continue
        out[addr] = TradeSighting(
            address=addr,
            tokens=set(row.get("tokens") or []),
            buy_events=int(row.get("buy_events") or 0),
            sell_events=int(row.get("sell_events") or 0),
            realized_profit=float(row.get("realized_profit") or 0),
            tags=set(row.get("tags") or []),
            sources=set(row.get("sources") or []),
        )
    return out


def merge_sightings(
    *groups: dict[str, TradeSighting],
) -> dict[str, TradeSighting]:
    merged: dict[str, TradeSighting] = {}
    for group in groups:
        for addr, sight in group.items():
            prev = merged.get(addr)
            if prev is None:
                merged[addr] = TradeSighting(
                    address=sight.address,
                    tokens=set(sight.tokens),
                    buy_events=sight.buy_events,
                    sell_events=sight.sell_events,
                    realized_profit=sight.realized_profit,
                    tags=set(sight.tags),
                    sources=set(sight.sources),
                )
                continue
            prev.tokens |= sight.tokens
            prev.buy_events += sight.buy_events
            prev.sell_events += sight.sell_events
            prev.realized_profit += sight.realized_profit
            prev.tags |= sight.tags
            prev.sources |= sight.sources
    return merged


def select_candidates(
    sightings: dict[str, TradeSighting],
    *,
    prefer_tags: list[str] | None = None,
    bot_tags: list[str] | None = None,
    max_candidates: int = 300,
    prefer_only: bool = False,
) -> list[TradeSighting]:
    """
    Prefer axiom/smart_degen-tagged makers. Never rank by multi-token count or
    single-token whale PnL alone (that selected sandwich/whale bots before).
    """
    prefer = {t.lower() for t in (prefer_tags or list(DEFAULT_PREFER_TAGS))}
    bots = {t.lower() for t in (bot_tags or list(DEFAULT_BOT_TAGS))}

    usable: list[TradeSighting] = []
    for sight in sightings.values():
        tags_l = {t.lower() for t in sight.tags}
        if tags_l & bots:
            continue
        # Seeds always keep; others need at least one buy signal
        if "seed" not in sight.sources and sight.buy_events <= 0:
            continue
        usable.append(sight)

    def is_pref(s: TradeSighting) -> bool:
        if "seed" in s.sources:
            return True
        return bool({t.lower() for t in s.tags} & prefer)

    preferred = [s for s in usable if is_pref(s)]
    others = [s for s in usable if not is_pref(s)]

    # Rank by activity breadth lightly, then buys — not whale realized_profit
    def _hop_bonus(s: TradeSighting) -> int:
        # Long-tail hub hops are where Grapes-like (non-trending) wallets appear
        for hop in (4, 3, 2):
            if f"hub_traders_h{hop}" in s.sources:
                return hop
        if "hub_traders_h1" in s.sources:
            return 1
        return 0

    preferred.sort(
        key=lambda s: (
            1 if "seed" in s.sources else 0,
            _hop_bonus(s),
            len(s.tokens),
            s.buy_events,
            s.realized_profit,
        ),
        reverse=True,
    )
    others.sort(
        key=lambda s: (_hop_bonus(s), len(s.tokens), s.buy_events, s.realized_profit),
        reverse=True,
    )

    if prefer_only:
        picked = preferred[:max_candidates]
    else:
        # Reserve slots: long-tail hub prefer first, then other prefer, then rest
        def is_long_tail(s: TradeSighting) -> bool:
            return bool(
                s.sources
                & {"hub_traders_h2", "hub_traders_h3", "hub_traders_h4"}
            )

        hub2_pref = [s for s in preferred if is_long_tail(s)]
        other_pref = [s for s in preferred if not is_long_tail(s)]
        hub2_slots = min(len(hub2_pref), max(int(max_candidates * 0.7), 1))
        picked = hub2_pref[:hub2_slots]
        room = max_candidates - len(picked)
        if room > 0:
            picked.extend(other_pref[:room])
        room = max_candidates - len(picked)
        if room > 0:
            picked.extend(others[:room])

    print(
        f"[select] usable={len(usable)} preferred={len(preferred)} "
        f"longtail_pref={sum(1 for s in preferred if s.sources & {'hub_traders_h2','hub_traders_h3','hub_traders_h4'})} "
        f"picked={len(picked)}"
    )
    return picked
