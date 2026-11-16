#!/usr/bin/env python3
"""Find Solana trader wallets with high winrate/PnL that are not token minters."""

from __future__ import annotations

import argparse
import csv
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml
from rich.console import Console
from rich.table import Table

from concurrent.futures import ThreadPoolExecutor, as_completed

from discovery import (
    TradeSighting,
    discover_from_seeds,
    discover_from_token_activity,
    discover_via_hub_expansion,
    extend_hub_sightings,
    merge_sightings,
    select_candidates,
    sightings_from_json,
    sightings_to_json,
)
from activity import estimate_snipe_ratio, fetch_wallet_activity
from filters import WalletStats, normalize, passes_filters
from gmgn import (
    GmgnAuthError,
    GmgnError,
    GmgnRateLimitError,
    _persist_open_ts_cache,
    collect_wallets,
    configure_open_ts_cache,
    fetch_wallet_common_stat,
    fetch_wallet_detail,
    fetch_wallet_profit_stat,
    configure_openapi_rate_limit,
    fetch_wallet_stats_openapi,
    get_access_token,
    get_api_key,
    has_gmgn_auth,
    load_access_token,
    load_api_key,
    resolve_winrate,
    token_seconds_left,
)

ROOT = Path(__file__).resolve().parent
console = Console()
NEED_TOKEN_FLAG = ROOT / "output" / "NEED_TOKEN"


def wait_for_fresh_token(token_path: Path, *, min_ttl_sec: float = 90.0) -> str:
    """
    Pause until .gmgn_token has a JWT with enough TTL left.

    Does not skip enrich work — caller retries after this returns.
    Paste a new Bearer into chat / overwrite .gmgn_token to resume.
    """
    NEED_TOKEN_FLAG.parent.mkdir(parents=True, exist_ok=True)
    NEED_TOKEN_FLAG.write_text(
        "GMGN Bearer expired. Paste a fresh Authorization token, then save to .gmgn_token\n"
        "  python bot.py --save-token --clipboard\n"
        "or paste into chat so the agent can write .gmgn_token\n"
    )
    left = token_seconds_left()
    console.print(
        "\n[bold red]TOKEN EXPIRED / REJECTED[/] — enrich paused (not skipped).\n"
        f"Current TTL≈{left:.0f}s. Update [cyan]{token_path}[/] with a fresh Bearer, then wait.\n"
        "Commands:\n"
        "  [cyan]python bot.py --save-token --clipboard[/]\n"
        "Or paste the JWT into chat.\n"
        f"Flag file: [cyan]{NEED_TOKEN_FLAG}[/]\n"
    )
    last_mtime: float | None = None
    if token_path.is_file():
        last_mtime = token_path.stat().st_mtime
    while True:
        time.sleep(2.0)
        if not token_path.is_file():
            continue
        mtime = token_path.stat().st_mtime
        if last_mtime is not None and mtime <= last_mtime:
            # Still poll exp in case file was rewritten with same mtime precision
            pass
        tok = load_access_token(token_file=token_path)
        if not tok:
            continue
        ttl = token_seconds_left(tok)
        if ttl is None or ttl < min_ttl_sec:
            if mtime != last_mtime:
                console.print(
                    f"[yellow]Token file updated but TTL too short[/] "
                    f"(≈{ttl if ttl is not None else 'n/a'}s, need ≥{min_ttl_sec:.0f}s)"
                )
                last_mtime = mtime
            continue
        # Live check — profit_stat can 403 intermittently; walletNew is enough.
        probe_addr = "ENGkTtUanFtqD5avHk5jJARwZNz5DX1AZ8TGQqASw8CT"
        ok = False
        try:
            fetch_wallet_profit_stat(probe_addr, period="7d")
            ok = True
        except GmgnAuthError:
            try:
                fetch_wallet_detail(probe_addr, chain="sol")
                ok = True
                console.print(
                    "[yellow]profit_stat probe failed; walletNew ok — accepting token[/]"
                )
            except GmgnAuthError:
                if mtime != last_mtime:
                    console.print(
                        "[yellow]New token still rejected by GMGN[/] — need a fresher Bearer"
                    )
                    last_mtime = mtime
                continue
            except GmgnError as exc:
                console.print(f"[yellow]Token TTL ok; probe warning:[/] {exc}")
                ok = True
        except GmgnError as exc:
            console.print(f"[yellow]Token TTL ok; probe warning:[/] {exc}")
            ok = True
        if not ok:
            continue
        last_mtime = mtime
        if NEED_TOKEN_FLAG.exists():
            NEED_TOKEN_FLAG.unlink(missing_ok=True)
        console.print(
            f"[bold green]Token accepted[/] (TTL≈{ttl:.0f}s) — resuming enrich\n"
        )
        return tok


def load_config(path: Path) -> dict[str, Any]:
    with path.open() as fh:
        return yaml.safe_load(fh) or {}


def save_results(wallets: list[WalletStats], output_dir: Path) -> tuple[Path, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    latest_json = output_dir / "wallets_latest.json"
    latest_csv = output_dir / "wallets_latest.csv"
    stamped_json = output_dir / f"wallets_{stamp}.json"
    stamped_csv = output_dir / f"wallets_{stamp}.csv"

    payload = {
        "scanned_at": datetime.now(timezone.utc).isoformat(),
        "count": len(wallets),
        "wallets": [w.to_dict() for w in wallets],
    }
    for path in (latest_json, stamped_json):
        path.write_text(json.dumps(payload, indent=2))

    fieldnames = [
        "address",
        "winrate_pct",
        "pnl_pct",
        "txs_7d",
        "buy_7d",
        "sell_7d",
        "created_token_count",
        "winrate_source",
        "realized_profit_7d",
        "tags",
        "source",
        "url",
    ]
    for path in (latest_csv, stamped_csv):
        with path.open("w", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=fieldnames)
            writer.writeheader()
            for wallet in wallets:
                row = wallet.to_dict()
                writer.writerow(
                    {
                        "address": row["address"],
                        "winrate_pct": row["winrate_pct"],
                        "pnl_pct": row["pnl_pct"],
                        "txs_7d": row["txs_7d"],
                        "buy_7d": row["buy_7d"],
                        "sell_7d": row["sell_7d"],
                        "created_token_count": row["created_token_count"],
                        "winrate_source": row["winrate_source"],
                        "realized_profit_7d": round(row["realized_profit_7d"], 2),
                        "tags": ",".join(row["tags"]),
                        "source": row["source"],
                        "url": row["url"],
                    }
                )
    return latest_json, latest_csv


def print_table(wallets: list[WalletStats], filters: dict[str, Any]) -> None:
    table = Table(
        title=(
            f"Traders | wr>{filters['min_winrate']*100:.0f}% "
            f"pnl>{filters['min_pnl']*100:.0f}% txs>{filters['min_txs_7d']} "
            f"| created_tokens=0"
        )
    )
    table.add_column("#", justify="right")
    table.add_column("Address")
    table.add_column("Winrate", justify="right")
    table.add_column("PnL", justify="right")
    table.add_column("Txs", justify="right")
    table.add_column("Created", justify="right")
    table.add_column("Src")
    table.add_column("Tags")

    for idx, wallet in enumerate(wallets, start=1):
        table.add_row(
            str(idx),
            wallet.address,
            f"{wallet.winrate_pct:.1f}%",
            f"{wallet.pnl_pct:.1f}%",
            str(wallet.txs_7d),
            str(wallet.created_token_count),
            wallet.source[:10],
            ", ".join(wallet.tags[:3]),
        )
    console.print(table)
    console.print(f"[bold green]{len(wallets)}[/] wallets matched")


def enrich_wallet(
    address: str,
    *,
    chain: str,
    delay_sec: float,
    source: str,
    need_winrate: bool = True,
    need_common: bool = True,
    leaderboard_row: dict[str, Any] | None = None,
) -> WalletStats | None:
    """Build WalletStats — prefers OpenAPI wallet_stats when API key is set."""
    detail: dict[str, Any] = {}
    common: dict[str, Any] = {}
    openapi_stats: dict[str, Any] | None = None

    # Fast path: one OpenAPI call covers wr + pnl + created + tags
    if get_api_key() and (need_winrate or need_common):
        try:
            openapi_stats = fetch_wallet_stats_openapi(address, chain=chain, period="7d")
            common_o = openapi_stats.get("common") if isinstance(
                openapi_stats.get("common"), dict
            ) else {}
            detail = {
                "pnl_7d": openapi_stats.get("realized_profit_pnl")
                or openapi_stats.get("pnl"),
                "buy_7d": openapi_stats.get("buy"),
                "sell_7d": openapi_stats.get("sell"),
                "realized_profit_7d": openapi_stats.get("realized_profit"),
                "tags": common_o.get("tags") or [],
                "name": common_o.get("name") or "",
                "twitter_username": common_o.get("twitter_username"),
                "last_active_timestamp": openapi_stats.get("last_timestamp"),
                "_openapi_stats": openapi_stats,
            }
            if need_common:
                if common_o.get("created_token_count") in (None, ""):
                    console.print(
                        f"    openapi missing created_token_count {address[:10]}"
                    )
                    return None
                common = common_o
        except GmgnAuthError:
            raise
        except GmgnError as exc:
            console.print(f"    openapi stats fail {address[:10]}: {exc}")
            openapi_stats = None

    if not detail:
        try:
            detail = fetch_wallet_detail(address, chain=chain)
        except GmgnAuthError:
            raise
        except GmgnError as exc:
            # CF often blocks public walletNew; profit_stat still has buy/sell/pnl.
            console.print(f"    detail fail {address[:10]}: {exc}")
            if get_access_token():
                try:
                    profit = fetch_wallet_profit_stat(address, chain=chain, period="7d")
                    detail = {
                        "pnl_7d": profit.get("realized_profit_pnl") or profit.get("pnl"),
                        "buy_7d": profit.get("buy"),
                        "sell_7d": profit.get("sell"),
                        "realized_profit_7d": profit.get("realized_profit"),
                        "tags": [],
                        "name": "",
                        "last_active_timestamp": profit.get("last_active_timestamp"),
                        "_profit_stat": profit,
                    }
                    console.print(f"    detail fallback profit_stat {address[:10]}")
                except GmgnAuthError:
                    raise
                except GmgnError as exc2:
                    console.print(f"    profit_stat fail {address[:10]}: {exc2}")
                    return None
            else:
                return None

    if need_common and not common:
        last_common_exc: Exception | None = None
        for attempt in range(1, 4):
            try:
                common = fetch_wallet_common_stat(address, chain=chain)
                last_common_exc = None
                break
            except GmgnAuthError:
                raise
            except GmgnRateLimitError as exc:
                last_common_exc = exc
                console.print(
                    f"    common_stat CF {address[:10]} "
                    f"(retry {attempt}/3) — backing off"
                )
                time.sleep(4 * attempt)
            except GmgnError as exc:
                # Never treat a failed minter check as created_tokens=0 (false PASS).
                console.print(f"    common_stat fail {address[:10]}: {exc}")
                return None
        if last_common_exc is not None:
            raise last_common_exc
        if common.get("created_token_count") in (None, ""):
            console.print(f"    common_stat missing created_token_count {address[:10]}")
            return None

    if delay_sec > 0:
        time.sleep(delay_sec)

    created_count = 0
    if need_common:
        created_count = int(common.get("created_token_count") or 0)

    row = {
        "address": address,
        "pnl_7d": detail.get("pnl_7d") or detail.get("pnl"),
        "buy_7d": detail.get("buy_7d") or detail.get("buy"),
        "sell_7d": detail.get("sell_7d") or detail.get("sell"),
        "realized_profit_7d": detail.get("realized_profit_7d"),
        "tags": common.get("tags") or detail.get("tags") or [],
        "name": common.get("name") or detail.get("name") or "",
        "twitter_username": common.get("twitter_username") or detail.get("twitter_username"),
        "last_active_timestamp": detail.get("last_active_timestamp"),
        "created_token_count": created_count,
        "source": source,
    }

    # Prefer leaderboard row buy/sell/pnl/txs when present (matches UI Analysis)
    if leaderboard_row:
        if leaderboard_row.get("pnl_7d") not in (None, ""):
            row["pnl_7d"] = leaderboard_row["pnl_7d"]
        if leaderboard_row.get("buy") not in (None, ""):
            row["buy_7d"] = leaderboard_row["buy"]
        if leaderboard_row.get("sell") not in (None, ""):
            row["sell_7d"] = leaderboard_row["sell"]
        if leaderboard_row.get("txs_7d") not in (None, ""):
            row["txs_7d"] = leaderboard_row["txs_7d"]
        if leaderboard_row.get("realized_profit_7d") not in (None, ""):
            row["realized_profit_7d"] = leaderboard_row["realized_profit_7d"]

    if not need_winrate:
        row["winrate_7d"] = 0.0
        row["winrate_source"] = "skipped"
    else:
        wr, wr_src = resolve_winrate(
            address,
            chain=chain,
            leaderboard_row=leaderboard_row,
            detail=detail,
            openapi_stats=openapi_stats,
        )
        row["winrate_7d"] = float(wr or 0.0)
        row["winrate_source"] = wr_src

    return normalize(row, chain=chain)


def check_address(address: str, cfg: dict[str, Any]) -> int:
    chain = cfg.get("chain", "sol")
    filters_cfg = cfg.get("filters") or {}
    anti = cfg.get("anti_minter") or {}
    delay = float(cfg.get("request_delay_sec", 0.3))
    min_buy_ratio = filters_cfg.get("min_buy_ratio")

    console.print(f"Checking [cyan]{address}[/]")
    lb_cfg = cfg.get("leaderboard") or {}
    raw = collect_wallets(
        chain=chain,
        period=cfg.get("period", "7d"),
        tags=lb_cfg.get("tags") or ["axiom", "arbitrager", "all", "pump_smart"],
        orderbys=["pnl_7d", "winrate_7d", "txs_7d"],
        delay_sec=delay,
    )
    wallet = enrich_wallet(
        address,
        chain=chain,
        delay_sec=delay,
        source="check",
        leaderboard_row=raw.get(address),
    )
    if wallet is None:
        return 1

    max_txs = filters_cfg.get("max_txs_7d")
    ok = passes_filters(
        wallet,
        min_winrate=float(filters_cfg.get("min_winrate", 0.58)),
        min_pnl=float(filters_cfg.get("min_pnl", 0.20)),
        min_txs_7d=int(filters_cfg.get("min_txs_7d", 200)),
        max_txs_7d=int(max_txs) if max_txs is not None else None,
        exclude_tags=cfg.get("exclude_tags") or [],
        max_created_tokens=int(anti.get("max_created_tokens", 0)),
        min_buy_ratio=float(min_buy_ratio) if min_buy_ratio is not None else None,
    )
    console.print(
        f"winrate={wallet.winrate_pct:.2f}% ({wallet.winrate_source})  "
        f"pnl={wallet.pnl_pct:.2f}%  txs={wallet.txs_7d}  "
        f"buy/sell={wallet.buy_7d}/{wallet.sell_7d}  "
        f"created_tokens={wallet.created_token_count}  tags={wallet.tags}"
    )
    if wallet.winrate_source == "missing":
        console.print(
            "[yellow]No GMGN winrate[/]. Put your logged-in Bearer token in "
            "[cyan].gmgn_token[/] (see README) so profit_stat can read UI winrate "
            "for non-ranked wallets like Grapes.b5."
        )
    if ok:
        console.print("[green]PASS[/] — would be included")
    else:
        console.print(f"[red]FAIL[/] — {', '.join(wallet.reject_reasons)}")
    console.print(wallet.url)
    return 0


def _load_hit_cache(path: Path) -> dict[str, dict[str, Any]]:
    if not path.is_file():
        return {}
    try:
        raw = json.loads(path.read_text())
        return raw if isinstance(raw, dict) else {}
    except Exception:
        return {}


def _save_hit_cache(path: Path, cache: dict[str, dict[str, Any]]) -> None:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(cache))
    except Exception:
        pass


def parallel_hub_prescreen(
    sightings: dict[str, TradeSighting],
    *,
    chain: str,
    prefer_tags: list[str],
    exclude_tags: list[str],
    filter_kwargs: dict[str, Any],
    pending: dict[str, str],
    note_pending,
    leaderboard_raw: dict[str, dict[str, Any]],
    max_checks: int = 2500,
    workers: int = 8,
    hit_cache_path: Path | None = None,
    hit_cache_ttl_sec: int = 21600,
) -> int:
    """
    Parallel walletNew pnl/txs filter over hub-expanded prefer-tagged wallets.

    Ranking by token_traders buy counts buries Grapes-like wallets; checking
    many hub_traders_h2 candidates with walletNew surfaces them instead.
    """
    prefer = {t.lower() for t in prefer_tags}
    bots = {t.lower() for t in exclude_tags}
    hit_cache = _load_hit_cache(hit_cache_path) if hit_cache_path else {}
    now = time.time()

    def is_pref(s: TradeSighting) -> bool:
        tags_l = {t.lower() for t in s.tags}
        if tags_l & bots:
            return False
        return bool(tags_l & prefer) or "seed" in s.sources

    # Long-tail hops (h2+) are where Grapes-like wallets appear
    hub2 = [
        s
        for s in sightings.values()
        if (
            "hub_traders_h2" in s.sources
            or "hub_traders_h3" in s.sources
            or "hub_traders_h4" in s.sources
        )
        and is_pref(s)
        and s.address not in pending
    ]
    hub2_addrs = {s.address for s in hub2}
    hub1 = [
        s
        for s in sightings.values()
        if s.address not in hub2_addrs
        and any(str(x).startswith("hub_traders") for x in s.sources)
        and is_pref(s)
        and s.address not in pending
    ]
    hub1_addrs = {s.address for s in hub1}
    other = [
        s
        for s in sightings.values()
        if s.address not in hub2_addrs
        and s.address not in hub1_addrs
        and is_pref(s)
        and s.address not in pending
    ]
    # More hub-token sightings first (Grapes-like show up on several niche mints).
    hub2.sort(key=lambda s: (-len(s.tokens), s.buy_events, s.address))
    hub1.sort(key=lambda s: (-len(s.tokens), s.buy_events, s.address))
    other.sort(key=lambda s: (-len(s.tokens), s.buy_events, s.address))
    full = hub2 + hub1 + other

    # Reuse recent pnl/txs hits — skip walletNew for those addresses
    cached_hits = 0
    to_fetch: list[TradeSighting] = []
    for s in full:
        if len(to_fetch) + cached_hits >= max_checks and s.address not in hit_cache:
            continue
        entry = hit_cache.get(s.address)
        if (
            entry
            and entry.get("pass")
            and now - float(entry.get("saved_at") or 0) <= hit_cache_ttl_sec
        ):
            note_pending(s.address, "trades")
            cached_hits += 1
            continue
        if len(to_fetch) < max_checks:
            to_fetch.append(s)
    queue = to_fetch[:max_checks]
    console.print(
        f"Hub parallel prescreen: fetch=[bold]{len(queue)}[/] "
        f"cached_hits={cached_hits} (h2={len(hub2)} h1={len(hub1)}) "
        f"workers={workers}"
    )

    def _one(addr: str) -> tuple[str, WalletStats | None]:
        try:
            detail = fetch_wallet_detail(addr, chain=chain)
        except GmgnError:
            return addr, None
        row = {
            "address": addr,
            "pnl_7d": detail.get("pnl_7d") or detail.get("pnl"),
            "buy_7d": detail.get("buy_7d") or detail.get("buy"),
            "sell_7d": detail.get("sell_7d") or detail.get("sell"),
            "realized_profit_7d": detail.get("realized_profit_7d"),
            "tags": detail.get("tags") or [],
            "winrate_7d": 0.0,
            "winrate_source": "skipped",
            "created_token_count": 0,
            "source": "trades",
        }
        lb = leaderboard_raw.get(addr)
        if lb:
            for k_src, k_dst in (
                ("pnl_7d", "pnl_7d"),
                ("buy", "buy_7d"),
                ("sell", "sell_7d"),
                ("txs_7d", "txs_7d"),
            ):
                if lb.get(k_src) not in (None, ""):
                    row[k_dst] = lb[k_src]
        return addr, normalize(row, chain=chain)

    hits = cached_hits
    done = 0
    with ThreadPoolExecutor(max_workers=max(workers, 1)) as pool:
        futures = {pool.submit(_one, s.address): s for s in queue}
        for fut in as_completed(futures):
            done += 1
            addr, rough = fut.result()
            if done % 50 == 0 or done == len(queue):
                console.print(f"  parallel [{done}/{len(queue)}] hits={hits}")
            if rough is None:
                continue
            if passes_filters(rough, require_winrate=False, **filter_kwargs):
                note_pending(addr, "trades")
                hits += 1
                hit_cache[addr] = {
                    "pass": True,
                    "saved_at": now,
                    "pnl": rough.pnl_7d,
                    "txs": rough.txs_7d,
                }
                console.print(
                    f"  [green]pnl/txs[/] {addr[:12]}... "
                    f"pnl={rough.pnl_pct:.1f}% txs={rough.txs_7d}"
                )
    if hit_cache_path:
        _save_hit_cache(hit_cache_path, hit_cache)
    return hits


def run_once(cfg: dict[str, Any]) -> list[WalletStats]:
    chain = cfg.get("chain", "sol")
    filters_cfg = cfg.get("filters") or {}
    exclude_tags = list(cfg.get("exclude_tags") or [])
    anti = cfg.get("anti_minter") or {}
    delay = float(cfg.get("request_delay_sec", 0.3))
    max_created = int(anti.get("max_created_tokens", 0))
    token_path = Path((cfg.get("gmgn") or {}).get("token_file") or (ROOT / ".gmgn_token"))
    if not token_path.is_absolute():
        token_path = ROOT / token_path
    out_dir = ROOT / cfg.get("output_dir", "output")
    configure_open_ts_cache(out_dir / "open_ts_cache.json")
    if not get_api_key():
        ttl_boot = token_seconds_left()
        if ttl_boot is None or ttl_boot < 90:
            console.print(
                "[yellow]No API key / Bearer expired — waiting for a fresh Bearer "
                "(or add .gmgn_api_key)[/]"
            )
            wait_for_fresh_token(token_path)

    min_wr = float(filters_cfg.get("min_winrate", 0.58))
    min_pnl = float(filters_cfg.get("min_pnl", 0.20))
    min_txs = int(filters_cfg.get("min_txs_7d", 200))
    max_txs = filters_cfg.get("max_txs_7d")
    max_txs_i = int(max_txs) if max_txs is not None else None
    prefer_tags = list(cfg.get("prefer_tags") or [])

    min_buy_ratio = filters_cfg.get("min_buy_ratio")
    sniper_cfg = cfg.get("anti_sniper") or {}
    sniper_on = bool(sniper_cfg.get("enabled", False))
    max_snipe_ratio = (
        float(sniper_cfg["max_snipe_ratio"])
        if sniper_on and sniper_cfg.get("max_snipe_ratio") is not None
        else None
    )
    filter_kwargs = dict(
        min_winrate=min_wr,
        min_pnl=min_pnl,
        min_txs_7d=min_txs,
        max_txs_7d=max_txs_i,
        exclude_tags=exclude_tags,
        max_created_tokens=max_created,
        min_buy_ratio=float(min_buy_ratio) if min_buy_ratio is not None else None,
        max_snipe_ratio=max_snipe_ratio,
    )

    # Addresses that survive pnl/txs + real winrate_7d, pending created_tokens check
    pending: dict[str, str] = {}  # address -> source
    raw: dict[str, dict[str, Any]] = {}

    pending_path = ROOT / cfg.get("output_dir", "output") / "pending_enrich.json"

    def flush_pending() -> None:
        try:
            pending_path.write_text(json.dumps(pending, indent=0))
        except Exception:  # noqa: BLE001
            pass

    def note_pending(address: str, source: str) -> None:
        if address and address not in pending:
            pending[address] = source
            flush_pending()

    # --- Leaderboard: GMGN's real winrate_7d (same number as the UI) ---
    lb_cfg = cfg.get("leaderboard") or {}
    if lb_cfg.get("enabled", True):
        raw = collect_wallets(
            chain=chain,
            period=cfg.get("period", "7d"),
            tags=lb_cfg.get("tags") or ["axiom", "smart_degen", "all"],
            orderbys=lb_cfg.get("orderbys") or ["pnl_7d", "txs_7d", "winrate_7d"],
            delay_sec=delay,
            min_winrate_7d=min_wr,
            min_pnl_7d=min_pnl,
        )
        console.print(f"Leaderboard unique wallets: [bold]{len(raw)}[/]")
        lb_hits = 0
        for row in raw.values():
            row = dict(row)
            row["source"] = "leaderboard"
            # Rank payload uses winrate_7d — map into normalize field
            if row.get("winrate_7d") not in (None, "") and row.get("winrate") in (None, ""):
                row["winrate"] = row.get("winrate_7d")
            row["winrate_source"] = (
                "leaderboard" if row.get("winrate_7d") not in (None, "") else ""
            )
            wallet = normalize(row, chain=chain)
            if wallet is None:
                continue
            if wallet.winrate_source != "leaderboard":
                continue  # no authoritative GMGN winrate → skip
            if passes_filters(wallet, require_winrate=True, **filter_kwargs):
                note_pending(wallet.address, "leaderboard")
                lb_hits += 1
        console.print(f"Leaderboard filter hits (real winrate_7d): [bold]{lb_hits}[/]")

    # --- Trade discovery (needs access token for UI winrate on non-ranked wallets) ---
    td_cfg = cfg.get("trade_discovery") or {}
    has_token = has_gmgn_auth()
    td_enabled = bool(td_cfg.get("enabled", False))
    if td_enabled and not has_token:
        console.print(
            "[yellow]trade_discovery enabled but no GMGN API key/Bearer — skipping. "
            "Add .gmgn_api_key or .gmgn_token (see README).[/]"
        )
        td_enabled = False

    sightings = {}
    if td_enabled:
        trade_sightings, _creators = discover_from_token_activity(
            chain=chain,
            timeframes=td_cfg.get("timeframes"),
            orderbys=td_cfg.get("orderbys"),
            tokens_limit=int(td_cfg.get("tokens_limit", 50)),
            trades_per_token=int(td_cfg.get("trades_per_token", 50)),
            trade_pages=int(td_cfg.get("trade_pages", 4)),
            delay_sec=delay,
        )
        sightings = merge_sightings(sightings, trade_sightings)

    # Hub expansion reaches non-trending tokens where Grapes-like wallets trade
    hub_cfg = cfg.get("hub_expansion") or {}
    if hub_cfg.get("enabled", False):
        cache_path = ROOT / cfg.get("output_dir", "output") / "hub_sightings_cache.json"
        cache_ttl = int(hub_cfg.get("cache_ttl_sec", 7200))
        hub_sightings: dict[str, TradeSighting] = {}
        loaded_cache = False
        stale_cache: dict[str, TradeSighting] = {}
        if cache_path.is_file() and not hub_cfg.get("force_refresh", False):
            try:
                cached = json.loads(cache_path.read_text())
                age = time.time() - float(cached.get("saved_at", 0))
                rows = cached.get("sightings") or []
                if rows:
                    parsed = sightings_from_json(rows)
                    if age <= cache_ttl:
                        hub_sightings = parsed
                        loaded_cache = True
                        console.print(
                            f"[cyan]Hub cache[/] loaded {len(hub_sightings)} wallets "
                            f"(age {int(age)}s / ttl {cache_ttl}s)"
                        )
                    else:
                        stale_cache = parsed
                        # With OpenAPI key, keep stale graph + extend instead of
                        # blocking on a browser Bearer for web hub refresh.
                        if get_api_key():
                            hub_sightings = stale_cache
                            loaded_cache = True
                            console.print(
                                f"[yellow]Hub cache stale[/] ({len(stale_cache)} wallets, "
                                f"age {int(age)}s) — keeping it (API key mode; no Bearer wait)"
                            )
                        else:
                            console.print(
                                f"[yellow]Hub cache stale[/] ({len(stale_cache)} wallets, "
                                f"age {int(age)}s > ttl {cache_ttl}s) — will refresh"
                            )
            except Exception as exc:  # noqa: BLE001
                console.print(f"[yellow]Hub cache unreadable:[/] {exc}")

        def _ensure_web_token(label: str) -> None:
            """Bearer only needed for gmgn.ai web discovery — skip if API key set."""
            if get_api_key():
                return
            left = token_seconds_left()
            if left is None or left < 90:
                console.print(
                    f"[yellow]{label}: token low/expired — pausing for a fresh Bearer[/]"
                )
                wait_for_fresh_token(token_path)

        # Hop2-only caches miss Grapes niches — extend one hop from prefer hubs
        if loaded_cache and int(hub_cfg.get("hops", 2)) >= 3 and hub_cfg.get(
            "extend_cache", True
        ):
            has_h3 = any(
                "hub_traders_h3" in s.sources for s in hub_sightings.values()
            )
            if not has_h3:
                console.print(
                    "[cyan]Hub extend[/] hop3 from prefer hubs "
                    "(cache has no hub_traders_h3 yet)"
                )
                try:
                    before_n = len(hub_sightings)
                    # OpenAPI: force serial workers — parallel always 429s
                    aw = int(hub_cfg.get("activity_workers", 1))
                    tw = int(hub_cfg.get("trader_workers", 1))
                    if get_api_key():
                        aw = min(aw, 1)
                        tw = min(tw, 1)
                    hub_sightings = extend_hub_sightings(
                        hub_sightings,
                        chain=chain,
                        prefer_tags=prefer_tags,
                        hubs_per_hop=int(hub_cfg.get("hubs_per_hop", 80)),
                        activity_pages=int(hub_cfg.get("activity_pages", 2)),
                        traders_limit=int(hub_cfg.get("traders_limit", 100)),
                        max_long_tail=int(hub_cfg.get("max_long_tail", 1500)),
                        trader_workers=tw,
                        activity_workers=aw,
                        hop_label=3,
                        ensure_token=lambda: _ensure_web_token("Hub extend"),
                    )
                    console.print(
                        f"[cyan]Hub extend[/] {before_n} → {len(hub_sightings)} wallets"
                    )
                    try:
                        cache_path.parent.mkdir(parents=True, exist_ok=True)
                        cache_path.write_text(
                            json.dumps(
                                {
                                    "saved_at": time.time(),
                                    "count": len(hub_sightings),
                                    "sightings": sightings_to_json(hub_sightings),
                                }
                            )
                        )
                        console.print(f"[cyan]Hub cache[/] updated → {cache_path}")
                    except Exception as exc:  # noqa: BLE001
                        console.print(f"[yellow]Hub cache save failed:[/] {exc}")
                except GmgnAuthError as exc:
                    console.print(f"[red]Hub extend auth fail:[/] {exc}")
                    if not get_api_key():
                        wait_for_fresh_token(token_path)
        if not loaded_cache:
            console.print("[cyan]Hub expansion[/] (co-trader graph from trending hubs)")
            hub_checkpoint = (
                ROOT / cfg.get("output_dir", "output") / "hub_checkpoint.json"
            )

            hub_kwargs = dict(
                chain=chain,
                timeframes=hub_cfg.get("timeframes") or td_cfg.get("timeframes"),
                orderbys=hub_cfg.get("orderbys") or td_cfg.get("orderbys"),
                tokens_limit=int(hub_cfg.get("tokens_limit", 25)),
                prefer_tags=prefer_tags,
                hops=int(hub_cfg.get("hops", 2)),
                hubs_per_hop=int(hub_cfg.get("hubs_per_hop", 50)),
                activity_pages=int(hub_cfg.get("activity_pages", 2)),
                expand_tokens=int(hub_cfg.get("expand_tokens", 80)),
                traders_limit=int(hub_cfg.get("traders_limit", 80)),
                delay_sec=float(hub_cfg.get("delay_sec", delay)),
                trader_workers=int(hub_cfg.get("trader_workers", 24)),
                activity_workers=int(hub_cfg.get("activity_workers", 12)),
                max_long_tail=int(hub_cfg.get("max_long_tail", 500)),
                checkpoint_path=hub_checkpoint,
                ensure_token=lambda: _ensure_web_token("Hub"),
            )
            while True:
                try:
                    hub_sightings = discover_via_hub_expansion(**hub_kwargs)
                    break
                except GmgnAuthError as exc:
                    console.print(f"[red]Hub expansion auth fail:[/] {exc}")
                    if get_api_key() and stale_cache:
                        hub_sightings = stale_cache
                        console.print(
                            f"[yellow]Using stale hub cache[/] "
                            f"({len(hub_sightings)} wallets) — API key mode"
                        )
                        break
                    wait_for_fresh_token(token_path)
            if not hub_sightings and stale_cache:
                hub_sightings = stale_cache
                console.print(
                    f"[yellow]Keeping stale hub cache[/] ({len(hub_sightings)} wallets) "
                    "— refresh returned empty/failed"
                )
            elif hub_sightings:
                try:
                    cache_path.parent.mkdir(parents=True, exist_ok=True)
                    cache_path.write_text(
                        json.dumps(
                            {
                                "saved_at": time.time(),
                                "count": len(hub_sightings),
                                "sightings": sightings_to_json(hub_sightings),
                            }
                        )
                    )
                    console.print(f"[cyan]Hub cache[/] saved → {cache_path}")
                except Exception as exc:  # noqa: BLE001
                    console.print(f"[yellow]Hub cache save failed:[/] {exc}")
            else:
                console.print(
                    "[yellow]Hub refresh empty — not overwriting any prior cache file[/]"
                )
        sightings = merge_sightings(sightings, hub_sightings)
        console.print(f"After hub expansion: [bold]{len(sightings)}[/] unique wallets")

    seed_cfg = cfg.get("seed_discovery") or {}
    if seed_cfg.get("enabled", False) and has_token:
        seeds = list(seed_cfg.get("wallets") or [])
        if seeds:
            seed_sightings = discover_from_seeds(
                seeds,
                chain=chain,
                activity_pages=int(seed_cfg.get("activity_pages", 6)),
                tokens_per_seed=int(seed_cfg.get("tokens_per_seed", 25)),
                trades_per_token=int(seed_cfg.get("trades_per_token", 50)),
                trade_pages=int(seed_cfg.get("trade_pages", 3)),
                include_traders=bool(seed_cfg.get("include_traders", True)),
                traders_limit=int(seed_cfg.get("traders_limit", 40)),
                delay_sec=delay,
            )
            sightings = merge_sightings(sightings, seed_sightings)

    if pending_path.is_file() and not pending:
        try:
            loaded = json.loads(pending_path.read_text())
            if isinstance(loaded, dict) and loaded:
                pending.update({str(k): str(v) for k, v in loaded.items()})
                console.print(
                    f"[cyan]Pending enrich[/] loaded {len(pending)} wallets "
                    "(still prescreening new hub candidates)"
                )
        except Exception as exc:  # noqa: BLE001
            console.print(f"[yellow]Pending enrich unreadable:[/] {exc}")

    # Always prescreen addresses not already in pending — a leftover 1-wallet
    # pending must not freeze the whole Grapes-like discovery funnel.
    if sightings:
        hub_cfg = cfg.get("hub_expansion") or {}
        use_parallel = bool(hub_cfg.get("enabled")) and any(
            any(str(x).startswith("hub_traders") for x in s.sources)
            for s in sightings.values()
        )
        if use_parallel:
            trade_hits = parallel_hub_prescreen(
                sightings,
                chain=chain,
                prefer_tags=prefer_tags,
                exclude_tags=exclude_tags,
                filter_kwargs=filter_kwargs,
                pending=pending,
                note_pending=note_pending,
                leaderboard_raw=raw,
                max_checks=int(hub_cfg.get("prescreen_limit", 2500)),
                workers=(
                    1
                    if get_api_key()
                    else int(hub_cfg.get("prescreen_workers", 8))
                ),
                hit_cache_path=ROOT
                / cfg.get("output_dir", "output")
                / "prescreen_hit_cache.json",
                hit_cache_ttl_sec=int(hub_cfg.get("hit_cache_ttl_sec", 21600)),
            )
            console.print(f"Hub parallel pnl/txs hits: [bold]{trade_hits}[/]")
            flush_pending()
        else:
            picked = select_candidates(
                sightings,
                prefer_tags=prefer_tags,
                bot_tags=exclude_tags,
                max_candidates=int(td_cfg.get("max_candidates", 800)),
                prefer_only=bool(td_cfg.get("prefer_only", False)),
            )
            console.print(
                f"Trade-discovery candidates: [bold]{len(picked)}[/] / {len(sightings)}"
            )
            trade_hits = 0
            for idx, sight in enumerate(picked, start=1):
                if sight.address in pending:
                    continue
                console.print(
                    f"  prescreen [{idx}/{len(picked)}] {sight.address[:12]}... "
                    f"tags={sorted(sight.tags)[:3]}"
                )
                rough = enrich_wallet(
                    sight.address,
                    chain=chain,
                    delay_sec=0,
                    source="trades",
                    need_winrate=False,
                    need_common=False,
                    leaderboard_row=raw.get(sight.address),
                )
                if rough is None:
                    continue
                if passes_filters(rough, require_winrate=False, **filter_kwargs):
                    note_pending(sight.address, "trades")
                    trade_hits += 1
            console.print(f"Trade pnl/txs hits: [bold]{trade_hits}[/]")

    # --- Full enrich: authoritative winrate + created_tokens (+ sniper after pass) ---
    if not get_api_key():
        ttl0 = token_seconds_left()
        if ttl0 is None or ttl0 < 90:
            console.print("[yellow]Token missing or nearly expired before enrich[/]")
            wait_for_fresh_token(token_path)
    enrich_workers = int(hub_cfg.get("enrich_workers", 2)) if hub_cfg else 2
    if get_api_key():
        enrich_workers = min(enrich_workers, 1)
    console.print(
        f"Full enrich on [bold]{len(pending)}[/] wallets "
        f"(workers={enrich_workers})"
    )
    final: list[WalletStats] = []
    pending_list = list(pending.items())
    # filter kwargs without sniper for the first pass (wr/created/tags only)
    pre_snipe_kwargs = {**filter_kwargs, "max_snipe_ratio": None}

    def _enrich_one(item: tuple[str, str]) -> tuple[str, str, WalletStats | None, str]:
        address, src = item
        try:
            wallet = enrich_wallet(
                address,
                chain=chain,
                delay_sec=0,
                source=src,
                leaderboard_row=raw.get(address),
            )
        except GmgnAuthError as exc:
            return address, src, None, f"auth:{exc}"
        except GmgnRateLimitError as exc:
            return address, src, None, f"cf:{exc}"
        except Exception as exc:  # noqa: BLE001
            return address, src, None, f"err:{exc}"
        if wallet is None:
            return address, src, None, "none"
        if wallet.winrate_source == "missing":
            return address, src, wallet, "no_wr"
        if not passes_filters(wallet, require_winrate=True, **pre_snipe_kwargs):
            return address, src, wallet, "pre_fail"
        # Sniper only after wr/created pass
        if sniper_on and max_snipe_ratio is not None:
            try:
                acts = fetch_wallet_activity(
                    address,
                    chain=chain,
                    max_pages=2,
                    delay_sec=0,
                )
                ratio, snipes, scored = estimate_snipe_ratio(
                    acts,
                    chain=chain,
                    max_seconds_after_open=int(
                        sniper_cfg.get("max_seconds_after_open", 3)
                    ),
                    sample_buys=int(sniper_cfg.get("sample_buys", 20)),
                    delay_sec=0,
                )
                wallet.snipe_ratio = ratio
                if ratio is not None and not passes_filters(
                    wallet, require_winrate=True, **filter_kwargs
                ):
                    return address, src, wallet, "snipe_fail"
                if ratio is not None:
                    return address, src, wallet, f"snipe_ok:{ratio:.0%}:{snipes}/{scored}"
            except GmgnAuthError as exc:
                return address, src, wallet, f"auth:{exc}"
            except GmgnRateLimitError as exc:
                return address, src, wallet, f"cf:{exc}"
            except GmgnError as exc:
                return address, src, wallet, f"snipe_skip:{exc}"
        return address, src, wallet, "ok"

    remaining = list(pending_list)
    done_n = 0
    total_n = len(remaining)
    cf_rounds = 0
    while remaining:
        batch = remaining[: max(enrich_workers, 1)]
        remaining = remaining[len(batch) :]
        auth_block: list[tuple[str, str]] = []
        cf_block: list[tuple[str, str]] = []
        with ThreadPoolExecutor(max_workers=max(enrich_workers, 1)) as pool:
            futs = [pool.submit(_enrich_one, item) for item in batch]
            for fut in as_completed(futs):
                address, src, wallet, status = fut.result()
                done_n += 1
                if status.startswith("auth:"):
                    auth_block.append((address, src))
                    console.print(f"  enrich [{done_n}/{total_n}] {address[:12]}... auth")
                    continue
                if status.startswith("cf:"):
                    cf_block.append((address, src))
                    console.print(
                        f"  enrich [{done_n}/{total_n}] {address[:12]}... "
                        f"[yellow]CF — will retry[/]"
                    )
                    continue
                console.print(f"  enrich [{done_n}/{total_n}] {address[:12]}... ({src})")
                if status.startswith("err:"):
                    console.print(f"    error: {status[4:]}")
                    continue
                if status == "none":
                    continue
                if status == "no_wr":
                    console.print("    dropped: no GMGN winrate in response")
                    continue
                if status == "pre_fail" and wallet is not None:
                    console.print(f"    dropped: {', '.join(wallet.reject_reasons)}")
                    continue
                if status == "snipe_fail" and wallet is not None:
                    console.print(
                        f"    dropped: {', '.join(wallet.reject_reasons)} "
                        f"(snipe_ratio={wallet.snipe_ratio})"
                    )
                    continue
                if status.startswith("snipe_skip:"):
                    console.print(f"    snipe check skipped: {status[11:]}")
                elif status.startswith("snipe_ok:"):
                    parts = status.split(":")
                    console.print(
                        f"    snipe_ratio={parts[1]} ({parts[2]} buys within "
                        f"{int(sniper_cfg.get('max_seconds_after_open', 3))}s of open)"
                    )
                if wallet is not None and (
                    status == "ok"
                    or status.startswith("snipe_ok")
                    or status.startswith("snipe_skip")
                ):
                    if passes_filters(wallet, require_winrate=True, **filter_kwargs):
                        final.append(wallet)
                        console.print(
                            f"    [green]MATCH[/] wr={wallet.winrate_pct:.1f}% "
                            f"({wallet.winrate_source}) pnl={wallet.pnl_pct:.1f}% "
                            f"txs={wallet.txs_7d} created={wallet.created_token_count}"
                        )
                    else:
                        console.print(
                            f"    dropped: {', '.join(wallet.reject_reasons)}"
                        )
        if auth_block:
            console.print(
                f"[red]auth fail[/] on {len(auth_block)} wallets — pausing for Bearer"
            )
            wait_for_fresh_token(token_path)
            remaining = auth_block + remaining
            done_n -= len(auth_block)
        if cf_block:
            cf_rounds += 1
            wait_s = min(8 * cf_rounds, 45)
            console.print(
                f"[yellow]CF rate-limit[/] on {len(cf_block)} wallets — "
                f"sleep {wait_s}s then retry (round {cf_rounds})"
            )
            # Keep unfinished wallets in pending so a kill/restart can resume
            for addr, src in cf_block:
                pending[addr] = src
            flush_pending()
            time.sleep(wait_s)
            remaining = cf_block + remaining
            done_n -= len(cf_block)
            if cf_rounds >= 8:
                console.print(
                    "[red]CF still blocking after 8 rounds — "
                    "saving remaining to pending_enrich.json; re-run later[/]"
                )
                for addr, src in remaining:
                    pending[addr] = src
                flush_pending()
                break

    _persist_open_ts_cache()
    final.sort(key=lambda w: (w.winrate_7d, w.pnl_7d, w.txs_7d), reverse=True)
    print_table(final, filters_cfg)
    if not final:
        hint = (
            "Add GMGN access token to [cyan].gmgn_token[/] and enable "
            "trade_discovery to find non-ranked traders like Grapes.b5."
            if not has_token
            else "No non-minter wallet currently passed the filters."
        )
        console.print(
            f"\n[yellow]0 matches[/] (wr>{min_wr:.0%} pnl>{min_pnl:.0%} "
            f"txs>{min_txs}, created_tokens=0).\n{hint}"
        )
    json_path, csv_path = save_results(final, ROOT / cfg.get("output_dir", "output"))
    console.print(f"Saved [cyan]{json_path}[/] and [cyan]{csv_path}[/]")
    # Only clear pending when nothing left to retry
    if not remaining and pending_path.is_file():
        try:
            pending_path.unlink()
            console.print(f"[cyan]Cleared[/] {pending_path.name}")
        except OSError:
            pass
    elif remaining:
        console.print(
            f"[yellow]Kept[/] {len(remaining)} wallets in {pending_path.name} for retry"
        )
    return final


def _read_clipboard() -> str:
    import shutil
    import subprocess

    if shutil.which("xclip"):
        return subprocess.check_output(
            ["xclip", "-selection", "clipboard", "-o"], text=True
        )
    if shutil.which("wl-paste"):
        return subprocess.check_output(["wl-paste"], text=True)
    raise RuntimeError("No clipboard tool (install xclip or wl-paste)")


def save_access_token(raw: str, path: Path) -> Path:
    token = raw.strip()
    if token.lower().startswith("authorization:"):
        token = token.split(":", 1)[1].strip()
    if token.lower().startswith("bearer "):
        token = token[7:].strip()
    if not token or len(token) < 20:
        raise ValueError("Token looks empty/too short — copy the full Bearer JWT")
    path.write_text(token + "\n")
    path.chmod(0o600)
    return path


def validate_access_token(cfg: dict[str, Any]) -> int:
    """Hit profit_stat for DfMxre + Grapes to confirm token works."""
    from gmgn import fetch_wallet_profit_stat, set_access_token, get_access_token

    if not get_access_token():
        console.print("[red]No token loaded[/]")
        return 1
    chain = cfg.get("chain", "sol")
    probes = [
        ("DfMxre", "DfMxre4cKmvogbLrPigxmibVTTQDuzjdXojWzjCXXhzj"),
        ("Grapes", "ENGkTtUanFtqD5avHk5jJARwZNz5DX1AZ8TGQqASw8CT"),
    ]
    ok_any = False
    for name, addr in probes:
        try:
            data = fetch_wallet_profit_stat(addr, chain=chain, period="7d")
            nested = data.get("pnl_detail") if isinstance(data.get("pnl_detail"), dict) else {}
            wr = data.get("winrate")
            if wr is None:
                wr = data.get("winrate_7d")
            if wr is None:
                wr = nested.get("winrate")
            console.print(
                f"[green]OK[/] {name}: winrate={wr} buy={data.get('buy')} sell={data.get('sell')}"
            )
            ok_any = True
        except GmgnError as exc:
            console.print(f"[red]FAIL[/] {name}: {exc}")
    if not ok_any:
        console.print(
            "[red]Token invalid/expired.[/] Copy a fresh Authorization Bearer "
            "from DevTools → Network → profit_stat/holdings, then:\n"
            "  python bot.py --save-token --clipboard"
        )
        return 1
    console.print("[bold green]Token works[/] — run: python bot.py")
    return 0


def probe_hub_discovery(cfg: dict[str, Any], address: str) -> int:
    """Validate hub expansion can surface a known Grapes-like wallet (public APIs)."""
    hub_cfg = cfg.get("hub_expansion") or {}
    td_cfg = cfg.get("trade_discovery") or {}
    prefer_tags = list(cfg.get("prefer_tags") or [])
    delay = float(hub_cfg.get("delay_sec", cfg.get("request_delay_sec", 0.1)))
    console.print(f"Probing hub expansion for [cyan]{address}[/]")
    sightings = discover_via_hub_expansion(
        chain=cfg.get("chain", "sol"),
        timeframes=hub_cfg.get("timeframes") or td_cfg.get("timeframes"),
        orderbys=hub_cfg.get("orderbys") or td_cfg.get("orderbys"),
        tokens_limit=int(hub_cfg.get("tokens_limit", 25)),
        prefer_tags=prefer_tags,
        hops=int(hub_cfg.get("hops", 2)),
        hubs_per_hop=int(hub_cfg.get("hubs_per_hop", 50)),
        activity_pages=int(hub_cfg.get("activity_pages", 2)),
        expand_tokens=int(hub_cfg.get("expand_tokens", 80)),
        traders_limit=int(hub_cfg.get("traders_limit", 80)),
        delay_sec=delay,
        trader_workers=int(hub_cfg.get("trader_workers", 24)),
        activity_workers=int(hub_cfg.get("activity_workers", 12)),
        max_long_tail=int(hub_cfg.get("max_long_tail", 500)),
    )
    # Persist for subsequent --once enrich within JWT TTL
    cache_path = ROOT / cfg.get("output_dir", "output") / "hub_sightings_cache.json"
    try:
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        cache_path.write_text(
            json.dumps(
                {
                    "saved_at": time.time(),
                    "count": len(sightings),
                    "sightings": sightings_to_json(sightings),
                }
            )
        )
        console.print(f"[cyan]Hub cache[/] saved → {cache_path}")
    except Exception as exc:  # noqa: BLE001
        console.print(f"[yellow]Hub cache save failed:[/] {exc}")
    hit = sightings.get(address)
    console.print(f"Candidates: [bold]{len(sightings)}[/]")
    if hit:
        console.print(
            f"[green]FOUND[/] {address}\n"
            f"  tags={sorted(hit.tags)}\n"
            f"  sources={sorted(hit.sources)}\n"
            f"  tokens_seen={len(hit.tokens)} buys={hit.buy_events}"
        )
        prefer = {t.lower() for t in prefer_tags}
        longtail = {"hub_traders_h2", "hub_traders_h3", "hub_traders_h4"}
        hub2_pref = sorted(
            (
                s
                for s in sightings.values()
                if (s.sources & longtail) and ({t.lower() for t in s.tags} & prefer)
            ),
            key=lambda s: (-len(s.tokens), s.buy_events, s.address),
        )
        limit = int(hub_cfg.get("prescreen_limit", 8000))
        addrs = [s.address for s in hub2_pref]
        in_queue = address in addrs[:limit]
        idx = addrs.index(address) + 1 if address in addrs else -1
        hit = next((s for s in hub2_pref if s.address == address), None)
        console.print(
            f"Long-tail prefer-tagged: {len(hub2_pref)} | "
            f"prescreen rank (tokens desc): {idx}/{limit} "
            f"tokens={len(hit.tokens) if hit else 0} buys={hit.buy_events if hit else None} | "
            f"would_prescreen={in_queue}"
        )
        return 0 if in_queue else 2
    console.print(f"[red]NOT FOUND[/] {address} — widen hub_expansion hops/hubs/tokens")
    return 1


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Solana trader wallet scanner (anti-minter)")
    p.add_argument("-c", "--config", default=str(ROOT / "config.yaml"))
    p.add_argument("--once", action="store_true")
    p.add_argument("--check", help="Explain why a wallet passes/fails filters")
    p.add_argument("--min-winrate", type=float)
    p.add_argument("--min-pnl", type=float)
    p.add_argument("--min-txs", type=int)
    p.add_argument(
        "--save-token",
        action="store_true",
        help="Save GMGN Bearer token to .gmgn_token (from --clipboard or stdin)",
    )
    p.add_argument(
        "--clipboard",
        action="store_true",
        help="With --save-token: read token from clipboard",
    )
    p.add_argument(
        "--validate-token",
        action="store_true",
        help="Test .gmgn_token against profit_stat",
    )
    p.add_argument(
        "--probe-hubs",
        action="store_true",
        help="Run hub-expansion discovery only; report if example wallet is reachable",
    )
    p.add_argument(
        "--probe-address",
        default="ENGkTtUanFtqD5avHk5jJARwZNz5DX1AZ8TGQqASw8CT",
        help="Address to look for during --probe-hubs (default: Grapes.b5)",
    )
    return p.parse_args()


def main() -> int:
    args = parse_args()
    cfg_path = Path(args.config)
    if not cfg_path.exists():
        console.print(f"[red]Config not found:[/] {cfg_path}")
        return 1
    cfg = load_config(cfg_path)
    cfg.setdefault("filters", {})
    if args.min_winrate is not None:
        cfg["filters"]["min_winrate"] = args.min_winrate
    if args.min_pnl is not None:
        cfg["filters"]["min_pnl"] = args.min_pnl
    if args.min_txs is not None:
        cfg["filters"]["min_txs_7d"] = args.min_txs

    gmgn_cfg = cfg.get("gmgn") or {}
    token_path = Path(gmgn_cfg.get("token_file") or (ROOT / ".gmgn_token"))
    api_key_path = Path(gmgn_cfg.get("api_key_file") or (ROOT / ".gmgn_api_key"))
    if not api_key_path.is_absolute():
        api_key_path = ROOT / api_key_path

    if args.save_token:
        try:
            raw = _read_clipboard() if args.clipboard else sys.stdin.read()
            save_access_token(raw, token_path)
        except Exception as exc:  # noqa: BLE001
            console.print(f"[red]Save failed:[/] {exc}")
            return 1
        console.print(f"[green]Saved[/] {token_path}")
        load_access_token(token_file=token_path)
        return validate_access_token(cfg)

    api_key = load_api_key(
        config_key=gmgn_cfg.get("api_key"),
        key_file=api_key_path,
    )
    token = load_access_token(
        config_token=gmgn_cfg.get("access_token"),
        token_file=token_path,
    )
    if api_key:
        hub_lim = cfg.get("hub_expansion") or {}
        rate = float(hub_lim.get("openapi_rate", gmgn_cfg.get("openapi_rate", 4.5)))
        cap = float(hub_lim.get("openapi_capacity", gmgn_cfg.get("openapi_capacity", 5)))
        configure_openapi_rate_limit(rate=rate, capacity=cap)
        console.print(
            f"[green]GMGN API key loaded[/] — OpenAPI "
            f"(leaky bucket {rate:g}/{cap:g}; Free plan is 5/5)"
        )
    elif token:
        console.print("[green]GMGN access token loaded[/] — profit_stat winrate enabled")
    else:
        console.print(
            "[yellow]No GMGN API key or Bearer[/] — enrich cannot read UI winrate.\n"
            "Preferred: put OpenAPI key in [cyan].gmgn_api_key[/]\n"
            "Or Bearer: [cyan]python bot.py --save-token --clipboard[/]"
        )

    if args.validate_token:
        if api_key:
            try:
                stats = fetch_wallet_stats_openapi(
                    "ENGkTtUanFtqD5avHk5jJARwZNz5DX1AZ8TGQqASw8CT",
                    chain=cfg.get("chain", "sol"),
                    period="7d",
                )
                wr = (stats.get("pnl_stat") or {}).get("winrate")
                console.print(
                    f"[green]API key OK[/] Grapes winrate={wr} "
                    f"created={(stats.get('common') or {}).get('created_token_count')}"
                )
                return 0
            except Exception as exc:  # noqa: BLE001
                console.print(f"[red]API key failed:[/] {exc}")
                return 1
        return validate_access_token(cfg)

    if args.probe_hubs:
        return probe_hub_discovery(cfg, args.probe_address.strip())

    if args.check:
        return check_address(args.check.strip(), cfg)

    interval = 0 if args.once else int(cfg.get("loop_interval_sec") or 0)
    if interval <= 0:
        run_once(cfg)
        return 0

    console.print(f"Looping every {interval}s (Ctrl+C to stop)")
    while True:
        try:
            run_once(cfg)
            console.print(f"Sleeping {interval}s...")
            time.sleep(interval)
        except KeyboardInterrupt:
            console.print("\nStopped.")
            return 0


if __name__ == "__main__":
    sys.exit(main())
