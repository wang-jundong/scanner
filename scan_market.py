#!/usr/bin/env python3
"""Market-wide scan for filter matches. No Grapes/example seed."""

from __future__ import annotations

import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from bot import enrich_wallet, load_config, print_table, save_results
from discovery import _ingest_trade_row, fetch_token_trades_pages
from filters import normalize, passes_filters
from gmgn import fetch_token_traders, fetch_trending_tokens, fetch_wallet_rank

ROOT = Path(__file__).resolve().parent


def log(msg: str) -> None:
    print(msg, flush=True)


def main() -> int:
    cfg = load_config(ROOT / "config.yaml")
    chain = cfg.get("chain", "sol")
    f = cfg["filters"]
    anti = cfg["anti_minter"]
    exclude = list(cfg.get("exclude_tags") or [])
    prefer = {t.lower() for t in (cfg.get("prefer_tags") or [])}
    ban = {t.lower() for t in exclude}
    delay = 0.08
    min_buy_ratio = f.get("min_buy_ratio")
    fk = dict(
        min_winrate=float(f["min_winrate"]),
        min_pnl=float(f["min_pnl"]),
        min_txs_7d=int(f["min_txs_7d"]),
        max_txs_7d=int(f["max_txs_7d"]),
        exclude_tags=exclude,
        max_created_tokens=int(anti["max_created_tokens"]),
        min_buy_ratio=float(min_buy_ratio) if min_buy_ratio is not None else None,
    )

    pending: dict[str, str] = {}

    # --- Leaderboard (axiom/arbitrager/etc) ---
    tags = (cfg.get("leaderboard") or {}).get("tags") or [
        "axiom",
        "arbitrager",
        "smart_degen",
        "all",
    ]
    orderbys = (cfg.get("leaderboard") or {}).get("orderbys") or [
        "pnl_7d",
        "txs_7d",
        "realized_profit_7d",
        "last_active",
    ]
    for tag in tags:
        for orderby in orderbys:
            try:
                rows = fetch_wallet_rank(
                    chain=chain, period="7d", tag=tag, orderby=orderby, limit=100
                )
            except Exception as exc:  # noqa: BLE001
                log(f"lb FAIL {tag}/{orderby}: {exc}")
                continue
            hits = 0
            for row in rows:
                wallet = normalize({**row, "source": "leaderboard"}, chain=chain)
                if wallet and passes_filters(wallet, require_winrate=False, **fk):
                    pending[wallet.address] = "leaderboard"
                    hits += 1
            log(f"lb {tag}/{orderby} rows={len(rows)} hits={hits} pending={len(pending)}")
            time.sleep(delay)

    log(f"after leaderboard pending={len(pending)}")

    # --- Trending makers with prefer tags ---
    tokens = fetch_trending_tokens(
        chain=chain,
        timeframes=["1m", "5m", "1h", "6h", "24h"],
        orderbys=["volume", "swaps"],
        limit=35,
        delay_sec=delay,
    )
    creators = {str(t["creator"]) for t in tokens if t.get("creator")}
    sightings: dict = {}
    for idx, token in enumerate(tokens[:90], start=1):
        mint = str(token.get("address") or "")
        if not mint:
            continue
        try:
            trades = fetch_token_trades_pages(
                mint, chain=chain, limit=50, max_pages=2, delay_sec=0.05
            )
        except Exception:  # noqa: BLE001
            trades = []
        for row in trades:
            tags_l = {str(x).lower() for x in (row.get("maker_tags") or [])}
            if prefer and not (tags_l & prefer):
                continue
            if tags_l & ban:
                continue
            _ingest_trade_row(
                sightings, row, mint=mint, creators=creators, source="trades"
            )
        try:
            traders = fetch_token_traders(mint, chain=chain, limit=25)
            for row in traders:
                tags_l = {
                    str(x).lower()
                    for x in (row.get("tags") or row.get("maker_tags") or [])
                }
                if prefer and not (tags_l & prefer):
                    continue
                if tags_l & ban:
                    continue
                _ingest_trade_row(
                    sightings, row, mint=mint, creators=creators, source="traders"
                )
        except Exception:  # noqa: BLE001
            pass
        if idx % 15 == 0 or idx == len(tokens[:90]):
            log(f"tokens {idx}/{min(90, len(tokens))} preferred_makers={len(sightings)}")
        time.sleep(delay)

    ranked = sorted(
        sightings.values(),
        key=lambda s: (len(s.tokens), s.buy_events),
        reverse=True,
    )[:500]
    log(f"screening {len(ranked)} trade candidates")

    def rough_check(address: str):
        return enrich_wallet(
            address,
            chain=chain,
            delay_sec=0,
            source="trades",
            need_winrate=False,
            need_common=False,
        )

    screened = 0
    with ThreadPoolExecutor(max_workers=6) as pool:
        futs = {
            pool.submit(rough_check, s.address): s
            for s in ranked
            if s.address not in pending
        }
        for fut in as_completed(futs):
            screened += 1
            sight = futs[fut]
            try:
                rough = fut.result()
            except Exception:  # noqa: BLE001
                continue
            if rough and passes_filters(rough, require_winrate=False, **fk):
                pending[sight.address] = "trades"
                log(
                    f"pre {sight.address[:12]} pnl={rough.pnl_pct:.1f} "
                    f"txs={rough.txs_7d} tags={sorted(sight.tags)[:3]}"
                )
            if screened % 40 == 0:
                log(f"  screened {screened}/{len(futs)} pending={len(pending)}")

    log(f"full enrich {len(pending)} wallets")
    final = []
    for idx, (address, src) in enumerate(list(pending.items()), start=1):
        wallet = enrich_wallet(address, chain=chain, delay_sec=delay, source=src)
        if wallet is None:
            continue
        ok = passes_filters(wallet, require_winrate=True, **fk)
        log(
            f"[{idx}/{len(pending)}] {address[:12]} wr={wallet.winrate_pct:.1f} "
            f"pnl={wallet.pnl_pct:.1f} txs={wallet.txs_7d} "
            f"created={wallet.created_token_count} ok={ok} {wallet.reject_reasons}"
        )
        if ok:
            final.append(wallet)

    final.sort(key=lambda w: (w.winrate_7d, w.pnl_7d, w.txs_7d), reverse=True)
    print_table(final, f)
    save_results(final, ROOT / cfg.get("output_dir", "output"))
    log(f"DONE matches={len(final)}")
    for wallet in final:
        log(
            f"{wallet.address} wr={wallet.winrate_pct:.1f} pnl={wallet.pnl_pct:.1f} "
            f"txs={wallet.txs_7d} created={wallet.created_token_count} "
            f"tags={wallet.tags} src={wallet.source}"
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
