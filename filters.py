"""Normalize wallet rows and apply performance / anti-minter filters."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Iterable


def _f(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _i(value: Any, default: int = 0) -> int:
    return int(_f(value, float(default)))


@dataclass(slots=True)
class WalletStats:
    address: str
    winrate_7d: float
    pnl_7d: float
    txs_7d: int
    realized_profit_7d: float
    buy_7d: int
    sell_7d: int
    volume_7d: float
    tags: list[str]
    name: str
    twitter: str
    last_active: int
    url: str
    created_token_count: int = 0
    winrate_source: str = ""
    reject_reasons: list[str] = field(default_factory=list)
    source: str = ""
    snipe_ratio: float | None = None

    @property
    def winrate_pct(self) -> float:
        return self.winrate_7d * 100

    @property
    def pnl_pct(self) -> float:
        return self.pnl_7d * 100

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["winrate_pct"] = round(self.winrate_pct, 2)
        data["pnl_pct"] = round(self.pnl_pct, 2)
        return data


def normalize(row: dict[str, Any], chain: str = "sol") -> WalletStats | None:
    address = row.get("wallet_address") or row.get("address")
    if not address:
        return None

    tags = row.get("tags") or row.get("wallet_tag") or []
    if isinstance(tags, str):
        tags = [tags]
    tags = [str(t) for t in tags]

    buy_7d = _i(row.get("buy_7d") or row.get("buy"))
    sell_7d = _i(row.get("sell_7d") or row.get("sell"))
    txs_7d = _i(row.get("txs_7d"))
    if txs_7d <= 0:
        txs_7d = buy_7d + sell_7d

    return WalletStats(
        address=str(address),
        winrate_7d=_f(row.get("winrate_7d") or row.get("winrate")),
        pnl_7d=_f(row.get("pnl_7d") or row.get("pnl")),
        txs_7d=txs_7d,
        realized_profit_7d=_f(row.get("realized_profit_7d")),
        buy_7d=buy_7d,
        sell_7d=sell_7d,
        volume_7d=_f(row.get("volume_7d")),
        tags=tags,
        name=str(row.get("name") or row.get("nickname") or ""),
        twitter=str(row.get("twitter_username") or row.get("twitter_name") or ""),
        last_active=_i(row.get("last_active") or row.get("last_active_timestamp")),
        url=f"https://gmgn.ai/{chain}/address/{address}",
        created_token_count=_i(row.get("created_token_count")),
        winrate_source=str(
            row.get("winrate_source")
            or (
                "leaderboard"
                if row.get("winrate_7d") not in (None, "")
                else ("walletNew" if row.get("winrate") not in (None, "") else "")
            )
        ),
        source=str(row.get("source") or "leaderboard"),
    )


def passes_filters(
    wallet: WalletStats,
    *,
    min_winrate: float,
    min_pnl: float,
    min_txs_7d: int,
    max_txs_7d: int | None = None,
    exclude_tags: Iterable[str] | None = None,
    max_created_tokens: int | None = None,
    require_winrate: bool = True,
    min_buy_ratio: float | None = None,
    max_snipe_ratio: float | None = None,
) -> bool:
    reasons: list[str] = []
    banned = {t.lower() for t in (exclude_tags or [])}

    if require_winrate and wallet.winrate_7d <= min_winrate:
        reasons.append("winrate")
    if wallet.pnl_7d <= min_pnl:
        reasons.append("pnl")
    if wallet.txs_7d <= min_txs_7d:
        reasons.append("txs_low")
    if max_txs_7d is not None and wallet.txs_7d > max_txs_7d:
        reasons.append("txs_bot")
    if banned and any(t.lower() in banned for t in wallet.tags):
        reasons.append("bot_tag")
    if (
        max_created_tokens is not None
        and wallet.created_token_count > max_created_tokens
    ):
        reasons.append(f"created_tokens={wallet.created_token_count}")
    # Soft floor only (not the sniper signal)
    if min_buy_ratio is not None:
        denom = max(wallet.buy_7d + wallet.sell_7d, 1)
        if (wallet.buy_7d / denom) < min_buy_ratio:
            reasons.append("buy_ratio")
    # Sniper = buys clustered at token open (right after launch / post-dev buy)
    if (
        max_snipe_ratio is not None
        and wallet.snipe_ratio is not None
        and wallet.snipe_ratio > max_snipe_ratio
    ):
        reasons.append(f"snipe_ratio={wallet.snipe_ratio:.0%}")

    wallet.reject_reasons = reasons
    return not reasons
