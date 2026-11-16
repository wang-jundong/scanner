# Solana Wallet Scanner

Finds Solana traders matching:

| Filter | Value |
|---|---|
| Win rate (7d) | `> 58%` — **GMGN UI winrate only** |
| PnL % (7d) | `> 20%` |
| Trade count (7d) | `> 200` and `≤ 5000` |
| Buy ratio | `buy/(buy+sell) ≥ 0.20` |
| Token creates | `0` |
| Bot tags | excluded |

## The winrate problem (why token is needed)

| Source | Works for | Notes |
|---|---|---|
| Leaderboard `winrate_7d` | Ranked wallets | Same number as the UI (e.g. DfMxre = 18.87%) |
| `walletNew.winrate` | Rarely | Usually `null` |
| Activity estimate | **Never** | Was wrong (false DfMxre match) |
| `profit_stat` (auth) | **Any wallet** | Needed for Grapes.b5-like non-ranked traders |

Without a GMGN login token, the scanner can only use the leaderboard — and right now every ranked wallet that passes wr/pnl/txs is a **minter**.

## Setup: GMGN auth (required for Grapes-like wallets)

GMGN does **not** expose UI winrate for non-ranked wallets without auth.
Prefer an **OpenAPI key** (no 30‑minute expiry).

### Option A — API key (recommended)

1. Create a key at [gmgn.ai/ai](https://gmgn.ai/ai)
2. Save it:

```bash
echo 'gmgn_...' > /home/wang/work/scanner/.gmgn_api_key
chmod 600 /home/wang/work/scanner/.gmgn_api_key
```

### Option B — Browser Bearer (expires ~30m)

1. Log into [gmgn.ai](https://gmgn.ai) → DevTools → Network → `profit_stat`
2. Copy `Authorization: Bearer eyJ...`
3. `python bot.py --save-token --clipboard`

## Run

```bash
cd /home/wang/work/scanner
source .venv/bin/activate
python bot.py --validate-token   # optional: confirms API key / Bearer
python bot.py
```

## Check one wallet

```bash
python bot.py --validate-token
python bot.py --check ENGkTtUanFtqD5avHk5jJARwZNz5DX1AZ8TGQqASw8CT
python bot.py --check DfMxre4cKmvogbLrPigxmibVTTQDuzjdXojWzjCXXhzj
```

With a valid token, Grapes should show winrate source `profit_stat` (not `missing`).
