# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A delta-neutral cross-exchange arbitrage runtime: it tracks order/quote events for **Variational**
(forwarded out of the browser via a Chrome extension, since Variational has no public API) and
auto-hedges the opposite side on **Lighter** (which has a real REST/WS API + Python SDK). There is
no test suite, no CI, and no build step — this is a single operator-run trading bot, forked from
"YourQuantGuy"'s project and then automated.

## Writing style

- Never use Korean characters/text in code, comments, commit messages, or chat output — Chinese
  or English only.
- Keep prose rigorous and concise — this applies to code comments, docs, commit messages, and all
  conversational replies/communication with the user. No filler, no hedging, no restating the
  obvious.

## Commands

```bash
# setup
python3 -m venv env311 && source env311/bin/activate
pip install -r requirements.txt

# create .env with at minimum:
#   LIGHTER_ACCOUNT_INDEX=...
#   LIGHTER_API_KEY_INDEX=...
#   LIGHTER_PRIVATE_KEY=...

# run (default: Chinese dashboard, auto-hedge on)
python main.py

# flags
python main.py --no-hedge        # disable Lighter auto-hedge
python main.py --lang en         # English dashboard

# lint (max-line-length=129, see .flake8)
flake8 main.py variational/
```

There are no automated tests. Validate changes by running the bot against the live Variational
page + Lighter mainnet and watching `log/runtime.log` / the dashboard — see `notes/` for prior
incident analysis if something looks wrong.

**Startup order matters**: `python main.py` must be running *before* clicking "Start" in the
Chrome extension popup, because the extension connects to three local WebSocket ports
(8766/8767/8768) that `main.py` opens. The extension retries every second, but starting it first
risks missing the initial state.

## Architecture

```
Variational trading page (browser)
   │  CDP-injected page script intercepts REST/WS traffic
   ▼
chrome_extension/ (background.js, popup.js)
   │  forwards raw frames over local WebSocket
   ▼
variational/listener.py
   │  - EventSink: classifies REST/WS frames, feeds VariationalMonitor
   │  - VariationalMonitor: parses quotes/trades/portfolio, exposes get_trading_state()
   │  - CommandBroker: PLACE_ORDER request/response bridge back to the extension
   │    (ports 8766=ws frames, 8767=rest responses, 8768=command broker)
   ▼
main.py: VariationalToLighterRuntime
   │  - signal_loop      : evaluates cross-exchange spread every 0.5s, fires open/close orders
   │  - trade_loop       : polls Variational fill events (50ms), fallback hedge trigger
   │  - handle_lighter_ws: Lighter WS fills/order-book updates, P&L calc
   │  - lighter_sync_loop: every 60s, reconciles actual Lighter position vs. Variational
   │                       position to detect single-leg exposure
   │  - bbo_loop         : periodic BBO snapshot to log/bbo_<TICKER>_<YYYYMM>.csv
   │  - dashboard_loop   : renders the Rich terminal dashboard
   ▼
Lighter mainnet (lighter-sdk SignerClient) + log/ (runtime.log, order_metrics.jsonl, trade_records.csv)
```

`main.py` is one large file; `OrderLifecycle` (a dataclass) is the per-trade record tracking both
legs (Variational fill + Lighter hedge fill + matched P&L), keyed by `trade_key` (first 8 chars of
the Variational `rfq_id`).

### Strategy logic (signal_loop, main.py)

Delta-neutral: hold opposite-direction positions on both venues to capture the spread between them
(long Variational + short Lighter, or vice versa). Each tick:

1. **Gate checks** — skip the tick if an order is in-flight, still in cooldown
   (`VAR_ORDER_COOLDOWN_SECONDS`), or the two venues' mid prices diverge more than
   `VAR_MAX_PRICE_DEVIATION_PCT` (likely bad data).
2. **Compute thresholds** — open/close thresholds are *dynamic*, scaled off Lighter's own
   bid/ask spread (`lighter_internal_pct × VAR_SPREAD_MULTIPLIER` for open,
   `× VAR_CLOSE_MULTIPLIER` for close), since taking Lighter's far side always costs at least
   its own internal spread.
3. **Close before open** — reversal close (spread flipped against the position) takes priority
   over narrowing-close (spread reverted toward zero), which takes priority over opening new
   positions. Total notional is capped by `VAR_MAX_TOTAL_NOTIONAL_USDC`.
4. **Hedge immediately on order success**, not on fill-event arrival — Variational reduce-only
   (closing) fills don't always push a WS event, so waiting for one can permanently strand a
   hedge. `trade_loop` still listens for fill events as a fallback, deduped via `_pre_hedged`
   against double-hedging.

**Lighter sign convention gotcha**: on Lighter, `position > 0` means SHORT and `position < 0`
means LONG — inverted from Variational and most other exchanges. This affects
`lighter_sync_loop`, `load_initial_positions`, `place_lighter_order`'s BUY guard, and the optimistic
qty update — all four must stay consistent if this logic changes.

### Where to look for history/context

`notes/architecture_and_fixes.md` and `notes/signal_loop_and_parameters.md` describe the current
signal_loop logic line-for-line (including exact main.py line numbers) and document four past bugs
(single-leg detection, missed reduce-only hedges, stale fill-trigger condition, log noise) with
root causes — read these before touching `signal_loop`, `process_variational_trade_event`, or
`_trigger_variational_order`. `notes/session_20260526.md` is an older running log; its "Lighter IOC
close" design was since replaced by the immediate-hedge-on-order-success approach above, so treat
it as historical rather than current behavior.

### Output (./log, gitignored)

- `runtime.log` — app log (CST timestamps)
- `order_metrics.jsonl` — append-only per-event audit trail (`variational_fill`, `lighter_fill`, etc.)
- `trade_records.csv` — current snapshot, overwritten each dashboard refresh
- `bbo_<TICKER>_<YYYYMM>.csv` — periodic best-bid/ask snapshots, split monthly so restarts don't clobber history

### Env vars (`.env`, gitignored)

Required: `LIGHTER_ACCOUNT_INDEX`, `LIGHTER_API_KEY_INDEX`, `LIGHTER_PRIVATE_KEY`.
Optional `LIGHTER_WS_SERVER_PINGS=true` forces legacy application-level ping/pong instead of
relying on WS protocol ping frames. Strategy parameters (`VAR_SPREAD_MULTIPLIER`,
`VAR_CLOSE_MULTIPLIER`, `VAR_MIN_OPEN_SPREAD_PCT`, `VAR_NARROW_CLOSE_PCT`,
`VAR_NARROW_CLOSE_DELTA_PCT`, `VAR_ORDER_NOTIONAL_USDC`, `VAR_MAX_TOTAL_NOTIONAL_USDC`,
`VAR_ORDER_COOLDOWN_SECONDS`, `VAR_MAX_PRICE_DEVIATION_PCT`) all have defaults in
`VariationalToLighterRuntime.__init__` (main.py) — see `notes/signal_loop_and_parameters.md` for
what each one does and its current production value.
