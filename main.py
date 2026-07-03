import argparse
import asyncio
import contextlib
import csv
import json
import logging
import os
import signal
import time
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from decimal import Decimal
from pathlib import Path
from statistics import median
from typing import Any

import requests
import websockets
from dotenv import load_dotenv
from lighter.signer_client import SignerClient
from rich.console import Console, Group
from rich.live import Live
from rich.panel import Panel
from rich.table import Table

from variational.listener import (
    HEARTBEAT_STALE_SECONDS,
    CommandBroker,
    EventSink,
    VariationalMonitor,
    run_command_server,
    run_receiver_server,
)

VARIATIONAL_TICKER_OVERRIDES = {
    "LIT": "LIGHTER",
}
VARIATIONAL_ASSET_TO_LIGHTER_TICKER = {v: k for k, v in VARIATIONAL_TICKER_OVERRIDES.items()}

FORWARDER_HOST = "127.0.0.1"
FORWARDER_WS_PORT = 8766
FORWARDER_REST_PORT = 8767
FORWARDER_COMMAND_PORT = 8768
LOG_DIR = Path("./log")
OUTPUT_DIR = LOG_DIR
APP_LOG_FILE = LOG_DIR / "runtime.log"
TRADE_RECORDS_CSV_FILE = LOG_DIR / "trade_records.csv"
READY_TIMEOUT_SECONDS = 60.0
POLL_INTERVAL_SECONDS = 0.05
HEDGE_SLIPPAGE_BPS = 100.0
DASHBOARD_REFRESH_SECONDS = 1.0
DASHBOARD_ORDERS = 8
SPREAD_HISTORY_SECONDS = 3600.0
ASSET_SWITCH_CONFIRM_TICKS = 3
LIGHTER_WS_URL = "wss://mainnet.zklighter.elliot.ai/stream"
LIGHTER_WS_PING_INTERVAL_SECONDS = 30
LIGHTER_WS_PING_TIMEOUT_SECONDS = 30


CST = timezone(timedelta(hours=8))

def utc_now() -> str:
    return datetime.now(CST).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]

def to_cst_str(iso: str) -> str:
    """Convert any ISO timestamp (UTC with Z, or offset-aware, or naive CST) to CST string."""
    try:
        iso_clean = iso.replace("Z", "+00:00")
        dt = datetime.fromisoformat(iso_clean)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=CST)
        return dt.astimezone(CST).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]
    except Exception:
        return iso


def quote_age_ms(ts_raw: Any) -> float | None:
    """Milliseconds elapsed since a quote's own timestamp (epoch or ISO8601)."""
    if ts_raw is None:
        return None
    try:
        ts_num = float(ts_raw)
        ts_sec = ts_num / 1000 if ts_num > 1e12 else ts_num
        return (time.time() - ts_sec) * 1000
    except (TypeError, ValueError):
        pass
    try:
        s = str(ts_raw).replace("Z", "+00:00")
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=CST)
        return (datetime.now(timezone.utc) - dt.astimezone(timezone.utc)).total_seconds() * 1000
    except Exception:
        return None


def to_decimal(value: Any) -> Decimal | None:
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except Exception:
        return None


def decimal_to_str(value: Decimal | None) -> str | None:
    if value is None:
        return None
    return f"{value:,.6f}"


def resolve_variational_ticker(ticker: str) -> str:
    return VARIATIONAL_TICKER_OVERRIDES.get(ticker.upper(), ticker.upper())


def resolve_lighter_ticker(variational_asset: str) -> str:
    asset = variational_asset.upper()
    return VARIATIONAL_ASSET_TO_LIGHTER_TICKER.get(asset, asset)


def required_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"{name} is not set")
    return value


def required_int_env(name: str) -> int:
    value = required_env(name)
    try:
        return int(value)
    except ValueError as exc:
        raise RuntimeError(f"{name} must be an integer, got: {value}") from exc


def env_flag(name: str) -> bool:
    value = os.getenv(name, "").strip().lower()
    return value in {"1", "true", "yes", "on"}


def spread_value(aggressive_buy_ask: Decimal | None, aggressive_sell_bid: Decimal | None) -> Decimal | None:
    if aggressive_buy_ask is None or aggressive_sell_bid is None:
        return None
    return aggressive_sell_bid - aggressive_buy_ask


def spread_percent(diff: Decimal | None, denominator: Decimal | None) -> Decimal | None:
    if diff is None or denominator is None or denominator == 0:
        return None
    return (diff / denominator) * Decimal("100")


def book_spread_percent(bid: Decimal | None, ask: Decimal | None) -> Decimal | None:
    if bid is None or ask is None:
        return None
    mid = (bid + ask) / Decimal("2")
    if mid == 0:
        return None
    return ((ask - bid) / mid) * Decimal("100")


def normalize_variational_status(status: str) -> str:
    lowered = status.strip().lower()
    if lowered == "confirmed":
        return "filled"
    return lowered


@dataclass(slots=True)
class OrderLifecycle:
    trade_key: str
    trade_id: str
    side: str
    qty: Decimal
    asset: str
    auto_hedge_enabled: bool
    last_variational_status: str

    var_fill_price: Decimal | None = None
    var_fill_ts_iso: str | None = None

    lighter_side: str | None = None
    lighter_client_order_id: int | None = None
    lighter_fill_price: Decimal | None = None
    lighter_fill_ts_iso: str | None = None
    lighter_tx_hash: str | None = None
    hedge_error: str | None = None

    matched_open_key: str | None = None   # FIFO matched open trade (for close legs)
    var_pnl: Decimal | None = None        # Variational内部盈亏：(var_close - var_open) × qty
    lt_pnl: Decimal | None = None         # Lighter内部盈亏：(lt_open - lt_close) × qty
    roundtrip_pnl: Decimal | None = None  # 总盈亏 = var_pnl + lt_pnl

    def to_payload(self) -> dict[str, Any]:
        return {
            "trade_key": self.trade_key,
            "trade_id": self.trade_id,
            "side": self.side,
            "qty": decimal_to_str(self.qty),
            "asset": self.asset,
            "variational_filled_price": decimal_to_str(self.var_fill_price),
            "variational_filled_at": self.var_fill_ts_iso,
            "lighter_order_side": self.lighter_side,
            "lighter_client_order_id": self.lighter_client_order_id,
            "lighter_filled_price": decimal_to_str(self.lighter_fill_price),
            "lighter_filled_at": self.lighter_fill_ts_iso,
            "auto_hedge_enabled": self.auto_hedge_enabled,
            "hedge_error": self.hedge_error,
            "last_variational_status": self.last_variational_status,
        }


class VariationalRuntime:
    def __init__(
        self,
        host: str,
        ws_port: int,
        rest_port: int,
        command_port: int,
        output_dir: Path | None,
        quiet: bool,
    ) -> None:
        self.monitor = VariationalMonitor(trade_limit=500, snapshot_file=None)
        self.sink = EventSink(output_dir=output_dir, quiet=quiet, monitor=self.monitor)
        self.broker = CommandBroker(quiet=True)
        self.host = host
        self.ws_port = ws_port
        self.rest_port = rest_port
        self.command_port = command_port
        self.ws_server = None
        self.rest_server = None
        self.command_server = None

    async def start(self) -> None:
        self.ws_server = await run_receiver_server("ws", self.host, self.ws_port, self.sink)
        self.rest_server = await run_receiver_server("rest", self.host, self.rest_port, self.sink)
        self.command_server = await run_command_server(self.host, self.command_port, self.broker)

    async def stop(self) -> None:
        if self.command_server is not None:
            self.command_server.close()
            await self.command_server.wait_closed()
        if self.ws_server is not None:
            self.ws_server.close()
            await self.ws_server.wait_closed()
        if self.rest_server is not None:
            self.rest_server.close()
            await self.rest_server.wait_closed()


class VariationalToLighterRuntime:
    def __init__(self, args: argparse.Namespace):
        self.args = args
        self.ticker: str | None = None
        self.variational_ticker: str | None = None
        self.accepted_assets: set[str] = set()

        self.stop_flag = False
        self.logger = logging.getLogger("var_lighter_runtime")
        self.logger.setLevel(logging.INFO)
        self.logger.handlers.clear()
        self.logger.propagate = False

        LOG_DIR.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(APP_LOG_FILE, encoding="utf-8")
        _cst = timezone(timedelta(hours=8))
        _fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(filename)s:%(lineno)d | %(message)s")
        _fmt.formatTime = lambda record, datefmt=None: datetime.fromtimestamp(record.created, tz=_cst).strftime("%y/%m/%d %H:%M:%S,%f")[:-3]
        file_handler.setFormatter(_fmt)
        self.logger.addHandler(file_handler)
        self.dashboard_console = Console()

        output_dir = OUTPUT_DIR.expanduser().resolve()
        self.runtime = VariationalRuntime(
            host=FORWARDER_HOST,
            ws_port=FORWARDER_WS_PORT,
            rest_port=FORWARDER_REST_PORT,
            command_port=FORWARDER_COMMAND_PORT,
            output_dir=None,
            quiet=True,
        )

        self.spread_avg_window_seconds = float(os.getenv("VAR_SPREAD_AVG_WINDOW_SECONDS", "300"))
        self.open_profit_margin_pct = Decimal(os.getenv("VAR_OPEN_PROFIT_MARGIN_PCT", "0.02"))
        # 开仓门槛随仓位利用率线性插值：空仓时门槛=margin*min_mult（放宽），
        # 接近满仓时门槛=margin*max_mult（收紧，只留给特别好的价差）
        self.open_margin_min_mult = Decimal(os.getenv("VAR_OPEN_MARGIN_MIN_MULT", "0.5"))
        self.open_margin_max_mult = Decimal(os.getenv("VAR_OPEN_MARGIN_MAX_MULT", "3.0"))
        self.close_profit_margin_pct = Decimal(os.getenv("VAR_CLOSE_PROFIT_MARGIN_PCT", "0.0"))
        self.narrow_close_pct = Decimal(os.getenv("VAR_NARROW_CLOSE_PCT", "0.01"))
        self.narrow_close_delta = Decimal(os.getenv("VAR_NARROW_CLOSE_DELTA_PCT", "0.02"))
        self.min_open_spread_pct = Decimal(os.getenv("VAR_MIN_OPEN_SPREAD_PCT", "0"))
        self.open_confirm_ticks = int(os.getenv("VAR_OPEN_CONFIRM_TICKS", "2"))
        self.max_price_deviation_pct = Decimal(os.getenv("VAR_MAX_PRICE_DEVIATION_PCT", "10"))
        self.order_notional_usdc = Decimal(os.getenv("VAR_ORDER_NOTIONAL_USDC", "300"))
        self.order_cooldown_seconds = float(os.getenv("VAR_ORDER_COOLDOWN_SECONDS", "120"))
        self.max_total_notional_usdc = Decimal(os.getenv("VAR_MAX_TOTAL_NOTIONAL_USDC", "1000"))
        self._open_long_notional: Decimal = Decimal("0")
        self._open_short_notional: Decimal = Decimal("0")
        self._lighter_actual_qty: Decimal = Decimal("0")  # signed qty synced from Lighter REST (negative=short)
        self._lighter_sync_interval: float = float(os.getenv("VAR_LIGHTER_SYNC_INTERVAL_SECONDS", "60"))
        self._single_leg_blocked: bool = False
        self._open_confirm_count: int = 0
        self._open_confirm_dir: str = ""  # "long" | "short"
        self._last_variational_order_ts: float = 0.0
        self._injection_fail_count: int = 0
        self._injection_fail_last_log_ts: float = 0.0
        self._order_in_flight: bool = False
        self._inflight_order_side: str | None = None   # side of the current in-flight Var order
        self._inflight_fill_event: dict | None = None  # fill event buffered while in-flight
        self._last_var_fill_seen_mono: float = 0.0     # monotonic time of last confirmed var fill event
        # (side, monotonic_ts, pending_key) — fill arrived after in-flight completed; fast-path
        # in process_variational_trade_event uses this to merge fill price into pending_rec.
        self._pre_hedged: list[tuple[str, float, str]] = []
        self.signal_task: asyncio.Task[None] | None = None
        self.bbo_task: asyncio.Task[None] | None = None
        self._bbo_log_interval = int(os.getenv("VAR_BBO_LOG_INTERVAL_SECONDS", "600"))
        self._bbo_output_dir = output_dir
        self._bbo_written_headers: set[str] = set()

        self.orders_file = output_dir / "order_metrics.jsonl" if output_dir else None
        self.trade_records_csv_file = output_dir / TRADE_RECORDS_CSV_FILE.name if output_dir else None
        self._order_write_lock = asyncio.Lock()
        self._trade_csv_write_lock = asyncio.Lock()
        self._trade_records_snapshot_sig: str | None = None

        self.records: dict[str, OrderLifecycle] = {}
        self.record_order: deque[str] = deque(maxlen=500)
        self.lighter_client_order_to_trade_key: dict[int, str] = {}
        self._open_trade_queue: deque[str] = deque()  # FIFO queue of unmatched open keys
        self._record_lock = asyncio.Lock()
        self.cross_spread_history: deque[tuple[float, float | None, float | None]] = deque()
        # (monotonic_ts, var_book_spread_pct, lighter_book_spread_pct) — for threshold baseline
        self._book_spread_history: deque[tuple[float, float, float]] = deque()
        self._asset_switch_lock = asyncio.Lock()
        self._asset_switch_candidate: str | None = None
        self._asset_switch_candidate_hits = 0

        self.trade_event_cursor = 0

        self.lighter_base_url = "https://mainnet.zklighter.elliot.ai"
        self.account_index = required_int_env("LIGHTER_ACCOUNT_INDEX")
        self.api_key_index = required_int_env("LIGHTER_API_KEY_INDEX")
        self.lighter_client: SignerClient | None = None
        self._lighter_signer_lock = asyncio.Lock()

        self.lighter_market_index = 0
        self.base_amount_multiplier = 0
        self.price_multiplier = 0

        self.lighter_order_book = {"bids": {}, "asks": {}}
        self.lighter_best_bid: Decimal | None = None
        self.lighter_best_ask: Decimal | None = None
        self.lighter_order_book_offset = 0
        self.lighter_order_book_ready = False
        self.lighter_snapshot_loaded = False
        self.lighter_order_book_sequence_gap = False
        self.lighter_order_book_lock = asyncio.Lock()

        self.lighter_ws_task: asyncio.Task[None] | None = None
        self.trade_task: asyncio.Task[None] | None = None
        self.dashboard_task: asyncio.Task[None] | None = None
        self._lighter_sync_task: asyncio.Task[None] | None = None

    def print_startup_next_steps(self) -> None:
        is_zh = self.args.lang == "zh"
        if is_zh:
            lines = [
                "Python 脚本已就位，请回到 Chrome 加载并启动扩展。若 Chrome 插件已启动，请刷新网页。",
                "Use `python main.py --lang en` for the English dashboard.",
            ]
            title = "启动指引"
        else:
            lines = [
                "Python runtime is ready. Go back to Chrome and load/start the extension.",
                "If the Chrome extension has already started, please refresh the webpage."
            ]
            title = "Startup Guide"
        self.dashboard_console.print(Panel("\n".join(lines), title=title, border_style="yellow"))

    def setup_signal_handlers(self) -> None:
        signal.signal(signal.SIGINT, self.shutdown)
        signal.signal(signal.SIGTERM, self.shutdown)

    def shutdown(self, signum=None, frame=None) -> None:
        self.stop_flag = True

    def initialize_lighter_client(self) -> SignerClient:
        if self.lighter_client is None:
            api_key_private_key = os.getenv("API_KEY_PRIVATE_KEY", "").strip() or required_env("LIGHTER_PRIVATE_KEY")
            self.lighter_client = SignerClient(
                url=self.lighter_base_url,
                account_index=self.account_index,
                api_private_keys={self.api_key_index: api_key_private_key},
            )
            err = self.lighter_client.check_client()
            if err is not None:
                raise RuntimeError(f"CheckClient error: {err}")
        return self.lighter_client

    def get_lighter_market_config(self) -> tuple[int, int, int]:
        if not self.ticker:
            raise RuntimeError("Ticker is not resolved yet")
        response = requests.get(
            f"{self.lighter_base_url}/api/v1/orderBooks",
            headers={"accept": "application/json"},
            timeout=10,
        )
        response.raise_for_status()
        data = response.json()

        for market in data.get("order_books", []):
            if market.get("symbol") == self.ticker:
                price_decimals = int(market["supported_price_decimals"])
                size_decimals = int(market["supported_size_decimals"])
                return int(market["market_id"]), pow(10, size_decimals), pow(10, price_decimals)

        raise RuntimeError(f"Ticker {self.ticker} not found in Lighter order books")

    async def detect_current_variational_asset(self) -> str | None:
        async with self.runtime.monitor._lock:
            if self.runtime.monitor.current_quote_asset:
                asset = str(self.runtime.monitor.current_quote_asset).strip().upper()
                quote = self.runtime.monitor.quotes.get(asset)
                if (
                    asset
                    and asset != "UNKNOWN"
                    and isinstance(quote, dict)
                    and to_decimal(quote.get("bid")) is not None
                    and to_decimal(quote.get("ask")) is not None
                ):
                    return asset

        return None

    async def wait_for_ticker_resolution(self) -> str:
        deadline = time.time() + READY_TIMEOUT_SECONDS
        while not self.stop_flag and time.time() < deadline:
            asset = await self.detect_current_variational_asset()
            if asset:
                return asset
            await asyncio.sleep(POLL_INTERVAL_SECONDS)

        raise RuntimeError("Timed out deriving ticker from Variational quote/trade messages")

    async def _reset_state_for_asset_switch(self) -> None:
        async with self._record_lock:
            self.records.clear()
            self.record_order.clear()
            self.lighter_client_order_to_trade_key.clear()
        self.cross_spread_history.clear()
        async with self._trade_csv_write_lock:
            self._trade_records_snapshot_sig = None

    async def activate_asset(self, variational_asset: str, reason: str) -> None:
        asset = variational_asset.strip().upper()
        if not asset or asset == "UNKNOWN":
            return

        async with self._asset_switch_lock:
            next_ticker = resolve_lighter_ticker(asset)
            if self.variational_ticker == asset and self.ticker == next_ticker:
                return

            self.variational_ticker = asset
            self.ticker = next_ticker
            self.accepted_assets = {
                asset,
                next_ticker,
                resolve_variational_ticker(next_ticker),
            }

            self.lighter_market_index, self.base_amount_multiplier, self.price_multiplier = self.get_lighter_market_config()
            await self.reset_lighter_order_book()
            await self._reset_state_for_asset_switch()

            if self.lighter_ws_task and not self.lighter_ws_task.done():
                self.lighter_ws_task.cancel()
                await asyncio.gather(self.lighter_ws_task, return_exceptions=True)

            self.lighter_ws_task = asyncio.create_task(self.handle_lighter_ws())
            await self.wait_for_lighter_order_book_ready()
            self.logger.info(
                "Switched market (%s): variational_asset=%s -> lighter_ticker=%s market_id=%s",
                reason,
                self.variational_ticker,
                self.ticker,
                self.lighter_market_index,
            )

    async def wait_for_variational_ready(self) -> None:
        deadline = time.time() + READY_TIMEOUT_SECONDS
        while not self.stop_flag and time.time() < deadline:
            state = await self.runtime.monitor.get_trading_state()
            hb_age = state.get("heartbeat_age")
            if hb_age is not None and hb_age <= HEARTBEAT_STALE_SECONDS:
                return
            await asyncio.sleep(POLL_INTERVAL_SECONDS)
        raise RuntimeError("Timed out waiting for Variational events stream heartbeat")

    async def wait_for_lighter_order_book_ready(self) -> None:
        deadline = time.time() + READY_TIMEOUT_SECONDS
        while not self.stop_flag and time.time() < deadline:
            if self.lighter_order_book_ready:
                return
            await asyncio.sleep(0.2)
        raise RuntimeError("Timed out waiting for Lighter order book")

    async def reset_lighter_order_book(self) -> None:
        async with self.lighter_order_book_lock:
            self.lighter_order_book["bids"].clear()
            self.lighter_order_book["asks"].clear()
            self.lighter_order_book_offset = 0
            self.lighter_order_book_ready = False
            self.lighter_snapshot_loaded = False
            self.lighter_order_book_sequence_gap = False
            self.lighter_best_bid = None
            self.lighter_best_ask = None

    def update_lighter_order_book(self, side: str, levels: list[Any]) -> None:
        for level in levels:
            if isinstance(level, list) and len(level) >= 2:
                price = Decimal(str(level[0]))
                size = Decimal(str(level[1]))
            elif isinstance(level, dict):
                price = Decimal(str(level.get("price", 0)))
                size = Decimal(str(level.get("size", 0)))
            else:
                continue

            if size > 0:
                self.lighter_order_book[side][price] = size
            else:
                self.lighter_order_book[side].pop(price, None)

    def validate_order_book_offset(self, new_offset: int) -> bool:
        return new_offset > self.lighter_order_book_offset

    async def request_fresh_snapshot(self, ws: Any) -> None:
        await ws.send(json.dumps({"type": "subscribe", "channel": f"order_book/{self.lighter_market_index}"}))

    async def handle_lighter_fill_update(self, order: dict[str, Any]) -> None:
        if order.get("status") != "filled":
            return

        client_order_id_raw = order.get("client_order_id")
        try:
            client_order_id = int(client_order_id_raw)
        except Exception:
            self.logger.warning(
                "Lighter fill: cannot parse client_order_id=%r — order ignored", client_order_id_raw
            )
            return

        fill_price: Decimal | None = None
        filled_quote = to_decimal(order.get("filled_quote_amount"))
        filled_base = to_decimal(order.get("filled_base_amount"))
        if filled_quote is not None and filled_base is not None and filled_base != 0:
            fill_price = filled_quote / filled_base
        else:
            self.logger.warning(
                "Lighter fill cid=%s: fill_price could not be computed "
                "(filled_quote=%s filled_base=%s) — price recorded as null",
                client_order_id_raw, filled_quote, filled_base,
            )

        now_iso = utc_now()

        async with self._record_lock:
            trade_key = self.lighter_client_order_to_trade_key.get(client_order_id)
            if not trade_key:
                return
            record = self.records.get(trade_key)
            if record is None:
                self.logger.error(
                    "Lighter fill cid=%s maps to trade_key=%r but record is missing — "
                    "possible data loss; fill ignored",
                    client_order_id, trade_key,
                )
                return
            if record.lighter_fill_ts_iso is not None:
                return

            record.lighter_fill_ts_iso = now_iso
            record.lighter_fill_price = fill_price

            # Compute round-trip P&L once both legs of a close trade are filled
            # P&L is computed per-exchange (funds don't cross exchanges):
            #   Var P&L  = (var_close - var_open) × qty   ← long position on Variational
            #   Lite P&L = (lt_open  - lt_close)  × qty   ← short position on Lighter
            if record.side == "sell" and record.matched_open_key:
                open_rec = self.records.get(record.matched_open_key)
                if (open_rec and open_rec.var_fill_price and open_rec.lighter_fill_price
                        and record.var_fill_price and fill_price):
                    qty = min(open_rec.qty, record.qty)
                    record.var_pnl = (record.var_fill_price - open_rec.var_fill_price) * qty
                    record.lt_pnl  = (open_rec.lighter_fill_price - fill_price) * qty
                    record.roundtrip_pnl = record.var_pnl + record.lt_pnl

            payload = record.to_payload()

        if record.roundtrip_pnl is not None:
            self.logger.info(
                "Lighter fill confirmed: side=%s price=%s qty=%s | roundtrip_pnl=%s (var=%s lt=%s)",
                record.lighter_side, fill_price, record.qty,
                record.roundtrip_pnl, record.var_pnl, record.lt_pnl,
            )
        else:
            self.logger.info(
                "Lighter fill confirmed: side=%s price=%s qty=%s",
                record.lighter_side, fill_price, record.qty,
            )
        await self.append_order_log("lighter_fill", payload)

    def build_lighter_ws_url(self) -> str:
        if env_flag("LIGHTER_WS_SERVER_PINGS"):
            return f"{LIGHTER_WS_URL}?server_pings=true"
        return LIGHTER_WS_URL

    async def handle_lighter_ws(self) -> None:
        while not self.stop_flag:
            try:
                await self.reset_lighter_order_book()
                url = self.build_lighter_ws_url()
                async with websockets.connect(
                    url,
                    ping_interval=LIGHTER_WS_PING_INTERVAL_SECONDS,
                    ping_timeout=LIGHTER_WS_PING_TIMEOUT_SECONDS,
                ) as ws:
                    await ws.send(json.dumps({"type": "subscribe", "channel": f"order_book/{self.lighter_market_index}"}))

                    account_orders_channel = f"account_orders/{self.lighter_market_index}/{self.account_index}"
                    try:
                        async with self._lighter_signer_lock:
                            if not self.lighter_client:
                                self.initialize_lighter_client()
                            auth_token, err = self.lighter_client.create_auth_token_with_expiry(
                                api_key_index=self.api_key_index
                            )
                        if err is None:
                            await ws.send(
                                json.dumps(
                                    {
                                        "type": "subscribe",
                                        "channel": account_orders_channel,
                                        "auth": auth_token,
                                    }
                                )
                            )
                        else:
                            self.logger.warning("Failed to create Lighter WS auth token: %s", err)
                    except Exception as exc:
                        self.logger.warning("Error creating Lighter WS auth token: %s", exc)

                    while not self.stop_flag:
                        raw = await ws.recv()
                        if isinstance(raw, bytes):
                            raw = raw.decode("utf-8", errors="replace")
                        data = json.loads(raw)
                        msg_type = data.get("type")

                        if msg_type == "subscribed/order_book":
                            async with self.lighter_order_book_lock:
                                self.lighter_order_book["bids"].clear()
                                self.lighter_order_book["asks"].clear()
                                order_book = data.get("order_book", {})
                                self.lighter_order_book_offset = int(order_book.get("offset", 0) or 0)
                                self.update_lighter_order_book("bids", order_book.get("bids", []))
                                self.update_lighter_order_book("asks", order_book.get("asks", []))
                                self.lighter_snapshot_loaded = True
                                self.lighter_order_book_ready = True
                                self.lighter_best_bid = (
                                    max(self.lighter_order_book["bids"].keys())
                                    if self.lighter_order_book["bids"]
                                    else None
                                )
                                self.lighter_best_ask = (
                                    min(self.lighter_order_book["asks"].keys())
                                    if self.lighter_order_book["asks"]
                                    else None
                                )

                        elif msg_type == "update/order_book" and self.lighter_snapshot_loaded:
                            order_book = data.get("order_book", {})
                            if "offset" not in order_book:
                                self.logger.debug(
                                    "handle_lighter_ws: update/order_book 消息缺少 offset 字段，跳过"
                                )
                                continue
                            new_offset = int(order_book["offset"])
                            async with self.lighter_order_book_lock:
                                if not self.validate_order_book_offset(new_offset):
                                    self.logger.warning(
                                        "handle_lighter_ws: 订单簿序列号跳变 "
                                        "(expected >%s, got %s) — 将重新订阅快照",
                                        self.lighter_order_book_offset, new_offset,
                                    )
                                    self.lighter_order_book_sequence_gap = True
                                else:
                                    self.update_lighter_order_book("bids", order_book.get("bids", []))
                                    self.update_lighter_order_book("asks", order_book.get("asks", []))
                                    self.lighter_order_book_offset = new_offset
                                    self.lighter_best_bid = (
                                        max(self.lighter_order_book["bids"].keys())
                                        if self.lighter_order_book["bids"]
                                        else None
                                    )
                                    self.lighter_best_ask = (
                                        min(self.lighter_order_book["asks"].keys())
                                        if self.lighter_order_book["asks"]
                                        else None
                                    )

                        elif msg_type == "update/account_orders":
                            orders = data.get("orders", {}).get(str(self.lighter_market_index), [])
                            for order in orders:
                                await self.handle_lighter_fill_update(order)

                        if self.lighter_order_book_sequence_gap:
                            await self.request_fresh_snapshot(ws)
                            self.lighter_order_book_sequence_gap = False

                        if msg_type == "ping":
                            await ws.send(json.dumps({"type": "pong"}))

            except asyncio.CancelledError:
                return
            except Exception as exc:
                self.logger.warning(
                    "Lighter websocket reconnect after error: %s (url=%s)",
                    exc,
                    self.build_lighter_ws_url(),
                )
                await asyncio.sleep(1)

    async def get_lighter_best_bid_ask(self) -> tuple[Decimal | None, Decimal | None]:
        async with self.lighter_order_book_lock:
            return self.lighter_best_bid, self.lighter_best_ask

    async def get_variational_best_bid_ask(self, preferred_asset: str | None):
        async with self.runtime.monitor._lock:
            quote = None
            if preferred_asset:
                quote = self.runtime.monitor.quotes.get(preferred_asset)
            if quote is None and self.variational_ticker:
                quote = self.runtime.monitor.quotes.get(self.variational_ticker)
            if quote is None and self.runtime.monitor.current_quote_asset:
                quote = self.runtime.monitor.quotes.get(self.runtime.monitor.current_quote_asset)

            if quote is None:
                return None, None, None, None
            return (
                to_decimal(quote.get("bid")), to_decimal(quote.get("ask")), str(quote.get("asset", "")),
                quote_age_ms(quote.get("timestamp")),
            )

    @staticmethod
    def trade_key(event: dict[str, Any]) -> str:
        trade_id = str(event.get("trade_id", "")).strip()
        if trade_id:
            return trade_id[:8]
        event_seq = str(event.get("event_seq", "")).strip()
        return f"seq:{event_seq}"

    async def append_order_log(self, event_type: str, payload: dict[str, Any]) -> None:
        if self.orders_file is None:
            return
        row = {
            "event": event_type,
            "logged_at": utc_now(),
            **payload,
        }
        line = json.dumps(row, ensure_ascii=True) + "\n"
        async with self._order_write_lock:
            await asyncio.to_thread(self.orders_file.parent.mkdir, parents=True, exist_ok=True)
            await asyncio.to_thread(self._append_line, self.orders_file, line)

    @staticmethod
    def _append_line(path: Path, line: str) -> None:
        with path.open("a", encoding="utf-8") as handle:
            handle.write(line)

    async def place_lighter_order(self, record: OrderLifecycle) -> None:
        if not self.args.auto_hedge:
            return

        side = "SELL" if record.side == "buy" else "BUY"

        # Guard: prevent creating an orphan Lighter position when the hedge leg is already gone.
        # Lighter API convention: positive qty = SHORT, negative qty = LONG.
        # A Lighter BUY closes an existing SHORT (positive qty). If there's no short, the BUY
        # would create a spurious LONG — only skip if Var is also not short (not a legitimate open).
        if side == "BUY" and self._lighter_actual_qty <= Decimal("0.001"):
            cur_pos = self.runtime.monitor.positions.get(self.variational_ticker or "", {})
            var_qty = to_decimal(cur_pos.get("qty")) or Decimal("0")
            if var_qty > Decimal("-0.001"):
                self.logger.warning(
                    "Lighter BUY hedge skipped — no short to close (lighter_qty=%s, var_qty=%s); "
                    "single-leg protection prevented orphan position",
                    self._lighter_actual_qty, var_qty,
                )
                async with self._record_lock:
                    record.hedge_error = "skipped_no_lighter_short"
                    payload = record.to_payload()
                await self.append_order_log("lighter_error", payload)
                return

        best_bid, best_ask = await self.get_lighter_best_bid_ask()
        if best_bid is None or best_ask is None:
            self.logger.warning(
                "place_lighter_order: 订单簿未就绪，无法对冲 (trade_key=%s side=%s qty=%s)",
                record.trade_key, side, record.qty,
            )
            async with self._record_lock:
                record.hedge_error = "Lighter order book not ready"
                payload = record.to_payload()
            await self.append_order_log("lighter_error", payload)
            return

        slippage = Decimal(str(HEDGE_SLIPPAGE_BPS)) / Decimal("10000")
        if side == "BUY":
            is_ask = False
            limit_price = best_ask * (Decimal("1") + slippage)
        else:
            is_ask = True
            limit_price = best_bid * (Decimal("1") - slippage)

        base_amount = int(record.qty * self.base_amount_multiplier)
        if base_amount <= 0:
            self.logger.warning(
                "place_lighter_order: qty=%s 转换后 base_amount=0，跳过对冲 (trade_key=%s multiplier=%s)",
                record.qty, record.trade_key, self.base_amount_multiplier,
            )
            async with self._record_lock:
                record.hedge_error = f"Hedge base amount rounds to zero ({record.qty})"
                payload = record.to_payload()
            await self.append_order_log("lighter_error", payload)
            return

        price_i = int(limit_price * self.price_multiplier)
        async with self._record_lock:
            client_order_id = int(time.time() * 1000)
            while client_order_id in self.lighter_client_order_to_trade_key:
                client_order_id += 1

        t_sign_start = time.monotonic()
        try:
            async with self._lighter_signer_lock:
                t_lock_acquired = time.monotonic()
                if not self.lighter_client:
                    self.initialize_lighter_client()
                _, tx_hash, error = await self.lighter_client.create_order(
                    market_index=self.lighter_market_index,
                    client_order_index=client_order_id,
                    base_amount=base_amount,
                    price=price_i,
                    is_ask=is_ask,
                    order_type=self.lighter_client.ORDER_TYPE_LIMIT,
                    time_in_force=self.lighter_client.ORDER_TIME_IN_FORCE_GOOD_TILL_TIME,
                    reduce_only=False,
                    trigger_price=0,
                )
            t_signed = time.monotonic()

            if error is not None:
                raise RuntimeError(f"Sign error: {error}")

            async with self._record_lock:
                record.lighter_side = side
                record.lighter_client_order_id = client_order_id
                record.lighter_tx_hash = tx_hash
                record.hedge_error = None
                self.lighter_client_order_to_trade_key[client_order_id] = record.trade_key
            # Optimistically update cached Lighter qty so the guard stays accurate between syncs.
            # Lighter API convention: positive = SHORT. SELL opens short (more positive), BUY closes it.
            if side == "SELL":
                self._lighter_actual_qty += record.qty
            else:
                self._lighter_actual_qty -= record.qty
            self.logger.info(
                "Lighter hedge sent: side=%s qty=%s limit_price=%s",
                side, record.qty, limit_price,
            )
            lock_wait_ms = (t_lock_acquired - t_sign_start) * 1000
            sign_submit_ms = (t_signed - t_lock_acquired) * 1000
            self.logger.info(
                "Lighter sign/submit timing: lock_wait=%.1fms sign_submit=%.1fms total=%.1fms",
                lock_wait_ms, sign_submit_ms, lock_wait_ms + sign_submit_ms,
            )
            await self.append_order_log("lighter_submit_timing", {
                "trade_key": record.trade_key,
                "side": side,
                "qty": str(record.qty),
                "lock_wait_ms": round(lock_wait_ms, 1),
                "sign_submit_ms": round(sign_submit_ms, 1),
                "total_ms": round(lock_wait_ms + sign_submit_ms, 1),
            })
        except Exception as exc:
            async with self._record_lock:
                record.lighter_side = side
                record.hedge_error = str(exc)
                payload = record.to_payload()
            await self.append_order_log("lighter_error", payload)

    def should_track_variational_event(self, event: dict[str, Any]) -> bool:
        side = str(event.get("side", "")).strip().lower()
        if side not in {"buy", "sell"}:
            return False

        qty = to_decimal(event.get("qty"))
        if qty is None or qty <= 0:
            return False

        asset = str(event.get("asset", "")).strip().upper()
        if not asset:
            return False
        return asset in self.accepted_assets

    async def process_variational_trade_event(self, event: dict[str, Any]) -> None:
        if not self.should_track_variational_event(event):
            if normalize_variational_status(str(event.get("status", ""))) == "filled":
                self.logger.warning(
                    "process_variational_trade_event: filled 事件被过滤 "
                    "(side=%s asset=%s qty=%s) — 不在跟踪范围",
                    event.get("side"), event.get("asset"), event.get("qty"),
                )
            return

        key = self.trade_key(event)
        side = str(event.get("side", "")).strip().lower()
        qty = to_decimal(event.get("qty"))
        if qty is None:
            self.logger.warning(
                "process_variational_trade_event: qty 无法解析 (raw=%r trade_id=%s) — 事件忽略",
                event.get("qty"), event.get("trade_id"),
            )
            return

        status = normalize_variational_status(str(event.get("status", "")))
        asset = str(event.get("asset", "")).strip().upper() or self.variational_ticker
        trade_id = str(event.get("trade_id", "")).strip()

        now_iso = utc_now()
        fill_iso = to_cst_str(str(event.get("timestamp"))) if event.get("timestamp") else now_iso

        # Fast path: if signal_loop already pre-hedged this fill, merge the actual fill price
        # into the existing pending record instead of creating a duplicate entry.
        if status == "filled":
            now_mono = time.monotonic()
            for i, (ph_side, ph_ts, ph_key) in enumerate(self._pre_hedged):
                if ph_side == side and now_mono - ph_ts < 180:
                    async with self._record_lock:
                        pending_rec = self.records.get(ph_key)
                        if pending_rec is not None:
                            self._pre_hedged.pop(i)
                            pending_rec.var_fill_price = to_decimal(event.get("price"))
                            pending_rec.var_fill_ts_iso = fill_iso
                            if side == "buy":
                                self._open_trade_queue.append(ph_key)
                            elif side == "sell" and self._open_trade_queue:
                                open_key = self._open_trade_queue.popleft()
                                open_rec = self.records.get(open_key)
                                if open_rec and open_rec.var_fill_price and pending_rec.var_fill_price:
                                    pending_rec.matched_open_key = open_key
                                    open_rec.matched_open_key = ph_key
                            filled_payload = pending_rec.to_payload()
                    if pending_rec is not None:
                        # var_fill_price updated in-memory; signal_loop already logged variational_fill.
                        self.logger.info(
                            "Var fill confirmed: side=%s price=%s qty=%s",
                            side, pending_rec.var_fill_price, pending_rec.qty,
                        )
                        await self.append_order_log("var_fill_price_update", filled_payload)
                        return
                    # pending_rec not found: pre-hedged token exists but record disappeared — state inconsistency.
                    self.logger.error(
                        "process_variational_trade_event: pre-hedged token 匹配 (key=%s side=%s) "
                        "但 record 已消失 — 对冲状态不一致",
                        ph_key, side,
                    )
                    break

            # Buffer path: fill arrived while signal_loop is awaiting the Var order response.
            # _order_in_flight is True from before the order is sent until place_lighter_order
            # completes, so this fill is for the current in-flight order.  Hand it off to
            # signal_loop (which will apply it to pending_rec after getting rfq_id) and return.
            if self._order_in_flight and self._inflight_order_side == side:
                self._inflight_fill_event = event
                self.logger.debug(
                    "process_variational_trade_event: fill buffered for in-flight order "
                    "(side=%s price=%s)", side, event.get("price"),
                )
                return

            # Filled event reached Python but matched neither pre_hedged nor in-flight buffer.
            # This is unusual — log details to help diagnose timing or side-mismatch issues.
            _now_mono2 = time.monotonic()
            _ph_summary = [
                f"(side={s} age={_now_mono2-t:.1f}s key={k[:8]})"
                for s, t, k in self._pre_hedged
            ]
            self.logger.warning(
                "process_variational_trade_event: filled 事件无匹配 pre_hedged token "
                "(side=%s price=%s trade_id=%s) — "
                "order_in_flight=%s inflight_side=%s pre_hedged=%s",
                side, event.get("price"), trade_id[:8] if trade_id else "?",
                self._order_in_flight, self._inflight_order_side,
                _ph_summary if _ph_summary else "[]",
            )

        created = False

        async with self._record_lock:
            record = self.records.get(key)
            if record is None:
                record = OrderLifecycle(
                    trade_key=key,
                    trade_id=trade_id,
                    side=side,
                    qty=qty,
                    asset=asset if asset else "UNKNOWN",
                    auto_hedge_enabled=self.args.auto_hedge,
                    last_variational_status=status,
                )
                self.records[key] = record
                self.record_order.append(key)
                created = True
            else:
                previous_status = record.last_variational_status
                record.last_variational_status = status

            if created:
                previous_status = ""

            should_set_fill = False
            if status == "filled":
                if record.var_fill_ts_iso is None:
                    should_set_fill = True
                elif previous_status != "filled":
                    should_set_fill = True

            if should_set_fill:
                record.var_fill_ts_iso = fill_iso
                record.var_fill_price = to_decimal(event.get("price"))
                # FIFO open/close matching for round-trip P&L
                if side == "buy":
                    self._open_trade_queue.append(key)
                elif side == "sell" and self._open_trade_queue:
                    open_key = self._open_trade_queue.popleft()
                    open_rec = self.records.get(open_key)
                    if open_rec and open_rec.var_fill_price and record.var_fill_price:
                        # open leg: lighter_fill - var_fill (positive = captured spread)
                        # close leg: var_fill - lighter_fill (negative if Lighter still higher)
                        # round-trip per BTC = open_diff + close_diff
                        open_diff = (open_rec.lighter_fill_price - open_rec.var_fill_price
                                     if open_rec.lighter_fill_price else Decimal("0"))
                        close_var_price = record.var_fill_price
                        # Lighter close price not yet set; will update when lighter fill arrives
                        record.matched_open_key = open_key
                        open_rec.matched_open_key = key  # back-reference
                filled_payload = record.to_payload()
            else:
                filled_payload = None

        if filled_payload is not None:
            self.logger.info(
                "Var fill confirmed: side=%s price=%s qty=%s",
                record.side, record.var_fill_price, record.qty,
            )
            await self.append_order_log("variational_fill", filled_payload)

        # Trigger hedge on fill confirmation only if signal_loop hasn't already pre-hedged it.
        # signal_loop places the Lighter hedge immediately on order acknowledgement (because
        # reduce-only fill events are not reliably delivered).  We consume one _pre_hedged
        # token per fill event so that legitimate second fills of a different order are still hedged.
        if self.args.auto_hedge and filled_payload is not None:
            async with self._record_lock:
                need_hedge = record.lighter_side is None and record.hedge_error is None
            if need_hedge:
                # Reach here only when the fill event arrived AFTER _order_in_flight was
                # cleared (i.e. signal_loop has completed).  Check _pre_hedged in case
                # signal_loop created a pending_rec but the fill event is arriving via a
                # second delivery or a different event path.
                now_mono = time.monotonic()
                already_hedged = False
                for i, (ph_side, ph_ts, ph_key) in enumerate(self._pre_hedged):
                    if ph_side == record.side and now_mono - ph_ts < 180:
                        self._pre_hedged.pop(i)
                        already_hedged = True
                        self.logger.debug(
                            "Hedge skipped — pre-hedged by signal_loop (side=%s key=%s)",
                            record.side, record.trade_key,
                        )
                        break
                if not already_hedged:
                    await self.place_lighter_order(record)

    async def trade_loop(self) -> None:
        while not self.stop_flag:
            current_asset = await self.detect_current_variational_asset()
            if current_asset:
                if current_asset == self.variational_ticker:
                    self._asset_switch_candidate = None
                    self._asset_switch_candidate_hits = 0
                else:
                    if current_asset == self._asset_switch_candidate:
                        self._asset_switch_candidate_hits += 1
                    else:
                        self._asset_switch_candidate = current_asset
                        self._asset_switch_candidate_hits = 1

                    if self._asset_switch_candidate_hits >= ASSET_SWITCH_CONFIRM_TICKS:
                        await self.activate_asset(current_asset, reason="quote_stream_debounced")
                        self._asset_switch_candidate = None
                        self._asset_switch_candidate_hits = 0
            else:
                self._asset_switch_candidate = None
                self._asset_switch_candidate_hits = 0

            events = await self.runtime.monitor.get_trade_events_since(self.trade_event_cursor, limit=500)
            _now_mono = time.monotonic()
            for event in events:
                self.trade_event_cursor = max(self.trade_event_cursor, int(event.get("event_seq", 0) or 0))
                if normalize_variational_status(str(event.get("status", ""))) == "filled":
                    self._last_var_fill_seen_mono = _now_mono
                await self.process_variational_trade_event(event)

            # Periodic health: log monitor's event-seq progress and fill-event staleness
            if _now_mono - getattr(self, "_trade_loop_health_ts", 0) >= 60:
                latest_seq = await self.runtime.monitor.get_latest_trade_event_seq()
                fill_age = _now_mono - self._last_var_fill_seen_mono if self._last_var_fill_seen_mono else None
                has_pos = (self._open_long_notional + self._open_short_notional) > 0
                self.logger.info(
                    "trade_loop health: cursor=%d monitor_seq=%d fill_age=%s has_pos=%s",
                    self.trade_event_cursor,
                    latest_seq,
                    f"{fill_age:.0f}s" if fill_age is not None else "never",
                    has_pos,
                )
                if fill_age is not None and fill_age > 300 and has_pos:
                    self.logger.warning(
                        "trade_loop: VAR fill events stale for %.0fs while holding positions "
                        "(cursor=%d monitor_seq=%d) — Chrome扩展fill事件可能中断",
                        fill_age, self.trade_event_cursor, latest_seq,
                    )
                self._trade_loop_health_ts = _now_mono

            await asyncio.sleep(POLL_INTERVAL_SECONDS)

    async def bbo_loop(self) -> None:
        while not self.stop_flag:
            await asyncio.sleep(self._bbo_log_interval)
            if self.stop_flag:
                break

            try:
                var_bid, var_ask, _, _ = await self.get_variational_best_bid_ask(self.variational_ticker)
                lighter_bid, lighter_ask = await self.get_lighter_best_bid_ask()
                if None in (var_bid, var_ask, lighter_bid, lighter_ask):
                    continue

                if self._book_spread_history:
                    _e = self._book_spread_history
                    _bsl = Decimal(str(sum(v for _, v, _ in _e) / len(_e))) + Decimal(str(sum(l for _, _, l in _e) / len(_e)))
                else:
                    _bsl = (book_spread_percent(var_bid, var_ask) or Decimal("0")) + (book_spread_percent(lighter_bid, lighter_ask) or Decimal("0"))
                open_thr = _bsl + self.open_profit_margin_pct
                close_thr = _bsl + self.close_profit_margin_pct
                long_pct = spread_percent(spread_value(var_ask, lighter_bid), var_ask)
                short_pct = spread_percent(spread_value(lighter_ask, var_bid), lighter_ask)

                await self._write_bbo_snapshot(
                    var_bid, var_ask, lighter_bid, lighter_ask,
                    long_pct, short_pct, open_thr, close_thr, event="periodic",
                )
            except Exception as exc:
                self.logger.error("bbo_loop error: %s", exc, exc_info=True)

    async def _write_bbo_snapshot(
        self,
        var_bid: Decimal, var_ask: Decimal,
        lighter_bid: Decimal, lighter_ask: Decimal,
        long_pct: Decimal | None, short_pct: Decimal | None,
        open_thr: Decimal, close_thr: Decimal,
        event: str,
    ) -> None:
        import csv as _csv

        all_pos = self.runtime.monitor.positions
        total_notional = sum(
            abs(to_decimal(p.get("value")) or Decimal("0"))
            for p in all_pos.values()
        )

        ticker = self.variational_ticker or "UNKNOWN"
        out_dir = self._bbo_output_dir or Path("./logs")
        out_dir.mkdir(parents=True, exist_ok=True)
        bbo_file = out_dir / f"bbo_{ticker}_{datetime.now(CST).strftime('%Y%m')}.csv"

        row = {
            "timestamp": utc_now(),
            "ticker": ticker,
            "event": event,
            "var_bid": f"{var_bid:,.6f}",
            "var_ask": f"{var_ask:,.6f}",
            "var_spread_pct": f"{(var_ask - var_bid) / var_bid * 100:.6f}",
            "lighter_bid": f"{lighter_bid:,.6f}",
            "lighter_ask": f"{lighter_ask:,.6f}",
            "lighter_spread_pct": f"{float((lighter_ask - lighter_bid) / lighter_bid * 100):.6f}",
            "long_spread_pct": f"{long_pct:.6f}" if long_pct is not None else "",
            "short_spread_pct": f"{short_pct:.6f}" if short_pct is not None else "",
            "open_threshold_pct": f"{open_thr:.6f}",
            "close_threshold_pct": f"{close_thr:.6f}",
            "total_notional_usdc": f"{total_notional:,.6f}",
        }
        headers = list(row.keys())
        needs_header = str(bbo_file) not in self._bbo_written_headers and not bbo_file.exists()

        def _write(path: Path, needs_hdr: bool, r: dict, hdrs: list) -> None:
            with path.open("a", newline="", encoding="utf-8") as f:
                w = _csv.DictWriter(f, fieldnames=hdrs)
                if needs_hdr:
                    w.writeheader()
                w.writerow(r)

        try:
            await asyncio.to_thread(_write, bbo_file, needs_header, row, headers)
            self._bbo_written_headers.add(str(bbo_file))
        except Exception as exc:
            self.logger.error("bbo snapshot write error: %s", exc, exc_info=True)

    async def load_initial_positions(self) -> None:
        from lighter import ApiClient, Configuration, AccountApi

        # ── Lighter side (REST API, authoritative) ──────────────────────────
        lighter_long_notional = Decimal("0")
        lighter_short_notional = Decimal("0")
        lighter_raw_qty = Decimal("0")
        try:
            api_client = ApiClient(configuration=Configuration(host=self.lighter_base_url))
            account_api = AccountApi(api_client)
            account_data = await account_api.account(by="index", value=str(self.account_index))
            await api_client.close()
            if account_data and account_data.accounts:
                acc = account_data.accounts[0]
                for pos in (acc.positions or []):
                    if pos.market_id != self.lighter_market_index:
                        continue
                    qty = Decimal(str(pos.position))
                    if abs(qty) < Decimal("0.000001"):
                        continue
                    lighter_raw_qty = qty
                    value = abs(Decimal(str(pos.position_value)))
                    # Lighter API convention: positive qty = SHORT position
                    if qty > 0:
                        lighter_long_notional = value   # Lighter SHORT → hedges Var LONG
                    else:
                        lighter_short_notional = value  # Lighter LONG → hedges Var SHORT
        except Exception as exc:
            self.logger.warning("Failed to query Lighter initial position: %s", exc)
        self._lighter_actual_qty = lighter_raw_qty

        # ── Variational side (monitor portfolio stream) ──────────────────────
        var_pos = self.runtime.monitor.positions.get(self.variational_ticker)
        if var_pos:
            var_qty = to_decimal(var_pos.get("qty"))
            var_value = to_decimal(var_pos.get("value"))
            if var_qty is not None and var_value is not None and abs(var_qty) > Decimal("0.000001"):
                var_notional = abs(var_value)
                if var_qty > 0:
                    if lighter_long_notional > 0:
                        diff_pct = abs(var_notional - lighter_long_notional) / lighter_long_notional * 100
                        if diff_pct > Decimal("5"):
                            self.logger.warning(
                                "Position mismatch: Variational LONG ~%s USDC vs Lighter SHORT ~%s USDC (%.1f%%)",
                                var_notional, lighter_long_notional, diff_pct,
                            )
                    self.logger.info("Variational initial position: LONG %s BTC (~%s USDC)", var_qty, var_notional)
                else:
                    self.logger.info("Variational initial position: SHORT %s BTC (~%s USDC)", var_qty, var_notional)
        else:
            self.logger.info("Variational portfolio not yet streamed; using Lighter position as initial state")

        # ── Apply to notional counters ────────────────────────────────────────
        self._open_long_notional = lighter_long_notional
        self._open_short_notional = lighter_short_notional
        if lighter_long_notional > 0 or lighter_short_notional > 0:
            self.logger.info(
                "Initial open notional set: long=%s USDC short=%s USDC",
                lighter_long_notional, lighter_short_notional,
            )

    async def _fetch_lighter_position_qty(self) -> Decimal | None:
        try:
            from lighter import ApiClient, Configuration, AccountApi
            api_client = ApiClient(configuration=Configuration(host=self.lighter_base_url))
            account_api = AccountApi(api_client)
            account_data = await account_api.account(by="index", value=str(self.account_index))
            await api_client.close()
            if account_data and account_data.accounts:
                acc = account_data.accounts[0]
                for pos in (acc.positions or []):
                    if pos.market_id != self.lighter_market_index:
                        continue
                    return Decimal(str(pos.position))
            return Decimal("0")
        except Exception as exc:
            self.logger.warning("Lighter position sync failed: %s", exc)
            return None

    async def lighter_sync_loop(self) -> None:
        while not self.stop_flag:
            await asyncio.sleep(self._lighter_sync_interval)
            lighter_qty = await self._fetch_lighter_position_qty()
            if lighter_qty is None:
                continue
            self._lighter_actual_qty = lighter_qty

            cur_pos = self.runtime.monitor.positions.get(self.variational_ticker or "", {})
            var_qty = to_decimal(cur_pos.get("qty")) or Decimal("0")

            trading_state = await self.runtime.monitor.get_trading_state()
            portfolio_age = trading_state.get("portfolio_age")
            _PORTFOLIO_STALE_SECONDS = 300
            if portfolio_age is not None and portfolio_age > _PORTFOLIO_STALE_SECONDS:
                self.logger.warning(
                    "lighter_sync_loop: portfolio data stale (%.0fs) — skipping var_qty check, "
                    "var_qty=%s may be outdated",
                    portfolio_age, var_qty,
                )
                continue

            var_long = var_qty > Decimal("0.001")
            var_short = var_qty < Decimal("-0.001")
            # Lighter API convention: positive qty = SHORT position, negative qty = LONG position
            lt_short = lighter_qty > Decimal("0.001")
            lt_long = lighter_qty < Decimal("-0.001")

            single_leg = (
                (var_long and not lt_short)
                or (var_short and not lt_long)
                or (not var_long and not var_short and abs(lighter_qty) > Decimal("0.001"))
            )
            if single_leg:
                self._single_leg_blocked = True
                self.logger.warning(
                    "SINGLE-LEG DETECTED: var_qty=%s lighter_qty=%s — new opens blocked",
                    var_qty, lighter_qty,
                )
            else:
                if self._single_leg_blocked:
                    self.logger.info(
                        "Single-leg cleared: var_qty=%s lighter_qty=%s — opens unblocked",
                        var_qty, lighter_qty,
                    )
                self._single_leg_blocked = False

    async def signal_loop(self) -> None:
        await asyncio.sleep(float(os.getenv("VAR_SIGNAL_STARTUP_DELAY_SECONDS", "15")))
        while not self.stop_flag:
            await asyncio.sleep(0.5)

            if self._order_in_flight:
                continue
            _now = time.monotonic()
            _cooldown_remaining = self.order_cooldown_seconds - (_now - self._last_variational_order_ts)
            if _cooldown_remaining > 0:
                if _now - getattr(self, "_cooldown_log_ts", 0) >= 60:
                #    self.logger.info(
                #        "signal_loop: cooldown %.0fs remaining", _cooldown_remaining,
                #    )
                    self._cooldown_log_ts = _now
                continue

            var_bid, var_ask, _, var_quote_age_ms = await self.get_variational_best_bid_ask(self.variational_ticker)
            lighter_bid, lighter_ask = await self.get_lighter_best_bid_ask()
            if None in (var_bid, var_ask, lighter_bid, lighter_ask):
                _now = time.monotonic()
                if _now - getattr(self, "_price_none_last_log_ts", 0) >= 60:
                    self.logger.warning(" price feed unavailable " "(var_bid=%s var_ask=%s lighter_bid=%s lighter_ask=%s) — skipping",
                        var_bid, var_ask, lighter_bid, lighter_ask,  )
                    self._price_none_last_log_ts = _now
                continue

            # Price sanity check: skip if two exchanges diverge >VAR_MAX_PRICE_DEVIATION_PCT
            var_mid = (var_bid + var_ask) / 2
            lighter_mid = (lighter_bid + lighter_ask) / 2
            price_deviation_pct = abs(var_mid - lighter_mid) / lighter_mid * 100
            if price_deviation_pct > self.max_price_deviation_pct:
                self.logger.warning(
                    "signal_loop: price deviation %.2f%% exceeds limit %.2f%% "
                    "(var_mid=%s lighter_mid=%s) — skipping",
                    price_deviation_pct, self.max_price_deviation_pct, var_mid, lighter_mid,
                )
                continue

            # Record instantaneous book spreads for rolling average
            _now_mono = time.monotonic()
            _var_bsp = float(book_spread_percent(var_bid, var_ask) or 0)
            _lit_bsp = float(book_spread_percent(lighter_bid, lighter_ask) or 0)
            self._book_spread_history.append((_now_mono, _var_bsp, _lit_bsp))
            _bsh_cutoff = _now_mono - self.spread_avg_window_seconds
            while self._book_spread_history and self._book_spread_history[0][0] < _bsh_cutoff:
                self._book_spread_history.popleft()

            # Threshold = avg(var_spread) + avg(lighter_spread) over the window, plus profit margin
            _entries = self._book_spread_history
            if len(_entries) < 60:
                if _now_mono - getattr(self, "_warmup_log_ts", 0) >= 30:
                    self.logger.info("signal_loop: warmup — %d/60 samples collected, skipping", len(_entries))
                    self._warmup_log_ts = _now_mono
                continue
            _avg_var = Decimal(str(sum(v for _, v, _ in _entries) / len(_entries)))
            _avg_lit = Decimal(str(sum(l for _, _, l in _entries) / len(_entries)))
            _spread_baseline = _avg_var + _avg_lit
            open_threshold = _spread_baseline + self.open_profit_margin_pct
            close_threshold = _spread_baseline + self.close_profit_margin_pct

            # 计算跨所价差百分比，正值代表 Lighter 买一比 Var 卖一贵，负值代表 Var 买一比 Lighter 卖一贵
            long_pct = spread_percent(spread_value(var_ask, lighter_bid), var_ask)
            short_pct = spread_percent(spread_value(lighter_ask, var_bid), lighter_ask)
            _now = time.monotonic()
            self.logger.info(
                    "signal_loop: long_pct=%.4f%% short_pct=%.4f%%  "
                    "open_thr=%.4f%%(%.4fU) close_thr=%.4f%%(%.4fU) narrow_close=%.4f%%(%.4fU)",
                    long_pct or 0, short_pct or 0,
                    open_threshold, float(open_threshold) / 100 * float(var_ask),
                    close_threshold, float(close_threshold) / 100 * float(var_ask),
                    self.narrow_close_pct, float(self.narrow_close_pct) / 100 * float(var_ask),
                )
            # 防止价差快照log过于频繁，限制每60秒输出一次
            if _now - getattr(self, "_spread_log_ts", 0) >= 60:
                self.logger.info("lt_ask=%s lt_bid=%s var_ask=%s var_bid=%s", lighter_ask, lighter_bid, var_ask, var_bid)
                self._spread_log_ts = _now

            all_pos = self.runtime.monitor.positions
            # Use internal notional counters for limit enforcement — they are updated
            # synchronously on every successful order and are not affected by monitor
            # update latency or unparseable value field formats.
            # 多头和空头敞口加起来，计算总的名义价值，用于和max_total_notional_usdc比较，防止过度开仓,
            actual_total = self._open_long_notional + self._open_short_notional
            # cur_qty是当前持仓数量，has_long和has_short是判断当前是否有多头或空头持仓
            cur_pos = all_pos.get(self.variational_ticker, {})
            cur_qty = to_decimal(cur_pos.get("qty")) or Decimal("0")
            _portfolio_state = await self.runtime.monitor.get_trading_state()
            _portfolio_age = _portfolio_state.get("portfolio_age")
            _PORTFOLIO_STALE_SEC = 300
            if _portfolio_age is not None and _portfolio_age > _PORTFOLIO_STALE_SEC:
                # Portfolio WS stale: fall back to notional counters to avoid ghost closes
                # from stale cur_qty. If notional counters say flat, we are flat.
                has_long = self._open_long_notional > Decimal("0")
                has_short = self._open_short_notional > Decimal("0")
                if _now_mono - getattr(self, "_portfolio_stale_log_ts", 0) >= 60:
                    self.logger.warning(
                        "signal_loop: portfolio stale (%.0fs) — has_long/has_short from notional "
                        "counters (long=%s short=%s) instead of cur_qty=%s",
                        _portfolio_age, has_long, has_short, cur_qty,
                    )
                    self._portfolio_stale_log_ts = _now_mono
            else:
                has_long = cur_qty > Decimal("0.000001")
                has_short = cur_qty < Decimal("-0.000001")

            # 仓位利用率越高，开仓门槛越严：空仓时按min_mult放宽，接近满仓时按max_mult收紧，
            # 只把剩下的一点配额留给特别好的机会
            _utilization = min(max(actual_total / self.max_total_notional_usdc, Decimal("0")), Decimal("1"))
            _margin_mult = self.open_margin_min_mult + (self.open_margin_max_mult - self.open_margin_min_mult) * _utilization
            scaled_open_threshold = _spread_baseline + self.open_profit_margin_pct * _margin_mult

            # 防止log过于频繁，限制每60秒输出一次
            if _now - getattr(self, "_pos_log_ts", 0) >= 60:
                self.logger.info(
                    "signal_loop: var_qty=%s lt_qty=%s total=%s/%sU util=%.1f%% margin_mult=%.2f  "
                    "long_pct=%.4f%% short_pct=%.4f%% open_thr_base=%.4f%% open_thr_scaled=%.4f%% "
                    "close_thr=%.4f%% narrow_close=%.4f%%  ",
                    cur_qty, self._lighter_actual_qty, actual_total, self.max_total_notional_usdc,
                    _utilization * 100, _margin_mult,
                    long_pct or 0, short_pct or 0, open_threshold, scaled_open_threshold,
                    close_threshold, self.narrow_close_pct
                )
                self._pos_log_ts = _now

            # 优先处理平仓, 有多仓而且平仓价差比大于平仓阈值，short_pct是当前空头价差百分比，要比平仓阈值close_threshold大，说明空头价差过大，触发平多条件
            if has_long and short_pct is not None and short_pct >= close_threshold:
                self.logger.info(
                    "signal_loop: [条件A] 反转平多 short_pct=%.4f%% >= close_thr=%.4f%% "
                    "(long_pct=%.4f%% open_thr=%.4f%% var_bid=%s var_ask=%s lt_bid=%s lt_ask=%s qty=%s)",
                    short_pct, close_threshold, long_pct or 0, open_threshold,
                    var_bid, var_ask, lighter_bid, lighter_ask, cur_qty,
                )
                qty = (self.order_notional_usdc / var_bid).quantize(Decimal("0.000001"))
                await self._write_bbo_snapshot(
                    var_bid, var_ask, lighter_bid, lighter_ask,
                    long_pct, short_pct, open_threshold, close_threshold, event="close",
                )
                await self._trigger_variational_order("sell", qty, short_pct, is_close=True, quote_age_ms=var_quote_age_ms)
                continue
            # 有空仓，而且做多价差大于平仓阈值。
            if has_short and long_pct is not None and long_pct >= close_threshold:
                self.logger.info(
                    "signal_loop: [条件A] 反转平空 long_pct=%.4f%% >= close_thr=%.4f%% "
                    "(short_pct=%.4f%% open_thr=%.4f%% var_bid=%s var_ask=%s lt_bid=%s lt_ask=%s qty=%s)",
                    long_pct, close_threshold, short_pct or 0, open_threshold,
                    var_bid, var_ask, lighter_bid, lighter_ask, cur_qty,
                )
                qty = (self.order_notional_usdc / var_ask).quantize(Decimal("0.000001"))
                await self._write_bbo_snapshot(
                    var_bid, var_ask, lighter_bid, lighter_ask,
                    long_pct, short_pct, open_threshold, close_threshold, event="close",
                )
                await self._trigger_variational_order("buy", qty, long_pct, is_close=True, quote_age_ms=var_quote_age_ms)
                continue


            #long_pct	做多价差%：(lighter_bid - var_ask) / var_ask，正值代表 Lighter 买一比 Var 卖一贵
            #short_pct	做空价差%：(var_bid - lighter_ask) / lighter_ask，正值代表 Var 买一比 Lighter 卖一贵
            #close_threshold	动态平仓阈值，= Lighter 内部买卖价差 × close_multiplier
            #narrow_close_pct	收窄平仓兜底阈值（环境变量 VAR_NARROW_CLOSE_PCT，默认 0.01%）
            #narrow_close_delta	动态收窄容忍量（VAR_NARROW_CLOSE_DELTA_PCT，默认 0.02%）
            #open_spread_pct	开仓时实际捕获的价差%（用来算动态平仓阈值）
            #narrow_threshold	本轮判断用的收窄阈值（动态或兜底）
            #oldest_rec	队列里最老的未平开仓记录（FIFO）

            # Narrow-spread close: close when spread has narrowed by at least DELTA from opening.
            # Falls back to absolute VAR_NARROW_CLOSE_PCT floor when no open record is available.       
            if has_long and long_pct is not None:
                narrow_threshold = self.narrow_close_pct  # fallback: +0.01% (near zero, close before losing)
                _using_dynamic = False
                if self._open_trade_queue:
                    # 账户里同方向持仓是一个整体仓位，分不出哪笔成交对应哪一份——
                    # 用队列里所有未平记录按qty加权平均开仓价差，而不是只看最老一笔，
                    # 避免某一笔开在价差异常宽/窄时刻的记录单独把阈值带偏、卡住整条队列。
                    total_qty = Decimal("0")
                    weighted_spread = Decimal("0")
                    for key in self._open_trade_queue:
                        rec = self.records.get(key)
                        if rec and rec.var_fill_price and rec.lighter_fill_price:
                            spread = (rec.lighter_fill_price - rec.var_fill_price) / rec.var_fill_price * 100
                            weighted_spread += spread * rec.qty
                            total_qty += rec.qty
                    if total_qty > 0:
                        open_spread_pct = weighted_spread / total_qty
                        narrow_threshold = open_spread_pct - self.narrow_close_delta
                        _using_dynamic = True
                        self.logger.info(
                            "signal_loop: [条件B] 持仓均价动态收窄阈值 long_pct=%.4f%% < thr=%.4f%% "
                            "(avg_open_spread=%.4f%% n=%d total_qty=%s)",
                            long_pct, narrow_threshold, open_spread_pct,
                            len(self._open_trade_queue), total_qty,
                        )
                    else:
                        self.logger.debug(
                            "signal_loop: [条件B] var_fill_price 缺失，使用兜底阈值 %.4f%%",
                            self.narrow_close_pct,
                        )
                if long_pct < narrow_threshold:
                    self.logger.info(
                        "signal_loop: [条件B] 收窄平多 long_pct=%.4f%% < thr=%.4f%% (%s)",
                        long_pct, narrow_threshold,
                        "动态" if _using_dynamic else "兜底",
                    )
                    qty = (self.order_notional_usdc / var_bid).quantize(Decimal("0.000001"))
                    await self._write_bbo_snapshot(
                        var_bid, var_ask, lighter_bid, lighter_ask,
                        long_pct, short_pct, open_threshold, close_threshold, event="close",
                    )
                    await self._trigger_variational_order("sell", qty, long_pct, is_close=True, quote_age_ms=var_quote_age_ms)
                    continue
            if has_short and short_pct is not None:
                narrow_threshold = -self.narrow_close_pct  # fallback: -0.01% (near zero, close before losing)
                _using_dynamic = False
                if self._open_trade_queue:
                    # 同上：按qty加权平均所有未平仓记录的开仓价差，而不是只看最老一笔
                    total_qty = Decimal("0")
                    weighted_spread = Decimal("0")
                    for key in self._open_trade_queue:
                        rec = self.records.get(key)
                        if rec and rec.var_fill_price and rec.lighter_fill_price:
                            spread = (rec.var_fill_price - rec.lighter_fill_price) / rec.lighter_fill_price * 100
                            weighted_spread += spread * rec.qty
                            total_qty += rec.qty
                    if total_qty > 0:
                        open_spread_pct = weighted_spread / total_qty
                        narrow_threshold = -(open_spread_pct - self.narrow_close_delta)
                        _using_dynamic = True
                        self.logger.info(
                            "signal_loop: [条件B] 持仓均价动态收窄阈值 short_pct=%.4f%% > thr=%.4f%% "
                            "(avg_open_spread=%.4f%% n=%d total_qty=%s)",
                            short_pct, narrow_threshold, open_spread_pct,
                            len(self._open_trade_queue), total_qty,
                        )
                    else:
                        self.logger.debug(
                            "signal_loop: [条件B] var_fill_price 缺失，使用兜底阈值 %.4f%%",
                            self.narrow_close_pct,
                        )
                if short_pct > narrow_threshold:
                    self.logger.info(
                        "signal_loop: [条件B] 收窄平空 short_pct=%.4f%% > thr=%.4f%% (%s)",
                        short_pct, narrow_threshold,
                        "动态" if _using_dynamic else "兜底",
                    )
                    qty = (self.order_notional_usdc / var_ask).quantize(Decimal("0.000001"))
                    await self._write_bbo_snapshot(
                        var_bid, var_ask, lighter_bid, lighter_ask,
                        long_pct, short_pct, open_threshold, close_threshold, event="close",
                    )
                    await self._trigger_variational_order("buy", qty, short_pct, is_close=True, quote_age_ms=var_quote_age_ms)
                    continue

            # 上面是处理完了平仓逻辑，接下来要看开仓逻辑。Block opens if single-leg mismatch is detected
            if self._single_leg_blocked:
                _now = time.monotonic()
                if _now - getattr(self, "_single_leg_log_ts", 0) >= 60:
                    self.logger.warning("signal_loop: 单腿保护激活，开仓已阻止")
                    self._single_leg_log_ts = _now
                continue

            # Open new position only if total notional is within limit
            if actual_total + self.order_notional_usdc > self.max_total_notional_usdc:
                _now = time.monotonic()
                if _now - getattr(self, "_notional_limit_last_log_ts", 0) >= 300:
                    self.logger.warning(
                        "signal_loop: notional limit reached (current=%s limit=%s) — open blocked",
                        actual_total, self.max_total_notional_usdc,
                    )
                    self._notional_limit_last_log_ts = _now
                continue
            # 开仓条件：需要连续 VAR_OPEN_CONFIRM_TICKS 个 tick 都满足才触发，过滤单tick噪音spike
            if long_pct is not None and long_pct >= scaled_open_threshold and long_pct >= self.min_open_spread_pct:
                if self._open_confirm_dir != "long":
                    self._open_confirm_count = 0
                    self._open_confirm_dir = "long"
                self._open_confirm_count += 1
                if self._open_confirm_count < self.open_confirm_ticks:
                    self.logger.info(
                        "signal_loop: 开仓确认中 %d/%d (long_pct=%.4f%% thr=%.4f%%)",
                        self._open_confirm_count, self.open_confirm_ticks, long_pct, scaled_open_threshold,
                    )
                else:
                    self._open_confirm_count = 0
                    self._open_confirm_dir = ""
                    qty = (self.order_notional_usdc / var_ask).quantize(Decimal("0.000001"))
                    await self._write_bbo_snapshot(
                        var_bid, var_ask, lighter_bid, lighter_ask,
                        long_pct, short_pct, scaled_open_threshold, close_threshold, event="open",
                    )
                    await self._trigger_variational_order("buy", qty, long_pct, quote_age_ms=var_quote_age_ms)
            elif short_pct is not None and short_pct >= scaled_open_threshold and short_pct >= self.min_open_spread_pct:
                if self._open_confirm_dir != "short":
                    self._open_confirm_count = 0
                    self._open_confirm_dir = "short"
                self._open_confirm_count += 1
                if self._open_confirm_count < self.open_confirm_ticks:
                    self.logger.info(
                        "signal_loop: 开仓确认中 %d/%d (short_pct=%.4f%% thr=%.4f%%)",
                        self._open_confirm_count, self.open_confirm_ticks, short_pct, scaled_open_threshold,
                    )
                else:
                    self._open_confirm_count = 0
                    self._open_confirm_dir = ""
                    qty = (self.order_notional_usdc / var_bid).quantize(Decimal("0.000001"))
                    await self._write_bbo_snapshot(
                        var_bid, var_ask, lighter_bid, lighter_ask,
                        long_pct, short_pct, scaled_open_threshold, close_threshold, event="open",
                    )
                    await self._trigger_variational_order("sell", qty, short_pct, quote_age_ms=var_quote_age_ms)
            else:
                self._open_confirm_count = 0
                self._open_confirm_dir = ""

    async def _trigger_variational_order(
        self, side: str, qty: Decimal, trigger_pct: Decimal, is_close: bool = False,
        quote_age_ms: float | None = None,
    ) -> None:
        self.logger.info(
            "trigger_variational_order: side=%s qty=%s spread=%.4f%% is_close=%s quote_age=%sms",
            side, qty, trigger_pct, is_close,
            f"{quote_age_ms:.1f}" if quote_age_ms is not None else "?",
        )
        if self._order_in_flight:
            return
        self._order_in_flight = True
        self._inflight_order_side = side
        qty_str = format(qty, "f")
        action = "CLOSE" if is_close else "OPEN"
        try:
            # Suppress "Signal triggered" noise while injection is consistently failing
            if self._injection_fail_count == 0:
                self.logger.info(
                    "Signal triggered (%s): side=%s qty=%s spread=%.4f%% long_exp=%sU short_exp=%sU",
                    action, side, qty_str, trigger_pct,
                    self._open_long_notional, self._open_short_notional,
                )
            result = await self.runtime.broker.place_order_internal(
                side=side,
                amount=qty_str,
                max_slippage=0.01,
                is_reduce_only=is_close,
            )
            if result.get("ok"):
                self._last_variational_order_ts = time.monotonic()
                self._injection_fail_count = 0
                self._injection_fail_last_log_ts = 0.0
                if is_close:
                    if side == "sell":
                        self._open_long_notional = max(Decimal("0"), self._open_long_notional - self.order_notional_usdc)
                    else:
                        self._open_short_notional = max(Decimal("0"), self._open_short_notional - self.order_notional_usdc)
                else:
                    if side == "buy":
                        self._open_long_notional += self.order_notional_usdc
                    else:
                        self._open_short_notional += self.order_notional_usdc
                rfq_id = (result.get("data") or {}).get("rfq_id", "-")
                _buf = self._inflight_fill_event
                _fill_price = to_decimal(_buf.get("price")) if _buf and str(_buf.get("side", "")).lower() == side else None
                self.logger.info(
                    "Variational order ok (%s): side=%s qty=%s price=%s spread=%.4f%% → long_exp=%sU short_exp=%sU",
                    action, side, qty_str, _fill_price or "pending", trigger_pct,
                    self._open_long_notional, self._open_short_notional,
                )
                self.logger.info(
                    "Variational submit timing: lock_wait=%.1fms api_elapsed=%.1fms submit_total=%.1fms quote_age=%sms",
                    result.get("_lock_wait_ms", 0.0), result.get("_api_elapsed_ms", 0.0),
                    result.get("_submit_total_ms", 0.0),
                    f"{quote_age_ms:.1f}" if quote_age_ms is not None else "?",
                )
                await self.append_order_log("variational_submit_timing", {
                    "trade_id": rfq_id,
                    "side": side,
                    "qty": qty_str,
                    "is_close": is_close,
                    "quote_age_ms": quote_age_ms,
                    "lock_wait_ms": result.get("_lock_wait_ms"),
                    "api_elapsed_ms": result.get("_api_elapsed_ms"),
                    "submit_total_ms": result.get("_submit_total_ms"),
                })
                # Place Lighter hedge immediately — Variational fill events are not
                # reliably delivered (reduce-only closes sometimes produce no event),
                # so we cannot wait for trade_loop to trigger the hedge.
                if self.args.auto_hedge:
                    pending_key = rfq_id[:8] if rfq_id and rfq_id != "-" else f"rfq:{int(time.monotonic()*1000)}"
                    pending_rec = OrderLifecycle(
                        trade_key=pending_key,
                        trade_id=rfq_id if rfq_id != "-" else pending_key,
                        side=side,
                        qty=qty,
                        asset=self.variational_ticker or "ETH",
                        auto_hedge_enabled=True,
                        last_variational_status="filled",
                    )
                    pending_rec.var_fill_ts_iso = utc_now()
                    async with self._record_lock:
                        if pending_key not in self.records:
                            self.records[pending_key] = pending_rec
                            self.record_order.append(pending_key)
                        # Apply fill price buffered by process_variational_trade_event if the
                        # fill event arrived while we were awaiting place_order_internal above.
                        buf = self._inflight_fill_event
                        if buf is not None and str(buf.get("side", "")).lower() == side:
                            pending_rec.var_fill_price = to_decimal(buf.get("price"))
                            ts_raw = buf.get("timestamp")
                            pending_rec.var_fill_ts_iso = to_cst_str(str(ts_raw)) if ts_raw else utc_now()
                            if side == "buy":
                                self._open_trade_queue.append(pending_key)
                            elif side == "sell" and self._open_trade_queue:
                                open_key = self._open_trade_queue.popleft()
                                open_rec = self.records.get(open_key)
                                if open_rec:
                                    pending_rec.matched_open_key = open_key
                                    open_rec.matched_open_key = pending_key
                            self._inflight_fill_event = None
                    # Pre-hedged token — for the case where the fill event arrives AFTER
                    # we release _order_in_flight (below); fast-path in
                    # process_variational_trade_event will merge fill price into pending_rec.
                    now_mono = time.monotonic()
                    self._pre_hedged.append((side, now_mono, pending_key))
                    self._pre_hedged = [(s, t, k) for s, t, k in self._pre_hedged if now_mono - t < 180]
                    await self.place_lighter_order(pending_rec)
                    await self.append_order_log("variational_fill", pending_rec.to_payload())
                    if pending_rec.hedge_error is not None:
                        self._pre_hedged = [(s, t, k) for s, t, k in self._pre_hedged if k != pending_key]
            else:
                error_msg = str(result.get("error", ""))
                if "Injection failed" in error_msg or "accept HTTP" in error_msg:
                    self._injection_fail_count += 1
                    since_last = time.monotonic() - self._injection_fail_last_log_ts
                    if self._injection_fail_count == 1 or since_last >= 600:
                        self.logger.warning(
                            "Variational injection failing (%s): side=%s spread=%.4f%% [attempt #%d] error=%r",
                            action, side, trigger_pct, self._injection_fail_count, error_msg,
                        )
                        self._injection_fail_last_log_ts = time.monotonic()
                else:
                    self._injection_fail_count = 0
                    self.logger.warning(
                        "Variational order failed (%s): side=%s qty=%s error=%s",
                        action, side, qty_str, error_msg,
                    )
        except Exception as exc:
            self._last_variational_order_ts = time.monotonic()
            self._injection_fail_count = 0
            self.logger.error("Variational order error: %s", exc)
        finally:
            self._inflight_fill_event = None
            self._inflight_order_side = None
            self._order_in_flight = False

    @staticmethod
    def _fmt_ts(iso: str | None) -> str:
        if not iso:
            return "-"
        try:
            dt = datetime.fromisoformat(iso)
            # Naive strings from utc_now()/to_cst_str() are already CST wall-clock time —
            # tag them as CST directly instead of letting astimezone() assume system-local tz.
            dt = dt.replace(tzinfo=CST) if dt.tzinfo is None else dt.astimezone(CST)
            ms = dt.strftime("%f")[:2]
            return dt.strftime(f"%y%m%d,%H:%M:%S.{ms}")
        except Exception:
            return "-"

    def _fmt_price(self, value: Decimal | None) -> str:
        if value is None:
            return "-"
        return f"{value:,.2f}"

    def _fmt_qty(self, value: Decimal | None) -> str:
        if value is None:
            return "-"
        return f"{value:.6f}".rstrip("0").rstrip(".")

    @staticmethod
    def _direction_labels(side: str) -> tuple[str, str]:
        side_n = side.strip().lower()
        if side_n == "buy":
            return "L Var / S Lighter", "L Var / S Lighter"
        if side_n == "sell":
            return "S Var / L Lighter", "S Var / L Lighter"
        side_u = side_n.upper() if side_n else "-"
        return side_u, side_u

    def _fmt_pct(self, value: Decimal | None) -> str:
        if value is None:
            return "-"
        return f"{value:.4f}%"

    def _fmt_signal_pct(
        self,
        current: Decimal | None,
        book_spread_baseline: Decimal | None,
        median_5m: float | None,
        median_30m: float | None,
        median_1h: float | None,
    ) -> str:
        if current is None:
            return "-"
        if book_spread_baseline is None:
            color = "red"
            return f"[{color}]{self._fmt_pct(current)}[/{color}]"

        adjusted = current - book_spread_baseline
        adjusted_f = float(adjusted)
        thresholds = [v for v in (median_5m, median_30m, median_1h) if v is not None]
        is_green = any(adjusted_f > threshold for threshold in thresholds)
        color = "green" if is_green else "red"
        return f"[{color}]{self._fmt_pct(current)}[/{color}]"

    @staticmethod
    def _fill_diff_by_direction(
        side: str,
        var_fill_price: Decimal | None,
        lighter_fill_price: Decimal | None,
    ) -> tuple[Decimal | None, Decimal | None]:
        side_n = side.strip().lower()
        if side_n == "buy":
            # Long Var / Short Lighter: lighter_fill - var_fill
            diff = spread_value(var_fill_price, lighter_fill_price)
            pct = spread_percent(diff, var_fill_price)
            return diff, pct
        if side_n == "sell":
            # Short Var / Long Lighter: var_fill - lighter_fill
            diff = spread_value(lighter_fill_price, var_fill_price)
            pct = spread_percent(diff, lighter_fill_price)
            return diff, pct
        diff = spread_value(lighter_fill_price, var_fill_price)
        pct = spread_percent(diff, var_fill_price)
        return diff, pct

    @staticmethod
    def _decimal_as_float(value: Decimal | None) -> float | None:
        if value is None:
            return None
        return float(value)

    @staticmethod
    def _fmt_median_pct(value: float | None) -> str:
        if value is None:
            return "-"
        return f"{value:.4f}%"

    def _record_cross_spreads(
        self,
        long_var_short_lighter_pct: Decimal | None,
        short_var_long_lighter_pct: Decimal | None,
    ) -> None:
        now = time.monotonic()
        self.cross_spread_history.append(
            (
                now,
                self._decimal_as_float(long_var_short_lighter_pct),
                self._decimal_as_float(short_var_long_lighter_pct),
            )
        )
        cutoff = now - SPREAD_HISTORY_SECONDS
        while self.cross_spread_history and self.cross_spread_history[0][0] < cutoff:
            self.cross_spread_history.popleft()

    def _median_cross_spread(self, window_seconds: float, long_side: bool) -> float | None:
        now = time.monotonic()
        cutoff = now - window_seconds
        value_index = 1 if long_side else 2
        values = [
            row[value_index]
            for row in self.cross_spread_history
            if row[0] >= cutoff and row[value_index] is not None
        ]
        if not values:
            return None
        return float(median(values))

    async def render_dashboard(self) -> Group:
        var_bid, var_ask, quote_asset, _ = await self.get_variational_best_bid_ask(self.variational_ticker)
        lighter_bid, lighter_ask = await self.get_lighter_best_bid_ask()
        var_book_spread = spread_value(var_bid, var_ask)
        lighter_book_spread = spread_value(lighter_bid, lighter_ask)
        var_book_spread_pct = book_spread_percent(var_bid, var_ask)
        lighter_book_spread_pct = book_spread_percent(lighter_bid, lighter_ask)
        spread_color_baseline: Decimal | None = None
        if var_book_spread_pct is not None and lighter_book_spread_pct is not None:
            spread_color_baseline = (var_book_spread_pct + lighter_book_spread_pct) / Decimal("2")

        long_var_short_lighter_pct = spread_percent(spread_value(var_ask, lighter_bid), var_ask)
        short_var_long_lighter_pct = spread_percent(spread_value(lighter_ask, var_bid), lighter_ask)
        self._record_cross_spreads(
            long_var_short_lighter_pct,
            short_var_long_lighter_pct,
        )

        long_pct_median_5m = self._median_cross_spread(5 * 60, long_side=True)
        long_pct_median_30m = self._median_cross_spread(30 * 60, long_side=True)
        long_pct_median_1h = self._median_cross_spread(60 * 60, long_side=True)
        short_pct_median_5m = self._median_cross_spread(5 * 60, long_side=False)
        short_pct_median_30m = self._median_cross_spread(30 * 60, long_side=False)
        short_pct_median_1h = self._median_cross_spread(60 * 60, long_side=False)

        async with self._record_lock:
            recent_keys = list(self.record_order)[-DASHBOARD_ORDERS:]
            rows = [self.records[key] for key in reversed(recent_keys) if key in self.records]

        is_zh = self.args.lang == "zh"
        header_title = "Variational <-> Lighter"
        auto_hedge_label = "对冲" if is_zh else "hedge"
        auto_hedge_on = "开" if is_zh else "ON"
        auto_hedge_off = "关" if is_zh else "OFF"
        quote_title = "最优买一 / 卖一" if is_zh else "Best Bid / Ask"
        col_exchange = "交易所" if is_zh else "Exchange"
        col_bid = "买一" if is_zh else "Bid"
        col_ask = "卖一" if is_zh else "Ask"
        col_book_spread = "买卖价差" if is_zh else "Bid/Ask Spread"
        col_book_spread_pct = "买卖价差%" if is_zh else "Bid/Ask Spread %"
        spread_title = "价差" if is_zh else "Spreads"
        col_metric = "指标" if is_zh else "Metric"
        col_formula = "公式" if is_zh else "Formula"
        col_value_pct = "当前值%" if is_zh else "Value %"
        col_median_5m_pct = "5分钟中位数%" if is_zh else "Median 5m %"
        col_median_30m_pct = "30分钟中位数%" if is_zh else "Median 30m %"
        col_median_1h_pct = "1小时中位数%" if is_zh else "Median 1h %"
        metric_long_short = "L Var / S Lighter"
        metric_short_long = "S Var / L Lighter"
        orders_title = "最近订单（最新在前）" if is_zh else "Recent Orders (latest first)"
        col_trade_id = "订单ID" if is_zh else "Trade ID"
        col_side = "方向" if is_zh else "Side"
        col_qty = "数量" if is_zh else "Qty"
        col_var_fill_px = "Var 成交价" if is_zh else "Var Fill Px"
        col_lighter_fill_px = "Lighter 成交价" if is_zh else "Lighter Fill Px"
        col_fill_ts = "成交时间" if is_zh else "Fill Time"
        col_fill_diff = "价差(U)" if is_zh else "Spread(U)"
        col_fill_diff_pct = "价差%" if is_zh else "Spread%"
        col_roundtrip_pnl = "回合盈亏U" if is_zh else "P&L(U)"
        no_orders_text = "（暂无订单）" if is_zh else "(no tracked orders yet)"
        variational_label = "Variational"
        lighter_label = "Lighter"
        hedge_color = "green" if self.args.auto_hedge else "red"
        hedge_text = auto_hedge_on if self.args.auto_hedge else auto_hedge_off

        now_mono = time.monotonic()
        cooldown_remaining = self.order_cooldown_seconds - (now_mono - self._last_variational_order_ts)
        all_positions = self.runtime.monitor.positions
        var_mid_dash, _, _, _ = await self.get_variational_best_bid_ask(self.variational_ticker)
        _var_ref = var_mid_dash or Decimal("0")
        def _pos_notional_dash(p: dict) -> Decimal:
            v = to_decimal(p.get("value"))
            if v is not None:
                return abs(v)
            qty = abs(to_decimal(p.get("qty")) or Decimal("0"))
            return qty * _var_ref
        total_notional = sum(_pos_notional_dash(p) for p in all_positions.values())

        # Dynamic thresholds from Lighter internal spread
        def _fmt_spread(val: Decimal | None) -> str:
            if val is None:
                return "-"
            color = "green" if val > 0 else "red"
            return f"[{color}]{val:.4f}%[/{color}]"

        if lighter_bid and lighter_ask and lighter_bid > 0 and var_ask and var_bid:
            if self._book_spread_history:
                _e = self._book_spread_history
                _bsl = Decimal(str(sum(v for _, v, _ in _e) / len(_e))) + Decimal(str(sum(l for _, _, l in _e) / len(_e)))
            else:
                _bsl = (book_spread_percent(var_bid, var_ask) or Decimal("0")) + (book_spread_percent(lighter_bid, lighter_ask) or Decimal("0"))
            open_thr = _bsl + self.open_profit_margin_pct
            close_thr = _bsl + self.close_profit_margin_pct
            open_thr_abs = open_thr * var_ask / 100
            close_thr_abs = close_thr * var_ask / 100
            long_abs = lighter_bid - var_ask
            short_abs = var_bid - lighter_ask

            def _fmt_abs(val: Decimal) -> str:
                color = "green" if val > 0 else "red"
                return f"[{color}]{val:+.2f}[/{color}]"

            min_floor_text = f"  绝对门槛≥[bold]{self.min_open_spread_pct:.4f}%[/bold]" if self.min_open_spread_pct > 0 else ""
            confirm_text = f"  确认{self._open_confirm_count}/{self.open_confirm_ticks}t" if self._open_confirm_count > 0 else ""
            threshold_text = (
                f"开仓阈值≥[bold]{open_thr:.4f}%[/bold]({open_thr_abs:.2f}U){min_floor_text}{confirm_text}  "
                f"平仓阈值≥[bold]{close_thr:.4f}%[/bold]({close_thr_abs:.2f}U)  "
                f"收窄平仓<[bold]{self.narrow_close_pct:.2f}%[/bold]  │  "
                f"当前价差 多{_fmt_spread(long_var_short_lighter_pct)}({_fmt_abs(long_abs)}) "
                f"空{_fmt_spread(short_var_long_lighter_pct)}({_fmt_abs(short_abs)})"
            )
        else:
            threshold_text = f"开仓余量+{self.open_profit_margin_pct}% 平仓余量+{self.close_profit_margin_pct}% (等待价格数据)"

        # Signal state
        if self._order_in_flight:
            state_text = "[yellow]下单中 IN FLIGHT[/yellow]"
        elif cooldown_remaining > 0:
            state_text = f"[yellow]冷却中 COOLDOWN {int(cooldown_remaining)}s[/yellow]"
        elif total_notional >= self.max_total_notional_usdc:
            state_text = "[cyan]满仓 CLOSE ONLY[/cyan]"
        else:
            state_text = "[green]监控中 MONITORING[/green]"

        # Current asset position
        cur_pos = all_positions.get(self.variational_ticker, {})
        cur_qty = to_decimal(cur_pos.get("qty")) or Decimal("0")
        cur_val = abs(to_decimal(cur_pos.get("value")) or Decimal("0"))
        cur_upnl = to_decimal(cur_pos.get("upnl"))
        if cur_qty == 0:
            cur_pos_text = "flat"
        else:
            direction = "多" if cur_qty > 0 else "空"
            upnl_str = ""
            if cur_upnl is not None:
                upnl_color = "green" if cur_upnl >= 0 else "red"
                upnl_pct = cur_upnl / cur_val * 100 if cur_val > 0 else Decimal("0")
                upnl_str = f"  UPnL=[{upnl_color}]{cur_upnl:+.2f}U({upnl_pct:+.3f}%)[/{upnl_color}]"
            cur_pos_text = f"{direction}{cur_val:.0f}U{upnl_str}"

        lighter_qty = self._lighter_actual_qty
        if lighter_qty > Decimal("0.001"):
            lt_pos_text = f"空{lighter_qty:.6f}".rstrip("0").rstrip(".")
        elif lighter_qty < Decimal("-0.001"):
            lt_pos_text = f"多{abs(lighter_qty):.6f}".rstrip("0").rstrip(".")
        else:
            lt_pos_text = "flat"

        header = Panel(
            f"[bold]{header_title}[/bold] | [bold]{self.ticker}[/bold] | "
            f"[bold {hedge_color}]{auto_hedge_label}={hedge_text}[/] | "
            f"状态={state_text} | {utc_now()}\n"
            f"信号: {threshold_text}\n"
            f"持仓: Var({self.ticker})={cur_pos_text}  Lighter={lt_pos_text}  总={total_notional:.0f}/{self.max_total_notional_usdc}U",
            border_style="cyan",
        )

        quote_table = Table(title=quote_title, show_header=True, expand=True)
        quote_table.add_column(col_exchange, style="bold")
        quote_table.add_column(col_bid, justify="right")
        quote_table.add_column(col_ask, justify="right")
        quote_table.add_column(col_book_spread, justify="right")
        quote_table.add_column(col_book_spread_pct, justify="right")
        quote_table.add_row(
            f"{variational_label} ({quote_asset or self.variational_ticker})",
            self._fmt_price(var_bid),
            self._fmt_price(var_ask),
            self._fmt_price(var_book_spread),
            self._fmt_pct(var_book_spread_pct),
        )
        quote_table.add_row(
            lighter_label,
            self._fmt_price(lighter_bid),
            self._fmt_price(lighter_ask),
            self._fmt_price(lighter_book_spread),
            self._fmt_pct(lighter_book_spread_pct),
        )

        spread_table = Table(title=spread_title, show_header=True, expand=True)
        spread_table.add_column(col_metric, style="bold")
        spread_table.add_column(col_formula)
        spread_table.add_column(col_value_pct, justify="right")
        spread_table.add_column(col_median_5m_pct, justify="right")
        spread_table.add_column(col_median_30m_pct, justify="right")
        spread_table.add_column(col_median_1h_pct, justify="right")
        spread_table.add_row(
            metric_long_short,
            "lighter_bid - var_ask",
            self._fmt_signal_pct(
                long_var_short_lighter_pct,
                spread_color_baseline,
                long_pct_median_5m,
                long_pct_median_30m,
                long_pct_median_1h,
            ),
            self._fmt_median_pct(long_pct_median_5m),
            self._fmt_median_pct(long_pct_median_30m),
            self._fmt_median_pct(long_pct_median_1h),
        )
        spread_table.add_row(
            metric_short_long,
            "var_bid - lighter_ask",
            self._fmt_signal_pct(
                short_var_long_lighter_pct,
                spread_color_baseline,
                short_pct_median_5m,
                short_pct_median_30m,
                short_pct_median_1h,
            ),
            self._fmt_median_pct(short_pct_median_5m),
            self._fmt_median_pct(short_pct_median_30m),
            self._fmt_median_pct(short_pct_median_1h),
        )

        orders_table = Table(title=orders_title, show_header=True, expand=True)
        orders_table.add_column(col_fill_ts)
        orders_table.add_column(col_trade_id)
        orders_table.add_column(col_side)
        orders_table.add_column(col_qty, justify="right")
        orders_table.add_column(col_var_fill_px, justify="right")
        orders_table.add_column(col_lighter_fill_px, justify="right")
        orders_table.add_column(col_fill_diff, justify="right")
        orders_table.add_column(col_fill_diff_pct, justify="right")
        orders_table.add_column(col_roundtrip_pnl, justify="right")

        if not rows:
            orders_table.add_row(
                no_orders_text,
                "-", "-", "-", "-", "-", "-", "-", "-",
            )
        else:
            for row in rows:
                payload = row.to_payload()
                trade_display = row.trade_id[:10] if row.trade_id else row.trade_key[:10]
                fill_diff, fill_diff_pct = self._fill_diff_by_direction(
                    row.side,
                    row.var_fill_price,
                    row.lighter_fill_price,
                )
                side_zh, side_en = self._direction_labels(row.side)
                side_display = side_zh if is_zh else side_en
                if row.roundtrip_pnl is not None:
                    pnl_color = "green" if row.roundtrip_pnl >= 0 else "red"
                    pnl_str = f"[{pnl_color}]{row.roundtrip_pnl:+.4f}[/{pnl_color}]"
                else:
                    pnl_str = "-"
                orders_table.add_row(
                    self._fmt_ts(row.var_fill_ts_iso),
                    trade_display,
                    side_display,
                    self._fmt_qty(row.qty),
                    self._fmt_price(row.var_fill_price),
                    self._fmt_price(row.lighter_fill_price),
                    self._fmt_price(fill_diff),
                    self._fmt_pct(fill_diff_pct),
                    pnl_str,
                )

        return Group(header, quote_table, spread_table, orders_table)

    async def export_trade_records_csv(self) -> None:
        if self.trade_records_csv_file is None:
            return

        async with self._record_lock:
            keys = list(self.record_order)
            rows: list[dict[str, Any]] = []
            for key in keys:
                record = self.records.get(key)
                if record is None:
                    continue
                payload = record.to_payload()
                fill_diff, fill_diff_pct = self._fill_diff_by_direction(
                    record.side,
                    record.var_fill_price,
                    record.lighter_fill_price,
                )
                side_zh, side_en = self._direction_labels(record.side)
                rows.append(
                    {
                        "trade_key": record.trade_key,
                        "trade_id": record.trade_id[:8] if record.trade_id else "",
                        "asset": record.asset,
                        "side_raw": record.side,
                        "direction_zh": side_zh,
                        "direction_en": side_en,
                        "qty": decimal_to_str(record.qty),
                        "variational_filled_price": payload["variational_filled_price"],
                        "variational_filled_at": payload["variational_filled_at"],
                        "lighter_order_side": payload["lighter_order_side"],
                        "lighter_client_order_id": payload["lighter_client_order_id"],
                        "lighter_filled_price": payload["lighter_filled_price"],
                        "lighter_filled_at": payload["lighter_filled_at"],
                        "fill_diff_var_minus_lighter": decimal_to_str(fill_diff),
                        "fill_diff_pct_vs_var": decimal_to_str(fill_diff_pct),
                        "var_pnl_usd": decimal_to_str(record.var_pnl) if record.var_pnl is not None else "",
                        "lt_pnl_usd": decimal_to_str(record.lt_pnl) if record.lt_pnl is not None else "",
                        "roundtrip_pnl_usd": decimal_to_str(record.roundtrip_pnl) if record.roundtrip_pnl is not None else "",
                        "auto_hedge_enabled": payload["auto_hedge_enabled"],
                        "hedge_error": payload["hedge_error"],
                        "last_variational_status": payload["last_variational_status"],
                    }
                )

        snapshot_sig = json.dumps(rows, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
        if snapshot_sig == self._trade_records_snapshot_sig:
            return

        fieldnames = [
            "trade_key",
            "trade_id",
            "asset",
            "side_raw",
            "direction_zh",
            "direction_en",
            "qty",
            "variational_filled_price",
            "variational_filled_at",
            "lighter_order_side",
            "lighter_client_order_id",
            "lighter_filled_price",
            "lighter_filled_at",
            "fill_diff_var_minus_lighter",
            "fill_diff_pct_vs_var",
            "var_pnl_usd",
            "lt_pnl_usd",
            "roundtrip_pnl_usd",
            "auto_hedge_enabled",
            "hedge_error",
            "last_variational_status",
        ]
        async with self._trade_csv_write_lock:
            if snapshot_sig == self._trade_records_snapshot_sig:
                return
            await asyncio.to_thread(self._write_csv_rows, self.trade_records_csv_file, fieldnames, rows)
            self._trade_records_snapshot_sig = snapshot_sig

    @staticmethod
    def _write_csv_rows(path: Path, fieldnames: list[str], rows: list[dict[str, Any]]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = path.with_name(f".{path.name}.tmp")
        with tmp_path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
        os.replace(tmp_path, path)

    async def dashboard_loop(self) -> None:
        refresh_interval = DASHBOARD_REFRESH_SECONDS
        refresh_per_second = max(1, int(round(1.0 / refresh_interval)))
        initial_render = await self.render_dashboard()
        await self.export_trade_records_csv()
        with Live(
            initial_render,
            console=self.dashboard_console,
            refresh_per_second=refresh_per_second,
            screen=True,
        ) as live:
            while not self.stop_flag:
                await asyncio.sleep(refresh_interval)
                live.update(await self.render_dashboard())
                await self.export_trade_records_csv()

    async def run(self) -> None:
        self.setup_signal_handlers()
        await self.runtime.start()
        self.print_startup_next_steps()
        self.logger.info(
            "Listening for Variational forwarder events on ws://%s:%s and ws://%s:%s",
            FORWARDER_HOST,
            FORWARDER_WS_PORT,
            FORWARDER_HOST,
            FORWARDER_REST_PORT,
        )

        await self.wait_for_variational_ready()
        self.logger.info("Variational heartbeat is live")
        self.initialize_lighter_client()
        initial_asset = await self.wait_for_ticker_resolution()
        await self.activate_asset(initial_asset, reason="startup")
        await self.load_initial_positions()

        self.trade_event_cursor = await self.runtime.monitor.get_latest_trade_event_seq()
        self.logger.info("Tracking new Variational trade events from seq>%s", self.trade_event_cursor)

        self.trade_task = asyncio.create_task(self.trade_loop())
        self.signal_task = asyncio.create_task(self.signal_loop())
        self.bbo_task = asyncio.create_task(self.bbo_loop())
        self.dashboard_task = asyncio.create_task(self.dashboard_loop())
        self._lighter_sync_task = asyncio.create_task(self.lighter_sync_loop())

        while not self.stop_flag:
            await asyncio.sleep(0.25)

    async def close(self) -> None:
        self.stop_flag = True

        if self.dashboard_task and not self.dashboard_task.done():
            self.dashboard_task.cancel()
            await asyncio.gather(self.dashboard_task, return_exceptions=True)

        if self.signal_task and not self.signal_task.done():
            self.signal_task.cancel()
            await asyncio.gather(self.signal_task, return_exceptions=True)

        if self.bbo_task and not self.bbo_task.done():
            self.bbo_task.cancel()
            await asyncio.gather(self.bbo_task, return_exceptions=True)

        if self._lighter_sync_task and not self._lighter_sync_task.done():
            self._lighter_sync_task.cancel()
            await asyncio.gather(self._lighter_sync_task, return_exceptions=True)

        if self.trade_task and not self.trade_task.done():
            self.trade_task.cancel()
            await asyncio.gather(self.trade_task, return_exceptions=True)

        if self.lighter_ws_task and not self.lighter_ws_task.done():
            self.lighter_ws_task.cancel()
            await asyncio.gather(self.lighter_ws_task, return_exceptions=True)

        if self.lighter_client is not None:
            close_method = getattr(self.lighter_client, "close", None)
            if callable(close_method):
                with contextlib.suppress(Exception):
                    close_result = close_method()
                    if asyncio.iscoroutine(close_result):
                        await close_result

        await self.runtime.stop()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Track Variational order lifecycle and optionally auto-hedge on Lighter (ticker auto-detected)."
    )
    parser.add_argument(
        "--lang",
        choices=["zh", "en"],
        default="zh",
        help="Dashboard language: zh (Chinese) or en (English). Default: zh",
    )
    parser.add_argument(
        "--no-hedge",
        action="store_false",
        dest="auto_hedge",
        help="Disable automatic Lighter hedge placement (default: enabled)",
    )
    parser.set_defaults(auto_hedge=True)
    return parser.parse_args()


async def _amain() -> None:
    load_dotenv()
    args = parse_args()
    runtime = VariationalToLighterRuntime(args)
    try:
        await runtime.run()
    finally:
        await runtime.close()


def main() -> None:
    asyncio.run(_amain())


if __name__ == "__main__":
    main()
