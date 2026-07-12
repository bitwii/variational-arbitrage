"""Local WebSocket receiver for the Variational Chrome CDP forwarder."""

from __future__ import annotations

import argparse
import asyncio
import base64
import json
import logging as _logging
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import websockets

_monitor_log = _logging.getLogger("var_lighter_runtime")


QUOTES_INDICATIVE_PATH = "/api/quotes/indicative"
QUOTES_ACCEPT_PATH = "/api/quotes/accept"
# 限流相关响应头的关键字，不区分大小写子串匹配——目前不知道 Variational 真正会用哪个头，
# 先把常见命名都覆盖到，宁可多打印几次也不要漏掉。
_RATE_LIMIT_HEADER_HINTS = ("ratelimit", "rate-limit", "retry-after", "x-rl-")
WS_EVENTS_PATH = "/events"
WS_PORTFOLIO_PATH = "/portfolio"
QUOTE_LOG_INTERVAL_SECONDS = 30
PORTFOLIO_LOG_INTERVAL_SECONDS = 300
HEARTBEAT_STALE_SECONDS = 11
HEARTBEAT_RECHECK_SECONDS = 10
HEARTBEAT_HOURLY_SECONDS = 3600


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass(slots=True)
class ListenerConfig:
    host: str = "127.0.0.1"
    ws_port: int = 8766
    rest_port: int = 8767
    command_port: int = 8768
    output_dir: Path | None = None
    quiet: bool = False
    monitor: bool = True
    trade_limit: int = 20
    snapshot_file: Path | None = None


@dataclass(slots=True)
class VariationalMonitor:
    trade_limit: int = 20
    snapshot_file: Path | None = None
    trade_event_limit: int = 2000
    quotes: dict[str, dict[str, Any]] = field(default_factory=dict)
    current_quote_asset: str | None = None
    positions: dict[str, dict[str, Any]] = field(default_factory=dict)
    recent_trades: list[dict[str, Any]] = field(default_factory=list)
    trade_events: list[dict[str, Any]] = field(default_factory=list)
    portfolio_summary: dict[str, Any] = field(default_factory=dict)
    last_update_at: str | None = None
    last_heartbeat_iso: str | None = None
    _last_quote_log_ts: float | None = None
    _last_portfolio_log_ts: float | None = None
    _last_heartbeat_monotonic: float | None = None
    _last_portfolio_update_monotonic: float | None = None
    _next_heartbeat_check_ts: float = 0.0
    _stale_alert_sent: bool = False
    _last_hourly_alert_hour: int = 0
    _next_trade_event_seq: int = 1
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock, repr=False)
    _position_schema_dumped: set[str] = field(default_factory=set)
    _rest_headers_dumped: set[str] = field(default_factory=set)

    def _check_rate_limit_headers(self, endpoint: str, payload: dict[str, Any]) -> None:
        headers = payload.get("headers")
        status = payload.get("status")
        if isinstance(headers, dict):
            hits = {
                k: v for k, v in headers.items()
                if any(hint in str(k).lower() for hint in _RATE_LIMIT_HEADER_HINTS)
            }
            if hits:
                _monitor_log.warning("[VAR_RATE_LIMIT_HEADER] endpoint=%s status=%s headers=%r", endpoint, status, hits)
        # 每个 endpoint 只完整打一次全部响应头，留个基线备查；限流命中的话上面那条不受这个限制。
        if endpoint not in self._rest_headers_dumped:
            self._rest_headers_dumped.add(endpoint)
            _monitor_log.info("[VAR_REST_HEADERS] endpoint=%s status=%s headers=%r", endpoint, status, headers)
        if isinstance(status, int) and status >= 400:
            _monitor_log.warning("[VAR_REST_ERROR_STATUS] endpoint=%s status=%s headers=%r", endpoint, status, headers)

    async def process_rest_event(self, payload: dict[str, Any]) -> list[str]:
        if payload.get("kind") != "rest_response":
            return []

        url = str(payload.get("url", ""))
        endpoint = classify_rest_endpoint(url)
        if endpoint is None:
            return []

        self._check_rate_limit_headers(endpoint, payload)

        if endpoint == QUOTES_ACCEPT_PATH:
            # accept 频率很低（一笔交易才一次），完整记一次原始响应方便以后分析，不用像
            # indicative 那样只留一次基线——每笔的实际下单结果都值得单独看。
            body = decode_response_body(payload)
            _monitor_log.info(
                "[VAR_ACCEPT_RESPONSE] status=%s body=%s",
                payload.get("status"), body if body is not None else "<decode failed>",
            )
            return []

        if endpoint != QUOTES_INDICATIVE_PATH:
            return []

        body = decode_response_body(payload)
        if body is None:
            return [f"[MONITOR] Failed to decode REST body for {url}"]

        parsed = try_parse_json(body)
        if parsed is None:
            return [f"[MONITOR] REST body is not JSON for {url}"]

        async with self._lock:
            self._update_quote(parsed)
            self.last_update_at = utc_now()
            if self.snapshot_file is not None:
                await asyncio.to_thread(write_json_file, self.snapshot_file, self.snapshot())

        return []

    async def process_ws_event(self, payload: dict[str, Any]) -> list[str]:
        kind = str(payload.get("kind", ""))
        # background.js 在 Network.webSocketClosed / webSocketFrameError 时早就把这两种事件
        # 转发过来了，但这里之前只认 "ws_frame"，两种诊断信号一直被静默丢弃——之前反复遇到
        # /events、/portfolio 长时间 stale 却查不出页面自己的 WS 到底是断线了、还是没断线但
        # 不再收到帧（僵尸连接），就是因为这个信号从来没被记录下来。
        if kind == "ws_closed":
            url = str(payload.get("url", ""))
            _monitor_log.warning(
                "[VAR_WS_CLOSED] 页面自己的 WebSocket 断开了：stream=%s url=%s requestId=%s "
                "ts=%s — 如果之后 heartbeat/portfolio 一直不恢复，说明页面没有重新建立这条连接",
                classify_ws_stream(url) or "?", url, payload.get("requestId"), payload.get("timestamp"),
            )
            return []
        if kind == "ws_frame_error":
            _monitor_log.warning(
                "[VAR_WS_FRAME_ERROR] url=%s requestId=%s error=%r",
                payload.get("url"), payload.get("requestId"), payload.get("errorMessage"),
            )
            return []
        if kind == "cdp_detached":
            # 扩展现在会在 chrome.debugger.onDetach 触发时把 reason 转发过来——之前这个信息
            # 只存在于 popup 里，popup 一关就没了，runtime.log 完全没有记录，没法事后排查
            # 断连原因。常见 reason：target_closed（标签页关闭/被丢弃）、canceled_by_user、
            # replaced_with_devtools_inspector（同一个标签页开了真正的 DevTools）。如果这里
            # 频繁出现且 reason 查不出名堂，多半是 MV3 service worker 被 Chrome 空闲回收。
            _monitor_log.warning(
                "[VAR_CDP_DETACHED] Chrome debugger 会话断开：tabId=%s reason=%s ts=%s",
                payload.get("tabId"), payload.get("reason"), payload.get("timestamp"),
            )
            return []
        if kind == "ws_created":
            # 断连之后页面到底有没有尝试重连，只能靠这个信号判断——如果 [VAR_WS_CLOSED] 之后
            # 一直没有对应 stream 的 [VAR_WS_CREATED]，说明页面根本没有发起新连接尝试；如果有
            # 但心跳还是没恢复，说明是新连接本身有问题（重连了但连不上/连上了收不到消息）。
            url = str(payload.get("url", ""))
            _monitor_log.warning(
                "[VAR_WS_CREATED] 页面建立了新的 WebSocket 连接：stream=%s url=%s requestId=%s ts=%s",
                classify_ws_stream(url) or "?", url, payload.get("requestId"), payload.get("timestamp"),
            )
            return []
        if kind != "ws_frame":
            return []
        if payload.get("direction") != "received":
            return []

        url = str(payload.get("url", ""))
        stream = classify_ws_stream(url)
        if stream is None:
            return []

        message_text = decode_ws_frame_payload(payload)
        if message_text is None:
            return [f"[MONITOR] Failed to decode WS frame for {url}"]

        parsed = try_parse_json(message_text)
        if parsed is None:
            # 之前这里直接静默丢弃——close 帧本身（opcode 8，带关闭代码+原因）如果被当成普通
            # 帧转发过来，内容不是 JSON，会一路走到这里被吞掉，永远看不到关闭原因。截断一下
            # 避免超大二进制帧把日志打爆，但保留前200字符应该够看出是不是 close 帧的内容。
            _monitor_log.warning(
                "[VAR_WS_UNPARSEABLE_FRAME] stream=%s url=%s opcode=%s raw=%r",
                stream, url, payload.get("opcode"), message_text[:200],
            )
            return []

        async with self._lock:
            lines: list[str] = []
            now_ts = asyncio.get_running_loop().time()
            if stream == WS_EVENTS_PATH:
                _msgs = self._iter_event_messages(parsed)
                _filled_count = 0
                for event in _msgs:
                    self._update_heartbeat(event, now_ts)
                    trade_line = self._update_trade_event(event)
                    if trade_line:
                        lines.append(trade_line)
                        portfolio_line = self._format_portfolio_line()
                        if portfolio_line:
                            lines.append(f"{portfolio_line} trigger=trade")
                            self._last_portfolio_log_ts = now_ts
                    _d = event.get("data") if isinstance(event.get("data"), dict) else event
                    if str(_d.get("status", "")).strip().lower() in ("filled", "confirmed"):
                        _filled_count += 1
                # Log frames that contain fill events so we can track WS delivery
                if _filled_count:
                    _monitor_log.info(
                        "[VAR_WS_FRAME] events_path frame: %d msgs, %d fill-status",
                        len(_msgs), _filled_count,
                    )
            elif stream == WS_PORTFOLIO_PATH:
                self._update_portfolio(parsed, now_ts)

            if not lines and stream != WS_PORTFOLIO_PATH:
                return []

            self.last_update_at = utc_now()
            if self.snapshot_file is not None:
                await asyncio.to_thread(write_json_file, self.snapshot_file, self.snapshot())
            return lines

    def _iter_event_messages(self, payload: Any) -> list[dict[str, Any]]:
        if isinstance(payload, dict):
            out = [payload]
            events = payload.get("events")
            if isinstance(events, list):
                out.extend([item for item in events if isinstance(item, dict)])
            data = payload.get("data")
            if isinstance(data, list):
                out.extend([item for item in data if isinstance(item, dict) and "type" in item])
            return out

        if isinstance(payload, list):
            return [item for item in payload if isinstance(item, dict)]

        return []

    async def emit_periodic_logs(self) -> tuple[list[str], list[str]]:
        lines: list[str] = []
        alerts: list[str] = []
        async with self._lock:
            now_ts = asyncio.get_running_loop().time()
            if self.quotes and self._should_log_quote(now_ts):
                quote_line = self._format_quote_line()
                if quote_line:
                    lines.append(quote_line)
                    self._last_quote_log_ts = now_ts

            if self.positions and self._should_log_portfolio(now_ts):
                portfolio_line = self._format_portfolio_line()
                if portfolio_line:
                    lines.append(f"{portfolio_line} trigger=interval")
                    self._last_portfolio_log_ts = now_ts

            alerts.extend(self._collect_heartbeat_alerts(now_ts))

        return lines, alerts

    def _update_quote(self, payload: Any) -> None:
        if not isinstance(payload, dict):
            return

        instrument = payload.get("instrument")
        if not isinstance(instrument, dict):
            return

        asset = str(instrument.get("underlying", "UNKNOWN"))
        bid = payload.get("bid")
        ask = payload.get("ask")
        mark = payload.get("mark_price")
        ts = payload.get("timestamp")

        self.quotes[asset] = {
            "asset": asset,
            "bid": bid,
            "ask": ask,
            "mark_price": mark,
            "timestamp": ts,
            "raw": payload,
        }
        self.current_quote_asset = asset

    def _update_trade_event(self, payload: Any) -> str | None:
        if not isinstance(payload, dict):
            return None
        event_type = str(payload.get("type", "")).strip().lower()

        # Diagnose fill-event loss: log ANY event that carries a fill status
        # (filled OR confirmed), even if the type check below would normally drop it.
        _d = payload.get("data") if isinstance(payload.get("data"), dict) else payload
        _status_raw = str(_d.get("status", "")).strip().lower() if isinstance(_d, dict) else ""
        if _status_raw in ("filled", "confirmed"):
            _tid = str(_d.get("id", "")) if isinstance(_d, dict) else ""
            _monitor_log.info(
                "[VAR_WS_FILL] status=%s type=%r trade_id=%s side=%s price=%s qty=%s"
                " — arrived in monitor (event_seq=%d)",
                _status_raw,
                event_type,
                _tid[:8] if _tid else "?",
                _d.get("side", "?") if isinstance(_d, dict) else "?",
                _d.get("price", "?") if isinstance(_d, dict) else "?",
                _d.get("qty", "?") if isinstance(_d, dict) else "?",
                self._next_trade_event_seq,
            )

        if "trade" not in event_type:
            return None

        data = payload.get("data") if isinstance(payload.get("data"), dict) else payload

        instrument = data.get("instrument")
        asset = "UNKNOWN"
        if isinstance(instrument, dict):
            asset = str(instrument.get("underlying", "UNKNOWN"))

        trade_id = str(data.get("id", ""))
        summary = {
            "timestamp": data.get("created_at") or payload.get("timestamp") or "-",
            "trade_id": trade_id,
            "side": data.get("side", "-"),
            "asset": asset,
            "price": data.get("price", "-"),
            "qty": data.get("qty", "-"),
            "status": data.get("status", "-"),
            "role": data.get("role", "-"),
            "received_at": utc_now(),
            "raw": payload,
        }

        summary["event_seq"] = self._next_trade_event_seq
        self._next_trade_event_seq += 1

        if trade_id:
            self.recent_trades = [t for t in self.recent_trades if t.get("trade_id") != trade_id]
        self.recent_trades.insert(0, summary)
        self.recent_trades = self.recent_trades[: self.trade_limit]
        self.trade_events.append(summary)
        if len(self.trade_events) > self.trade_event_limit:
            self.trade_events = self.trade_events[-self.trade_event_limit:]

        trade_id_short = trade_id[:8] if trade_id else "-"
        return (
            f"[MONITOR] TRADE {summary['side']} {summary['qty']} {summary['asset']} "
            f"@{summary['price']} status={summary['status']} role={summary['role']} id={trade_id_short}"
        )
    # 解析/portfolio WS消息,存每个资产的qty/均价、upnl/rpnl等信息,并存储portfolio_summary
    def _update_portfolio(self, payload: Any, now_ts: float) -> None:
        if not isinstance(payload, dict):
            return

        positions_data = payload.get("positions")
        if not isinstance(positions_data, list):
            return

        next_positions: dict[str, dict[str, Any]] = {}
        for item in positions_data:
            if not isinstance(item, dict):
                continue
            position_info = item.get("position_info")
            if not isinstance(position_info, dict):
                continue
            instrument = position_info.get("instrument")
            if not isinstance(instrument, dict):
                continue

            asset = str(instrument.get("underlying", "UNKNOWN"))
            # 诊断：qty 的正负号目前被当成方向在用（正=多/负=空），但从没验证过 Variational
            # 是否真的这样定义，还是像 Lighter 那样另有独立的方向字段而 qty 只是数量
            # （notes/injection_failure_analysis.md 场景E 就是这类假设错了导致的事故）。
            # 每个资产只打一次完整原始 payload，等下次真实数据回来后核对 schema。
            if asset not in self._position_schema_dumped:
                self._position_schema_dumped.add(asset)
                _monitor_log.info(
                    "[VAR_POSITION_SCHEMA] asset=%s qty=%r raw_position_info=%r raw_item_keys=%r",
                    asset, position_info.get("qty"), position_info, list(item.keys()),
                )
            next_positions[asset] = {
                "asset": asset,
                "qty": position_info.get("qty"),
                "avg_entry_price": position_info.get("avg_entry_price"),
                "updated_at": position_info.get("updated_at"),
                "value": item.get("value"),
                "upnl": item.get("upnl"),
                "rpnl": item.get("rpnl"),
                "raw": item,
            }

        pool = payload.get("pool_portfolio_result")
        margin = {}
        if isinstance(pool, dict):
            margin_raw = pool.get("margin_usage")
            if isinstance(margin_raw, dict):
                margin = {
                    "initial_margin": margin_raw.get("initial_margin"),
                    "maintenance_margin": margin_raw.get("maintenance_margin"),
                }

        self.positions = next_positions
        self._last_portfolio_update_monotonic = now_ts
        self.portfolio_summary = {
            "balance": pool.get("balance") if isinstance(pool, dict) else None,
            "upnl": pool.get("upnl") if isinstance(pool, dict) else None,
            "margin_usage": margin,
            "published_at": payload.get("published_at"),
            "raw": pool if isinstance(pool, dict) else {},
        }

    def _should_log_quote(self, now_ts: float) -> bool:
        if self._last_quote_log_ts is None:
            return True
        return now_ts - self._last_quote_log_ts >= QUOTE_LOG_INTERVAL_SECONDS

    def _should_log_portfolio(self, now_ts: float) -> bool:
        if self._last_portfolio_log_ts is None:
            return True
        return now_ts - self._last_portfolio_log_ts >= PORTFOLIO_LOG_INTERVAL_SECONDS
    # /events WS消息里的type=heartbeat消息,更新心跳时间戳和last_heartbeat_iso,并重置stale_alert_sent和last_hourly_alert_hour
    def _update_heartbeat(self, payload: Any, now_ts: float) -> None:
        if not isinstance(payload, dict):
            return
        if payload.get("type") != "heartbeat":
            return

        self._last_heartbeat_monotonic = now_ts
        timestamp = payload.get("timestamp")
        if isinstance(timestamp, str):
            self.last_heartbeat_iso = timestamp
        else:
            self.last_heartbeat_iso = utc_now()

        self._stale_alert_sent = False
        self._last_hourly_alert_hour = 0
        self._next_heartbeat_check_ts = now_ts + 1

    def _collect_heartbeat_alerts(self, now_ts: float) -> list[str]:
        if self._last_heartbeat_monotonic is None:
            return []
        if now_ts < self._next_heartbeat_check_ts:
            return []

        age_seconds = now_ts - self._last_heartbeat_monotonic
        if age_seconds <= HEARTBEAT_STALE_SECONDS:
            self._next_heartbeat_check_ts = now_ts + 1
            return []

        self._next_heartbeat_check_ts = now_ts + HEARTBEAT_RECHECK_SECONDS
        alerts: list[str] = []
        last_seen = self.last_heartbeat_iso or "unknown"
        if not self._stale_alert_sent:
            alerts.append(
                f"Heartbeat stale: last heartbeat {age_seconds:.1f}s ago (last_seen={last_seen})."
            )
            self._stale_alert_sent = True

        stale_hours = int(age_seconds // HEARTBEAT_HOURLY_SECONDS)
        if stale_hours >= 1 and stale_hours > self._last_hourly_alert_hour:
            alerts.append(
                f"Heartbeat still stale for {stale_hours}h (last_seen={last_seen})."
            )
            self._last_hourly_alert_hour = stale_hours

        return alerts

    def _format_quote_line(self) -> str | None:
        if not self.current_quote_asset:
            return None
        quote = self.quotes.get(self.current_quote_asset)
        if not quote:
            return None
        spread = compute_spread(quote.get("bid"), quote.get("ask"))
        spread_part = f" spread={spread}" if spread is not None else ""
        return (
            f"[MONITOR] QUOTE {self.current_quote_asset} bid={quote.get('bid')} "
            f"ask={quote.get('ask')}{spread_part} mark={quote.get('mark_price')}"
        )

    def _format_portfolio_line(self) -> str | None:
        if not self.current_quote_asset:
            return None
        row = self.positions.get(self.current_quote_asset)
        if row is None:
            position_part = f"{self.current_quote_asset} qty=0 upnl=0"
        else:
            position_part = (
                f"{self.current_quote_asset} qty={row.get('qty')} upnl={row.get('upnl')}"
            )
        return (
            f"[MONITOR] PORTFOLIO balance={self.portfolio_summary.get('balance')} "
            f"upnl={self.portfolio_summary.get('upnl')} asset={position_part}"
        )

    def snapshot(self) -> dict[str, Any]:
        return {
            "generated_at": utc_now(),
            "last_update_at": self.last_update_at,
            "current_quote_asset": self.current_quote_asset,
            "last_heartbeat_iso": self.last_heartbeat_iso,
            "quotes": self.quotes,
            "positions": self.positions,
            "recent_trades": self.recent_trades,
            "trade_events": self.trade_events,
            "portfolio_summary": self.portfolio_summary,
        }

    async def get_trading_state(self) -> dict[str, Any]:
        async with self._lock:
            now_ts = asyncio.get_running_loop().time()
            heartbeat_age: float | None = None
            if self._last_heartbeat_monotonic is not None:
                heartbeat_age = max(0.0, now_ts - self._last_heartbeat_monotonic)
            portfolio_age: float | None = None
            if self._last_portfolio_update_monotonic is not None:
                portfolio_age = max(0.0, now_ts - self._last_portfolio_update_monotonic)

            asset = self.current_quote_asset
            quote = self.quotes.get(asset) if asset else None
            row = self.positions.get(asset) if asset else None
            qty = 0.0
            if isinstance(row, dict):
                qty_val = as_float(row.get("qty"))
                if qty_val is not None:
                    qty = qty_val

            return {
                "asset": asset,
                "position": qty,
                "position_row": row,
                "quote": quote,
                "has_quote": quote is not None,
                "has_portfolio": bool(self.portfolio_summary),
                "last_update_at": self.last_update_at,
                "last_heartbeat_iso": self.last_heartbeat_iso,
                "heartbeat_age": heartbeat_age,
                "portfolio_age": portfolio_age,
            }

    async def get_trade_events_since(
        self,
        min_event_seq: int,
        limit: int = 200,
    ) -> list[dict[str, Any]]:
        async with self._lock:
            events = [event for event in self.trade_events if int(event.get("event_seq", 0)) > min_event_seq]
            if limit > 0:
                events = events[:limit]
            return events

    async def get_latest_trade_event_seq(self) -> int:
        async with self._lock:
            return self._next_trade_event_seq - 1


class EventSink:
    def __init__(
        self,
        output_dir: Path | None,
        quiet: bool = False,
        monitor: VariationalMonitor | None = None,
    ) -> None:
        self.output_dir = output_dir
        self.quiet = quiet
        self.monitor = monitor
        self._write_lock = asyncio.Lock()
        if self.output_dir is not None:
            self.output_dir.mkdir(parents=True, exist_ok=True)
    # 按照channel分流，在内部做CDP帧的解析和监控处理，最后写入文件
    async def handle(self, channel: str, raw_message: str) -> None:
        parsed: dict[str, Any] | str
        try:
            parsed = json.loads(raw_message)
        except json.JSONDecodeError:
            parsed = raw_message

        envelope = {
            "ingested_at": utc_now(),
            "channel": channel,
            "payload": parsed,
        }

        if self.monitor and isinstance(parsed, dict):
            lines: list[str] = []
            if channel == "rest":
                lines = await self.monitor.process_rest_event(parsed)
            elif channel == "ws":
                lines = await self.monitor.process_ws_event(parsed)
            if not self.quiet:
                for line in lines:
                    print(line, flush=True)

        if self.output_dir is not None:
            file_name = "ws_events.jsonl" if channel == "ws" else "rest_events.jsonl"
            await self._append_jsonl(self.output_dir / file_name, envelope)

    async def _append_jsonl(self, path: Path, obj: dict[str, Any]) -> None:
        line = json.dumps(obj, ensure_ascii=True) + "\n"
        async with self._write_lock:
            await asyncio.to_thread(_append_line, path, line)


class CommandBroker:
    def __init__(self, quiet: bool = False) -> None:
        self.quiet = quiet
        self._lock = asyncio.Lock()
        self._roles: dict[websockets.ServerConnection, str] = {}
        self._extension: websockets.ServerConnection | None = None
        self._pending_requests: dict[str, websockets.ServerConnection | asyncio.Future[dict[str, Any]]] = {}

    async def on_connect(self, websocket: websockets.ServerConnection) -> None:
        async with self._lock:
            self._roles[websocket] = "unknown"

    async def on_disconnect(self, websocket: websockets.ServerConnection) -> None:
        async with self._lock:
            role = self._roles.pop(websocket, "unknown")
            if websocket is self._extension:
                self._extension = None
                failures = list(self._pending_requests.items())
                self._pending_requests.clear()
                error_payload = {
                    "type": "ORDER_RESULT",
                    "ok": False,
                    "error": "Extension disconnected before order result.",
                    "timestamp": utc_now(),
                }
                for request_id, requester in failures:
                    if isinstance(requester, asyncio.Future):
                        if not requester.done():
                            requester.set_result({**error_payload, "requestId": request_id})
                    else:
                        await self._send(requester, {**error_payload, "requestId": request_id})

            stale_request_ids = [req for req, requester in self._pending_requests.items() if requester is websocket]
            for req in stale_request_ids:
                self._pending_requests.pop(req, None)

            if not self.quiet:
                print(f"[COMMAND] disconnected role={role}", flush=True)

    async def handle_raw_message(self, websocket: websockets.ServerConnection, raw: str) -> None:
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            await self._send(
                websocket,
                {
                    "type": "ERROR",
                    "ok": False,
                    "error": "Invalid JSON payload.",
                    "timestamp": utc_now(),
                },
            )
            return

        if not isinstance(payload, dict):
            await self._send(
                websocket,
                {
                    "type": "ERROR",
                    "ok": False,
                    "error": "Command payload must be an object.",
                    "timestamp": utc_now(),
                },
            )
            return

        msg_type = str(payload.get("type", "")).upper()
        if msg_type == "REGISTER":
            await self._handle_register(websocket, payload)
            return
        if msg_type == "PING":
            await self._send(websocket, {"type": "PONG", "timestamp": utc_now()})
            return
        if msg_type == "PLACE_ORDER":
            await self._handle_place_order(websocket, payload)
            return
        if msg_type == "ORDER_RESULT":
            await self._handle_order_result(payload)
            return

        await self._send(
            websocket,
            {
                "type": "ERROR",
                "ok": False,
                "error": f"Unsupported message type: {msg_type or 'UNKNOWN'}",
                "timestamp": utc_now(),
            },
        )

    async def _handle_register(self, websocket: websockets.ServerConnection, payload: dict[str, Any]) -> None:
        role = str(payload.get("role", "")).strip().lower() or "unknown"
        async with self._lock:
            self._roles[websocket] = role
            if role == "extension":
                self._extension = websocket

        await self._send(
            websocket,
            {
                "type": "REGISTER_ACK",
                "ok": True,
                "role": role,
                "timestamp": utc_now(),
            },
        )
        if not self.quiet:
            print(f"[COMMAND] registered role={role}", flush=True)

    async def _handle_place_order(self, websocket: websockets.ServerConnection, payload: dict[str, Any]) -> None:
        request_id = str(payload.get("requestId") or uuid.uuid4())
        side = str(payload.get("side", "")).upper()
        amount = str(payload.get("amount", "")).strip()

        if side not in {"BUY", "SELL"}:
            await self._send(
                websocket,
                {
                    "type": "ORDER_RESULT",
                    "requestId": request_id,
                    "ok": False,
                    "error": "Invalid side. Use BUY or SELL.",
                    "timestamp": utc_now(),
                },
            )
            return
        try:
            if float(amount) <= 0:
                raise ValueError
        except ValueError:
            await self._send(
                websocket,
                {
                    "type": "ORDER_RESULT",
                    "requestId": request_id,
                    "ok": False,
                    "error": "Invalid amount. Must be positive.",
                    "timestamp": utc_now(),
                },
            )
            return

        async with self._lock:
            extension = self._extension
            if extension is None:
                await self._send(
                    websocket,
                    {
                        "type": "ORDER_RESULT",
                        "requestId": request_id,
                        "ok": False,
                        "error": "No extension command client connected.",
                        "timestamp": utc_now(),
                    },
                )
                return

            self._pending_requests[request_id] = websocket
            forward_payload = {
                "type": "PLACE_ORDER",
                "requestId": request_id,
                "side": side,
                "amount": amount,
                "market": payload.get("market"),
                "account": payload.get("account"),
                "maxSlippage": payload.get("maxSlippage", 0.01),
                "isReduceOnly": bool(payload.get("isReduceOnly", False)),
                "timeoutMs": payload.get("timeoutMs"),
                "timestamp": utc_now(),
            }
            await self._send(extension, forward_payload)

        await self._send(
            websocket,
            {
                "type": "ORDER_DISPATCHED",
                "requestId": request_id,
                "ok": True,
                "timestamp": utc_now(),
            },
        )

    async def _handle_order_result(self, payload: dict[str, Any]) -> None:
        request_id = str(payload.get("requestId", "")).strip()
        if not request_id:
            return
        async with self._lock:
            requester = self._pending_requests.pop(request_id, None)

        if requester is not None:
            if isinstance(requester, asyncio.Future):
                if not requester.done():
                    requester.set_result(payload)
            else:
                await self._send(requester, payload)
            if not self.quiet:
                print(f"[COMMAND] order_result requestId={request_id} ok={payload.get('ok')}", flush=True)

    async def place_order_internal(
        self,
        side: str,
        amount: str,
        max_slippage: float = 0.01,
        is_reduce_only: bool = False,
        timeout_seconds: float = 30.0,
    ) -> dict[str, Any]:
        """Place an order via the extension without an external WebSocket caller."""
        request_id = str(uuid.uuid4())
        fut: asyncio.Future[dict[str, Any]] = asyncio.get_running_loop().create_future()

        t_start = time.monotonic()
        async with self._lock:
            t_lock_acquired = time.monotonic()
            if self._extension is None:
                return {"ok": False, "error": "No extension connected"}
            self._pending_requests[request_id] = fut
            await self._send(self._extension, {
                "type": "PLACE_ORDER",
                "requestId": request_id,
                "side": side.upper(),
                "amount": amount,
                "maxSlippage": max_slippage,
                "isReduceOnly": is_reduce_only,
                "timestamp": utc_now(),
            })
        t_sent = time.monotonic()

        try:
            result = await asyncio.wait_for(fut, timeout=timeout_seconds)
        except asyncio.TimeoutError:
            async with self._lock:
                self._pending_requests.pop(request_id, None)
            return {"ok": False, "error": f"Order timed out after {timeout_seconds}s"}

        t_done = time.monotonic()
        result["_lock_wait_ms"] = round((t_lock_acquired - t_start) * 1000, 1)
        result["_api_elapsed_ms"] = round((t_done - t_sent) * 1000, 1)
        result["_submit_total_ms"] = round((t_done - t_start) * 1000, 1)
        return result

    async def _send(self, websocket: websockets.ServerConnection, payload: dict[str, Any]) -> None:
        try:
            await websocket.send(json.dumps(payload, ensure_ascii=True))
        except Exception:
            return


def _append_line(path: Path, line: str) -> None:
    with path.open("a", encoding="utf-8") as f:
        f.write(line)


def write_json_file(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=True, indent=2)


def classify_rest_endpoint(url: str) -> str | None:
    try:
        path = urlparse(url).path
    except ValueError:
        return None
    if path == QUOTES_INDICATIVE_PATH:
        return QUOTES_INDICATIVE_PATH
    if path == QUOTES_ACCEPT_PATH:
        return QUOTES_ACCEPT_PATH
    return None


def classify_ws_stream(url: str) -> str | None:
    try:
        path = urlparse(url).path
    except ValueError:
        return None
    if path == WS_EVENTS_PATH:
        return WS_EVENTS_PATH
    if path == WS_PORTFOLIO_PATH:
        return WS_PORTFOLIO_PATH
    return None


def decode_response_body(payload: dict[str, Any]) -> str | None:
    body = payload.get("body")
    if not isinstance(body, str):
        return None
    if payload.get("base64Encoded"):
        try:
            return base64.b64decode(body).decode("utf-8", errors="replace")
        except Exception:
            return None
    return body


def decode_ws_frame_payload(payload: dict[str, Any]) -> str | None:
    data = payload.get("payloadData")
    if not isinstance(data, str):
        return None

    opcode = payload.get("opcode")
    if opcode == 2:
        stripped = data.lstrip()
        if stripped.startswith("{") or stripped.startswith("["):
            return data
        try:
            decoded = base64.b64decode(data)
            return decoded.decode("utf-8", errors="replace")
        except Exception:
            return data

    return data


def try_parse_json(text: str) -> Any | None:
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return None


def as_float(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def compute_spread(bid: Any, ask: Any) -> str | None:
    bid_val = as_float(bid)
    ask_val = as_float(ask)
    if bid_val is None or ask_val is None:
        return None
    return f"{ask_val - bid_val:.8f}"


async def run_receiver_server(
    channel: str,
    host: str,
    port: int,
    sink: EventSink,
) -> websockets.asyncio.server.Server:
    async def handler(websocket: websockets.ServerConnection) -> None:
        async for message in websocket:
            if isinstance(message, bytes):
                message = message.decode("utf-8", errors="replace")
            await sink.handle(channel, message)

    return await websockets.serve(handler, host, port, max_size=None, ping_interval=20, ping_timeout=20)


async def run_command_server(
    host: str,
    port: int,
    broker: CommandBroker,
) -> websockets.asyncio.server.Server:
    async def handler(websocket: websockets.ServerConnection) -> None:
        await broker.on_connect(websocket)
        try:
            async for message in websocket:
                if isinstance(message, bytes):
                    message = message.decode("utf-8", errors="replace")
                await broker.handle_raw_message(websocket, message)
        finally:
            await broker.on_disconnect(websocket)

    return await websockets.serve(handler, host, port, max_size=None, ping_interval=20, ping_timeout=20)

# 以下为测试代码，日常运行不会跑到
async def run(config: ListenerConfig) -> None:
    monitor = VariationalMonitor(trade_limit=config.trade_limit, snapshot_file=config.snapshot_file) if config.monitor else None
    sink = EventSink(config.output_dir, quiet=config.quiet, monitor=monitor)
    broker = CommandBroker(quiet=config.quiet)
    ws_server = await run_receiver_server("ws", config.host, config.ws_port, sink)
    rest_server = await run_receiver_server("rest", config.host, config.rest_port, sink)
    command_server = await run_command_server(config.host, config.command_port, broker)
    periodic_task: asyncio.Task[None] | None = None

    if monitor is not None:
        async def periodic_logger() -> None:
            while True:
                await asyncio.sleep(1)
                lines, alerts = await monitor.emit_periodic_logs()
                if not config.quiet:
                    for line in lines:
                        print(line, flush=True)
                for alert in alerts:
                    heartbeat_text = f"[HEARTBEAT_ALERT] {alert}"
                    if not config.quiet:
                        print(heartbeat_text, flush=True)

        periodic_task = asyncio.create_task(periodic_logger())

    print(
        f"Listening for Variational forwarder events on "
        f"ws://{config.host}:{config.ws_port} (WS) and "
        f"ws://{config.host}:{config.rest_port} (REST); "
        f"command broker ws://{config.host}:{config.command_port}",
        flush=True,
    )

    try:
        await asyncio.Future()
    except asyncio.CancelledError:
        pass
    finally:
        if periodic_task is not None:
            periodic_task.cancel()
            await asyncio.gather(periodic_task, return_exceptions=True)
        command_server.close()
        ws_server.close()
        rest_server.close()
        await command_server.wait_closed()
        await ws_server.wait_closed()
        await rest_server.wait_closed()


def parse_args() -> ListenerConfig:
    parser = argparse.ArgumentParser(description="Run local receivers for Variational CDP forwarder events.")
    parser.add_argument("--host", default="127.0.0.1", help="Host interface to bind receivers.")
    parser.add_argument("--ws-port", type=int, default=8766, help="Port for WebSocket frame events.")
    parser.add_argument("--rest-port", type=int, default=8767, help="Port for REST response events.")
    parser.add_argument("--command-port", type=int, default=8768, help="Port for PLACE_ORDER command broker.")
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="Optional directory for JSONL event files.",
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Suppress all monitor logs in terminal (still writes to files when --output-dir is used).",
    )
    parser.add_argument(
        "--no-monitor",
        action="store_true",
        help="Disable live monitor parsing for quotes/trades/positions.",
    )
    parser.add_argument(
        "--trade-limit",
        type=int,
        default=20,
        help="How many recent trade updates to keep in monitor state.",
    )
    parser.add_argument(
        "--snapshot-file",
        type=Path,
        default=None,
        help="Optional path for live monitor snapshot JSON.",
    )
    args = parser.parse_args()
    snapshot_file = args.snapshot_file
    if snapshot_file is None and args.output_dir is not None:
        snapshot_file = args.output_dir / "monitor_state.json"

    return ListenerConfig(
        host=args.host,
        ws_port=args.ws_port,
        rest_port=args.rest_port,
        command_port=args.command_port,
        output_dir=args.output_dir,
        quiet=args.quiet,
        monitor=not args.no_monitor,
        trade_limit=max(1, args.trade_limit),
        snapshot_file=snapshot_file,
    )


def main() -> None:
    config = parse_args()
    try:
        asyncio.run(run(config))
    except KeyboardInterrupt:
        print("\nReceiver stopped.", flush=True)


if __name__ == "__main__":
    main()
