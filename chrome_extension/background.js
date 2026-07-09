const DEBUGGER_VERSION = "1.3";
const MAX_QUEUE_SIZE = 1000;
const AUTO_RELOAD_COOLDOWN_MS = 5000;

const DEFAULT_CONFIG = {
  wsEndpoint: "ws://127.0.0.1:8766",
  restEndpoint: "ws://127.0.0.1:8767",
  commandEndpoint: "ws://127.0.0.1:8768",
  domainFilter: "variational",
  restAllowlist: [
    "https://omni.variational.io/api/quotes/indicative",
    "https://omni.variational.io/api/quotes/accept"
  ],
  wsAllowlist: [
    "wss://omni-ws-server.prod.ap-northeast-1.variational.io/events",
    "wss://omni-ws-server.prod.ap-northeast-1.variational.io/portfolio"
  ]
};

// Injected into the Variational page before any page scripts via
// Page.addScriptToEvaluateOnNewDocument.
//
// Hooks window.fetch to intercept /api/quotes/indicative responses and cache
// the latest quote (which contains the quote_id needed for order placement).
// Exposes window.__varAcceptQuote(side, maxSlippage, isReduceOnly) which calls
// POST /api/quotes/accept using the cached quote_id.
// Auth is automatic: same-origin fetch includes cookies (vr-token) and we
// read vr-connected-address from the cookie jar.
const PAGE_INJECTION_SCRIPT = `
(function () {
  if (window.__varHooked) return;
  window.__varHooked = true;

  var _fetch = window.fetch.bind(window);
  var _lastQuote = null;

  // Intercept indicative polls to keep instrument info fresh.
  window.fetch = function (input, init) {
    var url = typeof input === 'string' ? input : (input && input.url) || '';
    var p = _fetch(input, init);
    if (url.indexOf('/api/quotes/indicative') !== -1) {
      p.then(function (res) {
        if (res.ok) {
          res.clone().json().then(function (data) {
            if (data && data.instrument) {
              _lastQuote = data;
            }
          }).catch(function () {});
        }
      }).catch(function () {});
    }
    return p;
  };

  window.__varGetLastQuote = function () {
    return JSON.stringify(_lastQuote);
  };

  // Keep instrument cache alive — poll every 30s so orders never fail with
  // "No instrument cached yet" after a page reload clears _lastQuote.
  setInterval(function () {
    if (!_lastQuote || !_lastQuote.instrument) return;
    _fetch('https://omni.variational.io/api/quotes/indicative', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      credentials: 'include',
      body: JSON.stringify({ instrument: _lastQuote.instrument, qty: '0.001' })
    }).then(function (res) {
      if (res.ok) {
        res.clone().json().then(function (data) {
          if (data && data.instrument) _lastQuote = data;
        }).catch(function () {});
      }
    }).catch(function () {});
  }, 30000);

  // Two-step atomic order placement:
  //   1. POST /api/quotes/indicative with instrument + qty → fresh quote_id
  //   2. POST /api/quotes/accept with that quote_id
  window.__varAcceptQuote = function (side, qty, maxSlippage, isReduceOnly) {
    if (!_lastQuote || !_lastQuote.instrument) {
      return Promise.resolve(JSON.stringify({ ok: false, error: 'No instrument cached yet — wait for indicative poll' }));
    }
    var headers = { 'Content-Type': 'application/json' };
    var addrMatch = document.cookie.match(/vr-connected-address=([^;]+)/);
    if (addrMatch) headers['vr-connected-address'] = decodeURIComponent(addrMatch[1]);

    return _fetch('https://omni.variational.io/api/quotes/indicative', {
      method: 'POST',
      headers: headers,
      credentials: 'include',
      body: JSON.stringify({ instrument: _lastQuote.instrument, qty: String(qty) })
    }).then(function (res) {
      return res.json().then(function (quote) {
        var quoteId = quote.quote_id || quote.id;
        if (!res.ok || !quoteId) {
          return JSON.stringify({ ok: false, error: 'indicative failed: ' + JSON.stringify(quote) });
        }
        return _fetch('https://omni.variational.io/api/quotes/accept', {
          method: 'POST',
          headers: headers,
          credentials: 'include',
          body: JSON.stringify({
            quote_id: quoteId,
            side: side,
            max_slippage: maxSlippage !== undefined ? maxSlippage : 0.01,
            is_reduce_only: !!isReduceOnly
          })
        }).then(function (acceptRes) {
          return acceptRes.json().then(function (data) {
            if (acceptRes.ok) {
              return JSON.stringify({ ok: true, data: data });
            }
            var reason = data.message || data.error || data.detail || JSON.stringify(data).slice(0, 150);
            return JSON.stringify({ ok: false, error: 'accept HTTP ' + acceptRes.status + ': ' + reason });
          });
        });
      });
    }).catch(function (e) {
      return JSON.stringify({ ok: false, error: e.message });
    });
  };
})();
`;

const state = {
  active: false,
  attachedTabId: null,
  config: { ...DEFAULT_CONFIG },
  configLoaded: false,
  pendingResponses: new Map(),
  websocketMeta: new Map(),
  lastError: null,
  lastAutoReloadAt: 0,
  hookScriptId: null,
};

class ForwardSocket {
  constructor(label, configKey) {
    this.label = label;
    this.configKey = configKey;
    this.ws = null;
    this.status = "disconnected";
    this.queue = [];
    this.retryTimer = null;
  }

  get endpoint() {
    return state.config[this.configKey];
  }

  connect() {
    if (!state.active) {
      return;
    }

    if (this.ws && (this.ws.readyState === WebSocket.OPEN || this.ws.readyState === WebSocket.CONNECTING)) {
      return;
    }

    const endpoint = this.endpoint;
    if (!endpoint) {
      this.status = "disconnected";
      notifyStatus();
      return;
    }

    this.status = "connecting";
    notifyStatus();

    try {
      const socket = new WebSocket(endpoint);
      this.ws = socket;

      socket.onopen = () => {
        if (this.ws !== socket) {
          return;
        }
        this.status = "connected";
        this.flush();
        if (this.configKey === "wsEndpoint") {
          autoReloadAttachedTab("forward receiver connected");
        }
        notifyStatus();
      };

      socket.onclose = () => {
        if (this.ws !== socket) {
          return;
        }
        this.ws = null;
        this.status = "disconnected";
        notifyStatus();
        this.scheduleReconnect();
      };

      socket.onerror = () => {
        if (this.ws !== socket) {
          return;
        }
        this.status = "error";
        notifyStatus();
      };
    } catch (error) {
      this.status = "error";
      state.lastError = `${this.label} socket connect failed: ${error.message}`;
      notifyStatus();
      this.scheduleReconnect();
    }
  }

  send(payload) {
    const data = JSON.stringify(payload);
    if (this.ws && this.ws.readyState === WebSocket.OPEN) {
      this.ws.send(data);
      return;
    }

    this.queue.push(data);
    if (this.queue.length > MAX_QUEUE_SIZE) {
      this.queue.shift();
    }
    this.connect();
  }

  flush() {
    if (!this.ws || this.ws.readyState !== WebSocket.OPEN) {
      return;
    }
    while (this.queue.length > 0) {
      this.ws.send(this.queue.shift());
    }
  }

  scheduleReconnect() {
    if (!state.active || this.retryTimer) {
      return;
    }
    this.retryTimer = setTimeout(() => {
      this.retryTimer = null;
      this.connect();
    }, 1000);
  }

  restart() {
    this.close();
    this.connect();
  }

  close() {
    if (this.retryTimer) {
      clearTimeout(this.retryTimer);
      this.retryTimer = null;
    }
    if (this.ws) {
      this.ws.close();
      this.ws = null;
    }
    this.status = "disconnected";
    notifyStatus();
  }
}

class CommandSocket {
  constructor() {
    this.ws = null;
    this.status = "disconnected";
    this.retryTimer = null;
  }

  get endpoint() {
    return state.config.commandEndpoint;
  }

  connect() {
    if (!state.active) return;
    if (this.ws && (this.ws.readyState === WebSocket.OPEN || this.ws.readyState === WebSocket.CONNECTING)) return;

    const endpoint = this.endpoint;
    if (!endpoint) {
      this.status = "disconnected";
      notifyStatus();
      return;
    }

    this.status = "connecting";
    notifyStatus();

    try {
      const socket = new WebSocket(endpoint);
      this.ws = socket;

      socket.onopen = () => {
        if (this.ws !== socket) return;
        this.status = "connected";
        socket.send(JSON.stringify({ type: "REGISTER", role: "extension" }));
        notifyStatus();
      };

      socket.onmessage = (event) => {
        if (this.ws !== socket) return;
        this.handleMessage(event.data);
      };

      socket.onclose = () => {
        if (this.ws !== socket) return;
        this.ws = null;
        this.status = "disconnected";
        notifyStatus();
        this.scheduleReconnect();
      };

      socket.onerror = () => {
        if (this.ws !== socket) return;
        this.status = "error";
        notifyStatus();
      };
    } catch (error) {
      this.status = "error";
      state.lastError = `Command socket connect failed: ${error.message}`;
      notifyStatus();
      this.scheduleReconnect();
    }
  }

  handleMessage(raw) {
    let msg;
    try {
      msg = JSON.parse(raw);
    } catch {
      return;
    }
    if (msg.type === "PLACE_ORDER") {
      this.executePlaceOrder(msg).catch((err) => {
        this.sendResult(msg.requestId, false, { error: err.message });
      });
    }
  }

  async executePlaceOrder(msg) {
    const tabId = state.attachedTabId;
    if (!tabId) {
      this.sendResult(msg.requestId, false, { error: "No attached tab" });
      return;
    }

    const side = JSON.stringify((msg.side || "").toLowerCase());
    const qty = JSON.stringify(String(msg.amount || "0"));
    const maxSlippage = msg.maxSlippage !== undefined ? msg.maxSlippage : 0.01;
    const isReduceOnly = msg.isReduceOnly ? "true" : "false";
    const expression = `window.__varAcceptQuote(${side}, ${qty}, ${maxSlippage}, ${isReduceOnly})`;

    let evalResult;
    try {
      evalResult = await sendDebuggerCommand(tabId, "Runtime.evaluate", {
        expression,
        awaitPromise: true,
        returnByValue: true,
      });
    } catch (err) {
      this.sendResult(msg.requestId, false, { error: `CDP error: ${err.message}` });
      return;
    }

    let parsed = {};
    try {
      parsed = JSON.parse(evalResult?.result?.value || "{}");
    } catch {
      parsed = { ok: false, error: "Could not parse injection result" };
    }

    if (parsed.ok) {
      this.sendResult(msg.requestId, true, { data: parsed.data });
    } else {
      this.sendResult(msg.requestId, false, { error: parsed.error || "Injection failed" });
    }
  }

  sendResult(requestId, ok, extra) {
    this.send({ type: "ORDER_RESULT", requestId, ok, timestamp: nowIso(), ...extra });
  }

  send(payload) {
    if (this.ws && this.ws.readyState === WebSocket.OPEN) {
      this.ws.send(JSON.stringify(payload));
    }
  }

  scheduleReconnect() {
    if (!state.active || this.retryTimer) return;
    this.retryTimer = setTimeout(() => {
      this.retryTimer = null;
      this.connect();
    }, 1000);
  }

  restart() {
    this.close();
    this.connect();
  }

  close() {
    if (this.retryTimer) {
      clearTimeout(this.retryTimer);
      this.retryTimer = null;
    }
    if (this.ws) {
      this.ws.close();
      this.ws = null;
    }
    this.status = "disconnected";
    notifyStatus();
  }
}

const wsForwarder = new ForwardSocket("websocket", "wsEndpoint");
const restForwarder = new ForwardSocket("rest", "restEndpoint");
const commandSocket = new CommandSocket();

function autoReloadAttachedTab(reason) {
  if (!state.active || state.attachedTabId == null) {
    return;
  }
  const now = Date.now();
  if (now - state.lastAutoReloadAt < AUTO_RELOAD_COOLDOWN_MS) {
    return;
  }
  state.lastAutoReloadAt = now;

  chrome.tabs.reload(state.attachedTabId, {}, () => {
    const err = chrome.runtime.lastError;
    if (err) {
      state.lastError = `Auto reload failed (${reason}): ${err.message}`;
    } else {
      state.lastError = null;
    }
    notifyStatus();
  });
}

async function ensureConfigLoaded() {
  if (state.configLoaded) {
    return;
  }
  const stored = await chrome.storage.local.get("forwarderConfig");
  state.config = sanitizeConfig(stored.forwarderConfig);
  state.configLoaded = true;
}

function sanitizeConfig(incoming = {}) {
  return {
    wsEndpoint: asStringOrDefault(incoming.wsEndpoint, DEFAULT_CONFIG.wsEndpoint),
    restEndpoint: asStringOrDefault(incoming.restEndpoint, DEFAULT_CONFIG.restEndpoint),
    commandEndpoint: asStringOrDefault(incoming.commandEndpoint, DEFAULT_CONFIG.commandEndpoint),
    domainFilter: asStringOrDefault(incoming.domainFilter, DEFAULT_CONFIG.domainFilter),
    restAllowlist: sanitizeRestAllowlist(incoming.restAllowlist),
    wsAllowlist: sanitizeAllowlist(incoming.wsAllowlist, DEFAULT_CONFIG.wsAllowlist)
  };
}

function asStringOrDefault(value, fallback) {
  if (typeof value !== "string") {
    return fallback;
  }
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : fallback;
}

function nowIso() {
  return new Date().toISOString();
}

function sanitizeAllowlist(value, fallback) {
  if (!Array.isArray(value)) {
    return [...fallback];
  }
  const cleaned = value
    .filter((item) => typeof item === "string")
    .map((item) => item.trim())
    .filter((item) => item.length > 0);
  if (!cleaned.length) {
    return [...fallback];
  }
  return cleaned;
}

function sanitizeRestAllowlist(value) {
  const cleaned = sanitizeAllowlist(value, DEFAULT_CONFIG.restAllowlist);
  // 只允许我们自己硬编码的默认集合里的条目通过，不能是任意用户/存储配置传进来的URL——
  // 之前这里写死只比对 restAllowlist[0]，只要默认集合超过一条就会把后面的条目全部过滤掉。
  const strict = cleaned.filter((item) => DEFAULT_CONFIG.restAllowlist.includes(item));
  if (!strict.length) {
    return [...DEFAULT_CONFIG.restAllowlist];
  }
  return strict;
}

function matchesDomainFilter(url) {
  const filter = state.config.domainFilter.trim().toLowerCase();
  if (!filter) {
    return true;
  }
  return (url || "").toLowerCase().includes(filter);
}

function normalizeUrlParts(rawUrl) {
  try {
    const parsed = new URL(rawUrl);
    return {
      originPath: `${parsed.origin}${parsed.pathname}`,
      full: parsed.toString()
    };
  } catch {
    return {
      originPath: rawUrl,
      full: rawUrl
    };
  }
}

function getMatchedRestPattern(url) {
  const patterns = state.config.restAllowlist || [];
  return getMatchedPattern(url, patterns);
}

function getMatchedWsPattern(url) {
  const patterns = state.config.wsAllowlist || [];
  return getMatchedPattern(url, patterns);
}

function getMatchedPattern(url, patterns) {
  if (!patterns.length) {
    return null;
  }

  const target = normalizeUrlParts(url);
  for (const pattern of patterns) {
    const normalizedPattern = normalizeUrlParts(pattern);
    if (target.originPath === normalizedPattern.originPath || target.full.startsWith(pattern)) {
      return pattern;
    }
  }
  return null;
}

async function debuggerAttach(tabId) {
  await new Promise((resolve, reject) => {
    chrome.debugger.attach({ tabId }, DEBUGGER_VERSION, () => {
      const err = chrome.runtime.lastError;
      if (err) {
        reject(new Error(err.message));
        return;
      }
      resolve();
    });
  });
}

async function debuggerDetach(tabId) {
  await new Promise((resolve, reject) => {
    chrome.debugger.detach({ tabId }, () => {
      const err = chrome.runtime.lastError;
      if (err) {
        reject(new Error(err.message));
        return;
      }
      resolve();
    });
  });
}

async function sendDebuggerCommand(tabId, method, params = {}) {
  return new Promise((resolve, reject) => {
    chrome.debugger.sendCommand({ tabId }, method, params, (result) => {
      const err = chrome.runtime.lastError;
      if (err) {
        reject(new Error(err.message));
        return;
      }
      resolve(result || {});
    });
  });
}

async function getActiveTabId() {
  const tabs = await chrome.tabs.query({ active: true, currentWindow: true });
  if (!tabs.length || tabs[0].id == null) {
    throw new Error("No active tab found.");
  }
  return tabs[0].id;
}

async function startForwarding(tabId = null) {
  await ensureConfigLoaded();

  if (state.active) {
    return getStatus();
  }

  const targetTabId = tabId ?? (await getActiveTabId());
  await debuggerAttach(targetTabId);

  try {
    await sendDebuggerCommand(targetTabId, "Network.enable");
    await sendDebuggerCommand(targetTabId, "Page.enable");
    const hookResult = await sendDebuggerCommand(
      targetTabId,
      "Page.addScriptToEvaluateOnNewDocument",
      { source: PAGE_INJECTION_SCRIPT }
    );
    state.hookScriptId = hookResult.identifier || null;
  } catch (error) {
    await debuggerDetach(targetTabId);
    throw error;
  }

  state.active = true;
  state.attachedTabId = targetTabId;
  state.lastError = null;
  wsForwarder.connect();
  restForwarder.connect();
  commandSocket.connect();
  autoReloadAttachedTab("forwarder started");
  notifyStatus();
  return getStatus();
}

async function stopForwarding() {
  const attachedTabId = state.attachedTabId;
  cleanupForwardingState();
  if (attachedTabId != null) {
    try {
      await debuggerDetach(attachedTabId);
    } catch (error) {
      state.lastError = `Debugger detach failed: ${error.message}`;
    }
  }
  notifyStatus();
  return getStatus();
}

function cleanupForwardingState() {
  state.active = false;
  state.pendingResponses.clear();
  state.websocketMeta.clear();
  state.attachedTabId = null;
  state.lastAutoReloadAt = 0;
  state.hookScriptId = null;
  wsForwarder.close();
  restForwarder.close();
  commandSocket.close();
}

function getStatus() {
  return {
    active: state.active,
    attachedTabId: state.attachedTabId,
    config: state.config,
    sockets: {
      websocket: wsForwarder.status,
      rest: restForwarder.status,
      command: commandSocket.status,
    },
    lastError: state.lastError
  };
}

function notifyStatus() {
  chrome.runtime.sendMessage({ event: "status", status: getStatus() }).catch(() => {
    // No listeners (popup closed), safe to ignore.
  });
}

function trackResponse(params) {
  if (!params?.response?.url || !matchesDomainFilter(params.response.url)) {
    return;
  }
  if (params.type !== "Fetch" && params.type !== "XHR") {
    return;
  }

  const matchedPattern = getMatchedRestPattern(params.response.url);
  if (!matchedPattern) {
    return;
  }

  state.pendingResponses.set(params.requestId, {
    requestId: params.requestId,
    url: params.response.url,
    status: params.response.status,
    statusText: params.response.statusText,
    mimeType: params.response.mimeType,
    headers: params.response.headers,
    type: params.type,
    matchedPattern,
    capturedAt: nowIso()
  });
}

async function forwardResponseBody(requestId, encodedDataLength) {
  const meta = state.pendingResponses.get(requestId);
  if (!meta || state.attachedTabId == null) {
    return;
  }
  state.pendingResponses.delete(requestId);

  try {
    const result = await sendDebuggerCommand(state.attachedTabId, "Network.getResponseBody", { requestId });
    restForwarder.send({
      kind: "rest_response",
      requestId,
      timestamp: nowIso(),
      encodedDataLength,
      ...meta,
      body: result.body ?? "",
      base64Encoded: Boolean(result.base64Encoded)
    });
  } catch (error) {
    restForwarder.send({
      kind: "rest_response_error",
      requestId,
      timestamp: nowIso(),
      ...meta,
      error: error.message
    });
  }
}

function forwardWebSocketFrame(direction, params) {
  const meta = state.websocketMeta.get(params.requestId);
  if (!meta) {
    return;
  }

  wsForwarder.send({
    kind: "ws_frame",
    direction,
    requestId: params.requestId,
    url: meta.url,
    matchedPattern: meta.matchedPattern || "",
    timestamp: nowIso(),
    opcode: params.response?.opcode,
    mask: params.response?.mask,
    payloadData: params.response?.payloadData ?? ""
  });
}

async function handleDebuggerEvent(source, method, params) {
  if (!state.active || source.tabId !== state.attachedTabId) {
    return;
  }

  if (method === "Network.responseReceived") {
    trackResponse(params);
    return;
  }

  if (method === "Network.loadingFinished") {
    await forwardResponseBody(params.requestId, params.encodedDataLength);
    return;
  }

  if (method === "Network.loadingFailed") {
    state.pendingResponses.delete(params.requestId);
    return;
  }

  if (method === "Network.webSocketCreated") {
    const matchedPattern = getMatchedWsPattern(params.url);
    if (matchesDomainFilter(params.url) && matchedPattern) {
      state.websocketMeta.set(params.requestId, {
        url: params.url,
        matchedPattern,
        createdAt: nowIso()
      });
      // 之前这里只在本地记了一下，从来没告诉过 Python 侧"页面又建了一条新连接"——
      // 断连之后要判断"页面到底有没有尝试重连"，全靠这条信号，之前完全看不到。
      wsForwarder.send({
        kind: "ws_created",
        requestId: params.requestId,
        url: params.url,
        matchedPattern,
        timestamp: nowIso()
      });
    }
    return;
  }

  if (method === "Network.webSocketClosed") {
    const meta = state.websocketMeta.get(params.requestId);
    if (!meta) {
      return;
    }
    wsForwarder.send({
      kind: "ws_closed",
      requestId: params.requestId,
      url: meta.url,
      matchedPattern: meta.matchedPattern || "",
      timestamp: nowIso()
    });
    state.websocketMeta.delete(params.requestId);
    return;
  }

  if (method === "Network.webSocketFrameReceived") {
    forwardWebSocketFrame("received", params);
    return;
  }

  if (method === "Network.webSocketFrameSent") {
    forwardWebSocketFrame("sent", params);
    return;
  }

  if (method === "Network.webSocketFrameError") {
    const meta = state.websocketMeta.get(params.requestId);
    if (!meta) {
      return;
    }
    wsForwarder.send({
      kind: "ws_frame_error",
      requestId: params.requestId,
      url: meta.url,
      matchedPattern: meta.matchedPattern || "",
      timestamp: nowIso(),
      errorMessage: params.errorMessage || "Unknown WebSocket frame error"
    });
  }
}

chrome.debugger.onEvent.addListener((source, method, params) => {
  handleDebuggerEvent(source, method, params).catch((error) => {
    state.lastError = `CDP event handling failed: ${error.message}`;
    notifyStatus();
  });
});

chrome.debugger.onDetach.addListener((source, reason) => {
  if (source.tabId !== state.attachedTabId) {
    return;
  }
  state.lastError = `Debugger detached: ${reason}`;
  cleanupForwardingState();
  notifyStatus();
});

chrome.runtime.onMessage.addListener((message, sender, sendResponse) => {
  (async () => {
    await ensureConfigLoaded();

    if (message.action === "getStatus") {
      return { ok: true, status: getStatus() };
    }

    if (message.action === "updateConfig") {
      state.config = sanitizeConfig(message.config);
      await chrome.storage.local.set({ forwarderConfig: state.config });
      if (state.active) {
        wsForwarder.restart();
        restForwarder.restart();
        commandSocket.restart();
      }
      notifyStatus();
      return { ok: true, status: getStatus() };
    }

    if (message.action === "start") {
      const status = await startForwarding(message.tabId ?? null);
      return { ok: true, status };
    }

    if (message.action === "stop") {
      const status = await stopForwarding();
      return { ok: true, status };
    }

    return { ok: false, error: `Unknown action: ${message.action}` };
  })()
    .then((response) => sendResponse(response))
    .catch((error) => sendResponse({ ok: false, error: error.message }));

  return true;
});

chrome.runtime.onInstalled.addListener(() => {
  ensureConfigLoaded().catch(() => {
    // Ignore config load errors during install.
  });
});
