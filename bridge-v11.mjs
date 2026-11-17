#!/usr/bin/env node
/**
 * Chatwoot ↔ OpenClaw 直连桥 v1.1（单实例多收件箱动态路由）
 *
 * 数据流：
 *   Chatwoot webhook（全部品牌 bot 的 outgoing_url 统一指向本服务）
 *     → HMAC 验签（双格式自适应）→ 过滤 → 持久化去重
 *     → 路由表解析（inbox → Agent + bot token）
 *     → 会话锁串行 → OpenClaw WS RPC → 回复 → Chatwoot API（bot 身份）
 *
 * 运行：/Users/mac6/.local/opt/node24/bin/node bridge.mjs
 * 依赖：仅 Node 24 内置（WebSocket / fetch / crypto / fs）
 */
import http from "node:http";
import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";

// ---------- 配置 ----------
const CFG = {
  port: Number(process.env.BRIDGE_PORT || 8300),
  chatwootBase: process.env.CHATWOOT_BASE_URL || "https://chat.blueaisoft.com",
  accountId: process.env.CHATWOOT_ACCOUNT_ID || "1",
  gatewayUrl: process.env.OPENCLAW_GATEWAY_URL || "ws://127.0.0.1:18789",
  gatewayToken: process.env.OPENCLAW_GATEWAY_TOKEN || "",
  agentTimeoutMs: Number(process.env.AGENT_TIMEOUT_MS || 30000),
  slowLogMs: Number(process.env.SLOW_REQUEST_LOG_MS || 20000),
  // 路由表：inbox → {agent, botToken}（JSON 文件，支持 SIGHUP 热重载）
  routeFile: process.env.ROUTE_FILE || "",
  // 兼容旧环境变量（单品牌模式回退）
  webhookSecret: process.env.CHATWOOT_AGENTBOT_WEBHOOK_SECRET || "",
  botToken: process.env.BLUEWMS_BOT_TOKEN || "",
  agentId: process.env.OPENCLAW_AGENT_ID || "",
  inboxId: process.env.TARGET_INBOX_ID || "",
  dbFile: process.env.BRIDGE_DB || "",
};

// ---------- 路由表（支持热重载） ----------
let ROUTES = { secrets: [], inboxMap: {} };

function loadRoutes() {
  const routes = { secrets: [], inboxMap: {} };
  // 多 secret 验签列表
  try {
    if (process.env.WEBHOOK_SECRETS) {
      routes.secrets = JSON.parse(process.env.WEBHOOK_SECRETS);
    }
  } catch {}
  // inbox → agent/botToken 映射
  try {
    if (process.env.INBOX_ROUTE_MAP) {
      const m = JSON.parse(process.env.INBOX_ROUTE_MAP);
      for (const [inbox, conf] of Object.entries(m)) {
        routes.inboxMap[inbox] = {
          agent: conf.agent,
          botToken: conf.botToken,
          secret: conf.secret || null,
        };
      }
    }
  } catch {}
  // 路由文件（JSON，支持热重载）优先
  if (CFG.routeFile && fs.existsSync(CFG.routeFile)) {
    try {
      const conf = JSON.parse(fs.readFileSync(CFG.routeFile, "utf8"));
      if (Array.isArray(conf.secrets)) routes.secrets = conf.secrets;
      if (conf.inboxMap) routes.inboxMap = conf.inboxMap;
    } catch (e) {
      console.error(`[ERROR] 路由文件解析失败: ${e.message}`);
    }
  }
  // 兼容旧单品牌环境变量（映射表为空时回退）
  if (Object.keys(routes.inboxMap).length === 0 && CFG.inboxId && CFG.agentId) {
    routes.inboxMap[String(CFG.inboxId)] = {
      agent: CFG.agentId,
      botToken: CFG.botToken,
      secret: CFG.webhookSecret || null,
    };
    if (CFG.webhookSecret) routes.secrets.push(CFG.webhookSecret);
  }
  ROUTES = routes;
  console.log(
    `[INFO] 路由表加载: ${Object.keys(routes.inboxMap).length} 个 inbox, ${routes.secrets.length} 个 secret`
  );
}
loadRoutes();

// SIGHUP 热重载路由
process.on("SIGHUP", () => {
  console.log("[INFO] 收到 SIGHUP，重载路由表");
  loadRoutes();
});
// 每 60s 轮询路由文件 mtime（无 SIGHUP 权限时兜底）
let lastMtime = 0;
setInterval(() => {
  if (!CFG.routeFile || !fs.existsSync(CFG.routeFile)) return;
  const mt = fs.statSync(CFG.routeFile).mtimeMs;
  if (lastMtime && mt !== lastMtime) {
    console.log("[INFO] 路由文件变更，重载路由表");
    loadRoutes();
  }
  lastMtime = mt;
}, 60_000).unref();

function routeFor(inboxId) {
  return ROUTES.inboxMap[String(inboxId)] || null;
}
function secretsList() {
  return ROUTES.secrets.length ? ROUTES.secrets : [];
}

// ---------- 持久化去重（SQLite 更轻替代：JSONL 追加 + 启动加载近 24h） ----------
const DEDUP_FILE = process.env.DEDUP_FILE || path.join(path.dirname(new URL(import.meta.url).pathname), "dedup.jsonl");
const seenMessages = new Map(); // messageId -> ts
const DEDUP_WINDOW = 24 * 60 * 60 * 1000;

function loadDedup() {
  if (!fs.existsSync(DEDUP_FILE)) return;
  const cutoff = Date.now() - DEDUP_WINDOW;
  try {
    const lines = fs.readFileSync(DEDUP_FILE, "utf8").trim().split("\n").slice(-5000);
    for (const line of lines) {
      try {
        const { id, ts } = JSON.parse(line);
        if (ts > cutoff) seenMessages.set(id, ts);
      } catch {}
    }
    // 清理旧记录后重写文件
    rewriteDedup();
    console.log(`[INFO] 去重水位恢复: ${seenMessages.size} 条`);
  } catch {}
}
function rewriteDedup() {
  const cutoff = Date.now() - DEDUP_WINDOW;
  const entries = [...seenMessages.entries()].filter(([, ts]) => ts > cutoff);
  fs.writeFileSync(DEDUP_FILE, entries.map(([id, ts]) => JSON.stringify({ id, ts })).join("\n") + "\n");
}
let dedupDirty = 0;
function markSeen(msgId) {
  seenMessages.set(msgId, Date.now());
  if (++dedupDirty % 20 === 0) rewriteDedup(); // 每 20 条落盘一次
}
loadDedup();
setInterval(() => {
  const cutoff = Date.now() - DEDUP_WINDOW;
  for (const [k, v] of seenMessages) if (v < cutoff) seenMessages.delete(k);
  if (dedupDirty > 0) { rewriteDedup(); dedupDirty = 0; }
}, 300_000).unref();

// ---------- 会话锁（串行）与并发控制 ----------
const convLocks = new Map();
function enqueueByConversation(convId, task) {
  const prev = convLocks.get(convId) || Promise.resolve();
  const next = prev.then(task, task);
  convLocks.set(convId, next);
  next.finally(() => {
    if (convLocks.get(convId) === next) convLocks.delete(convId);
  });
  return next;
}

// 有界并发（全局 + 每品牌）与队列
const MAX_GLOBAL = Number(process.env.MAX_GLOBAL_CONCURRENCY || 4);
const MAX_PER_INBOX = Number(process.env.MAX_PER_INBOX_CONCURRENCY || 2);
const MAX_QUEUE = Number(process.env.MAX_QUEUE_SIZE || 100);
let globalRunning = 0;
const inboxRunning = new Map();
const waitQueue = []; // {run}
setInterval(() => {
  while (waitQueue.length && globalRunning < MAX_GLOBAL) {
    const item = waitQueue.shift();
    if (item.brandRunning >= MAX_PER_INBOX) { waitQueue.push(item); break; }
    globalRunning++;
    item.run().finally(() => globalRunning--);
  }
}, 500).unref();

function scheduleTask(inboxId, task) {
  const brandRunning = inboxRunning.get(inboxId) || 0;
  if (globalRunning >= MAX_GLOBAL || brandRunning >= MAX_PER_INBOX || waitQueue.length >= MAX_QUEUE) {
    return { queued: false, run: task };
  }
  return { queued: true, run: task };
}

// ---------- 统计（SQLite 落盘简化为 JSONL + 内存聚合） ----------
const STATS_FILE = process.env.STATS_FILE || path.join(path.dirname(new URL(import.meta.url).pathname), "stats.jsonl");
const stats = { total: 0, ok: 0, fallback: 0, latencies: [], byInbox: {} };
function recordStat(inboxId, ok, degraded, latency) {
  stats.total++;
  if (ok && !degraded) stats.ok++;
  if (degraded) stats.fallback++;
  if (latency) stats.latencies.push(latency);
  if (stats.latencies.length > 5000) stats.latencies = stats.latencies.slice(-5000);
  const b = (stats.byInbox[inboxId] ||= { total: 0, ok: 0, fallback: 0 });
  b.total++;
  if (ok && !degraded) b.ok++;
  if (degraded) b.fallback++;
  fs.appendFileSync(STATS_FILE, JSON.stringify({ ts: Date.now(), inbox: inboxId, ok: !degraded, latency }) + "\n");
}

// ---------- 日志 ----------
const LOG = (level, msg, extra) =>
  console.log(`${new Date().toISOString()} [${level}] ${msg}${extra ? " " + JSON.stringify(extra) : ""}`);

// ---------- Chatwoot 验签（双格式自适应） ----------
function verifySignature(rawBody, signatureHeader, timestampHeader, secrets) {
  if (!secrets.length) return true;
  if (!signatureHeader) return false;
  const sig = String(signatureHeader).replace(/^sha256=/, "");
  const candidates = [rawBody];
  if (timestampHeader) candidates.unshift(`${timestampHeader}.${rawBody}`);
  const sigBuf = Buffer.from(sig);
  return secrets.some((secret) =>
    candidates.some((c) => {
      const expected = crypto.createHmac("sha256", secret).update(c).digest("hex");
      const expBuf = Buffer.from(expected);
      return expBuf.length === sigBuf.length && crypto.timingSafeEqual(expBuf, sigBuf);
    })
  );
}

// ---------- OpenClaw WS RPC ----------
class GatewayRPC {
  constructor() {
    this.ws = null;
    this.reqId = 0;
    this.pending = new Map();
    this.eventWaiters = [];
    this.frameListeners = new Set();
    this.connecting = null;
  }
  async ensureConnected() {
    if (this.ws && this.ws.readyState === WebSocket.OPEN) return;
    if (this.connecting) return this.connecting;
    this.connecting = new Promise((resolve, reject) => {
      const ws = new WebSocket(CFG.gatewayUrl);
      const timer = setTimeout(() => { ws.close(); reject(new Error("gateway connect timeout")); }, 8000);
      ws.onopen = () => {
        clearTimeout(timer);
        this.ws = ws;
        this.send({
          type: "req", id: `hs-${++this.reqId}`, method: "connect",
          params: {
            minProtocol: 4, maxProtocol: 4,
            client: { id: "gateway-client", version: "1.0.0", platform: "darwin", mode: "backend" },
            role: "operator", scopes: ["operator.admin"], auth: { token: CFG.gatewayToken },
          },
        });
        const hsId = `hs-${this.reqId}`;
        this.pending.set(hsId, {
          resolve: () => { LOG("INFO", "Gateway 已连接"); resolve(); },
          reject,
          timer: setTimeout(() => reject(new Error("handshake timeout")), 8000),
        });
      };
      ws.onmessage = (ev) => this.handleFrame(String(ev.data));
      ws.onerror = () => {};
      ws.onclose = () => {
        clearTimeout(timer); this.ws = null;
        for (const [, p] of this.pending) { clearTimeout(p.timer); p.reject(new Error("gateway closed")); }
        this.pending.clear();
      };
    });
    try { await this.connecting; } finally { this.connecting = null; }
  }
  send(obj) { this.ws.send(JSON.stringify(obj)); }
  handleFrame(raw) {
    let f;
    try { f = JSON.parse(raw); } catch { return; }
    if (f.type === "res" && f.id) {
      const p = this.pending.get(f.id);
      if (p) {
        this.pending.delete(f.id); clearTimeout(p.timer);
        f.ok ? p.resolve(f.payload) : p.reject(new Error(f.error?.message || "rpc error"));
      }
      return;
    }
    for (let i = this.eventWaiters.length - 1; i >= 0; i--) {
      const w = this.eventWaiters[i];
      if (w.predicate(f)) {
        this.eventWaiters.splice(i, 1); clearTimeout(w.timer); w.resolve(f); break;
      }
    }
    for (const l of this.frameListeners) { try { l(f); } catch {} }
  }
  async request(method, params, timeoutMs = 15000) {
    await this.ensureConnected();
    const id = `r-${++this.reqId}`;
    const p = new Promise((resolve, reject) => {
      const timer = setTimeout(() => { this.pending.delete(id); reject(new Error(`rpc ${method} timeout`)); }, timeoutMs);
      this.pending.set(id, { resolve, reject, timer });
    });
    this.send({ type: "req", id, method, params });
    return p;
  }
  waitForEvent(predicate, timeoutMs) {
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error("event wait timeout")), timeoutMs);
      this.eventWaiters.push({ predicate, resolve, timer });
    });
  }
  /** 发消息并收集回复（v1.1：delta 追加/快照覆盖/replace 替换三语义 + runId 隔离 + 迟到丢弃） */
  async chat(agentKey, message, timeoutMs) {
    await this.ensureConnected();
    const created = await this.request("sessions.create", { key: agentKey }, 15000);
    LOG("INFO", "session ready", { key: agentKey });
    const sendId = `s-${++this.reqId}`;
    const sendPromise = new Promise((resolve, reject) => {
      const timer = setTimeout(() => { this.pending.delete(sendId); reject(new Error("send timeout")); }, timeoutMs);
      this.pending.set(sendId, { resolve, reject, timer });
    });
    this.send({ type: "req", id: sendId, method: "sessions.send", params: { key: agentKey, message } });
    const started = await sendPromise;
    const runId = started?.runId;
    let text = "";
    let finalized = false;
    const collector = (f) => {
      if (finalized) return; // 迟到帧一律丢弃
      if (f.type !== "event" || f.event !== "agent") return;
      const p = f.payload || {};
      if (runId && p.runId !== runId) return;
      if (p.stream !== "assistant") return;
      const d = p.data || {};
      if (d.replace === true) {
        // replace 语义：替换当前全部文本
        if (typeof d.delta === "string") text = d.delta;
        else if (typeof d.text === "string") text = d.text;
      } else if (typeof d.delta === "string" && d.delta) {
        text += d.delta;
      } else if (!("delta" in d) && typeof d.text === "string") {
        text = d.text; // 全量快照
      }
    };
    this.frameListeners.add(collector);
    try {
      await this.waitForEvent((f) => {
        if (f.type !== "event" || f.event !== "agent") return false;
        const p = f.payload || {};
        if (runId && p.runId !== runId) return false;
        return p.stream === "lifecycle" && (p.data?.stopReason || p.data?.phase === "done");
      }, timeoutMs);
    } finally {
      finalized = true;
      this.frameListeners.delete(collector);
    }
    return text.trim();
  }
}
const gw = new GatewayRPC();

// ---------- Chatwoot API ----------
async function sendChatwootReply(conversationId, content, botToken) {
  const url = `${CFG.chatwootBase}/api/v1/accounts/${CFG.accountId}/conversations/${conversationId}/messages`;
  const res = await fetch(url, {
    method: "POST",
    headers: { "api-access-token": botToken, "Content-Type": "application/json" },
    body: JSON.stringify({ content, message_type: "outgoing", private: false }),
  });
  if (!res.ok) throw new Error(`chatwoot send ${res.status}: ${(await res.text()).slice(0, 120)}`);
  return res.json();
}
async function addPrivateNote(conversationId, content) {
  const url = `${CFG.chatwootBase}/api/v1/accounts/${CFG.accountId}/conversations/${conversationId}/messages`;
  try {
    await fetch(url, {
      method: "POST",
      headers: { "api-access-token": CFG.botToken || Object.values(ROUTES.inboxMap)[0]?.botToken, "Content-Type": "application/json" },
      body: JSON.stringify({ content, message_type: "outgoing", private: true }),
    });
  } catch {}
}
async function setConversationOpen(conversationId) {
  try {
    const url = `${CFG.chatwootBase}/api/v1/accounts/${CFG.accountId}/conversations/${conversationId}`;
    const token = Object.values(ROUTES.inboxMap)[0]?.botToken;
    // toggle_status 端点（AgentBot 模式下 pending→open 转人工）
    await fetch(`${url}/toggle_status`, {
      method: "POST",
      headers: { "api-access-token": token, "Content-Type": "application/json" },
      body: JSON.stringify({ status: "open" }),
    });
  } catch {}
}

// ---------- 处理一条 incoming ----------
const FALLBACK_TEXT = "抱歉，我这边暂时无法回答，已记录您的问题，人工稍后会跟进。";

async function processMessage(payload, route) {
  const msgId = payload.id;
  const convId = payload.conversation?.id;
  const inboxId = String(payload.inbox?.id);
  const content = String(payload.content || "").trim();
  const t0 = Date.now();

  if (seenMessages.has(msgId)) return { duplicate: true };
  markSeen(msgId);

  const agentKey = `agent:${route.agent}:cw:1:${inboxId}:${convId}`;
  LOG("INFO", "处理消息", { msgId, convId, inbox: inboxId, agent: route.agent });

  return enqueueByConversation(`${route.agent}:${convId}`, async () => {
    let reply = "";
    let degraded = false;
    try {
      reply = await gw.chat(agentKey, content, CFG.agentTimeoutMs);
    } catch (err) {
      degraded = true;
      LOG("WARN", "OpenClaw 失败，走兜底文案", { msgId, err: String(err).slice(0, 100) });
      reply = FALLBACK_TEXT;
    }
    const latency = Date.now() - t0;
    if (latency > CFG.slowLogMs) LOG("WARN", "慢请求", { msgId, latency });

    if (!reply) {
      reply = FALLBACK_TEXT;
      degraded = true;
    }

    try {
      await sendChatwootReply(convId, reply, route.botToken);
    } catch (err) {
      degraded = true;
      LOG("ERROR", "回复发送失败", { msgId, err: String(err).slice(0, 100) });
    }

    recordStat(inboxId, !degraded, degraded, latency);
    LOG("INFO", "已回复", { msgId, latency, degraded, chars: reply.length, agent: route.agent });
    return { ok: true, latency, degraded };
  });
}

async function handleMessage(payload) {
  const msgId = payload.id;
  const convId = payload.conversation?.id;
  const inboxId = String(payload.inbox?.id);
  const content = String(payload.content || "").trim();
  if (!msgId || !convId || !content) return { ignored: true, reason: "missing fields" };

  // 路由：未映射 inbox → 降级三件套（兜底文案 + 转人工 + 私有备注）
  const route = routeFor(inboxId);
  if (!route) {
    LOG("WARN", "未映射 inbox，降级处理", { inboxId, msgId });
    recordStat(inboxId, false, true, null);
    try { await sendChatwootReply(convId, FALLBACK_TEXT, CFG.botToken || undefined); } catch {}
    try { await addPrivateNote(convId, `[桥] 未映射 inbox ${inboxId} 的消息，已发兜底文案（msgId=${msgId}）`); } catch {}
    return { degraded: true, reason: "unrouted inbox" };
  }

  if (seenMessages.has(msgId)) return { duplicate: true };
  markSeen(msgId);

  const agentKey = `agent:${route.agent}:cw:1:${inboxId}:${convId}`;
  LOG("INFO", "处理消息", { msgId, convId, inbox: inboxId, agent: route.agent });

  const task = async () => {
    let reply = "";
    let degraded = false;
    const t0 = Date.now();
    try {
      reply = await gw.chat(agentKey, content, CFG.agentTimeoutMs);
    } catch (err) {
      degraded = true;
      LOG("WARN", "OpenClaw 失败，走兜底", { msgId, err: String(err).slice(0, 100) });
      reply = FALLBACK_TEXT;
    }
    const latency = Date.now() - t0;
    if (latency > CFG.slowLogMs) LOG("WARN", "慢请求", { msgId, latency });

    if (!reply) { reply = FALLBACK_TEXT; degraded = true; }

    try {
      await sendChatwootReply(convId, reply, route.botToken);
    } catch (err) {
      degraded = true;
      LOG("ERROR", "回复发送失败", { msgId, err: String(err).slice(0, 100) });
    }

    recordStat(inboxId, !degraded, degraded, latency);
    LOG("INFO", "已回复", { msgId, latency, degraded, chars: reply.length, agent: route.agent });
    return { ok: true, latency, degraded };
  };

  const { queued, run } = scheduleTask(inboxId, task);
  if (!queued) {
    // 队列满：直接执行（跳过并发限制）但记录告警
    LOG("WARN", "并发已满，直接执行", { msgId });
    return run();
  }
  return run();
}

// ---------- HTTP 服务 ----------
const server = http.createServer(async (req, res) => {
  if (req.method === "GET" && req.url === "/healthz") {
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify({
      ok: true,
      routes: Object.keys(ROUTES.inboxMap),
      queue: waitQueue.length,
      globalRunning,
      gateway: gw.ws?.readyState === WebSocket.OPEN ? "connected" : "disconnected",
    }));
    return;
  }
  if (req.method === "GET" && req.url === "/stats") {
    // 仅 loopback
    const remote = req.socket.remoteAddress;
    if (!remote || !remote.includes("127.0.0.1") && remote !== "::1") {
      res.writeHead(403).end(JSON.stringify({ error: "forbidden" }));
      return;
    }
    const lat = [...stats.latencies].sort((a, b) => a - b);
    const pct = (p) => (lat.length ? lat[Math.min(lat.length - 1, Math.floor(lat.length * p))] : null);
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify({
      total: stats.total, ok: stats.ok, fallback: stats.fallback,
      byInbox: stats.byInbox,
      latency: { p50: pct(0.5), p90: pct(0.9), p95: pct(0.95) },
      queue: waitQueue.length, globalRunning,
    }));
    return;
  }
  if (req.method === "POST" && req.url === "/webhook/chatwoot") {
    let raw = "";
    req.on("data", (c) => (raw += c));
    req.on("end", async () => {
      if (!verifySignature(raw, req.headers["x-chatwoot-signature"], req.headers["x-chatwoot-timestamp"], secretsList())) {
        res.writeHead(401).end(JSON.stringify({ error: "invalid signature" }));
        LOG("WARN", "验签失败", { ip: req.socket.remoteAddress });
        return;
      }
      let payload;
      try { payload = JSON.parse(raw); } catch {
        res.writeHead(400).end(JSON.stringify({ error: "invalid json" }));
        return;
      }
      if (payload.event !== "message_created" || payload.message_type !== "incoming" || payload.private) {
        res.writeHead(200).end(JSON.stringify({ ignored: true }));
        return;
      }
      res.writeHead(200).end(JSON.stringify({ accepted: true }));
      try {
        const r = await handleMessage(payload);
        if (r?.duplicate) LOG("INFO", "重复消息已去重", { msgId: payload.id });
      } catch (err) {
        LOG("ERROR", "handleMessage 异常", { err: String(err).slice(0, 120) });
      }
    });
    return;
  }
  res.writeHead(404).end();
});

server.on("error", (e) => {
  LOG("ERROR", `Server error: ${e.message}`);
  if (e.code === "EADDRINUSE") process.exit(1);
});

server.listen(CFG.port, () => {
  LOG("INFO", `直连桥 v1.1 单实例启动: ${CFG.port} | 路由: ${Object.keys(ROUTES.inboxMap).join(",")} | 并发 ${MAX_GLOBAL}/${MAX_PER_INBOX} | 队列 ${MAX_QUEUE}`);
});
