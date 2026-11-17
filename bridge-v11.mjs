#!/usr/bin/env node
/**
 * Chatwoot ↔ OpenClaw 直连桥 v1.1（单实例多收件箱动态路由）
 *
 * 【本文件是唯一生产入口】launchd 服务 com.blueaisoft.cw-oc-bridge 运行本文件；
 * 旧版 bridge.mjs（单品牌时代）已从仓库删除。
 * 所有品牌 bot 的 outgoing_url 都指向本服务同一端口，由 routes.json 决定
 * 「哪个收件箱 → 用哪个 Agent 生成 + 以哪个 bot 身份回复」，加品牌只需改配置。
 *
 * 数据流（一条客户消息的完整旅程）：
 *   Chatwoot AgentBot 回调（HMAC 签名）
 *     → 验签（双格式自适应）→ 事件过滤 → 持久化去重
 *     → 路由表解析（inbox → Agent + bot token）
 *     → 并发准入 → OpenClaw WS RPC 生成回复（30s 超时）
 *     → 以品牌 bot 身份发回 Chatwoot；超时/失败 → 兜底文案 + 私有备注
 *
 * 运行：/Users/mac6/.local/opt/node24/bin/node bridge-v11.mjs
 * 依赖：仅 Node 24 内置（WebSocket / fetch / crypto / fs），零第三方依赖
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

// ---------- 持久化去重（防 Chatwoot 重投/重启后重复处理 → 双回复） ----------
// 机制：messageId → 时间戳（内存 Map），窗口 24h；
// 每 20 条增量落盘 JSONL + 每 5min 压缩重写，启动时回读最近 5000 行恢复水位。
// 不去重的后果：客户同一条消息收到两次 AI 回复。
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
// 【v1.1 实际行为，接手者必读】
//   主链路是 handleMessage()（由 HTTP 入口调用），它只经过 scheduleTask() 的
//   「准入检查」——满载时跳过限流直接执行并打 WARN 告警。
//   本节的 enqueueByConversation（同会话串行）和 waitQueue 调度器（队列排队）
//   仅被下方遗留的 processMessage() 使用，主链路未接入：
//   waitQueue 实际恒为空（scheduleTask 不入队），调度循环空转。
//   同会话严格串行 / 队列排队属后续版本工作项（设计见 docs/ 架构设计 §2.3）。
const convLocks = new Map();
// 把同一会话的任务串成 Promise 链：后到的消息等前一条处理完再执行。
// 【遗留】当前主链路未使用，仅 processMessage() 调用。
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
// 【遗留】本调度器只消费 waitQueue，而 scheduleTask 从不入队（见下），
// 故此循环实际空转；并发计数 globalRunning 也因此恒为 0。
// 保留原因：v1.1 规划的完整排队机制在此，接入主链路时可复用。
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

// 并发准入检查：达到任一上限时返回 queued:false。
// 【注意】无论 queued 与否，调用方都会立即执行 task（见 handleMessage 尾部）——
// 即 v1.1 实际语义是「满载告警 + 直通」，并未真正限流排队。
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
// Chatwoot 有两种回调，签名格式不同（源码 lib/webhooks/trigger.rb）：
//   ① AgentBot 回调（当前主链路）：HMAC(secret, "时间戳.消息体")，头 X-Chatwoot-Timestamp
//   ② 账户级 webhook：HMAC(secret, 消息体)，无时间戳头
// 两种候选依次试算，secrets 池存多个 bot 的候选 secret；
// timingSafeEqual 恒定时间比对防时序侧信道。
// 曾因只实现纯 body 格式 + secret 配错，AgentBot 投递全 401，
// Chatwoot 自动把会话转 open 导致浮窗瘫痪 2 小时（见 docs/排错手册）。
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
// 与 OpenClaw Gateway 保持一条 WebSocket 长连接复用：
//   - 请求帧 {type:"req", id, method, params}，响应帧 {type:"res", id, ok, payload}，
//     用自增 id 关联（注意 type 是 "req" 而非 "request"，且没有 jsonrpc 字段——
//     写错会被 Gateway 以 1008 断连，这是实测踩过的坑）
//   - 帧分发三分支：res（请求-响应）/ event（流式事件）/ 广播监听器（collector）
//   - 所有等待点都有强制超时（Gateway 假死时 recv 永不返回，无超时会挂死整条链路）
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
        // connect 握手参数是 Gateway 硬契约：minProtocol/maxProtocol=4，
        // client 必须含 id/version/platform/mode 四字段，role/scopes/auth 缺一不可。
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
  /** 发消息并收集回复（一次完整的 Agent 调用）：
   *  ① sessions.create 建/复用会话（key 相同则复用，上下文得以延续）
   *  ② sessions.send 发客户消息，拿到 runId
   *  ③ collector 持续收集 assistant 流式帧：
   *     - runId 过滤：只认本次发送的 run（多会话共享连接，防跨会话串扰）
   *     - 三语义：delta 追加 / text 全量快照覆盖 / replace 整段替换
   *  ④ 等 lifecycle done 帧或超时；超时已兜底的消息，迟到的 AI 帧一律丢弃
   *     （否则客户先收兜底文案再收迟到回复 = 双回复事故） */
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

// ---------- Chatwoot API（回复出口） ----------
// sendChatwootReply：以「品牌 bot 的 access_token」身份发消息，客户在浮窗看到的是
//                    该品牌机器人而非真人客服；botToken 来自路由表 inboxMap。
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
// 转人工：pending → open（open 会话 AgentBot 不再处理，交人工接管）。
// 【未接入】当前无调用方：超时/失败降级时只发兜底文案 + 私有备注，
// 会话仍留在 pending 由 bot 继续服务；如需「降级即转人工」在降级分支补调用本函数。
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

// ---------- 消息处理 ----------
// FALLBACK_TEXT：兜底文案。Agent 超时/异常/空回复时发给客户，保证客户永远有响应。
const FALLBACK_TEXT = "抱歉，我这边暂时无法回答，已记录您的问题，人工稍后会跟进。";

// 【遗留死代码】processMessage 未被任何入口调用（HTTP 入口走 handleMessage），
// 与 handleMessage 逻辑高度重复，仅会话串行部分（enqueueByConversation）与之不同。
// 保留仅作 v1.1 会话串行实现的参考，可安全删除。
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

  // 路由：未映射 inbox → 降级处理（兜底文案 + 私有备注，保证客户不失联）
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

  // 会话级 session key：格式 agent:{agent}:cw:{account}:{inbox}:{conv}。
  // 一个会话一个 OpenClaw session（key 相同复用），上下文自然延续；
  // inbox 编号入 key，同 Agent 服务多品牌时上下文互不串。
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
      // 验签失败必须回 401：Chatwoot 收到非 2xx 会把会话自动转 open（人工接管），
      // 客户不至于干等机器人；但若 secret 配错，所有会话都会被转走 = 浮窗瘫痪。
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
        // 只处理「客户发的非私有消息」：bot 自己的回复、agent 手工消息、
        // 私有备注都不应触发 AI（否则会自我循环回复）。
        res.writeHead(200).end(JSON.stringify({ ignored: true }));
        return;
      }
      // 先回 200 再异步处理：Chatwoot 侧有投递超时（约 5s），
      // 而 AI 生成要 4-20s，若同步等待会因超时被判失败 → 会话被转 open。
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
