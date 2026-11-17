#!/usr/bin/env node
/**
 * Chatwoot ↔ OpenClaw 直连桥（bluewms-support）
 * 全新独立实现：不依赖 chatwoot-agent 项目任何代码。
 *
 * 数据流：
 *   Chatwoot webhook → 本服务(HMAC 验签/过滤/会话锁/超时兜底)
 *     → OpenClaw Gateway WS RPC (agent:bluewms-support:...)
 *     → 回复文本 → Chatwoot REST API（BlueWMS-Agent bot 身份）
 *
 * 运行：/Users/mac6/.local/opt/node24/bin/node bridge.mjs
 * 依赖：仅 Node 24 内置（WebSocket / fetch / crypto）
 */
import http from "node:http";
import crypto from "node:crypto";

// ---------- 配置（环境变量可覆盖） ----------
const CFG = {
  port: Number(process.env.BRIDGE_PORT || 8300),
  chatwootBase: process.env.CHATWOOT_BASE_URL || "https://chat.blueaisoft.com",
  accountId: process.env.CHATWOOT_ACCOUNT_ID || "1",
  webhookSecret: process.env.CHATWOOT_AGENTBOT_WEBHOOK_SECRET || "",
  // BlueWMS bot access token（发送身份）
  botToken:
    process.env.BLUEWMS_BOT_TOKEN ||
    "eRmx9P31W9HmbcHqj9CCGUgv",
  gatewayUrl: process.env.OPENCLAW_GATEWAY_URL || "ws://127.0.0.1:18789",
  gatewayToken: process.env.OPENCLAW_GATEWAY_TOKEN || "",
  agentId: process.env.OPENCLAW_AGENT_ID || "bluewms-support",
  inboxId: Number(process.env.TARGET_INBOX_ID || 7),
  agentTimeoutMs: Number(process.env.AGENT_TIMEOUT_MS || 30000),
  log: (level, msg, extra) =>
    console.log(
      `${new Date().toISOString()} [${level}] ${msg}${extra ? " " + JSON.stringify(extra) : ""}`
    ),
};

// ---------- 会话级串行锁 + 去重 ----------
const convLocks = new Map(); // conversationId -> Promise（尾随串行）
const seenMessages = new Map(); // messageId -> ts（5 分钟滑动去重）
setInterval(() => {
  const cutoff = Date.now() - 5 * 60 * 1000;
  for (const [k, v] of seenMessages) if (v < cutoff) seenMessages.delete(k);
}, 60_000).unref();

function enqueueByConversation(convId, task) {
  const prev = convLocks.get(convId) || Promise.resolve();
  const next = prev.then(task, task);
  convLocks.set(convId, next);
  next.finally(() => {
    if (convLocks.get(convId) === next) convLocks.delete(convId);
  });
  return next;
}

// ---------- Chatwoot 验签（HMAC-SHA256(secret, body)，账户级格式） ----------
function verifySignature(rawBody, signatureHeader, timestampHeader) {
  if (!CFG.webhookSecret) return true; // 未配置则跳过（本地测试）
  if (!signatureHeader) return false;
  const sig = String(signatureHeader).replace(/^sha256=/, "");
  // Chatwoot 两种签名格式（源码 lib/webhooks/trigger.rb 确认）：
  //   AgentBot:  HMAC(secret, "{ts}.{body}") + X-Chatwoot-Timestamp
  //   账户级:    HMAC(secret, body)
  const candidates = [rawBody];
  if (timestampHeader) candidates.unshift(`${timestampHeader}.${rawBody}`);
  const sigBuf = Buffer.from(sig);
  return candidates.some((c) => {
    const expected = crypto.createHmac("sha256", CFG.webhookSecret).update(c).digest("hex");
    const expBuf = Buffer.from(expected);
    return expBuf.length === sigBuf.length && crypto.timingSafeEqual(expBuf, sigBuf);
  });
}

// ---------- OpenClaw WS RPC ----------
class GatewayRPC {
  constructor() {
    this.ws = null;
    this.reqId = 0;
    this.pending = new Map(); // id -> {resolve, reject, timer}
    this.eventWaiters = []; // {predicate, resolve, timer}
    this.frameListeners = new Set(); // 每帧回调（收集增量）
    this.connecting = null;
  }

  async ensureConnected() {
    if (this.ws && this.ws.readyState === WebSocket.OPEN) return;
    if (this.connecting) return this.connecting;
    this.connecting = new Promise((resolve, reject) => {
      const ws = new WebSocket(CFG.gatewayUrl);
      const timer = setTimeout(() => {
        ws.close();
        reject(new Error("gateway connect timeout"));
      }, 8000);
      ws.onopen = () => {
        clearTimeout(timer);
        this.ws = ws;
        // v4 握手
        this.send({
          type: "req",
          id: `hs-${++this.reqId}`,
          method: "connect",
          params: {
            minProtocol: 4,
            maxProtocol: 4,
            client: { id: "gateway-client", version: "1.0.0", platform: "darwin", mode: "backend" },
            role: "operator",
            scopes: ["operator.admin"],
            auth: { token: CFG.gatewayToken },
          },
        });
        const hsId = `hs-${this.reqId}`;
        this.pending.set(hsId, {
          resolve: () => {
            CFG.log("INFO", "Gateway 已连接（v4 握手通过）");
            resolve();
          },
          reject,
          timer: setTimeout(() => reject(new Error("gateway handshake timeout")), 8000),
        });
      };
      ws.onmessage = (ev) => this.handleFrame(String(ev.data));
      ws.onerror = () => {};
      ws.onclose = () => {
        clearTimeout(timer);
        this.ws = null;
        for (const [, p] of this.pending) {
          clearTimeout(p.timer);
          p.reject(new Error("gateway connection closed"));
        }
        this.pending.clear();
      };
    });
    try {
      await this.connecting;
    } finally {
      this.connecting = null;
    }
  }

  send(obj) {
    this.ws.send(JSON.stringify(obj));
  }

  handleFrame(raw) {
    let f;
    try {
      f = JSON.parse(raw);
    } catch {
      return;
    }
    if (f.type === "res" && f.id) {
      const p = this.pending.get(f.id);
      if (p) {
        this.pending.delete(f.id);
        clearTimeout(p.timer);
        if (f.ok) p.resolve(f.payload);
        else p.reject(new Error(f.error?.message || "rpc error"));
      }
      return;
    }
    // 事件帧分发给等待者 + 每帧监听器
    for (let i = this.eventWaiters.length - 1; i >= 0; i--) {
      const w = this.eventWaiters[i];
      if (w.predicate(f)) {
        this.eventWaiters.splice(i, 1);
        clearTimeout(w.timer);
        w.resolve(f);
        break;
      }
    }
    for (const listener of this.frameListeners) {
      try {
        listener(f);
      } catch {}
    }
  }

  async request(method, params, timeoutMs = 15000) {
    await this.ensureConnected();
    const id = `r-${++this.reqId}`;
    const p = new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        this.pending.delete(id);
        reject(new Error(`rpc ${method} timeout`));
      }, timeoutMs);
      this.pending.set(id, { resolve, reject, timer });
    });
    this.send({ type: "req", id, method, params });
    return p;
  }

  /** 等待满足谓词的 agent 事件帧 */
  waitForEvent(predicate, timeoutMs) {
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error("event wait timeout")), timeoutMs);
      this.eventWaiters.push({ predicate, resolve, timer });
    });
  }

  /** 发消息并等待本轮结束，累积 assistant 增量 */
  async chat(agentKey, message, timeoutMs) {
    await this.ensureConnected();
    const created = await this.request("sessions.create", { key: agentKey }, 15000);
    CFG.log("INFO", "session ready", { key: agentKey, existed: created?.session?.existed ?? "?" });
    const sendId = `s-${++this.reqId}`;
    const sendPromise = new Promise((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error("send timeout")), timeoutMs);
      this.pending.set(sendId, { resolve, reject, timer });
    });
    this.send({
      type: "req",
      id: sendId,
      method: "sessions.send",
      params: { key: agentKey, message },
    });
    const started = await sendPromise;
    const runId = started?.runId;
    let text = "";
    // 收集 assistant 增量（直接挂到 handleFrame 分发，不用 waitForEvent 队列）
    const collector = (f) => {
      if (f.type !== "event" || f.event !== "agent") return;
      const p = f.payload || {};
      if (runId && p.runId !== runId) return;
      if (p.stream !== "assistant") return;
      const d = p.data || {};
      if (typeof d.delta === "string" && d.delta) text += d.delta;
      else if (!("delta" in d) && typeof d.text === "string") text = d.text;
    };
    this.frameListeners.add(collector);
    try {
      // 等本轮结束（lifecycle stopReason）
      await this.waitForEvent((f) => {
        if (f.type !== "event" || f.event !== "agent") return false;
        const p = f.payload || {};
        if (runId && p.runId !== runId) return false;
        return p.stream === "lifecycle" && (p.data?.stopReason || p.data?.phase === "done");
      }, timeoutMs);
    } finally {
      this.frameListeners.delete(collector);
    }
    return text.trim();
  }
}

const gw = new GatewayRPC();

// ---------- Chatwoot API ----------
async function sendChatwootReply(conversationId, content) {
  const url = `${CFG.chatwootBase}/api/v1/accounts/${CFG.accountId}/conversations/${conversationId}/messages`;
  const res = await fetch(url, {
    method: "POST",
    headers: {
      "api-access-token": CFG.botToken,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ content, message_type: "outgoing", private: false }),
  });
  if (!res.ok) throw new Error(`chatwoot send ${res.status}: ${(await res.text()).slice(0, 120)}`);
  return res.json();
}

// ---------- 处理一条 incoming ----------
async function handleMessage(payload) {
  const msgId = payload.id;
  const convId = payload.conversation?.id;
  const inboxId = payload.inbox?.id;
  const content = String(payload.content || "").trim();
  if (!msgId || !convId || !content) return { ignored: true, reason: "missing fields" };
  if (inboxId !== CFG.inboxId) return { ignored: true, reason: `inbox ${inboxId} != ${CFG.inboxId}` };

  if (seenMessages.has(msgId)) return { duplicate: true };
  seenMessages.set(msgId, Date.now());

  const sessionKey = `agent:${CFG.agentId}:cw:1:${inboxId}:${convId}`;
  CFG.log("INFO", "处理消息", { msgId, convId, sessionKey });

  return enqueueByConversation(convId, async () => {
    let reply = "";
    let degraded = false;
    const t0 = Date.now();
    try {
      reply = await gw.chat(sessionKey, content, CFG.agentTimeoutMs);
    } catch (err) {
      degraded = true;
      CFG.log("WARN", "OpenClaw 失败，走兜底文案", { msgId, err: String(err) });
      reply = "抱歉，我这边暂时无法回答，已记录您的问题，人工稍后会跟进。";
    }
    const latency = Date.now() - t0;
    await sendChatwootReply(convId, reply);
    CFG.log("INFO", "已回复", { msgId, latency, degraded, chars: reply.length });
    return { ok: true, latency, degraded };
  });
}

// ---------- HTTP 服务 ----------
const server = http.createServer(async (req, res) => {
  if (req.method === "GET" && req.url === "/healthz") {
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ ok: true, agent: CFG.agentId, inbox: CFG.inboxId }));
    return;
  }
  if (req.method === "POST" && req.url === "/webhook/chatwoot") {
    let raw = "";
    req.on("data", (c) => (raw += c));
    req.on("end", async () => {
      if (!verifySignature(raw, req.headers["x-chatwoot-signature"], req.headers["x-chatwoot-timestamp"])) {
        res.writeHead(401).end(JSON.stringify({ error: "invalid signature" }));
        return;
      }
      let payload;
      try {
        payload = JSON.parse(raw);
      } catch {
        res.writeHead(400).end(JSON.stringify({ error: "invalid json" }));
        return;
      }
      if (payload.event !== "message_created" || payload.message_type !== "incoming" || payload.private) {
        res.writeHead(200).end(JSON.stringify({ ignored: true }));
        return;
      }
      res.writeHead(200).end(JSON.stringify({ accepted: true }));
      // 异步处理（Chatwoot 需要快速 200）
      try {
        await handleMessage(payload);
      } catch (err) {
        CFG.log("ERROR", "handleMessage 异常", { err: String(err) });
      }
    });
    return;
  }
  res.writeHead(404).end();
});

server.listen(CFG.port, () => {
  CFG.log("INFO", `Chatwoot↔OpenClaw 直连桥启动：${CFG.port} → agent=${CFG.agentId} inbox=${CFG.inboxId}`);
});
