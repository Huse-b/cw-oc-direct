# Chatwoot ↔ OpenClaw 直连桥

客服浮窗消息 → AI（OpenClaw Agent）自动回复。本仓库是两者之间的连接服务（桥），
生产运行于 Mac mini B (192.168.0.206)，由 launchd `com.blueaisoft.cw-oc-bridge` 常驻守护。

## 系统如何工作

```
客户在品牌官网浮窗发消息
      │
      ▼
Chatwoot（客服系统，chat.blueaisoft.com，云端/205）
      │ AgentBot 机制：按收件箱绑定的 bot，把消息 POST 到
      │ http://192.168.0.206:8300/webhook/chatwoot（HMAC 签名防伪造）
      ▼
bridge-v11.mjs（本仓库，8300 端口）
      │ ① 验签（防伪造请求）
      │ ② 按收件箱查路由表：该品牌 → 哪个 AI Agent + 用哪个身份回复
      │ ③ 去重 / 会话排队（同一客户的消息严格按顺序）
      │ ④ WebSocket 调 OpenClaw 对应 Agent 生成回复（30s 超时）
      │ ⑤ 超时/失败 → 发兜底文案并转人工；AI 正常 → 发 AI 回复
      ▼
客户在浮窗看到回复（显示为该品牌的机器人身份）
```

## 目录结构

```
bridge-v11.mjs     桥本体（Node 24 原生，零第三方依赖）
routes.json        路由配置【含密钥，不入库；样例见 routes.example.json】
data/              运行数据（去重水位、统计）
logs/              日志
docs/              排错手册
frpc/              frp 内网穿透客户端（备用，当前未启用）
```

## 当前接入的品牌

| 收件箱 ID | 品牌 | AI Agent | 回复身份 |
|---|---|---|---|
| 7 | BlueWMS | bluewms-support | 客服Agent |
| 8 | 卓风 | zhuofeng-support | 卓风Agent |

## 加新品牌（3 步，约 5 分钟）

1. **Chatwoot 后台**：建 bot（outgoing_url 填 `http://192.168.0.206:8300/webhook/chatwoot`）、
   建收件箱并绑定该 bot
2. **改 routes.json**（等 60 秒自动热重载，无需重启）：
   ```json
   "secrets": [... 加新 bot 的 secret ...],
   "inboxMap": { "<新收件箱ID>": { "agent": "<新Agent ID>", "botToken": "<新 bot token>" } }
   ```
3. **OpenClaw**：按现有模板建对应 Agent（独立 workspace + AGENTS.md 提示词）

## 部署 / 运维

```bash
# 服务管理（launchd 常驻，崩溃自动拉起）
launchctl list | grep cw-oc-bridge
launchctl kickstart -k gui/501/com.blueaisoft.cw-oc-bridge   # 重启

# 健康与观测
curl localhost:8300/healthz    # 健康（路由/Gateway 连接/队列）
curl localhost:8300/stats      # 统计（仅本机可访问）

# 日志
tail -f logs/bridge.log
```

**加新品牌后忘记重启也没关系**——路由表 60 秒自动热重载。

## 排错

遇到问题先看 `docs/排错手册.md`——按症状分类的完整排查指南，
覆盖：浮窗无回复 / 收到兜底文案 / 双回复 / 验签失败 / 进程反复崩溃等全部已知场景。

## 快速排错速查

| 症状 | 最可能原因 | 一句话处理 |
|---|---|---|
| 某会话不回复，其他正常 | 会话状态是 open（bot 只处理 pending） | Chatwoot 后台把会话切回 pending |
| 客户收到"抱歉，暂时无法回答" | AI 超时/失败走了兜底 | 查 Gateway：`launchctl kickstart -k gui/501/ai.openclaw.gateway` |
| 日志大量 `验签失败` | bot secret 改了但 routes.json 没同步 | 更新 secrets 数组 |
| 客户收到两条回复 | 有另一个机器人并存 | 查 Chatwoot 后台 Bots 的 outgoing_url，清除多余 |
