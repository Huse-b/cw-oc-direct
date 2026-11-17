# Chatwoot ↔ OpenClaw 直连桥

生产运行：本目录（launchd `com.blueaisoft.cw-oc-bridge`，端口 8300）

## 快速了解
- `bridge-v11.mjs` — 桥本体（Node 24 原生零依赖）
- `routes.json` — 路由配置【含密钥，不入库】（样例见仓库 routes.example.json）
- `docs/` — 架构设计与运维手册

## 运维速查
| 操作 | 命令 |
|---|---|
| 健康检查 | `curl localhost:8300/healthz` |
| 仪表盘（本机） | `curl localhost:8300/stats` |
| 重启 | `launchctl kickstart -k gui/501/com.blueaisoft.cw-oc-bridge` |
| 路由热重载 | `kill -HUP $(pgrep -f bridge-v11)` 或改 routes.json 等 60s |
| 回滚 FastAPI | 旧仓库 /Users/mac6/projects/chatwoot-legacy |

## 加新品牌（3 分钟）
1. Chatwoot 建 bot + inbox 绑定
2. routes.json 的 inboxMap/secrets 加条目
3. 等 60s 热重载（或 SIGHUP）
