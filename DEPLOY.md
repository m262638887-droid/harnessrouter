# HR + Cline Proxy 部署说明

打包时间: 2026-07-31 10:19:47 UTC
内容: HarnessRouter OpenAI 兼容代理 + Cline free 模型请求头代理  
**本包不含任何真实 API Key / 日志 / 状态缓存**

---

## 1. 目录结构

```
hr-cline-proxies/
  requirements.txt
  DEPLOY.md                 # 本文件
  hr-proxy/
    hr_openai_proxy.py      # HR 多 key 轮询 + harness 管理
    boot.sh / start.sh      # 启动脚本
    keys.txt                # 空；填入 sk-hr-...（一行一个）
    keys.txt.example
    harness_state.json      # 空；运行时自动写入 harness id
  cline-proxy/
    proxy.py                # 注入 Cline product-surface 头，透传 Authorization
    boot.sh
```

---

## 2. 环境要求

- Linux x86_64/arm64
- Python 3.9+
- 能访问外网（或自备 HTTP 出站代理 / Cloudflare Worker 转发）
- 依赖: `pip install -r requirements.txt`

```bash
python3 -m venv .venv && source .venv/bin/activate   # 可选
pip install -r requirements.txt
```

---

## 3. HarnessRouter 代理（hr-proxy）

### 作用
- 对 new-api / 客户端暴露 **OpenAI Chat Completions** 接口
- 内部用多个 `sk-hr-...` 轮询，自动创建/复用 codex、claude-code、hermes harness
- 默认 **假流**（收齐再吐），`HR_TRUE_STREAM=1` 可真流式
- 长上下文可自动把 claude-code 切到 hermes

### 配置密钥
编辑 `hr-proxy/keys.txt`（一行一个，不要引号、不要逗号）:

```
sk-hr-xxxxxxxx
sk-hr-yyyyyyyy
```

### 启动

```bash
cd hr-proxy
export PROXY_TOKEN='sk-hr-proxy-change-me'   # 客户端访问本代理的 Bearer，务必改掉
export PROXY_PORT=18790
# 若机器不能直连 api.harnessrouter.ai，设置出站前缀（worker 需支持 ?url=）:
# export HR_OUTBOUND='https://your-worker.example/?url='
bash boot.sh
curl -s http://127.0.0.1:18790/health
```

### 客户端 / new-api 渠道

| 项 | 值 |
|----|-----|
| Base URL | `http://127.0.0.1:18790` 或 `http://127.0.0.1:18790/v1` |
| API Key | 与 `PROXY_TOKEN` 相同（不是 sk-hr 上游 key） |
| 模型示例 | `hr/codex/gpt-5.4`、`hr/claude-code/claude-sonnet-4`、`hr/hermes/kimi-k3` 及短别名 |

上游 `sk-hr-...` **只写在 keys.txt**，由代理轮询；不要填进 new-api 渠道密钥（除非你改架构）。

### 常用环境变量

| 变量 | 默认 | 说明 |
|------|------|------|
| PROXY_HOST | 0.0.0.0 | 监听地址 |
| PROXY_PORT | 18790 | 监听端口 |
| PROXY_TOKEN | sk-hr-proxy-change-me | 客户端鉴权 |
| HR_API_BASE | https://api.harnessrouter.ai | HR API |
| HR_OUTBOUND | 空 | 出站前缀，如 `https://worker/?url=` |
| HR_KEYS_FILE | ./keys.txt | 密钥文件 |
| HR_STATE_FILE | ./harness_state.json | harness 缓存 |
| HR_THIN_MODE | 1 | 精简 system |
| HR_TRUE_STREAM | 0 | 1=真流式（无空回复保护） |

### 健康检查

```bash
curl -s http://127.0.0.1:18790/health
# keys_total / keys_disabled / keys_harness_ready
```

### 冒烟

```bash
curl -s http://127.0.0.1:18790/v1/chat/completions \
  -H "Authorization: Bearer $PROXY_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"model":"hr/codex/gpt-5.4","messages":[{"role":"user","content":"Reply with exactly: OK"}],"max_tokens":32}'
```

---

## 4. Cline 代理（cline-proxy）

### 作用
- 透传渠道里的 `Authorization: Bearer sk_...`（支持 new-api 多密钥轮询）
- 自动注入 Cline 官方 product-surface 头，解锁 `cline-free/*`  
  关键头: `X-CLIENT-TYPE: cline-cli`（或 `VSCode Extension`）
- 可选解开 `{"data":{...},"success":true}` 包装，方便 OpenAI 客户端

### 启动

```bash
cd cline-proxy
export CLINE_PROXY_PORT=3015
# 不能直连时:
# export CLINE_UPSTREAM='https://your-worker.example/?url=https://api.cline.bot/api/v1'
export CLINE_UPSTREAM='https://api.cline.bot/api/v1'
bash boot.sh
```

### new-api 渠道建议

| 项 | 值 |
|----|-----|
| Base URL | `http://127.0.0.1:3015` （或带 `/v1`） |
| Key | 渠道内填多个 `sk_...`，换行分隔，走官方轮询 |
| 模型 | `cline-free/glm-5.2` 或映射 `zai/glm-5.2` → `cline-free/glm-5.2` |
| header_override | **不用填**（代理已注入） |

### 冒烟

```bash
curl -s http://127.0.0.1:3015/v1/chat/completions \
  -H "Authorization: Bearer sk_YOUR_CLINE_KEY" \
  -H "Content-Type: application/json" \
  -d '{"model":"cline-free/glm-5.2","messages":[{"role":"user","content":"Reply with exactly: OK"}],"max_tokens":32}'
```

GLM 有时正文在 `reasoning_content`：new-api 可开 `thinking_to_content`。

---

## 5. 推荐部署顺序

1. `pip install -r requirements.txt`
2. 填写 `hr-proxy/keys.txt`，改 `PROXY_TOKEN`，`bash hr-proxy/boot.sh`
3. `curl health` + 冒烟 chat
4. `bash cline-proxy/boot.sh`，用真实 cline key 冒烟
5. new-api 增加两个渠道指向上述本地端口
6. （可选）systemd / tmux / supervisord 保活

### systemd 示例（hr-proxy）

```ini
[Unit]
Description=HR OpenAI Proxy
After=network.target

[Service]
WorkingDirectory=/opt/hr-cline-proxies/hr-proxy
Environment=PROXY_TOKEN=sk-hr-proxy-change-me
Environment=PROXY_PORT=18790
ExecStart=/usr/bin/python3 -u hr_openai_proxy.py
Restart=always
RestartSec=3

[Install]
WantedBy=multi-user.target
```

---

## 6. 出站访问说明

部分机器（云沙箱等）无法直连 `api.harnessrouter.ai` / `api.cline.bot`，需要:

- 自建 Worker: `https://<worker>/?url=<target>`  
  然后 HR: `HR_OUTBOUND=https://<worker>/?url=`  
  Cline: `CLINE_UPSTREAM=https://<worker>/?url=https://api.cline.bot/api/v1`
- 或系统级 HTTP/SOCKS 代理（需自行改代码或设 `ALL_PROXY`，视环境而定）

本包默认按**可直连**编写；有 worker 时用环境变量覆盖即可。

---

## 7. 运维

```bash
# 日志
tail -f hr-proxy/proxy.log
tail -f cline-proxy/proxy.log

# 停
pkill -f hr_openai_proxy.py
pkill -f cline-proxy/proxy.py

# 换 HR 密钥后
# 1) 编辑 keys.txt  2) 如 key 全换可清空 harness_state.json 为 {}
# 3) bash boot.sh
```

HR 返回 `missing or invalid API key` / `run auth 401` → 上游 key 作废，与代理无关。  
Cline 返回 product surfaces 403 → 确认走的是本 cline-proxy，且未把头覆盖掉。

---

## 8. 安全

- 不要把真实 `keys.txt`、proxy.log、harness_state.json 提交到 git
- 修改默认 `PROXY_TOKEN`
- 监听 `127.0.0.1` 若仅本机 new-api 使用: `PROXY_HOST=127.0.0.1` / `CLINE_PROXY_HOST=127.0.0.1`

---

## 9. 版本备注（打包时行为）

- HR: thin mode 开、真流式默认关、Codex 协议 JSON 清洗、空/短回复重试
- Cline: 注入 X-CLIENT-TYPE 等头；非流式尝试 unwrap `data` 包装
- 端口约定: HR `18790`，Cline `3015`（可改）
