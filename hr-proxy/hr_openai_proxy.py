#!/usr/bin/env python3
"""HarnessRouter multi-key OpenAI-compatible proxy with round-robin."""
from __future__ import annotations

import json
import os
import re
import threading
import time
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

API_BASE = os.environ.get("HR_API_BASE", "https://api.harnessrouter.ai").rstrip("/")
OUTBOUND = os.environ.get("HR_OUTBOUND", "https://worker.djsksmdskkz.workers.dev").rstrip("?&/")
HOST = os.environ.get("PROXY_HOST", "0.0.0.0")
PORT = int(os.environ.get("PROXY_PORT", "18790"))
PROXY_TOKEN = os.environ.get("PROXY_TOKEN", "sk-hr-proxy-change-me").strip().strip()
KEYS_FILE = os.environ.get("HR_KEYS_FILE", "/root/hr-proxy/keys.txt")
STATE_FILE = os.environ.get("HR_STATE_FILE", "/root/hr-proxy/harness_state.json")
BASES = ("codex", "claude-code", "hermes")
DEFAULT_MODELS = {
    "codex": "gpt-5.4",
    "claude-code": "claude-sonnet-4.6",
    "hermes": "gpt-5.4",
}
SYS_PROMPT = "You are a helpful assistant."
CHAT_PREAMBLE = ""
THIN_MODE = os.environ.get("HR_THIN_MODE", "1") != "0"
TRUE_STREAM = os.environ.get("HR_TRUE_STREAM", "0") == "1"

lock = threading.Lock()
rr_idx = 0
# key -> {"codex": id, "claude-code": id, "hermes": id, "disabled": bool, "err": str}
state: dict[str, dict[str, Any]] = {}
keys: list[str] = []
models_cache: dict[str, Any] = {"ts": 0, "data": None}
MODELS_TTL = 180


def upstream(path: str) -> str:
    full = f"{API_BASE}{path}"
    if OUTBOUND:
        return f"{OUTBOUND}?url={full}"
    return full


def load_keys() -> list[str]:
    p = Path(KEYS_FILE)
    if not p.exists():
        return []
    out = []
    seen = set()
    for line in p.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        m = re.search(r"sk-hr-[a-f0-9]{64}", line)
        if not m:
            continue
        k = m.group(0)
        if k not in seen:
            seen.add(k)
            out.append(k)
    return out


def load_state():
    global state
    p = Path(STATE_FILE)
    if p.exists():
        try:
            state = json.loads(p.read_text())
        except Exception:
            state = {}
    else:
        state = {}


def save_state():
    Path(STATE_FILE).parent.mkdir(parents=True, exist_ok=True)
    tmp = STATE_FILE + ".tmp"
    with open(tmp, "w") as f:
        json.dump(state, f)
    os.replace(tmp, STATE_FILE)


def hr_headers(key: str, stream: bool = False, extra: dict | None = None) -> dict:
    h = {
        "Authorization": f"Bearer {key}",
        "Accept": "text/event-stream" if stream else "application/json",
        "Accept-Encoding": "identity",
    }
    if extra:
        h.update(extra)
    return h


def ensure_harnesses(key: str) -> dict[str, str]:
    """Return base->harness_id for key, creating if needed."""
    with lock:
        st = state.get(key) or {}
        if st.get("disabled"):
            raise RuntimeError(st.get("err") or "key disabled")
        if all(st.get(b) for b in BASES):
            return {b: st[b] for b in BASES}

    # fetch existing
    try:
        r = requests.get(upstream("/v1/harnesses"), headers=hr_headers(key), timeout=60, verify=False)
    except Exception as e:
        raise RuntimeError(f"list harnesses failed: {e}")
    if r.status_code in (401, 403):
        with lock:
            state[key] = {"disabled": True, "err": f"auth {r.status_code}"}
            save_state()
        raise RuntimeError(f"key rejected {r.status_code}")
    if r.status_code >= 400:
        raise RuntimeError(f"list harnesses HTTP {r.status_code}: {r.text[:300]}")

    existing = {}
    existing_meta = {}
    for h in (r.json() or {}).get("harnesses") or []:
        base = h.get("base") or ""
        hid = h.get("id") or ""
        name = (h.get("name") or "").lower()
        if base in BASES and hid:
            # prefer our proxy-named ones
            if base not in existing or "proxy" in name:
                existing[base] = hid
                existing_meta[base] = h

    created = dict(existing)
    # keep harness system prompt minimal
    for base, hid in list(created.items()):
        meta = existing_meta.get(base) or {}
        sp = (meta.get("systemPrompt") or meta.get("system_prompt") or "").strip()
        name = meta.get("name") or f"Proxy {base}"
        if sp == SYS_PROMPT:
            continue
        try:
            body = {"name": name, "system_prompt": SYS_PROMPT, "maxStep": 1}
            ur = requests.put(
                upstream(f"/v1/harnesses/{hid}"),
                headers=hr_headers(key, extra={"Content-Type": "application/json"}),
                json=body,
                timeout=60,
                verify=False,
            )
            print(f"[harness-upd] ...{key[-8:]} {base} {ur.status_code}", flush=True)
        except Exception as e:
            print(f"[harness-upd] ...{key[-8:]} {base} err {e}", flush=True)
    for base in BASES:
        if created.get(base):
            continue
        body = {
            "name": f"Proxy {base}",
            "base": base,
            "default_model": DEFAULT_MODELS[base],
            "system_prompt": SYS_PROMPT,
            "mcp_servers": [],
            "skills": [],
            "maxStep": 1,
        }
        cr = requests.post(
            upstream("/v1/harnesses"),
            headers=hr_headers(key, extra={"Content-Type": "application/json"}),
            json=body,
            timeout=60,
            verify=False,
        )
        if cr.status_code >= 400:
            raise RuntimeError(f"create {base} failed {cr.status_code}: {cr.text[:300]}")
        hid = (cr.json() or {}).get("id")
        if not hid:
            raise RuntimeError(f"create {base} no id")
        created[base] = hid
        print(f"[harness] key=...{key[-8:]} base={base} id={hid}", flush=True)

    with lock:
        st = state.get(key) or {}
        st.update(created)
        st["disabled"] = False
        st.pop("err", None)
        state[key] = st
        save_state()
    return {b: created[b] for b in BASES}


def pick_key() -> str:
    global rr_idx
    with lock:
        alive = [k for k in keys if not (state.get(k) or {}).get("disabled")]
        if not alive:
            # try all keys again (maybe transient)
            alive = list(keys)
        if not alive:
            raise RuntimeError("no keys configured")
        k = alive[rr_idx % len(alive)]
        rr_idx += 1
        return k


def mark_bad(key: str, reason: str):
    with lock:
        st = state.get(key) or {}
        st["disabled"] = True
        st["err"] = reason
        state[key] = st
        save_state()
    print(f"[disable] ...{key[-8:]} {reason}", flush=True)


def load_models_any() -> dict[str, Any]:
    now = time.time()
    if models_cache["data"] and now - models_cache["ts"] < MODELS_TTL:
        return models_cache["data"]
    last_err = None
    for _ in range(min(5, max(1, len(keys)))):
        key = pick_key()
        try:
            r = requests.get(upstream("/v1/models"), headers=hr_headers(key), timeout=60, verify=False)
            if r.status_code in (401, 403):
                mark_bad(key, f"models auth {r.status_code}")
                continue
            r.raise_for_status()
            data = r.json()
            models_cache["data"] = data
            models_cache["ts"] = now
            return data
        except Exception as e:
            last_err = e
            continue
    raise RuntimeError(f"models fetch failed: {last_err}")


def openai_model_list() -> list[dict]:
    data = load_models_any()
    out = []
    seen = set()
    for alias in BASES:
        mid = f"hr/{alias}"
        out.append({"id": mid, "object": "model", "created": int(time.time()), "owned_by": "harnessrouter"})
        seen.add(mid)
    backends = (data or {}).get("backends") or {}
    bmap = {"codex": "codex", "claude": "claude-code", "hermes": "hermes"}
    for bkey, binfo in backends.items():
        base = bmap.get(bkey, bkey)
        for m in binfo.get("models") or []:
            if not m.get("available", True):
                continue
            mid = f"hr/{base}/{m['id']}"
            if mid in seen:
                continue
            seen.add(mid)
            out.append({"id": mid, "object": "model", "created": int(time.time()), "owned_by": f"harnessrouter/{base}"})
    return out


def parse_model(name: str) -> tuple[str, str | None]:
    n = (name or "").strip()
    if n.startswith("hr/"):
        n = n[3:]
    for p in ("harnessrouter/", "hr-"):
        if n.startswith(p):
            n = n[len(p) :]
    if "/" in n:
        base, model = n.split("/", 1)
        base = base.strip().lower()
        if base == "claude":
            base = "claude-code"
        return base, model.strip() or None
    base = n.lower()
    if base in ("cc", "claude", "claude-code"):
        return "claude-code", None
    if base in ("coex", "codex", "gpt"):
        return "codex", None
    if base == "hermes":
        return "hermes", None
    try:
        data = load_models_any()
        for bkey, binfo in (data.get("backends") or {}).items():
            for m in binfo.get("models") or []:
                if m.get("id") == n:
                    bmap = {"codex": "codex", "claude": "claude-code", "hermes": "hermes"}
                    return bmap.get(bkey, bkey), n
    except Exception:
        pass
    return "hermes", n


# HR Claude Code spawns `claude` with prompt in argv; huge tavern histories hit E2BIG.
# Char budgets ~ rough tokens*2-3 for CJK. Tavern often 30k-150k tokens.
# Claude-code puts prompt in argv (ARG_MAX ~2MB); start high and shrink on E2BIG.
MAX_INPUT_CHARS = int(os.environ.get("HR_MAX_INPUT_CHARS", "800000"))
MAX_INPUT_CHARS_CLAUDE = int(os.environ.get("HR_MAX_INPUT_CHARS_CLAUDE", "220000"))
MAX_INPUT_CHARS_HERMES = int(os.environ.get("HR_MAX_INPUT_CHARS_HERMES", "800000"))
MAX_SYSTEM_CHARS = int(os.environ.get("HR_MAX_SYSTEM_CHARS", "220000"))
MAX_MSG_CHARS = int(os.environ.get("HR_MAX_MSG_CHARS", "100000"))
MAX_ASSISTANT_HIST_CHARS = int(os.environ.get("HR_MAX_ASSISTANT_HIST_CHARS", "30000"))
DEFAULT_MAX_OUTPUT = int(os.environ.get("HR_DEFAULT_MAX_OUTPUT", "8192"))
# auto route long tavern packs away from claude argv limit
LONG_CTX_CHARS = int(os.environ.get("HR_LONG_CTX_CHARS", "100000"))
CLAUDE_E2BIG_FALLBACK = os.environ.get("HR_CLAUDE_E2BIG_FALLBACK", "1") != "0"
HEARTBEAT_SECS = float(os.environ.get("HR_HEARTBEAT_SECS", "8"))


def input_limit_for_base(base: str) -> int:
    if base in ("claude-code", "claude"):
        return MAX_INPUT_CHARS_CLAUDE
    if base == "hermes":
        return MAX_INPUT_CHARS_HERMES
    return MAX_INPUT_CHARS


def _content_to_text(content) -> str:
    if content is None:
        return ""
    if isinstance(content, list):
        texts = []
        for c in content:
            if isinstance(c, dict) and c.get("type") in ("text", "input_text"):
                texts.append(c.get("text") or "")
            elif isinstance(c, str):
                texts.append(c)
            elif isinstance(c, dict) and c.get("type") == "image_url":
                texts.append("[image]")
        return "\n".join(texts)
    return str(content)


def _clip(s: str, n: int) -> str:
    if n <= 0 or len(s) <= n:
        return s
    if n <= 32:
        return s[:n]
    # prefer keeping the end (latest instructions) for chat turns;
    # for system/cards callers pass head-biased clip via _clip_head.
    head = max(64, n // 5)
    tail = n - head - 24
    if tail < 64:
        return s[: n - 16] + "\n...[truncated]..."
    return s[:head] + "\n...[truncated]...\n" + s[-tail:]


def _clip_head(s: str, n: int) -> str:
    """Keep beginning (character cards / system) then a little tail."""
    if n <= 0 or len(s) <= n:
        return s
    if n <= 32:
        return s[:n]
    head = int(n * 0.85)
    tail = n - head - 24
    if tail < 32:
        return s[: n - 16] + "\n...[truncated]..."
    return s[:head] + "\n...[truncated]...\n" + s[-tail:]


def looks_like_rp(messages: list[dict]) -> bool:
    blob = []
    for m in (messages or [])[:6]:
        blob.append(_content_to_text(m.get("content"))[:2000])
    for m in (messages or [])[-4:]:
        blob.append(_content_to_text(m.get("content"))[:2000])
    t = "\n".join(blob).lower()
    keys = (
        "roleplay", "role play", "you are ", "character", "scenario", "nsfw",
        "酒馆", "角色卡", "角色扮演", "续写", "剧情", "世界观", "人设",
        "{{user}}", "{{char}}", "<char>", "sillytavern", "tavern",
    )
    return any(k in t for k in keys)


def messages_to_input(messages: list[dict], max_chars: int | None = None, base: str = "hermes") -> str:
    """Pack chat messages for HR agents.

    Strategy for tavern/RP:
    - keep system/character card from the head (large budget)
    - always keep the latest user message as fully as possible
    - keep recent turns; compress older assistant turns more than user turns
    - drop oldest first
    """
    limit = input_limit_for_base(base) if max_chars is None else max_chars
    systems: list[str] = []
    turns: list[tuple[str, str]] = []
    for m in messages or []:
        role = (m.get("role") or "user").lower()
        content = _content_to_text(m.get("content"))
        if role == "system":
            systems.append(content)
        elif role == "assistant":
            turns.append(("[assistant]", content))
        else:
            # user / tool / etc
            turns.append(("[user]", content))

    sys_text = _clip_head("\n\n".join(systems), MAX_SYSTEM_CHARS)
    parts: list[str] = []
    if sys_text.strip():
        parts.append(f"[system]\n{sys_text}")

    # Always reserve room for the latest user turn.
    latest = turns[-1] if turns else None
    earlier = turns[:-1] if turns else []

    def pack_turn(tag: str, content: str, is_latest: bool = False) -> str:
        if is_latest:
            # leave most of remaining budget to latest user
            return f"{tag}\n{content}"
        if tag == "[assistant]":
            content = _clip(content, MAX_ASSISTANT_HIST_CHARS)
        else:
            content = _clip(content, MAX_MSG_CHARS)
        return f"{tag}\n{content}"

    kept_rev: list[str] = []
    used = sum(len(x) + 2 for x in parts)

    latest_block = None
    if latest:
        tag, content = latest
        # cap latest only at remaining-ish hard limit
        latest_cap = min(MAX_MSG_CHARS * 2, max(4000, limit - used - 2000))
        latest_block = f"{tag}\n{_clip(content, latest_cap)}"
        used += len(latest_block) + 2

    for tag, content in reversed(earlier):
        block = pack_turn(tag, content, is_latest=False)
        add = len(block) + 2
        if used + add > limit:
            # try tighter clip once
            room = limit - used - len(tag) - 5
            if room < 80:
                break
            block = f"{tag}\n{_clip(content, room)}"
            add = len(block) + 2
            if used + add > limit:
                break
        kept_rev.append(block)
        used += add

    kept_rev.reverse()
    parts.extend(kept_rev)
    if latest_block:
        # if still over, shrink latest
        if sum(len(x) + 2 for x in parts) + len(latest_block) > limit:
            room = max(500, limit - sum(len(x) + 2 for x in parts) - 10)
            tag = latest[0]
            latest_block = f"{tag}\n{_clip(latest[1], room)}"
        parts.append(latest_block)

    out = "\n\n".join(parts).strip() or "Hello"
    if len(out) > limit:
        out = _clip(out, limit)
    return out


def wrap_input(user_input: str, messages: list[dict] | None = None) -> str:
    """Thin chat proxy: no injected prefixes."""
    return user_input


def shrink_input(text: str, factor: float = 0.5) -> str:
    """Shrink after E2BIG: keep system card head + newest dialogue."""
    n = max(12000, int(len(text) * factor))
    if len(text) <= n:
        return text
    # try split system vs rest
    if text.startswith("[system]") or "\n[system]\n" in text[:20]:
        # find end of first system block (next role tag)
        import re as _re
        m = _re.search(r"\n\n\[(user|assistant)\]\n", text)
        if m:
            sys_part = text[: m.start()]
            rest = text[m.start() :].lstrip("\n")
            sys_budget = min(len(sys_part), max(8000, int(n * 0.45)))
            rest_budget = max(4000, n - sys_budget - 4)
            return _clip_head(sys_part, sys_budget) + "\n\n" + _clip(rest, rest_budget)
    return _clip(text, n)


def is_e2big_error(text: str) -> bool:
    if not text:
        return False
    tl = text.lower()
    return (
        "argument list too long" in tl
        or "errno 7" in tl
        or "e2big" in tl
        or ("harness_error" in tl and "spawn" in tl)
    )


def extract_max_output(body: dict) -> int | None:
    for k in ("max_tokens", "max_output_tokens", "max_completion_tokens"):
        v = body.get(k)
        if v is None:
            continue
        try:
            n = int(v)
            if n > 0:
                return min(n, 128000)
        except Exception:
            pass
    return DEFAULT_MAX_OUTPUT



def format_error_message(err) -> str:
    """Always produce a full, readable error string for the client."""
    if err is None:
        return "upstream failed with unknown error"
    if isinstance(err, str):
        msg = err.strip()
    else:
        try:
            if isinstance(err, dict):
                # common shapes
                for k in ("message", "msg", "error", "detail", "code"):
                    if err.get(k):
                        v = err.get(k)
                        if isinstance(v, (dict, list)):
                            msg = json.dumps(err, ensure_ascii=False)
                        else:
                            msg = str(v)
                            extra = {kk: vv for kk, vv in err.items() if kk != k}
                            if extra:
                                msg = msg + " | " + json.dumps(extra, ensure_ascii=False)
                        break
                else:
                    msg = json.dumps(err, ensure_ascii=False)
            else:
                msg = json.dumps(err, ensure_ascii=False)
        except Exception:
            msg = str(err)
    msg = (msg or "").strip()
    if len(msg) > 8000:
        msg = msg[:8000] + "...(truncated)"
    # user-facing complete notice
    if is_e2big_error(msg):
        return (
            "[渠道提示] 上下文过长，Claude Code 无法启动（Argument list too long）。"
            "代理已自动压缩重试；若仍失败请减少酒馆历史/世界书后重试。\n"
            f"详情: {msg}"
        )
    if "harness_error" in msg.lower() or "spawn" in msg.lower():
        return f"[渠道提示] 上游 Agent 执行失败。\n详情: {msg}"
    return f"[渠道错误] {msg}"


def text_from_response_obj(resp_obj: dict | None) -> str:
    """Extract full assistant text from a completed/failed response object."""
    if not isinstance(resp_obj, dict):
        return ""
    # direct fields
    for k in ("output_text", "text"):
        v = resp_obj.get(k)
        if isinstance(v, str) and v.strip():
            return v
    chunks: list[str] = []
    out = resp_obj.get("output")
    if isinstance(out, list):
        for item in out:
            if not isinstance(item, dict):
                continue
            # message item
            content = item.get("content")
            if isinstance(content, list):
                for part in content:
                    if not isinstance(part, dict):
                        continue
                    if part.get("type") in ("output_text", "text"):
                        tx = part.get("text") or ""
                        if tx:
                            chunks.append(tx)
                    elif isinstance(part.get("text"), str):
                        chunks.append(part["text"])
            # sometimes text on item
            if item.get("type") in ("message", "output_text") and isinstance(item.get("text"), str):
                chunks.append(item["text"])
    # content field
    content = resp_obj.get("content")
    if isinstance(content, list):
        for part in content:
            if isinstance(part, dict) and part.get("type") in ("output_text", "text"):
                tx = part.get("text") or ""
                if tx:
                    chunks.append(tx)
    return "".join(chunks)



def _extract_balanced_json_objects(s: str) -> list[tuple[int, int, dict]]:
    """Find top-level {...} JSON objects in a possibly concatenated stream."""
    out = []
    i = 0
    n = len(s)
    while i < n:
        if s[i] != "{":
            i += 1
            continue
        depth = 0
        in_str = False
        esc = False
        start = i
        j = i
        while j < n:
            ch = s[j]
            if in_str:
                if esc:
                    esc = False
                elif ch == "\\":
                    esc = True
                elif ch == '"':
                    in_str = False
            else:
                if ch == '"':
                    in_str = True
                elif ch == "{":
                    depth += 1
                elif ch == "}":
                    depth -= 1
                    if depth == 0:
                        chunk = s[start : j + 1]
                        try:
                            obj = json.loads(chunk)
                            if isinstance(obj, dict):
                                out.append((start, j + 1, obj))
                        except Exception:
                            pass
                        i = j + 1
                        break
            j += 1
        else:
            break
        if j >= n and depth != 0:
            break
    return out


def sanitize_agent_text(text: str) -> str:
    """Strip Codex/HR protocol JSON (userMessage/agentMessage/tool dumps) to plain reply.

    Codex often streams blobs like:
      {"type":"userMessage",...}FINAL_ANSWER{"type":"agentMessage","text":"FINAL_ANSWER",...}
    Clients see this as long '乱码'. Keep only human-facing assistant text.
    """
    if not text:
        return text
    s = text.strip()
    if not s:
        return s
    # Fast path: no protocol markers
    if '"type"' not in s or not any(
        x in s
        for x in (
            "userMessage",
            "agentMessage",
            "toolCall",
            "toolResult",
            "function_call",
            "reasoning",
            "\"phase\"",
        )
    ):
        # still collapse hermes execute_code JSON wrappers if whole body is one object
        if s.startswith("{") and s.endswith("}") and len(s) < 20000:
            try:
                o = json.loads(s)
                if isinstance(o, dict):
                    for k in ("text", "message", "content", "output", "result"):
                        v = o.get(k)
                        if isinstance(v, str) and v.strip() and "type" not in o:
                            return v.strip()
                    # hermes style {"code": "..."} during tool use — prefer not dump raw
                    if set(o.keys()) <= {"code", "stdout", "stderr", "output"} and isinstance(o.get("code"), str):
                        # if looks like dumped tool payload with embedded source, try extract source only
                        code = o.get("code") or ""
                        if "def " in code or "print(" in code or "import " in code:
                            # may be triple-quoted assignment dump; leave as code block body if pure
                            return code if len(code) < 4000 else s
            except Exception:
                pass
        return s

    objs = _extract_balanced_json_objects(s)
    if not objs:
        return s

    agent_texts: list[str] = []
    final_texts: list[str] = []
    covered = [False] * len(s)
    for a, b, o in objs:
        for k in range(a, min(b, len(covered))):
            covered[k] = True
        typ = (o.get("type") or o.get("role") or "").lower()
        phase = (o.get("phase") or "").lower()
        # agent final message
        tx = o.get("text")
        if not isinstance(tx, str):
            content = o.get("content")
            if isinstance(content, str):
                tx = content
            elif isinstance(content, list):
                parts = []
                for p in content:
                    if isinstance(p, dict) and isinstance(p.get("text"), str):
                        parts.append(p["text"])
                    elif isinstance(p, str):
                        parts.append(p)
                tx = "".join(parts)
        if isinstance(tx, str):
            tx = tx.strip()
        else:
            tx = ""
        if typ in ("agentmessage", "assistant", "message", "output_text") and tx:
            agent_texts.append(tx)
            if phase in ("final_answer", "final", "answer", "complete", "completed") or o.get("final") is True:
                final_texts.append(tx)
        # skip userMessage / tool dumps intentionally

    # residual plain text between JSON objects
    residual_parts = []
    buf = []
    for i, ch in enumerate(s):
        if covered[i]:
            if buf:
                residual_parts.append("".join(buf))
                buf = []
        else:
            buf.append(ch)
    if buf:
        residual_parts.append("".join(buf))
    residual = "".join(residual_parts).strip()
    # residual often duplicates final answer
    if final_texts:
        out = final_texts[-1].strip()
        return out or residual or s
    if agent_texts:
        # last agent message usually the answer
        out = agent_texts[-1].strip()
        # if residual is short and equal-ish, prefer agent
        if residual and residual not in out and out not in residual and len(residual) > len(out):
            # prefer longer plain residual only if agent text is tiny protocol echo
            if len(out) <= 3 and len(residual) > 10:
                return residual
        return out or residual or s
    if residual:
        return residual
    return s


def is_bad_short_reply(text: str, user_wants_short: bool = False) -> bool:
    """True if reply is empty/error fragment/unusable — not merely brief."""
    if text is None:
        return True
    s = text.strip()
    if not s:
        return True
    # already a full user-facing notice — do not retry-loop it away
    if s.startswith("[渠道") or s.startswith("[error]") or s.startswith("[proxy error]"):
        return False
    low = s.lower()
    if is_e2big_error(s):
        return True
    if "harness_error" in low and len(s) < 200:
        return True
    # error crumbs (few chars of failed/errno/etc.)
    if len(s) <= 48 and any(x in low for x in ("error", "failed", "errno", "spawn", "traceback", "exception", "e2big", "argument list")):
        return True
    if user_wants_short:
        return False
    # empty-ish only: true zero-content leftovers like "." or "…"
    if len(s) <= 2 and s in {".", "…", "...", "?", "!", "。", "？", "！"}:
        return True
    return False


def is_empty_reply(text: str) -> bool:
    return not (text or "").strip()


def user_wants_short_reply(messages: list | None, body: dict | None = None) -> bool:
    max_out = None
    if body:
        for k in ("max_tokens", "max_output_tokens", "max_completion_tokens"):
            if body.get(k) is not None:
                try:
                    max_out = int(body.get(k))
                except Exception:
                    pass
                break
    if max_out is not None and max_out <= 64:
        return True
    blob = ""
    for m in (messages or [])[-3:]:
        if (m.get("role") or "").lower() == "user":
            blob += _content_to_text(m.get("content"))[:500]
    b = blob.lower()
    keys = ("只回复", "仅回复", "只要", "one word", "say ok", "reply ok", "yes/no", "y/n", "只要一个", "简短回复", "两个字")
    return any(k in b for k in keys)


def text_from_event(ev: dict) -> str:
    t = ev.get("type") or ""
    if t == "response.output_text.delta":
        return ev.get("delta") or ""
    if t.endswith(".delta") and isinstance(ev.get("delta"), str):
        if "reasoning" in t:
            return ""
        return ev.get("delta") or ""
    return ""


def iter_sse_json(resp: requests.Response):
    buf = ""
    for chunk in resp.iter_content(chunk_size=2048, decode_unicode=True):
        if chunk is None:
            continue
        if isinstance(chunk, bytes):
            chunk = chunk.decode("utf-8", errors="replace")
        buf += chunk
        while "\n\n" in buf:
            block, buf = buf.split("\n\n", 1)
            data_lines = []
            for line in block.splitlines():
                if line.startswith("data:"):
                    data_lines.append(line[5:].lstrip())
            if not data_lines:
                continue
            data = "\n".join(data_lines).strip()
            if not data or data == "[DONE]":
                continue
            try:
                yield json.loads(data)
            except Exception:
                continue


# session continuity per client conversation key: maps to last key+session
SESS: dict[str, dict[str, str]] = {}
SESS_LOCK = threading.Lock()


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server_version = "hr-openai-proxy/0.3"

    def log_message(self, fmt, *args):
        print(f"[{time.strftime('%H:%M:%S')}] {self.address_string()} {fmt % args}", flush=True)

    def _send(self, code: int, body: bytes, content_type: str = "application/json"):
        self.send_response(code)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(body)

    def _json(self, code: int, obj: Any):
        self._send(code, json.dumps(obj, ensure_ascii=False).encode("utf-8"))

    def _err(self, code: int, msg: str):
        self._json(code, {"error": {"message": msg, "type": "invalid_request_error", "code": code}})

    def do_OPTIONS(self):
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Authorization, Content-Type, x-api-key")
        self.end_headers()

    def auth_ok(self) -> bool:
        auth = self.headers.get("Authorization") or ""
        tok = auth[7:].strip() if auth.lower().startswith("bearer ") else (self.headers.get("x-api-key") or "")
        if not PROXY_TOKEN:
            return True
        return tok == PROXY_TOKEN

    def do_GET(self):
        path = self.path.split("?", 1)[0]
        if path in ("/", "/health"):
            with lock:
                n = len(keys)
                dis = sum(1 for k in keys if (state.get(k) or {}).get("disabled"))
                ready = sum(1 for k in keys if all((state.get(k) or {}).get(b) for b in BASES))
            return self._json(
                200,
                {
                    "ok": True,
                    "service": "hr-openai-proxy",
                    "port": PORT,
                    "keys_total": n,
                    "keys_disabled": dis,
                    "keys_harness_ready": ready,
                    "outbound": OUTBOUND,
                },
            )
        if not self.auth_ok():
            return self._err(401, "Unauthorized")
        if path in ("/v1/models", "/models"):
            try:
                return self._json(200, {"object": "list", "data": openai_model_list()})
            except Exception as e:
                return self._err(502, f"models fetch failed: {e}")
        return self._err(404, "not found")

    def do_POST(self):
        path = self.path.split("?", 1)[0]
        if not self.auth_ok():
            return self._err(401, "Unauthorized")
        length = int(self.headers.get("Content-Length") or 0)
        raw = self.rfile.read(length) if length else b"{}"
        try:
            body = json.loads(raw.decode("utf-8") or "{}")
        except Exception:
            return self._err(400, "invalid json")
        if path in ("/v1/chat/completions", "/chat/completions", "/v1/responses", "/responses"):
            return self.handle_chat(body, responses_style=path.endswith("responses"))
        return self._err(404, "not found")

    def _build_user_input(self, body, msgs, base, responses_style):
        lim = input_limit_for_base(base)
        if responses_style and body.get("input") is not None:
            user_input = body.get("input")
            if not isinstance(user_input, str):
                user_input = json.dumps(user_input, ensure_ascii=False)
            if len(user_input) > lim:
                print(f"[truncate] raw input {len(user_input)} -> {lim}", flush=True)
                user_input = _clip(user_input, lim)
            return wrap_input(user_input, msgs), lim
        packed = messages_to_input(msgs, max_chars=lim, base=base)
        user_input = wrap_input(packed, msgs)
        raw_est = sum(len(_content_to_text(m.get("content"))) for m in msgs)
        if raw_est > len(user_input) + 1000:
            print(f"[truncate] messages ~{raw_est} chars -> {len(user_input)} base={base}", flush=True)
        return user_input, lim

    def handle_chat(self, body: dict, responses_style: bool = False):
        model_name = body.get("model") or "hr/hermes"
        base, model_id = parse_model(model_name)
        if base not in BASES:
            return self._err(400, f"unknown base {base}")

        stream = bool(body.get("stream", False))
        max_out = extract_max_output(body)
        msgs = body.get("messages") or []
        wants_short = user_wants_short_reply(msgs, body)
        raw_est = 0
        if not (responses_style and body.get("input") is not None):
            raw_est = sum(len(_content_to_text(m.get("content"))) for m in msgs)
        elif isinstance(body.get("input"), str):
            raw_est = len(body.get("input") or "")

        # Long tavern packs: prefer hermes (no claude argv E2BIG). Keep requested model id.
        orig_base = base
        if base == "claude-code" and raw_est >= LONG_CTX_CHARS and CLAUDE_E2BIG_FALLBACK:
            print(f"[route] long_ctx raw={raw_est} claude-code -> hermes model={model_id}", flush=True)
            base = "hermes"

        user_input, lim = self._build_user_input(body, msgs, base, responses_style)

        conv = None
        meta_in = body.get("metadata") or {}
        if isinstance(meta_in, dict):
            conv = meta_in.get("session_id") or meta_in.get("conversation_id")
        conv = conv or body.get("user") or body.get("conversation_id")

        sticky_key = None
        prev_resp = None
        session_id = None
        if conv:
            with SESS_LOCK:
                st = SESS.get(str(conv))
                if st:
                    sticky_key = st.get("key")
                    prev_resp = st.get("response_id")
                    session_id = st.get("session_id")

        chat_id = f"chatcmpl-{uuid.uuid4().hex[:24]}"
        created = int(time.time())
        last_err = None
        tries = min(10, max(4, min(12, len(keys))))
        e2big_hits = 0
        empty_hits = 0
        stream_started = False
        fallback_hermes_done = base == "hermes"

        # Start SSE early so tavern/client does not idle-timeout (client_gone / ct=0)
        if stream:
            try:
                self.begin_stream(chat_id, model_name, created)
                stream_started = True
                self.stream_heartbeat("upstream-start")
            except Exception as e:
                print(f"[stream-start] {e}", flush=True)

        def hb(tag="work"):
            if stream_started:
                try:
                    self.stream_heartbeat(tag)
                except Exception:
                    pass

        for attempt in range(tries):
            hb(f"attempt-{attempt}-base-{base}")
            key = sticky_key if (attempt == 0 and sticky_key) else pick_key()
            sticky_key = None
            try:
                hmap = ensure_harnesses(key)
            except Exception as e:
                last_err = str(e)
                continue
            hid = hmap.get(base)
            if not hid:
                last_err = f"no harness for {base}"
                # try hermes fallback harness on same key
                if base != "hermes" and hmap.get("hermes"):
                    base = "hermes"
                    hid = hmap.get("hermes")
                    user_input, lim = self._build_user_input(body, msgs, base, responses_style)
                    fallback_hermes_done = True
                    print(f"[route] missing harness -> hermes", flush=True)
                else:
                    continue

            payload: dict[str, Any] = {"input": user_input, "stream": True}
            if model_id:
                payload["model"] = model_id
            if max_out:
                payload["max_output_tokens"] = max_out
            if session_id and prev_resp:
                with SESS_LOCK:
                    st = SESS.get(str(conv)) if conv else None
                if st and st.get("key") == key:
                    payload["previous_response_id"] = prev_resp
                    payload["metadata"] = {"session_id": session_id}

            idem = str(uuid.uuid4())
            url = upstream(f"/{hid}/v1/responses")
            headers = hr_headers(key, stream=True, extra={"Content-Type": "application/json", "Idempotency-Key": idem})
            try:
                resp = requests.post(url, json=payload, headers=headers, stream=True, timeout=900, verify=False)
            except Exception as e:
                last_err = f"upstream unreachable: {e}"
                continue

            if resp.status_code in (401, 403):
                mark_bad(key, f"run auth {resp.status_code}")
                last_err = f"auth {resp.status_code}"
                try:
                    resp.close()
                except Exception:
                    pass
                continue
            if resp.status_code == 429:
                last_err = "rate limited"
                try:
                    resp.close()
                except Exception:
                    pass
                continue
            if resp.status_code >= 400:
                err_txt = resp.text[:2000]
                last_err = f"HTTP {resp.status_code}: {err_txt[:400]}"
                try:
                    resp.close()
                except Exception:
                    pass
                if resp.status_code == 400 and "max_output_tokens" in payload and attempt < tries - 1:
                    if "max_output" in err_txt.lower() or "unknown" in err_txt.lower() or "invalid" in err_txt.lower():
                        print("[payload] drop max_output_tokens after 400", flush=True)
                        max_out = None
                        continue
                if is_e2big_error(err_txt):
                    e2big_hits += 1
                    before = len(user_input)
                    # after repeated e2big on claude, switch to hermes with full rebuild
                    if base == "claude-code" and (e2big_hits >= 1) and not fallback_hermes_done:
                        print(f"[route] e2big on claude -> hermes (hits={e2big_hits})", flush=True)
                        base = "hermes"
                        fallback_hermes_done = True
                        user_input, lim = self._build_user_input(body, msgs, base, responses_style)
                        session_id = None
                        prev_resp = None
                        continue
                    user_input = shrink_input(user_input, 0.55)
                    print(f"[e2big] shrink input {before} -> {len(user_input)} retry", flush=True)
                    session_id = None
                    prev_resp = None
                    continue
                continue

            # Default = collect-then-emit (fixes empty/truncated). Optional true-stream via HR_TRUE_STREAM=1.
            use_true_stream = bool(stream and TRUE_STREAM and empty_hits == 0 and e2big_hits == 0)
            if use_true_stream:
                peeked = []
                e2big = False
                failed_early = None
                try:
                    for ev in iter_sse_json(resp):
                        peeked.append(ev)
                        tt = ev.get("type") or ""
                        if tt == "response.failed":
                            err = (ev.get("response") or {}).get("error") or ev.get("error") or ev
                            err_s = err if isinstance(err, str) else json.dumps(err, ensure_ascii=False)
                            if is_e2big_error(err_s):
                                e2big = True
                            else:
                                failed_early = err_s
                            break
                        if tt in ("response.output_text.delta", "response.completed", "response.incomplete"):
                            break
                        if len(peeked) >= 12:
                            break
                except Exception as e:
                    last_err = f"stream peek failed: {e}"
                    try:
                        resp.close()
                    except Exception:
                        pass
                    continue
                if e2big:
                    try:
                        resp.close()
                    except Exception:
                        pass
                    e2big_hits += 1
                    last_err = "argument list too long"
                    if base == "claude-code" and not fallback_hermes_done:
                        print("[route] e2big-stream claude -> hermes", flush=True)
                        base = "hermes"
                        fallback_hermes_done = True
                        user_input, lim = self._build_user_input(body, msgs, base, responses_style)
                        session_id = None
                        prev_resp = None
                        continue
                    before = len(user_input)
                    user_input = shrink_input(user_input, 0.55)
                    print(f"[e2big-stream] shrink {before} -> {len(user_input)}", flush=True)
                    session_id = None
                    prev_resp = None
                    continue
                if failed_early and attempt < tries - 1:
                    try:
                        resp.close()
                    except Exception:
                        pass
                    last_err = failed_early
                    print(f"[retry] early-fail attempt={attempt} err={str(last_err)[:160]}", flush=True)
                    session_id = None
                    prev_resp = None
                    continue
                print(f"[stream] true-stream base={base} key=...{key[-6:]}", flush=True)
                return self.stream_live(
                    resp, chat_id, model_name, created, conv, key, hid,
                    peeked=peeked, already_started=stream_started,
                )

            # Collect full upstream first so client never sees blank/truncated mid-stream errors.
            collected = self.collect_upstream(
                resp, conv, key, hid, model_name, heartbeat=hb if stream_started else None
            )
            status = collected.get("status") or "unknown"
            text = collected.get("text") or ""
            err_s = collected.get("error") or ""
            print(
                f"[collect] status={status} text_len={len(text)} err_len={len(err_s)} base={base} key=...{key[-6:]}",
                flush=True,
            )

            if status == "e2big" or is_e2big_error(err_s) or is_e2big_error(text):
                e2big_hits += 1
                last_err = err_s or text or "argument list too long"
                if base == "claude-code" and not fallback_hermes_done:
                    print("[route] e2big-collect claude -> hermes", flush=True)
                    base = "hermes"
                    fallback_hermes_done = True
                    user_input, lim = self._build_user_input(body, msgs, base, responses_style)
                    session_id = None
                    prev_resp = None
                    continue
                before = len(user_input)
                user_input = shrink_input(user_input, 0.55)
                print(f"[e2big-collect] shrink {before} -> {len(user_input)}", flush=True)
                session_id = None
                prev_resp = None
                continue

            if status == "failed" or (not text and err_s):
                last_err = err_s or "upstream failed"
                if attempt < tries - 1:
                    print(f"[retry] failed attempt={attempt} err={last_err[:160]}", flush=True)
                    if base == "claude-code" and raw_est >= 40000 and not fallback_hermes_done:
                        base = "hermes"
                        fallback_hermes_done = True
                        user_input, lim = self._build_user_input(body, msgs, base, responses_style)
                    session_id = None
                    prev_resp = None
                    continue
                text = format_error_message(err_s or last_err)

            # EMPTY reply: retry (main source of ct=0)
            if is_empty_reply(text) and attempt < tries - 1:
                empty_hits += 1
                print(f"[retry-empty] attempt={attempt} empty_hits={empty_hits} base={base}", flush=True)
                last_err = "empty upstream response"
                if empty_hits >= 1 and base != "hermes":
                    base = "hermes"
                    fallback_hermes_done = True
                    user_input, lim = self._build_user_input(body, msgs, base, responses_style)
                # thin boost: one short line only, no fat RP essay
                boost = "\n\nPlease answer the latest user message fully. Do not return empty."
                if boost not in user_input:
                    user_input = user_input + boost
                if len(user_input) > input_limit_for_base(base):
                    user_input = shrink_input(user_input, 0.65)
                session_id = None
                prev_resp = None
                continue

            # short unusable reply: retry (keep protection)
            if (not wants_short) and is_bad_short_reply(text) and attempt < tries - 1 and status in (
                "completed", "incomplete", "unknown", "failed"
            ):
                print(f"[retry-short] len={len(text)} preview={text[:60]!r} attempt={attempt}", flush=True)
                last_err = f"short_reply:{text[:80]}"
                if len(user_input) > 120000:
                    user_input = shrink_input(user_input, 0.7)
                if attempt >= 2 and base != "hermes":
                    base = "hermes"
                    fallback_hermes_done = True
                    user_input, lim = self._build_user_input(body, msgs, base, responses_style)
                boost = "\n\nPrevious reply was too short. Write a complete answer to the latest user message."
                if boost not in user_input:
                    user_input = user_input + boost
                session_id = None
                prev_resp = None
                continue

            if not text:
                text = format_error_message(err_s or last_err or "empty upstream response")

            if stream_started:
                return self.finish_stream_text(text, chat_id, model_name, created)
            if stream:
                return self.emit_stream_text(text, chat_id, model_name, created)
            return self.emit_json_text(text, chat_id, model_name, created)

        msg = format_error_message(last_err or "all upstream keys failed")
        print(f"[final-fail] {msg[:200]}", flush=True)
        if stream_started:
            return self.finish_stream_text(msg, chat_id, model_name, created)
        if stream:
            return self.emit_stream_text(msg, chat_id, model_name, created)
        return self.emit_json_text(msg, chat_id, model_name, created)

    def _save_sess(self, conv, session_id, response_id, key, hid, model_name):
        if conv and (session_id or response_id):
            with SESS_LOCK:
                SESS[str(conv)] = {
                    "session_id": session_id or "",
                    "response_id": response_id or "",
                    "key": key,
                    "harness_id": hid,
                    "model": model_name,
                }


    def collect_upstream(self, resp, conv, key, hid, model_name, heartbeat=None) -> dict:
        """Read full HR SSE; optional heartbeat keeps tavern clients alive."""
        response_id = None
        session_id = None
        full: list[str] = []
        failed_err = None
        status = "unknown"
        final_resp = None
        last_hb = time.time()
        try:
            for ev in iter_sse_json(resp):
                if heartbeat and (time.time() - last_hb) >= HEARTBEAT_SECS:
                    try:
                        heartbeat("upstream")
                    except Exception:
                        pass
                    last_hb = time.time()
                t = ev.get("type") or ""
                r = ev.get("response")
                if t == "response.created" and isinstance(r, dict):
                    response_id = r.get("id") or response_id
                    md = r.get("metadata") or {}
                    if isinstance(md, dict):
                        session_id = md.get("session_id") or session_id
                if isinstance(r, dict):
                    response_id = r.get("id") or response_id
                    md = r.get("metadata") or {}
                    if isinstance(md, dict):
                        session_id = md.get("session_id") or session_id
                piece = text_from_event(ev)
                if piece:
                    full.append(piece)
                # also catch output_text.done full blob if delta was missed
                if t in ("response.output_text.done", "response.content_part.done"):
                    part = ev.get("part") or ev.get("content") or {}
                    if isinstance(part, dict):
                        tx = part.get("text") or ""
                        if tx and tx not in "".join(full):
                            # if deltas already built same text, skip; else if full shorter replace
                            joined = "".join(full)
                            if len(tx) > len(joined):
                                full = [tx]
                    tx2 = ev.get("text")
                    if isinstance(tx2, str) and len(tx2) > len("".join(full)):
                        full = [tx2]
                if t == "response.failed":
                    status = "failed"
                    final_resp = r if isinstance(r, dict) else {}
                    err = (final_resp or {}).get("error") or ev.get("error") or ev
                    failed_err = format_error_message(err)
                    # still try extract any text
                    extra = text_from_response_obj(final_resp)
                    if extra and len(extra) > len("".join(full)):
                        full = [extra]
                    break
                if t in ("response.completed", "response.incomplete"):
                    status = "completed" if t == "response.completed" else "incomplete"
                    final_resp = r if isinstance(r, dict) else {}
                    extra = text_from_response_obj(final_resp)
                    joined = "".join(full)
                    if extra and len(extra) > len(joined):
                        full = [extra]
                    # incomplete may carry error details
                    if not "".join(full):
                        err = (final_resp or {}).get("error") or (final_resp or {}).get("incomplete_details")
                        if err:
                            failed_err = format_error_message(err)
                            status = "failed"
                    break
        except Exception as e:
            status = "failed"
            failed_err = format_error_message(f"stream read error: {e}")
        finally:
            try:
                resp.close()
            except Exception:
                pass
        self._save_sess(conv, session_id, response_id, key, hid, model_name)
        text = sanitize_agent_text("".join(full).strip())
        err_s = (failed_err or "").strip()
        if status == "failed" and is_e2big_error(err_s):
            status = "e2big"
        if text and is_e2big_error(text) and len(text) < 500:
            status = "e2big"
            err_s = err_s or text
        return {"status": status, "text": text, "error": err_s, "response_id": response_id, "session_id": session_id}


    def stream_live(self, resp, chat_id, model_name, created, conv, key, hid, peeked=None, already_started=False):
        """Passthrough HR tokens as OpenAI SSE (chat-like latency)."""
        if not already_started:
            self.begin_stream(chat_id, model_name, created)
        response_id = None
        session_id = None
        full = []
        try:
            def _events():
                if peeked:
                    for ev in peeked:
                        yield ev
                yield from iter_sse_json(resp)

            for ev in _events():
                tt = ev.get("type") or ""
                r = ev.get("response")
                if isinstance(r, dict):
                    response_id = r.get("id") or response_id
                    md = r.get("metadata") or {}
                    if isinstance(md, dict):
                        session_id = md.get("session_id") or session_id
                piece = text_from_event(ev)
                if piece:
                    full.append(piece)
                    chunk = {
                        "id": chat_id,
                        "object": "chat.completion.chunk",
                        "created": created,
                        "model": model_name,
                        "choices": [{"index": 0, "delta": {"content": piece}, "finish_reason": None}],
                    }
                    self.wfile.write(f"data: {json.dumps(chunk, ensure_ascii=False)}\n\n".encode())
                    self.wfile.flush()
                if tt in ("response.completed", "response.incomplete", "response.failed"):
                    if tt == "response.failed" and not full:
                        err = (ev.get("response") or {}).get("error") or ev.get("error") or tt
                        msg = format_error_message(err)
                        chunk = {
                            "id": chat_id,
                            "object": "chat.completion.chunk",
                            "created": created,
                            "model": model_name,
                            "choices": [{"index": 0, "delta": {"content": msg}, "finish_reason": None}],
                        }
                        self.wfile.write(f"data: {json.dumps(chunk, ensure_ascii=False)}\n\n".encode())
                    elif tt in ("response.completed", "response.incomplete") and not full:
                        extra = text_from_response_obj(r if isinstance(r, dict) else {})
                        if extra:
                            chunk = {
                                "id": chat_id,
                                "object": "chat.completion.chunk",
                                "created": created,
                                "model": model_name,
                                "choices": [{"index": 0, "delta": {"content": extra}, "finish_reason": None}],
                            }
                            self.wfile.write(f"data: {json.dumps(chunk, ensure_ascii=False)}\n\n".encode())
                            full.append(extra)
                    reason = "error" if tt == "response.failed" else "stop"
                    endc = {
                        "id": chat_id,
                        "object": "chat.completion.chunk",
                        "created": created,
                        "model": model_name,
                        "choices": [{"index": 0, "delta": {}, "finish_reason": reason}],
                    }
                    self.wfile.write(f"data: {json.dumps(endc, ensure_ascii=False)}\n\n".encode())
                    self.wfile.write(b"data: [DONE]\n\n")
                    self.wfile.flush()
                    break
            else:
                endc = {
                    "id": chat_id,
                    "object": "chat.completion.chunk",
                    "created": created,
                    "model": model_name,
                    "choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}],
                }
                self.wfile.write(f"data: {json.dumps(endc, ensure_ascii=False)}\n\n".encode())
                self.wfile.write(b"data: [DONE]\n\n")
                self.wfile.flush()
        except Exception as e:
            try:
                msg = format_error_message(f"stream error: {e}")
                chunk = {
                    "id": chat_id,
                    "object": "chat.completion.chunk",
                    "created": created,
                    "model": model_name,
                    "choices": [{"index": 0, "delta": {"content": msg}, "finish_reason": "stop"}],
                }
                self.wfile.write(f"data: {json.dumps(chunk, ensure_ascii=False)}\n\n".encode())
                self.wfile.write(b"data: [DONE]\n\n")
                self.wfile.flush()
            except Exception:
                pass
        finally:
            try:
                resp.close()
            except Exception:
                pass
            self._save_sess(conv, session_id, response_id, key, hid, model_name)
        return None

    def begin_stream(self, chat_id: str, model_name: str, created: int):
        """Open SSE immediately so long agent runs do not look idle to tavern clients."""
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream; charset=utf-8")
        self.send_header("Cache-Control", "no-cache, no-transform")
        self.send_header("Connection", "keep-alive")
        self.send_header("X-Accel-Buffering", "no")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        role_chunk = {
            "id": chat_id,
            "object": "chat.completion.chunk",
            "created": created,
            "model": model_name,
            "choices": [{"index": 0, "delta": {"role": "assistant"}, "finish_reason": None}],
        }
        self.wfile.write(f"data: {json.dumps(role_chunk, ensure_ascii=False)}\n\n".encode())
        # zero-width content keeps some clients "receiving" without visible garbage
        ping = {
            "id": chat_id,
            "object": "chat.completion.chunk",
            "created": created,
            "model": model_name,
            "choices": [{"index": 0, "delta": {"content": ""}, "finish_reason": None}],
        }
        self.wfile.write(f"data: {json.dumps(ping, ensure_ascii=False)}\n\n".encode())
        self.wfile.write(b": keepalive\n\n")
        self.wfile.flush()
        self._stream_meta = {"chat_id": chat_id, "model": model_name, "created": created}

    def stream_heartbeat(self, tag: str = "ping"):
        """SSE comment + empty delta; prevents proxy/client idle cuts on long tavern jobs."""
        meta = getattr(self, "_stream_meta", None) or {}
        chat_id = meta.get("chat_id") or "chatcmpl-hb"
        model_name = meta.get("model") or "hr"
        created = meta.get("created") or int(time.time())
        try:
            self.wfile.write(f": keepalive {tag} {int(time.time())}\n\n".encode())
            chunk = {
                "id": chat_id,
                "object": "chat.completion.chunk",
                "created": created,
                "model": model_name,
                "choices": [{"index": 0, "delta": {"content": ""}, "finish_reason": None}],
            }
            self.wfile.write(f"data: {json.dumps(chunk, ensure_ascii=False)}\n\n".encode())
            self.wfile.flush()
        except Exception:
            pass

    def finish_stream_text(self, text: str, chat_id: str, model_name: str, created: int):
        text = sanitize_agent_text(text or "")
        """Write final content on an already-opened SSE response."""
        try:
            step = 120
            body = text or ""
            if not body:
                body = format_error_message("empty upstream response")
            for i in range(0, len(body), step):
                piece = body[i : i + step]
                chunk = {
                    "id": chat_id,
                    "object": "chat.completion.chunk",
                    "created": created,
                    "model": model_name,
                    "choices": [{"index": 0, "delta": {"content": piece}, "finish_reason": None}],
                }
                self.wfile.write(f"data: {json.dumps(chunk, ensure_ascii=False)}\n\n".encode())
                if i == 0 or i + step >= len(body):
                    self.wfile.flush()
            end = {
                "id": chat_id,
                "object": "chat.completion.chunk",
                "created": created,
                "model": model_name,
                "choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}],
            }
            self.wfile.write(f"data: {json.dumps(end, ensure_ascii=False)}\n\n".encode())
            self.wfile.write(b"data: [DONE]\n\n")
            self.wfile.flush()
        except Exception as e:
            try:
                chunk = {
                    "id": chat_id,
                    "object": "chat.completion.chunk",
                    "created": created,
                    "model": model_name,
                    "choices": [
                        {
                            "index": 0,
                            "delta": {"content": format_error_message(f"emit failed: {e}")},
                            "finish_reason": "stop",
                        }
                    ],
                }
                self.wfile.write(f"data: {json.dumps(chunk, ensure_ascii=False)}\n\n".encode())
                self.wfile.write(b"data: [DONE]\n\n")
                self.wfile.flush()
            except Exception:
                pass
        return None

    def emit_stream_text(self, text: str, chat_id: str, model_name: str, created: int):
        text = sanitize_agent_text(text or "")
        """SSE-emit already-final text in readable chunks (complete, never truncated mid-error)."""
        try:
            self.begin_stream(chat_id, model_name, created)
        except Exception:
            pass
        return self.finish_stream_text(text, chat_id, model_name, created)

    def emit_json_text(self, text: str, chat_id: str, model_name: str, created: int):
        text = sanitize_agent_text(text or "")
        out = {
            "id": chat_id,
            "object": "chat.completion",
            "created": created,
            "model": model_name,
            "choices": [
                {
                    "index": 0,
                    "message": {"role": "assistant", "content": text},
                    "finish_reason": "stop",
                }
            ],
            "usage": {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0},
        }
        self._json(200, out)
        return None


def main():
    global keys
    load_state()
    keys = load_keys()
    if not keys:
        raise SystemExit(f"no keys in {KEYS_FILE}")
    print(f"loaded {len(keys)} keys from {KEYS_FILE}", flush=True)
    httpd = ThreadingHTTPServer((HOST, PORT), Handler)
    print(f"hr-openai-proxy on http://{HOST}:{PORT} via {OUTBOUND or 'direct'}", flush=True)
    print(f"client token set: {bool(PROXY_TOKEN)}", flush=True)
    httpd.serve_forever()


if __name__ == "__main__":
    main()
