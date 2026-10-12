"""LLM 全量账单（siwu_llm_billing）—— 统计**所有** LLM 调用，含插件直连的那些。

为什么需要它：AstrBot 自带的用量统计（provider_stats 表）只在 agent 管线里写，
见 core/pipeline/process_stage/method/agent_sub_stages/internal.py 的
db_helper.insert_provider_stat(...)，且 agent_type 写死为 "internal"。于是：

- 走 event.request_llm() 的回复生成           → 被记录
- 插件直接调 context.get_using_provider().text_chat() 的 → **完全不被记录**
  （group_chat_plus 读空气决策、livingmemory 记忆抽取/反思、
   self_learning 黑话预筛/批学习/表达模式学习、图片描述等）

实测本机：某日日志里有 1171 次 chat/completions 请求，provider_stats 只有 24 行。

本插件的做法：在 **Provider 层**包装 text_chat / text_chat_stream，于是无论调用方
是管线还是插件直连，都能拿到真实 usage（含缓存命中），并按调用栈归因到「谁花的钱」。

设计约定（遵循插件模板）：
    - 采集在 provider 层，不在 pipeline hook，这是本插件与 cost-control 类插件的根本区别
    - 只读登记 + 本地 SQLite，不改动任何请求内容、不拦截、不注入
    - 包装层绝不影响主流程：异常、记录失败一律吞掉并降级
    - 持久化数据放 get_astrbot_data_path()/llm_billing/，不放插件目录
    - terminate() 必须还原被包装的方法，否则重载会叠加多层包装
"""

import asyncio
import contextvars
import inspect
import os
import sqlite3
import sys
import time
from datetime import datetime, timedelta

from astrbot.api import logger, star
from astrbot.api.all import AstrBotConfig, AstrMessageEvent, MessageChain
from astrbot.api.event import filter
from astrbot.api.provider import ProviderRequest

try:
    from astrbot.core.utils.astrbot_path import get_astrbot_data_path
except Exception:  # noqa: BLE001 - 兼容旧版本
    def get_astrbot_data_path() -> str:
        return os.path.join(os.getcwd(), "data")


DATA_DIR = os.path.join(get_astrbot_data_path(), "llm_billing")
DB_PATH = os.path.join(DATA_DIR, "billing.db")
# 归因时跳过本文件自身的包装层帧，否则会把调用方错认成自己
_SELF_FILE = os.path.abspath(__file__)

# 当前请求所属会话（由 on_llm_request 写入，仅供归因，不改请求内容）
_UMO: contextvars.ContextVar[str] = contextvars.ContextVar("siwu_bill_umo", default="")

# model 名 -> (非缓存输入, 缓存命中输入, 输出) USD / 百万 token
DEFAULT_PRICES: dict[str, tuple[float, float, float]] = {
    "mimo-v2.6-flash": (0.14, 0.0028, 0.28),
    "mimo-v2.6-pro": (0.435, 0.0036, 0.87),
    "mimo-v2.5": (0.14, 0.0028, 0.28),
    "mimo-v2.5-pro": (0.435, 0.0036, 0.87),
}

ORIGIN_LABELS = {
    "reply": "回复生成（管线）",
    "caption": "图片描述（内置）",
    "decision": "群聊决策/读空气",
    "jargon": "黑话挖掘",
    "learning": "风格/人格学习",
    "memory": "记忆抽取/反思",
    "other": "其他插件",
    "unknown": "未知来源",
}

# 归因规则：栈里出现这些模块/函数名 → 归类（顺序敏感）
_ORIGIN_RULES: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("caption", ("get_image_caption", "group_chat_context")),
    ("memory", ("memory_processor", "memory_reflection", "memory_engine", "consolidation_manager")),
    ("learning", ("progressive_learning", "realtime_processor", "style_analyzer",
                  "persona_updater", "expression_learning", "expression_pattern")),
    ("jargon", ("jargon_miner", "jargon_query", "jargon_")),
    ("decision", ("decision_ai", "probability_manager", "proactive_chat", "attention_manager")),
)
_PIPELINE_MARKS = ("process_stage", "agent_sub_stages", "third_party", "core.pipeline")


# ----------------------------------------------------------------------
# 定价
# ----------------------------------------------------------------------
def parse_prices(raw: object) -> dict[str, tuple[float, float, float]]:
    """解析配置里的单价表。

    每行格式：``provider|model = 输入, 缓存命中, 输出``（USD / 百万 token）。
    只写模型名或只写 provider 也可以，按最长匹配生效。
    """
    prices = dict(DEFAULT_PRICES)
    if isinstance(raw, str):
        lines = raw.splitlines()
    elif isinstance(raw, (list, tuple)):
        lines = list(raw)
    else:
        lines = []
    for line in lines:
        text = str(line or "").strip()
        if not text or text.startswith("#") or "=" not in text:
            continue
        key, _, value = text.partition("=")
        parts = [p.strip() for p in value.replace("，", ",").split(",")]
        try:
            nums = [float(p) for p in parts if p != ""]
        except ValueError:
            logger.warning("[llm_billing] 单价行解析失败，已跳过: %s", text)
            continue
        if not nums:
            continue
        nums = (nums + [nums[-1], nums[-1]])[:3]
        prices[key.strip()] = (nums[0], nums[1], nums[2])
    return prices


def match_price(prices: dict[str, tuple[float, float, float]], pid: str, model: str):
    """按 ``provider|model`` → ``model`` → ``provider`` → 模糊最长匹配找单价。"""
    pid = str(pid or "")
    model = str(model or "")
    candidates = [f"{pid}|{model}", model, pid, pid.split("/")[-1]]
    for key in candidates:
        key = str(key or "").strip()
        if key and key in prices:
            return prices[key]
    best = None
    hay = f"{pid}|{model}".lower()
    for key, val in prices.items():
        k = key.lower().strip()
        hit = k and (k in hay or (model and k == str(model).lower()))
        if hit and (best is None or len(k) > len(best[0])):
            best = (k, val)
    return best[1] if best else None


def compute_cost(prices, pid: str, model: str, in_other: int, in_cached: int, out: int) -> float:
    price = match_price(prices, pid, model)
    if not price:
        return 0.0
    p_in, p_cached, p_out = price
    return (in_other / 1e6) * p_in + (in_cached / 1e6) * p_cached + (out / 1e6) * p_out


# ----------------------------------------------------------------------
# Provider 层包装
# ----------------------------------------------------------------------
_ORIGINALS: dict[tuple[type, str], object] = {}
_BUFFER: list[dict] = []
_SEEN_FRAMES = 40


def _stack_info():
    """收集调用栈（跳过 provider 自身与包装层），返回 (frames, 插件目录名)。"""
    frames: list[str] = []
    plugin = ""
    try:
        f = sys._getframe(2)
    except Exception:  # noqa: BLE001
        return frames, plugin
    n = 0
    while f is not None and n < _SEEN_FRAMES:
        fn = (f.f_code.co_filename or "").replace("\\", "/")
        if os.path.abspath(f.f_code.co_filename or "") == _SELF_FILE:
            f = f.f_back
            n += 1
            continue
        if "/provider/sources/" not in fn and not fn.endswith("/provider/provider.py"):
            frames.append(f"{f.f_globals.get('__name__', '?')}:{f.f_code.co_name}")
            if not plugin and "/data/plugins/" in fn:
                plugin = fn.split("/data/plugins/")[1].split("/")[0]
        f = f.f_back
        n += 1
    return frames, plugin


def _classify(frames: list[str], plugin: str) -> str:
    """按栈归因：先认具体来源（决策/记忆/黑话/学习/图片描述），再判通用管线。

    顺序很关键：群聊决策、图片描述等调用**也发生在管线上下文里**，若先判
    ``process_stage`` 之类标记，它们会被误判成「回复生成」。
    """
    joined = " ".join(frames).lower()
    for origin, marks in _ORIGIN_RULES:
        if any(mark in joined for mark in marks):
            return origin
    if any(mark in joined for mark in _PIPELINE_MARKS):
        return "reply"
    return "other" if plugin else "unknown"


def _usage_of(resp) -> tuple[int, int, int]:
    u = getattr(resp, "usage", None)
    def g(key: str) -> int:
        try:
            return int(getattr(u, key, 0) or 0)
        except Exception:  # noqa: BLE001
            return 0
    return g("input_other"), g("input_cached"), g("output")


def _prompt_size(kw: dict) -> tuple[int, int]:
    total = 0
    try:
        total += len(str(kw.get("prompt") or ""))
        for item in kw.get("contexts") or []:
            total += len(str(item.get("content") if isinstance(item, dict) else item))
    except Exception:  # noqa: BLE001, S110
        pass
    return total, len(str(kw.get("system_prompt") or ""))


def _emit(self, meth: str, resp, kw: dict, started: float, err: str | None) -> None:
    try:
        pid = ""
        cfg = getattr(self, "provider_config", None)
        if isinstance(cfg, dict):
            pid = str(cfg.get("id") or "")
        try:
            model = str(self.get_model())
        except Exception:  # noqa: BLE001
            model = ""
        in_other, in_cached, out = _usage_of(resp) if resp is not None else (0, 0, 0)
        pchars, schars = _prompt_size(kw)
        frames, plugin = _stack_info()
        _BUFFER.append({
            "ts": time.time(),
            "kind": meth,
            "cls": type(self).__name__,
            "pid": pid,
            "model": model,
            "dur_ms": round((time.time() - started) * 1000, 1),
            "prompt_chars": pchars,
            "system_chars": schars,
            "in_other": in_other,
            "in_cached": in_cached,
            "out": out,
            "origin": _classify(frames, plugin),
            "plugin": plugin,
            "umo": _UMO.get(),
            "stack": " | ".join(frames[:4])[:400],
            "err": (err or "")[:200],
        })
        del _BUFFER[:-2000]
    except Exception as e:  # noqa: BLE001
        logger.debug("[llm_billing] 记录失败: %s", e)


def _wrap(meth: str, fn):
    if meth == "text_chat":
        async def wrapper(self, *args, **kwargs):
            started, resp, err = time.time(), None, None
            try:
                resp = await fn(self, *args, **kwargs)
                return resp
            except Exception as e:
                err = f"{type(e).__name__}: {e}"
                raise
            finally:
                _emit(self, meth, resp, kwargs, started, err)
        return wrapper

    async def wrapper_stream(self, *args, **kwargs):
        started, last, err = time.time(), None, None
        try:
            async for item in fn(self, *args, **kwargs):
                last = item
                yield item
        except Exception as e:
            err = f"{type(e).__name__}: {e}"
            raise
        finally:
            _emit(self, meth, last, kwargs, started, err)
    return wrapper_stream


def patch_providers() -> tuple[int, list[str]]:
    """包装所有 Provider 子类的 text_chat / text_chat_stream（幂等）。"""
    from astrbot.core.provider.provider import Provider

    classes: list[type] = [Provider]

    def walk(cls):
        for sub in cls.__subclasses__():
            classes.append(sub)
            walk(sub)

    walk(Provider)
    patched = 0
    for cls in classes:
        for meth in ("text_chat", "text_chat_stream"):
            fn = cls.__dict__.get(meth)
            if fn is None or getattr(fn, "_siwu_billing", False):
                continue
            if meth == "text_chat" and not inspect.iscoroutinefunction(fn):
                continue
            if meth == "text_chat_stream" and not inspect.isasyncgenfunction(fn):
                continue
            new = _wrap(meth, fn)
            new._siwu_billing = True
            try:
                setattr(cls, meth, new)
            except Exception:  # noqa: BLE001, S112
                continue
            _ORIGINALS[(cls, meth)] = fn
            patched += 1
    return patched, [c.__name__ for c in classes]


def unpatch_providers() -> int:
    n = 0
    for (cls, meth), fn in list(_ORIGINALS.items()):
        try:
            setattr(cls, meth, fn)
            n += 1
        except Exception:  # noqa: BLE001, S110
            pass
        _ORIGINALS.pop((cls, meth), None)
    return n


# ----------------------------------------------------------------------
# SQLite 存储
# ----------------------------------------------------------------------
class Store:
    def __init__(self, path: str = DB_PATH) -> None:
        self.path = path
        os.makedirs(os.path.dirname(path), exist_ok=True)
        self._init()

    def _conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path, timeout=10)
        conn.execute("PRAGMA journal_mode=WAL")
        return conn

    def _init(self) -> None:
        with self._conn() as conn:
            conn.execute(
                """CREATE TABLE IF NOT EXISTS calls(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts REAL NOT NULL, kind TEXT, cls TEXT, pid TEXT, model TEXT,
                    dur_ms REAL, prompt_chars INTEGER, system_chars INTEGER,
                    in_other INTEGER, in_cached INTEGER, out INTEGER,
                    origin TEXT, plugin TEXT, umo TEXT, stack TEXT, err TEXT)"""
            )
            conn.execute("CREATE INDEX IF NOT EXISTS idx_calls_ts ON calls(ts)")
            conn.execute("CREATE TABLE IF NOT EXISTS meta(k TEXT PRIMARY KEY, v TEXT)")
            # 迁移：老库补 stack 列（诊断归因用）
            cols = {row[1] for row in conn.execute("PRAGMA table_info(calls)")}
            if "stack" not in cols:
                conn.execute("ALTER TABLE calls ADD COLUMN stack TEXT")

    def insert(self, rows: list[dict]) -> None:
        if not rows:
            return
        for row in rows:
            row.setdefault("stack", "")
        with self._conn() as conn:
            conn.executemany(
                """INSERT INTO calls(ts,kind,cls,pid,model,dur_ms,prompt_chars,system_chars,
                       in_other,in_cached,out,origin,plugin,umo,stack,err)
                   VALUES(:ts,:kind,:cls,:pid,:model,:dur_ms,:prompt_chars,:system_chars,
                       :in_other,:in_cached,:out,:origin,:plugin,:umo,:stack,:err)""",
                rows,
            )

    def totals(self, start: float, end: float) -> dict:
        with self._conn() as conn:
            row = conn.execute(
                """SELECT count(*), coalesce(sum(in_other),0), coalesce(sum(in_cached),0),
                          coalesce(sum(out),0), coalesce(sum(case when err<>'' then 1 else 0 end),0)
                   FROM calls WHERE ts>=? AND ts<?""", (start, end)).fetchone()
        return {"calls": row[0], "in_other": row[1], "in_cached": row[2], "out": row[3], "errors": row[4]}

    def group(self, dim: str, start: float, end: float, limit: int = 8) -> list[dict]:
        dim = dim if dim in ("origin", "model", "plugin", "pid") else "origin"
        with self._conn() as conn:
            rows = conn.execute(
                f"""SELECT coalesce(nullif({dim},''),'(未知)') k, count(*),
                           coalesce(sum(in_other),0), coalesce(sum(in_cached),0), coalesce(sum(out),0)
                    FROM calls WHERE ts>=? AND ts<?
                    GROUP BY k ORDER BY (coalesce(sum(in_other),0)+coalesce(sum(in_cached),0)+coalesce(sum(out),0)) DESC
                    LIMIT ?""", (start, end, limit)).fetchall()
        return [{"key": r[0], "calls": r[1], "in_other": r[2], "in_cached": r[3], "out": r[4]} for r in rows]

    def group_pairs(self, dim1: str, dim2: str, start: float, end: float) -> list[dict]:
        """按两个维度交叉聚合（用于按来源分摊成本，需要 model 才能定价）。"""
        dim1 = dim1 if dim1 in ("origin", "plugin", "pid") else "origin"
        dim2 = dim2 if dim2 in ("model", "pid") else "model"
        with self._conn() as conn:
            rows = conn.execute(
                f"""SELECT coalesce(nullif({dim1},''),'(未知)') a,
                           coalesce(nullif({dim2},''),'(未知)') b,
                           count(*), coalesce(sum(in_other),0),
                           coalesce(sum(in_cached),0), coalesce(sum(out),0)
                    FROM calls WHERE ts>=? AND ts<? GROUP BY a, b""",
                (start, end)).fetchall()
        return [{"a": r[0], "b": r[1], "calls": r[2], "in_other": r[3],
                 "in_cached": r[4], "out": r[5]} for r in rows]

    def recent(self, limit: int = 8) -> list[dict]:
        """最近若干次调用（含调用栈快照），用于核对归因。"""
        with self._conn() as conn:
            rows = conn.execute(
                """SELECT ts,kind,pid,model,in_other,in_cached,out,origin,plugin,stack
                   FROM calls ORDER BY ts DESC LIMIT ?""", (limit,)).fetchall()
        keys = ("ts", "kind", "pid", "model", "in_other", "in_cached", "out",
                "origin", "plugin", "stack")
        return [dict(zip(keys, row)) for row in rows]

    def recent_umo_for(self, group_id: str) -> str:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT umo FROM calls WHERE umo LIKE ? ORDER BY ts DESC LIMIT 1",
                (f"%{group_id}",)).fetchone()
        return row[0] if row else ""

    def cleanup(self, days: int) -> int:
        if days <= 0:
            return 0
        cutoff = time.time() - days * 86400
        with self._conn() as conn:
            cur = conn.execute("DELETE FROM calls WHERE ts<?", (cutoff,))
        return cur.rowcount or 0

    def get_meta(self, key: str) -> str:
        with self._conn() as conn:
            row = conn.execute("SELECT v FROM meta WHERE k=?", (key,)).fetchone()
        return row[0] if row else ""

    def set_meta(self, key: str, value: str) -> None:
        with self._conn() as conn:
            conn.execute("INSERT INTO meta(k,v) VALUES(?,?) ON CONFLICT(k) DO UPDATE SET v=excluded.v",
                         (key, value))


def _fmt_tokens(n: int) -> str:
    if n >= 1_000_000:
        return f"{n / 1_000_000:.2f}M"
    if n >= 1000:
        return f"{n / 1000:.1f}k"
    return str(n)


# ----------------------------------------------------------------------
# 插件
# ----------------------------------------------------------------------
class LlmBillingPlugin(star.Star):
    def __init__(self, context, config: AstrBotConfig = None):
        super().__init__(context, config)
        self.config = config or {}
        self.store: Store | None = None
        self._flush_task: asyncio.Task | None = None
        self._report_task: asyncio.Task | None = None

    # ---------------- 生命周期 ----------------
    async def initialize(self) -> None:
        if not self._on():
            logger.info("[llm_billing] 配置已关闭，跳过 provider 包装")
            return
        self.store = Store()
        patched, classes = patch_providers()
        logger.info("[llm_billing] 已包装 %s 个方法，覆盖 %s 个 Provider 类: %s",
                    patched, len(classes), classes)
        self._flush_task = asyncio.create_task(self._flush_loop())
        self._report_task = asyncio.create_task(self._report_loop())

    async def terminate(self) -> None:
        for task in (self._flush_task, self._report_task):
            if task:
                task.cancel()
        await self._flush()
        n = unpatch_providers()
        logger.info("[llm_billing] 已还原 %s 个 Provider 方法", n)

    def _on(self) -> bool:
        return bool(self.config.get("bill_enabled", True))

    # ---------------- 后台任务 ----------------
    async def _flush_loop(self) -> None:
        while True:
            await asyncio.sleep(5)
            try:
                await self._flush()
                n, _ = patch_providers()
                if n:
                    logger.info("[llm_billing] 补包装 %s 个新 Provider 方法", n)
            except asyncio.CancelledError:
                raise
            except Exception as e:  # noqa: BLE001
                logger.warning("[llm_billing] flush 失败: %s", e)

    async def _flush(self) -> None:
        if not self.store or not _BUFFER:
            return
        rows, _BUFFER[:] = _BUFFER[:], []
        try:
            await asyncio.to_thread(self.store.insert, rows)
        except Exception as e:  # noqa: BLE001
            logger.warning("[llm_billing] 写入失败，丢弃 %s 条: %s", len(rows), e)

    async def _report_loop(self) -> None:
        while True:
            await asyncio.sleep(30)
            try:
                if not self.config.get("bill_report_enabled", False):
                    continue
                now = datetime.now()  # noqa: DTZ005 - 按服务器本地时区
                target_time = str(self.config.get("bill_report_time", "21:00") or "21:00").strip()
                if now.strftime("%H:%M") != target_time:
                    continue
                today = now.strftime("%Y-%m-%d")
                if self.store and self.store.get_meta("last_report") == today:
                    continue
                await self._flush()
                text = self._render("今日", 0)
                sent = await self._broadcast(text)
                if sent and self.store:
                    self.store.set_meta("last_report", today)
                    self.store.cleanup(int(self.config.get("bill_retention_days", 180) or 0))
            except asyncio.CancelledError:
                raise
            except Exception as e:  # noqa: BLE001
                logger.warning("[llm_billing] 日报失败: %s", e)

    def _targets(self) -> list[str]:
        raw = self.config.get("bill_report_targets", [])
        if isinstance(raw, str):
            raw = [x for x in raw.replace("，", ",").split(",")]
        targets = []
        for item in raw or []:
            text = str(item or "").strip()
            if not text:
                continue
            if ":" not in text and self.store:
                umo = self.store.recent_umo_for(text)
                text = umo or text
            targets.append(text)
        return targets

    async def _broadcast(self, text: str) -> bool:
        ok = False
        for session in self._targets():
            try:
                if await self.context.send_message(session, MessageChain().message(text)):
                    ok = True
                else:
                    logger.warning("[llm_billing] 日报发送失败: %s", session)
            except Exception as e:  # noqa: BLE001
                logger.warning("[llm_billing] 日报发送异常 %s: %s", session, e)
        return ok

    # ---------------- 归因用：记录当前会话 ----------------
    @filter.on_llm_request()
    async def _mark_umo(self, event: AstrMessageEvent, req: ProviderRequest):
        try:
            _UMO.set(event.unified_msg_origin or "")
        except Exception:  # noqa: BLE001, S110
            pass

    # ---------------- 查询命令 ----------------
    @filter.command("账单")
    async def bill_command(self, event: AstrMessageEvent):
        """查询 LLM 全量账单：账单 / 账单 周 / 账单 月 / 账单 来源 / 账单 模型 / 账单 插件。"""
        if not self._on() or not self.store:
            event.stop_event()
            yield event.make_result().message("LLM 账单插件已关闭。")
            return
        if not self._allowed(event):
            event.stop_event()
            yield event.make_result().message("没有查看账单的权限。")
            return
        await self._flush()
        parts = (event.get_message_str() or "").split()
        arg = parts[1].strip().lower() if len(parts) > 1 else ""
        if arg in ("周", "week", "7", "7d"):
            text = self._render("近 7 天", 7)
        elif arg in ("月", "month", "30", "30d"):
            text = self._render("近 30 天", 30)
        elif arg in ("来源", "origin", "谁"):
            text = self._render_origin(0)
        elif arg in ("模型", "model"):
            text = self._render_group("今日按模型", "model", 0)
        elif arg in ("插件", "plugin"):
            text = self._render_group("今日按插件", "plugin", 0)
        elif arg in ("明细", "最近", "recent"):
            text = self._render_recent()
        else:
            text = self._render("今日", 0)
        event.stop_event()
        yield event.make_result().message(text)

    def _allowed(self, event: AstrMessageEvent) -> bool:
        if not self.config.get("bill_admin_only", True):
            return True
        if str(getattr(event, "role", "") or "").lower() == "admin":
            return True
        try:
            admins = self.context.get_config().get("admins_id") or []
            return str(event.get_sender_id() or "") in {str(a) for a in admins}
        except Exception:  # noqa: BLE001
            return False

    # ---------------- 渲染 ----------------
    def _prices(self) -> dict[str, tuple[float, float, float]]:
        return parse_prices(self.config.get("bill_prices"))

    def _money(self, usd: float) -> str:
        symbol = str(self.config.get("bill_currency_symbol", "¥") or "¥")
        rate = float(self.config.get("bill_usd_cny", 7.2) or 7.2)
        return f"{symbol}{usd * rate:.2f}"

    def _window(self, days: int) -> tuple[float, float]:
        now = datetime.now()  # noqa: DTZ005 - 按服务器本地时区
        start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        if days > 0:
            start = start - timedelta(days=days - 1)
        return start.timestamp(), (start + timedelta(days=days if days > 0 else 1)).timestamp()

    def _origin_costs(self, start: float, end: float) -> tuple[dict[str, float], dict[str, int]]:
        """按来源分摊成本：来源本身不含单价，必须交叉 model 才能算钱。"""
        prices = self._prices()
        cost: dict[str, float] = {}
        calls: dict[str, int] = {}
        for row in self.store.group_pairs("origin", "model", start, end):
            key = row["a"]
            cost[key] = cost.get(key, 0.0) + compute_cost(
                prices, "", row["b"], row["in_other"], row["in_cached"], row["out"])
            calls[key] = calls.get(key, 0) + row["calls"]
        return cost, calls

    def _render(self, title: str, days: int) -> str:
        start, end = self._window(days)
        total = self.store.totals(start, end)
        prices = self._prices()
        by_model = self.store.group("model", start, end, limit=4)
        usd = sum(
            compute_cost(prices, "", row["key"], row["in_other"], row["in_cached"], row["out"])
            for row in self.store.group("model", start, end, limit=50)
        )
        lines = [
            f"📊 LLM 账单 · {title}",
            f"调用 {total['calls']} 次（失败 {total['errors']}）｜花费 {self._money(usd)}",
            (
                f"输入 {_fmt_tokens(total['in_other'])}"
                f"（缓存命中 {_fmt_tokens(total['in_cached'])}）｜输出 {_fmt_tokens(total['out'])}"
            ),
        ]
        origin_cost, origin_calls = self._origin_costs(start, end)
        if origin_cost:
            lines.append("按来源：")
            for key, cost in sorted(origin_cost.items(), key=lambda kv: kv[1], reverse=True)[:5]:
                lines.append(
                    f"  · {ORIGIN_LABELS.get(key, key)}：{origin_calls.get(key, 0)} 次 "
                    f"{self._money(cost)}"
                )
        if by_model:
            lines.append("按模型：")
            for row in by_model:
                cost = compute_cost(prices, "", row["key"], row["in_other"], row["in_cached"], row["out"])
                lines.append(f"  · {row['key']}：{row['calls']} 次 {self._money(cost)}")
        lines.append("（/账单 来源｜/账单 模型｜/账单 插件｜/账单 明细｜/账单 周｜/账单 月）")
        return "\n".join(lines)

    def _render_recent(self, limit: int = 8) -> str:
        rows = self.store.recent(limit)
        if not rows:
            return "📊 最近调用：暂无数据"
        prices = self._prices()
        lines = ["📊 最近调用（时间｜来源｜插件｜token）"]
        for index, row in enumerate(rows):
            ts = datetime.fromtimestamp(row["ts"]).strftime("%H:%M:%S")  # noqa: DTZ006 - 本地时区
            label = ORIGIN_LABELS.get(row["origin"], row["origin"])
            cost = compute_cost(prices, row["pid"], row["model"],
                                row["in_other"], row["in_cached"], row["out"])
            lines.append(
                f"  {ts} {label}｜{row['plugin'] or '内置'}｜"
                f"{_fmt_tokens(row['in_other'])}(+{_fmt_tokens(row['in_cached'])} 缓存)"
                f"→{_fmt_tokens(row['out'])}｜{self._money(cost)}"
            )
            if index < 3 and row.get("stack"):
                lines.append(f"      ↳ {str(row['stack'])[:110]}")
        return "\n".join(lines)

    def _render_origin(self, days: int) -> str:
        start, end = self._window(days)
        cost, calls = self._origin_costs(start, end)
        if not cost:
            return "📊 今日按来源：暂无数据"
        lines = ["📊 今日按来源（花费 / 次数）"]
        for key, value in sorted(cost.items(), key=lambda kv: kv[1], reverse=True):
            lines.append(f"  · {ORIGIN_LABELS.get(key, key)}：{self._money(value)} / {calls.get(key, 0)} 次")
        return "\n".join(lines)

    def _render_group(self, title: str, dim: str, days: int) -> str:
        start, end = self._window(days)
        prices = self._prices()
        rows = self.store.group(dim, start, end, limit=10)
        if not rows:
            return f"📊 {title}：暂无数据"
        lines = [f"📊 {title}"]
        for row in rows:
            cost = compute_cost(prices, "", row["key"], row["in_other"], row["in_cached"], row["out"])
            label = ORIGIN_LABELS.get(row["key"], row["key"]) if dim == "origin" else row["key"]
            lines.append(
                f"  · {label}：{row['calls']} 次 {self._money(cost)}"
                f"（缓存 {_fmt_tokens(row['in_cached'])}）"
            )
        return "\n".join(lines)
