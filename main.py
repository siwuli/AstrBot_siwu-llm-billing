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
归因采集的是**协程创建时**的栈：agent 会把调用塞进子任务执行（_await_or_stop 里的
create_task、wait_for 等），执行栈里只剩 asyncio 内部帧，只有创建栈才含调用方。

向量化调用（embedding）同样登记：provider 层采集调用方，SDK 层（OpenAI 兼容 /
阿里云百炼）钩子读 usage 里的输入 token。

单价支持峰谷：键写 ``model@peak`` / ``model@offpeak``，时段由 bill_peak_windows 定义。

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
import re
import sqlite3
import sys
import threading
import time
from datetime import datetime, timedelta, timezone

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

# model 名 -> (非缓存输入, 缓存命中输入, 输出)，单位：USD / 百万 token
# 键可带 @peak / @offpeak 后缀，表示高峰 / 空闲时段价（见 bill_peak_windows）
DEFAULT_PRICES: dict[str, tuple[float, float, float]] = {
    # 小米 MiMo（官方英文价，USD）
    "mimo-v2.6-flash": (0.14, 0.0028, 0.28),
    "mimo-v2.6-pro": (0.435, 0.0036, 0.87),
    "mimo-v2.6-pro-ultraspeed": (4.35, 0.036, 8.7),
    # 已下线但仍可调用的 MiMo V2.5（沿用原价）
    "mimo-v2.5": (0.14, 0.0028, 0.28),
    "mimo-v2.5-pro": (0.435, 0.0036, 0.87),
    # DeepSeek V4.1 Flash 系列（官方 ¥2/¥0.04/¥8 高峰，¥1/¥0.02/¥4 空闲，按 7.2 折 USD）
    "deepseek-v4.1-flash@peak": (0.2778, 0.0056, 1.1111),
    "deepseek-v4.1-flash@offpeak": (0.1389, 0.0028, 0.5556),
    "deepseek-v4-flash@peak": (0.2778, 0.0056, 1.1111),
    "deepseek-v4-flash@offpeak": (0.1389, 0.0028, 0.5556),
    "deepseek-v4-flash-vision-exp@peak": (0.2778, 0.0056, 1.1111),
    "deepseek-v4-flash-vision-exp@offpeak": (0.1389, 0.0028, 0.5556),
    # DeepSeek V4 Pro（官方 ¥9/¥0.30/¥27 高峰，¥4.5/¥0.15/¥13.5 空闲）
    "deepseek-v4-pro@peak": (1.25, 0.0417, 3.75),
    "deepseek-v4-pro@offpeak": (0.625, 0.0208, 1.875),
    # 硅基流动 Qwen3-VL-32B-Instruct（输入 $0.2 / 输出 $0.6 每百万 token）
    "Qwen/Qwen3-VL-32B-Instruct": (0.2, 0.2, 0.6),
    # 向量模型：前两格填输入价（缓存/输出不适用）
    "Qwen/Qwen3-Embedding-4B": (0.02, 0.02, 0.02),
    "qwen3.7-text-embedding-flash": (0.0174, 0.0174, 0.0174),
}

# 高峰时段定义：<周几> <HH:MM>-<HH:MM>，分号分隔；1=周一 … 7=周日。
# 默认取自 DeepSeek 官方口径（北京时间周一至周五 9:00-12:00、14:00-18:00，
# 其余时段含周末与法定节假日为空闲时段）。
DEFAULT_PEAK_WINDOWS = "1-5 09:00-12:00; 1-5 14:00-18:00"

ORIGIN_LABELS = {
    "reply": "回复生成（管线）",
    "caption": "图片描述（内置）",
    "decision": "群聊决策/读空气",
    "jargon": "黑话挖掘",
    "learning": "风格/人格学习",
    "memory": "记忆抽取/反思",
    "embedding": "向量化（embedding）",
    "other": "其他插件",
    "unknown": "未知来源",
}

# 归因规则：栈里出现这些模块/函数名 → 归类（顺序敏感）
_ORIGIN_RULES: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("caption", ("get_image_caption", "group_chat_context")),
    ("memory", ("memory_processor", "memory_reflection", "memory_engine", "consolidation_manager")),
    ("learning", ("progressive_learning", "realtime_processor", "style_analyzer",
                  "persona_updater", "expression_learning", "expression_pattern",
                  "framework_llm_adapter")),
    ("jargon", ("jargon_miner", "jargon_query", "jargon_")),
    ("decision", ("decision_ai", "probability_manager", "proactive_chat", "attention_manager")),
)
_PIPELINE_MARKS = ("process_stage", "agent_sub_stages", "third_party", "core.pipeline",
                   "astr_main_agent")


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
        key = re.sub(r"\s*@\s*", "@", key.strip())
        prices[key] = (nums[0], nums[1], nums[2])
    return prices


# 中国法定节假日（官方峰谷口径：节假日全天计入空闲时段）
#
# DeepSeek 价目页脚注：「北京时间周一至周五（不含中国法定节假日）9:00-12:00、
# 14:00-18:00 为高峰时段；其余时段，包括周末及中国法定节假日全天均为空闲时段。」
# 所以只排除周末不够：节假日落在一周内时，峰段也必须按闲时计，否则会多计一倍。
# 数据来源：国务院办公厅关于 2026 年部分节假日安排的通知（国办发明电〔2025〕7 号）。
# 调休补班的周六/周日不计高峰——计费规则只看「周一至周五」，调休不改变这一点。
CN_HOLIDAYS_2026: tuple[str, ...] = (
    "2026-01-01", "2026-01-02", "2026-01-03",                          # 元旦
    "2026-02-15", "2026-02-16", "2026-02-17", "2026-02-18", "2026-02-19",
    "2026-02-20", "2026-02-21", "2026-02-22", "2026-02-23",            # 春节
    "2026-04-04", "2026-04-05", "2026-04-06",                          # 清明
    "2026-05-01", "2026-05-02", "2026-05-03", "2026-05-04", "2026-05-05",  # 劳动节
    "2026-06-19", "2026-06-20", "2026-06-21",                          # 端午
    "2026-09-25", "2026-09-26", "2026-09-27",                          # 中秋
    "2026-10-01", "2026-10-02", "2026-10-03", "2026-10-04",
    "2026-10-05", "2026-10-06", "2026-10-07",                          # 国庆
)
DEFAULT_HOLIDAYS = ",".join(CN_HOLIDAYS_2026)
DEFAULT_TIMEZONE = "Asia/Shanghai"
_HOLIDAY_CACHE: dict[str, set[str]] = {}


def normalize_holidays(raw: object) -> set[str]:
    """把节假日配置规整成 ``{'YYYY-MM-DD'}``；``off`` / ``none`` / ``-`` 表示停用。"""
    if isinstance(raw, (list, tuple)):
        items = [str(item) for item in raw]
    else:
        text = str(raw or "")
        if text.strip().lower() in {"off", "none", "-", "0"}:
            return set()
        items = re.split(r"[\s,，;；]+", text)
    out = set()
    for item in items:
        match = re.fullmatch(r"(\d{4})[-/.](\d{1,2})[-/.](\d{1,2})", str(item).strip())
        if match:
            out.add(f"{int(match.group(1)):04d}-{int(match.group(2)):02d}-{int(match.group(3)):02d}")
    return out


_UTC_NAMES = {"utc", "etc/utc", "gmt", "etc/gmt", "z"}
_TZ_WARNED: set[str] = set()


def _zone(name: object):
    """取时区；UTC 直接用 stdlib（有的镜像把 /usr/share/zoneinfo/UTC 覆盖成了本地时区）。"""
    key = str(name or "").strip() or DEFAULT_TIMEZONE
    if key.lower() in _UTC_NAMES:
        return timezone.utc
    try:
        from zoneinfo import ZoneInfo
        return ZoneInfo(key)
    except Exception as e:  # noqa: BLE001 - 缺 tzdata 时退回服务器本地时区（并告警一次）
        if key not in _TZ_WARNED:
            _TZ_WARNED.add(key)
            logger.warning("[llm_billing] 时区 %s 不可用（%s），峰谷判定退回服务器本地时区", key, e)
        return None


def _holiday_set(raw: object) -> set[str]:
    key = raw if isinstance(raw, str) else repr(raw)
    if key not in _HOLIDAY_CACHE:
        if len(_HOLIDAY_CACHE) > 32:
            _HOLIDAY_CACHE.clear()
        _HOLIDAY_CACHE[key] = normalize_holidays(raw)
    return _HOLIDAY_CACHE[key]


def parse_peak_windows(raw: object) -> list[tuple[set[int], int, int]]:
    """解析高峰时段配置：``1-5 09:00-12:00; 1-5 14:00-18:00``。

    返回 [(周几集合, 起始分钟, 结束分钟)]，1=周一 … 7=周日。
    """
    rules: list[tuple[set[int], int, int]] = []
    text = str(raw or "").replace("；", ";").replace("，", ",")
    for chunk in text.split(";"):
        chunk = chunk.strip()
        if not chunk:
            continue
        parts = chunk.split()
        if len(parts) != 2 or "-" not in parts[1]:
            continue
        days: set[int] = set()
        for piece in parts[0].replace("，", ",").split(","):
            piece = piece.strip()
            if "-" in piece:
                a, _, b = piece.partition("-")
                try:
                    days.update(range(int(a), int(b) + 1))
                except ValueError:
                    continue
            else:
                try:
                    days.add(int(piece))
                except ValueError:
                    continue
        try:
            start, _, end = parts[1].partition("-")
            sh, sm = (int(x) for x in start.split(":"))
            eh, em = (int(x) for x in end.split(":"))
        except ValueError:
            continue
        if days:
            rules.append((days, sh * 60 + sm, eh * 60 + em))
    return rules


def is_peak(ts: float, windows: object, holidays: object = "", tz: object = DEFAULT_TIMEZONE) -> bool:
    """判断某次调用是否落在高峰时段（口径见 CN_HOLIDAYS_2026 上方注释）。"""
    try:
        when = datetime.fromtimestamp(ts, _zone(tz))
    except (OSError, OverflowError, ValueError):
        return False
    if when.strftime("%Y-%m-%d") in _holiday_set(holidays):
        return False
    minute = when.hour * 60 + when.minute
    weekday = when.isoweekday()
    for days, start, end in parse_peak_windows(windows):
        if weekday in days and start <= minute < end:
            return True
    return False


def match_price(prices: dict[str, tuple[float, float, float]], pid: str, model: str,
                window: str = ""):
    """按 ``provider|model`` → ``model`` → ``provider`` → 模糊最长匹配找单价。

    ``window`` 为 ``peak`` / ``offpeak`` 时优先用带 ``@peak`` / ``@offpeak`` 后缀的单价，
    其次用不带后缀的通用价，最后退回另一时段价（哪个时段没配就用另一个）。
    """
    pid = str(pid or "")
    model = str(model or "")
    suffixes = [f"@{window}"] if window else []
    suffixes += ["", f"@{'offpeak' if window == 'peak' else 'peak'}"] if window else [""]
    for suffix in suffixes:
        for key in (f"{pid}|{model}", model, pid, pid.split("/")[-1]):
            candidate = f"{str(key or '').strip()}{suffix}"
            if candidate.strip("@") and candidate in prices:
                return prices[candidate]
    best = None
    hay = f"{pid}|{model}".lower()
    for key, val in prices.items():
        base, _, suffix = key.partition("@")
        if suffix and suffix != window:
            continue
        k = base.lower().strip()
        hit = k and (k in hay or (model and k == str(model).lower()))
        if hit and (best is None or len(k) > len(best[0])):
            best = (k, val)
    return best[1] if best else None


def compute_cost(prices, pid: str, model: str, in_other: int, in_cached: int, out: int,
                 window: str = "") -> float:
    price = match_price(prices, pid, model, window)
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


def _stack_info(start=None):
    """收集调用栈（跳过 provider 自身与包装层），返回 (frames, 插件目录名)。

    ``start`` 为起始帧，默认取本函数的上上层。协程**创建时**的帧链同样用本函数
    采集——AstrBot 会把 provider 调用丢进子任务执行（见 :func:`_wrap`），那时
    执行栈里只剩 asyncio 内部帧。
    """
    frames: list[str] = []
    plugin = ""
    try:
        f = start if start is not None else sys._getframe(2)
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


def _emit(self, meth: str, resp, kw: dict, started: float, err: str | None,
          created: tuple[list[str], str] | None = None) -> None:
    """落一条记录。``created`` 是协程创建时采到的 (frames, plugin)，优先用它归因。"""
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
        if created:
            made_frames, made_plugin = created
            frames = made_frames + [f for f in frames if f not in made_frames]
            plugin = made_plugin or plugin
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
            "thread": threading.current_thread().name,
            "stack": " | ".join(frames[:4])[:400],
            "err": (err or "")[:200],
        })
        del _BUFFER[:-2000]
    except Exception as e:  # noqa: BLE001
        logger.debug("[llm_billing] 记录失败: %s", e)


def _wrap(meth: str, fn):
    """包装 provider 调用，并在**协程创建时**采一次调用栈。

    AstrBot 的 agent 会把 provider 调用塞进子任务再 await
    （``tool_loop_agent_runner._await_or_stop`` 里的 ``asyncio.create_task``、
    ``provider.test`` 的 ``asyncio.wait_for``、WebChat 的 ``create_task`` 等），
    子任务执行时栈里只剩 asyncio 内部帧，调用方（管线或插件）全部丢失，
    归因于是退化成「未知来源」。协程对象是在调用方帧里构造的，所以包装层先
    同步采一次栈，执行时再把「创建栈 + 执行栈」一起交给归因。
    """
    if meth == "text_chat":
        def wrapper(self, *args, **kwargs):
            created = _stack_info(sys._getframe(1))
            coro = fn(self, *args, **kwargs)

            async def _invoke():
                started, resp, err = time.time(), None, None
                try:
                    resp = await coro
                    return resp
                except Exception as e:
                    err = f"{type(e).__name__}: {e}"
                    raise
                finally:
                    _emit(self, meth, resp, kwargs, started, err, created)
            return _invoke()

        if hasattr(inspect, "markcoroutinefunction"):  # 3.12+：保持协程函数语义
            inspect.markcoroutinefunction(wrapper)
        return wrapper

    def wrapper_stream(self, *args, **kwargs):
        created = _stack_info(sys._getframe(1))
        agen = fn(self, *args, **kwargs)

        async def _iterate():
            started, last, err = time.time(), None, None
            try:
                async for item in agen:
                    last = item
                    yield item
            except Exception as e:
                err = f"{type(e).__name__}: {e}"
                raise
            finally:
                _emit(self, meth, last, kwargs, started, err, created)
        return _iterate()
    return wrapper_stream


# ----------------------------------------------------------------------
# 向量化（embedding）通道
# ----------------------------------------------------------------------
# provider 层采集到的调用方，供 SDK 层钩子拿到 usage 后落账
_EMB_CTX: contextvars.ContextVar[dict | None] = contextvars.ContextVar("siwu_bill_emb", default=None)
_EMB_ORIGINALS: list[tuple[object, str, object]] = []
_EMB_PATCHED: set[str] = set()
_EMB_PROVIDERS: dict[tuple[type, str], object] = {}


def _push(row: dict) -> None:
    _BUFFER.append(row)
    del _BUFFER[:-2000]


def _emb_texts(args: tuple, kwargs: dict) -> list[str]:
    if args:
        first = args[0]
        items = first if isinstance(first, (list, tuple)) else [first]
    else:
        raw = kwargs.get("input") or kwargs.get("texts") or kwargs.get("text") or ""
        items = raw if isinstance(raw, (list, tuple)) else [raw]
    return [str(item) for item in items]


def _emb_usage(resp) -> int:
    """从各家响应里取输入 token 数（拿不到就记 0）。"""
    usage = getattr(resp, "usage", None)
    if usage is None:
        return 0
    for key in ("prompt_tokens", "input_tokens", "total_tokens"):
        value = usage.get(key) if isinstance(usage, dict) else getattr(usage, key, None)
        if value:
            try:
                return int(value)
            except (TypeError, ValueError):
                return 0
    return 0


def _emb_model(resp, kwargs: dict, ctx: dict | None = None) -> str:
    model = getattr(resp, "model", "") or kwargs.get("model") or ""
    if not model and ctx:
        model = str(ctx.get("model") or "")
    return str(model)


def _emit_embedding(model: str, tokens: int, texts: list[str], started: float,
                    err: str | None, created: tuple[list[str], str] | None = None) -> None:
    """记一条 embedding 调用：只有输入 token，来源固定为「向量化」。"""
    try:
        ctx = _EMB_CTX.get() or {}
        frames, plugin = created or ([], "")
        made = ctx.get("frames") or []
        if made:
            frames = made + [f for f in frames if f not in made]
        _push({
            "ts": time.time(),
            "kind": "embedding",
            "cls": "",
            "pid": str(ctx.get("pid") or ""),
            "model": model or str(ctx.get("model") or ""),
            "dur_ms": round((time.time() - started) * 1000, 1),
            "prompt_chars": sum(len(text) for text in texts),
            "system_chars": len(texts),
            "in_other": tokens, "in_cached": 0, "out": 0,
            "origin": "embedding",
            "plugin": str(ctx.get("plugin") or plugin),
            "umo": _UMO.get(),
            "thread": threading.current_thread().name,
            "stack": " | ".join(frames[:4])[:400],
            "err": (err or "")[:200],
        })
    except Exception as e:  # noqa: BLE001
        logger.debug("[llm_billing] embedding 记录失败: %s", e)


def _emb_provider_model(self) -> str:
    try:
        model = getattr(self, "model", "") or ""
        if not model:
            model = str((getattr(self, "provider_config", None) or {}).get("embedding_model") or "")
        return str(model)
    except Exception:  # noqa: BLE001
        return ""


def _wrap_embedding_provider(fn):
    """包一层只为采集调用方；token 由 SDK 层钩子在拿到 usage 后落账。"""
    def wrapper(self, *args, **kwargs):
        frames, plugin = _stack_info(sys._getframe(1))
        ctx = {"frames": frames, "plugin": plugin, "model": _emb_provider_model(self)}
        token = _EMB_CTX.set(ctx)
        try:
            result = fn(self, *args, **kwargs)
        except Exception:
            _EMB_CTX.reset(token)
            raise
        if inspect.iscoroutine(result):
            async def _await_it():
                try:
                    return await result
                finally:
                    _EMB_CTX.reset(token)
            return _await_it()
        _EMB_CTX.reset(token)
        return result

    wrapper._siwu_billing = True
    return wrapper


def _wrap_openai_embedding_create(original, is_async: bool):
    if not is_async:
        def create_sync(self, *args, **kwargs):
            created = _stack_info(sys._getframe(1))
            texts = _emb_texts(args, kwargs)
            started, resp, err = time.time(), None, None
            try:
                resp = original(self, *args, **kwargs)
                return resp
            except Exception as e:
                err = f"{type(e).__name__}: {e}"
                raise
            finally:
                _emit_embedding(_emb_model(resp, kwargs), _emb_usage(resp), texts, started, err, created)
        return create_sync

    def create(self, *args, **kwargs):
        created = _stack_info(sys._getframe(1))
        texts = _emb_texts(args, kwargs)
        started = time.time()
        coro = original(self, *args, **kwargs)

        async def _run():
            resp, err = None, None
            try:
                resp = await coro
                return resp
            except Exception as e:
                err = f"{type(e).__name__}: {e}"
                raise
            finally:
                _emit_embedding(_emb_model(resp, kwargs), _emb_usage(resp), texts, started, err, created)
        return _run()
    return create


def _wrap_dashscope_embedding_call(original):
    def call(*args, **kwargs):
        created = _stack_info(sys._getframe(1))
        texts = _emb_texts(args, kwargs)
        started, resp, err = time.time(), None, None
        try:
            resp = original(*args, **kwargs)
            return resp
        except Exception as e:
            err = f"{type(e).__name__}: {e}"
            raise
        finally:
            _emit_embedding(str(kwargs.get("model") or ""), _emb_usage(resp), texts, started, err, created)
    return call


def patch_embedding_providers() -> tuple[int, list[str]]:
    """包装 EmbeddingProvider 的 get_embedding(s)，用于归因（幂等）。"""
    try:
        from astrbot.core.provider.provider import EmbeddingProvider
    except Exception as e:  # noqa: BLE001
        logger.debug("[llm_billing] 找不到 EmbeddingProvider: %s", e)
        return 0, []

    classes: list[type] = [EmbeddingProvider]

    def walk(cls):
        for sub in cls.__subclasses__():
            classes.append(sub)
            walk(sub)

    walk(EmbeddingProvider)
    patched = 0
    for cls in classes:
        for meth in ("get_embedding", "get_embeddings"):
            fn = cls.__dict__.get(meth)
            if fn is None or getattr(fn, "_siwu_billing", False):
                continue
            if not inspect.iscoroutinefunction(fn):
                continue
            setattr(cls, meth, _wrap_embedding_provider(fn))
            _EMB_PROVIDERS[(cls, meth)] = fn
            patched += 1
    return patched, [c.__name__ for c in classes]


def unpatch_embedding_providers() -> int:
    n = 0
    for (cls, meth), fn in list(_EMB_PROVIDERS.items()):
        try:
            setattr(cls, meth, fn)
            n += 1
        except Exception:  # noqa: BLE001, S110
            pass
        _EMB_PROVIDERS.pop((cls, meth), None)
    return n


def patch_embeddings() -> tuple[int, list[str]]:
    """挂上能拿到 usage 的 embedding SDK 钩子（OpenAI 兼容 / 阿里云百炼）。"""
    hooked: list[str] = []

    def _hook(obj, name: str, factory) -> None:
        if obj is None:
            return
        key = f"{getattr(obj, '__module__', '?')}.{getattr(obj, '__name__', name)}"
        if key in _EMB_PATCHED:
            return
        raw = obj.__dict__.get(name)
        original = getattr(obj, name, None)  # classmethod 要取绑定后的可调用对象
        if raw is None or original is None:
            return
        try:
            setattr(obj, name, factory(original))
        except Exception as e:  # noqa: BLE001
            logger.debug("[llm_billing] embedding 钩子挂载失败 %s: %s", key, e)
            return
        _EMB_ORIGINALS.append((obj, name, raw))
        _EMB_PATCHED.add(key)
        hooked.append(key)

    try:
        from openai.resources.embeddings import AsyncEmbeddings, Embeddings
        _hook(AsyncEmbeddings, "create", lambda fn: _wrap_openai_embedding_create(fn, True))
        _hook(Embeddings, "create", lambda fn: _wrap_openai_embedding_create(fn, False))
    except Exception as e:  # noqa: BLE001
        logger.debug("[llm_billing] openai embedding 钩子不可用: %s", e)

    try:
        import dashscope
        for cls_name in ("TextEmbedding", "MultiModalEmbedding"):
            _hook(getattr(dashscope, cls_name, None), "call", _wrap_dashscope_embedding_call)
    except Exception as e:  # noqa: BLE001
        logger.debug("[llm_billing] dashscope embedding 钩子不可用: %s", e)

    return len(hooked), hooked


def unpatch_embeddings() -> int:
    n = 0
    for obj, name, original in reversed(_EMB_ORIGINALS):
        try:
            setattr(obj, name, original)
            n += 1
        except Exception:  # noqa: BLE001, S110
            pass
    _EMB_ORIGINALS.clear()
    _EMB_PATCHED.clear()
    return n


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
                    origin TEXT, plugin TEXT, umo TEXT, stack TEXT, thread TEXT, err TEXT)"""
            )
            conn.execute("CREATE INDEX IF NOT EXISTS idx_calls_ts ON calls(ts)")
            conn.execute("CREATE TABLE IF NOT EXISTS meta(k TEXT PRIMARY KEY, v TEXT)")
            # 迁移：老库补 stack / thread 列（诊断归因用）
            cols = {row[1] for row in conn.execute("PRAGMA table_info(calls)")}
            if "stack" not in cols:
                conn.execute("ALTER TABLE calls ADD COLUMN stack TEXT")
            if "thread" not in cols:
                conn.execute("ALTER TABLE calls ADD COLUMN thread TEXT")

    def insert(self, rows: list[dict]) -> None:
        if not rows:
            return
        for row in rows:
            row.setdefault("stack", "")
            row.setdefault("thread", "")
        with self._conn() as conn:
            conn.executemany(
                """INSERT INTO calls(ts,kind,cls,pid,model,dur_ms,prompt_chars,system_chars,
                       in_other,in_cached,out,origin,plugin,umo,stack,thread,err)
                   VALUES(:ts,:kind,:cls,:pid,:model,:dur_ms,:prompt_chars,:system_chars,
                       :in_other,:in_cached,:out,:origin,:plugin,:umo,:stack,:thread,:err)""",
                rows,
            )

    def totals(self, start: float, end: float) -> dict:
        with self._conn() as conn:
            row = conn.execute(
                """SELECT count(*), coalesce(sum(in_other),0), coalesce(sum(in_cached),0),
                          coalesce(sum(out),0), coalesce(sum(case when err<>'' then 1 else 0 end),0)
                   FROM calls WHERE ts>=? AND ts<?""", (start, end)).fetchone()
        return {"calls": row[0], "in_other": row[1], "in_cached": row[2], "out": row[3], "errors": row[4]}

    def rows(self, start: float, end: float) -> list[dict]:
        """区间内逐条调用：峰谷价按调用时刻判定，所以报表必须逐行计价。"""
        with self._conn() as conn:
            rows = conn.execute(
                """SELECT ts, kind, pid, model, in_other, in_cached, out, origin, plugin
                   FROM calls WHERE ts>=? AND ts<?""", (start, end)).fetchall()
        keys = ("ts", "kind", "pid", "model", "in_other", "in_cached", "out", "origin", "plugin")
        return [dict(zip(keys, row)) for row in rows]

    def recent(self, limit: int = 8) -> list[dict]:
        """最近若干次调用（含调用栈快照），用于核对归因。"""
        with self._conn() as conn:
            rows = conn.execute(
                """SELECT ts,kind,pid,model,in_other,in_cached,out,origin,plugin,stack,thread
                   FROM calls ORDER BY ts DESC LIMIT ?""", (limit,)).fetchall()
        keys = ("ts", "kind", "pid", "model", "in_other", "in_cached", "out",
                "origin", "plugin", "stack", "thread")
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
        emb_providers, emb_classes = patch_embedding_providers()
        emb_hooks, hooks = patch_embeddings()
        logger.info("[llm_billing] 已包装 %s 个方法，覆盖 %s 个 Provider 类: %s",
                    patched, len(classes), classes)
        logger.info("[llm_billing] embedding：%s 个 provider 方法（%s）、%s 个出账钩子 %s",
                    emb_providers, emb_classes, emb_hooks, hooks)
        self._flush_task = asyncio.create_task(self._flush_loop())
        self._report_task = asyncio.create_task(self._report_loop())

    async def terminate(self) -> None:
        for task in (self._flush_task, self._report_task):
            if task:
                task.cancel()
        await self._flush()
        n = unpatch_providers()
        m = unpatch_embedding_providers()
        k = unpatch_embeddings()
        logger.info("[llm_billing] 已还原 %s 个 Provider 方法、%s 个 embedding 方法、%s 个 embedding 钩子",
                    n, m, k)

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
                m, _ = patch_embedding_providers()
                if m:
                    logger.info("[llm_billing] 补包装 %s 个新 embedding 方法", m)
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

    def _peak_windows(self) -> str:
        return str(self.config.get("bill_peak_windows", DEFAULT_PEAK_WINDOWS)
                   or DEFAULT_PEAK_WINDOWS)

    def _holidays(self) -> str:
        raw = self.config.get("bill_holidays", DEFAULT_HOLIDAYS)
        return DEFAULT_HOLIDAYS if raw is None or raw == "" else raw

    def _timezone(self) -> str:
        return str(self.config.get("bill_timezone", DEFAULT_TIMEZONE) or DEFAULT_TIMEZONE)

    def _row_cost(self, prices, row: dict) -> float:
        """逐行计价：同一模型高峰/空闲价不同，必须先定时刻再乘单价。"""
        window = ("peak" if is_peak(row["ts"], self._peak_windows(),
                                    self._holidays(), self._timezone())
                  else "offpeak")
        return compute_cost(prices, row["pid"], row["model"], row["in_other"],
                            row["in_cached"], row["out"], window)

    def _aggregate(self, rows: list[dict], dim: str) -> dict[str, dict]:
        prices = self._prices()
        agg: dict[str, dict] = {}
        for row in rows:
            key = str(row.get(dim) or "")
            if dim == "plugin" and not key:
                key = "内置/核心"  # 内置 star（如图片描述）不属于任何插件
            elif not key:
                key = "(未知)"
            item = agg.setdefault(key, {"calls": 0, "cost": 0.0,
                                        "in_other": 0, "in_cached": 0, "out": 0})
            item["calls"] += 1
            item["in_other"] += row["in_other"] or 0
            item["in_cached"] += row["in_cached"] or 0
            item["out"] += row["out"] or 0
            item["cost"] += self._row_cost(prices, row)
        return agg

    def _window(self, days: int) -> tuple[float, float]:
        now = datetime.now()  # noqa: DTZ005 - 按服务器本地时区
        start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        if days > 0:
            start = start - timedelta(days=days - 1)
        return start.timestamp(), (start + timedelta(days=days if days > 0 else 1)).timestamp()

    def _render(self, title: str, days: int) -> str:
        start, end = self._window(days)
        total = self.store.totals(start, end)
        rows = self.store.rows(start, end)
        prices = self._prices()
        usd = sum(self._row_cost(prices, row) for row in rows)
        lines = [
            f"📊 LLM 账单 · {title}",
            f"调用 {total['calls']} 次（失败 {total['errors']}）｜花费 {self._money(usd)}",
            (
                f"输入 {_fmt_tokens(total['in_other'])}"
                f"（缓存命中 {_fmt_tokens(total['in_cached'])}）｜输出 {_fmt_tokens(total['out'])}"
            ),
        ]
        by_origin = self._aggregate(rows, "origin")
        if by_origin:
            lines.append("按来源：")
            for key, item in sorted(by_origin.items(), key=lambda kv: kv[1]["cost"], reverse=True)[:5]:
                lines.append(f"  · {ORIGIN_LABELS.get(key, key)}：{item['calls']} 次 "
                             f"{self._money(item['cost'])}")
        by_model = self._aggregate(rows, "model")
        if by_model:
            lines.append("按模型：")
            for key, item in sorted(by_model.items(), key=lambda kv: kv[1]["cost"], reverse=True)[:4]:
                lines.append(f"  · {key}：{item['calls']} 次 {self._money(item['cost'])}")
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
            cost = self._row_cost(prices, row)
            if row["kind"] == "embedding":
                detail = f"向量 {_fmt_tokens(row['in_other'])}"
            else:
                detail = (f"{_fmt_tokens(row['in_other'])}(+{_fmt_tokens(row['in_cached'])} 缓存)"
                          f"→{_fmt_tokens(row['out'])}")
            lines.append(
                f"  {ts} {label}｜{row['plugin'] or '内置'}｜{detail}｜{self._money(cost)}"
            )
            if index < 3 and (row.get("stack") or row.get("thread")):
                lines.append(f"      ↳ [{row.get('thread') or '?'}] {str(row.get('stack') or '')[:100]}")
        return "\n".join(lines)

    def _render_origin(self, days: int) -> str:
        start, end = self._window(days)
        agg = self._aggregate(self.store.rows(start, end), "origin")
        if not agg:
            return "📊 今日按来源：暂无数据"
        lines = ["📊 今日按来源（花费 / 次数）"]
        for key, item in sorted(agg.items(), key=lambda kv: kv[1]["cost"], reverse=True):
            lines.append(f"  · {ORIGIN_LABELS.get(key, key)}：{self._money(item['cost'])} / {item['calls']} 次")
        return "\n".join(lines)

    def _render_group(self, title: str, dim: str, days: int) -> str:
        start, end = self._window(days)
        agg = self._aggregate(self.store.rows(start, end), dim)
        if not agg:
            return f"📊 {title}：暂无数据"
        lines = [f"📊 {title}"]
        ranked = sorted(agg.items(), key=lambda kv: kv[1]["cost"], reverse=True)[:10]
        for key, item in ranked:
            label = ORIGIN_LABELS.get(key, key) if dim == "origin" else key
            lines.append(
                f"  · {label}：{item['calls']} 次 {self._money(item['cost'])}"
                f"（缓存 {_fmt_tokens(item['in_cached'])}）"
            )
        return "\n".join(lines)
