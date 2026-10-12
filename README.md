# LLM 全量账单（siwu_llm_billing）

统计**所有** LLM 调用，包括 AstrBot 自带统计看不见的那些，并按「谁发起的」归因花费。

## 为什么需要它

AstrBot 自带的用量统计只在 agent 管线里记账：

```python
# core/pipeline/process_stage/method/agent_sub_stages/internal.py:576
await db_helper.insert_provider_stat(..., agent_type="internal")
```

全项目只有这一个写入点，所以：

| 调用方 | 是否被 AstrBot 记录 |
|---|---|
| `event.request_llm()` 的回复生成 | ✅ 记录 |
| 插件直接 `context.get_using_provider().text_chat(...)` | ❌ **完全不记录** |

被漏掉的典型调用：group_chat_plus 读空气/AI 决策、livingmemory 记忆抽取与反思、
self_learning 黑话 LLM 预筛 / 批学习 / 表达模式学习、图片描述等。

实测某台机器：一天日志里有 **1171** 次 `chat/completions` 请求，`provider_stats` 只有 **24** 行。

同类插件（如基于 `@filter.on_llm_request` / `on_llm_response` 的用量插件）也踩同一个坑——
这两个 hook 由管线触发，插件直连调用不会经过它们，所以它们的「缓存破坏诊断 / 上下文归因」
同样只覆盖管线内那一小部分。

## 它是怎么做的

1. **在 Provider 层包装** `text_chat` / `text_chat_stream`（覆盖 `Provider` 全部子类，
   含 `ProviderOpenAIOfficial` / `ProviderAnthropic` / `ProviderGoogleGenAI` 等重写了这两个方法的实现），
   之后无论调用方是管线还是插件直连，都会经过本插件。
2. 从返回的 `LLMResponse.usage` 取真实 token：非缓存输入 / 缓存命中输入 / 输出
   （流式请求由 `stream_options={"include_usage": True}` 提供，取最后一块的 usage）。
3. **按调用栈归因**：栈里出现 `/data/plugins/<目录>/` 就是哪个插件发起的；
   出现 `agent_sub_stages` / `process_stage` 判定为「回复生成（管线）」；
   出现 `decision_ai` / `memory_processor` / `jargon_miner` / `progressive_learning`
   等模块名则细分为「群聊决策」「记忆抽取/反思」「黑话挖掘」「风格/人格学习」。
4. 本地 SQLite 累积（`data/llm_billing/billing.db`），按配置里的单价表换算金额。

不改动请求内容、不拦截、不注入；包装层的任何异常都被吞掉，不影响主流程。

## 安装

- 面板 → 插件管理 → 从链接安装：`https://github.com/siwuli/AstrBot_siwu-llm-billing`
- 或把本目录放进 `data/plugins/` 后重载插件

无第三方依赖，AstrBot `>=4.0.0`。

## 配置

| 配置项 | 默认 | 说明 |
|---|---|---|
| `bill_enabled` | `true` | 总开关 |
| `bill_admin_only` | `true` | 仅管理员可查询 |
| `bill_currency_symbol` | `¥` | 显示符号 |
| `bill_usd_cny` | `7.2` | 汇率 |
| `bill_prices` | 小米 MiMo 四条 | 每行 `provider\|model = 非缓存输入, 缓存命中, 输出`（USD / 百万 token） |
| `bill_report_enabled` | `false` | 每日日报 |
| `bill_report_time` | `21:00` | 日报时间 |
| `bill_report_targets` | `[]` | 接收会话 UMO，或只填群号（自动补全） |
| `bill_retention_days` | `180` | 数据保留天数 |

单价表按「最长匹配」生效，写法可以是 `provider|model`、`model` 或 `provider`。
小米官方价与 OpenRouter 转售价不同，请以自己账号的单价为准填写。

## 命令

| 命令 | 说明 |
|---|---|
| `账单` | 今日概览：调用次数、花费、token 构成、来源 Top5、模型分布 |
| `账单 来源` | 今日各来源花费与次数（谁在花钱） |
| `账单 模型` / `账单 插件` | 按模型 / 按插件聚合 |
| `账单 明细` | 最近 8 次调用（含调用栈快照，用来核对归因） |
| `账单 周` / `账单 月` | 近 7 天 / 近 30 天 |

## 已知边界

- **只统计安装之后的调用**：历史数据不在本插件库里（AstrBot 的 `provider_stats` 可作对照，
  但它只覆盖管线部分）。
- 需要 provider 返回 usage；不返回 usage 的调用会记 0 token（但次数仍会记）。
- `umo`（会话标识）只有经过管线 hook 的请求才拿得到；插件直连调用记录里该字段为空，
  归因靠 `plugin` + `stack` + `thread`（线程名）三列。
- 归因规则基于调用栈关键字，未命中的会归到「其他插件」并保留调用栈/线程快照（`账单 明细` 可查），
  便于后续补充规则。
- 「账单 插件」里 `内置/核心` 指不属于任何插件、由 AstrBot 内置 star 发起的调用（如群聊图片描述）。
- embedding（如硅基流动向量）不是 `text_chat`，不在统计范围内。
