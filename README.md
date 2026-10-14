# LLM 全量账单（siwu_llm_billing）

统计所有 LLM 调用的 token 与花费，按调用来源归因，支持单价换算与每日日报。

## 功能
- 在 Provider 层包装 text_chat / text_chat_stream
- 插件直连 provider 的调用同样被统计，不只是 agent 管线
- 记录非缓存输入、缓存命中输入和输出三类 token
- 按调用栈归因到回复生成、图片描述、群聊决策、记忆抽取等来源
- 按配置的单价表换算金额，数据存本地 SQLite
- 提供账单查询命令与每日日报推送

## 指令
| 指令 | 说明 |
| --- | --- |
| `账单` | 今日概览：次数、花费、token 构成与来源 |
| `账单 来源` | 今日各来源的花费与次数 |
| `账单 模型` / `账单 插件` | 按模型 / 按插件聚合 |
| `账单 明细` | 最近 8 次调用，含调用栈快照 |
| `账单 周` / `账单 月` | 近 7 天 / 近 30 天 |
默认仅管理员可用，由 bill_admin_only 控制。

## 配置
| 配置项 | 默认 | 说明 |
| --- | --- | --- |
| `bill_enabled` | `true` | 总开关，关闭后不再包装 Provider |
| `bill_admin_only` | `true` | 账单命令仅管理员可用 |
| `bill_currency_symbol` | `¥` | 显示用货币符号 |
| `bill_usd_cny` | `7.2` | USD 换算显示货币的汇率 |
| `bill_prices` | 见默认 | 模型单价表（USD / 百万 token） |
| `bill_report_enabled` | `false` | 启用每日账单日报 |
| `bill_report_time` | `21:00` | 日报推送时间（HH:MM） |
| `bill_report_targets` | `[]` | 日报接收会话 UMO 或群号 |
| `bill_retention_days` | `180` | 数据保留天数，0 表示不清理 |

## 安装
1. 面板 → 插件管理 → 从链接安装：https://github.com/siwuli/AstrBot_siwu-llm-billing
2. 或把目录放进 `data/plugins/` 后重载插件；无第三方依赖

## 版本记录
| 版本 | 说明 |
| --- | --- |
| 1.0.5 | 精简文档 |
| 1.0.4 | 全量统计、单价换算与日报 |
