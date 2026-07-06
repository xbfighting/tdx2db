# AGENTS.md

面向 AI agent 的 tdx2db 使用指南。你（agent）在装有 tdx2db 的环境中同步、查询 A 股行情数据时，本文档是首要参考。贡献代码请另见 `CLAUDE.md` 与 `CONTRIBUTING.md`。

## 这是什么

tdx2db 读取本地通达信（TDX）行情软件的数据文件，增量同步到 PostgreSQL / MySQL / SQLite。产出物是一个可直接 SQL 查询的行情数据库——**你不需要通过 tdx2db 读数据，直接查库即可**；tdx2db 只负责写入。

```
通达信盘后数据下载 → tdx2db sync → 数据库 ← 你的 SQL 查询
```

## CLI 速查

```bash
tdx2db sync                  # 日常唯一命令：增量同步日线 + 5/15/30/60 分钟线
tdx2db status                # 只读：每表行数/覆盖股票数/日期范围（不需要 TDX_PATH）
tdx2db status --json         # 机器可读，优先用这个
tdx2db stock-list --db-only  # 同步股票列表
```

配置经 `.env`（从当前工作目录读取）或全局参数 `--tdx-path / --db-type / --db-name` 等。密码只能放 `.env`，无命令行参数。退出码：0 成功，非 0 失败——**但退出码 0 不代表数据写入成功，必须用 `status` 验证**（见下"同步后验证"）。

## 数据库 Schema

| 表 | 唯一约束 | 说明 |
|----|----------|------|
| `daily_data` | (code, date) | 日线 OHLCV + 11 条均线 |
| `minute{5,15,30,60}_data` | (code, datetime) | 分钟线；15/30/60 由 5 分钟重采样 |
| `stock_info` | code | 股票列表（name 是占位符，见"陷阱"） |

- 行情表通用列：`code, market, datetime, date, open, high, low, close, volume, amount, ma5, ma10, ma13, ma21, ma34, ma55, ma60, ma89, ma144, ma233, ma250`
- 均线：`ma13/21/34/55/89/144/233` 是斐波那契窗口（缠论强弱分析主力），`ma233` 常被当作年线；上市天数不足窗口的行为 NULL
- 周线/月线没有表，由日线 `resample` 得到
- 收录范围：深市 000/001/002/300/301、沪市 60xxxx/688xxx；**无北交所、ETF、指数**

## 典型查询（内部项目验证过的形态）

```sql
-- 1. 单票日线区间 + 均线
SELECT date, open, high, low, close, volume, amount,
       ma5, ma13, ma21, ma34, ma55, ma89, ma144, ma233
FROM daily_data
WHERE code = '600036' AND date BETWEEN '2026-01-01' AND '2026-07-01'
ORDER BY date;

-- 2. 某日全市场截面（maN IS NOT NULL 兼作"数据成熟"哨兵，排除上市不足 N 日的新股）
SELECT code, close, ma5, ma13, ma233
FROM daily_data
WHERE date = '2026-07-03' AND ma233 IS NOT NULL;

-- 3. 最新交易日探测（加 ma5 条件确保当日指标已计算完成）
SELECT MAX(date) FROM daily_data WHERE ma5 IS NOT NULL;
```

## 陷阱（每一条都有真实事故背书）

1. **code 格式跨表不一致**：`stock_info.code` 带前缀（`sz000001`），行情表是 6 位纯数字（`000001`）。用 `LIKE 'sh688%'` 查 `daily_data` 会静默零匹配。跨表需 `RIGHT(stock_info.code, 6)`——但内部实践根本不用 `stock_info`（见 3）。
2. **停牌日必须严格 `date = X` 等值匹配**：想"取不到就用前一天"时要显式写出回退逻辑，`<=` 取最末行会静默错配到停牌前一天。
3. **`stock_info.name` 是 `深A000001` 式占位符**，不是真实股票名。真实名称/板块归属从通达信导出的 CSV 获取（code 记得 `zfill(6)` 对齐）。
4. **数据不复权**（设计决策，不会改）：与行情软件默认前复权对比时，分红除权的股票形状会不同。复权在消费端自行处理。
5. **同步不完整会静默算出子集**：`MAX(date)` 已是今天不代表全市场都进来了。见下面的验证流程。

## 同步后验证（必做）

```bash
tdx2db sync && tdx2db status --json
```

检查 `status` 输出：`daily_data` 的 `latest` 是否为预期交易日；有 `warnings`（衍生分钟表覆盖缺口）时按提示重跑即可自愈。更严格的覆盖度自检（内部生产流程的做法）——当日行数与前一交易日对比，偏差超 5% 说明同步不完整：

```sql
SELECT
  (SELECT COUNT(*) FROM daily_data WHERE date = (SELECT MAX(date) FROM daily_data)) AS today_n,
  (SELECT COUNT(*) FROM daily_data
   WHERE date = (SELECT MAX(date) FROM daily_data
                 WHERE date < (SELECT MAX(date) FROM daily_data))) AS prev_n;
```

## 数据契约（不要试图"修复"这些）

- 不复权，默认口径永不改变
- `daily_data`/`minute*` 的 code 保持 6 位纯数字
- 行情表不存股票名称
- 均线窗口固定为 `[5,10,13,21,34,55,60,89,144,233,250]`
