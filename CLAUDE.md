# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## 项目概述

tdx2db：从本地通达信(TDX)行情软件读取 A 股日线数据，增量同步到数据库。支持作为 Python 包被其他项目调用。

## 常用命令

```bash
# 安装依赖
pip install -r requirements.txt
# 或安装为可编辑包（支持 import）
pip install -e .

# 一键增量同步日线数据 — 日常使用这一个命令
python main.py sync

# 单独同步
python main.py daily --incremental
python main.py daily --code 000001 --start 20240101

# 同步股票列表
python main.py stock-list

# 运行测试
python -m pytest tests/ -v
```

## 架构

四层管道，单向数据流：

```
CLI (cli.py)  → Reader (reader.py) → Processor (processor.py) → Storage (storage.py)
    ↓                ↓                      ↓                        ↓
 argparse        pytdx 读取本地       校验 + 复权处理           SQLAlchemy 批量写库
 命令分发 +      .day 文件            (OHLCV 校验, 前/后复权)   支持增量 ON CONFLICT
 同步编排                                                        表名白名单保护
```

- **cli.py**: 命令分发 + `sync_all_daily()` 逐股票流式同步
- **config.py**: 全局单例 `config`，从 `.env` 加载配置
- **logger.py**: 全局单例 `logger`
- **`__init__.py`**: 暴露 `TdxDailySync` 公共 API

## 关键数据流

逐股票流式处理，不全量加载到内存：

1. 读取 `vipdoc/{sz,sh,bj}/lday/*.day` → `process_daily_data()` 校验 OHLCV + 复权
2. 增量策略：`get_all_latest_dates()` 一次查询所有股票最新日期；若有除权事件则 `delete_stock_data()` + 全量重写
3. `save_incremental()` 使用 `ON CONFLICT DO NOTHING`（PG）/ `INSERT OR IGNORE`（SQLite）/ `INSERT IGNORE`（MySQL）

## 数据库表

| 表名 | 唯一约束 | 用途 |
|------|----------|------|
| `daily_data` | (code, date) | 日线数据，date 为 YYYYMMDD 整数 |
| `stock_info` | code | 股票列表 |

唯一约束由 SQLAlchemy `UniqueConstraint` 在建表时自动创建，无需手动执行 SQL 脚本。

## 股票代码格式

- CLI `--code` 参数：纯 6 位数字，如 `000001`、`600000`、`920001`，市场自动识别
- 内部流转层：带市场前缀，如 `sz000001`、`sh600000`、`bj920001`（reader 内部使用）
- 数据库层：纯 6 位数字，如 `000001`（reader 写入时截取）
- 深圳 market=0，上海 market=1，北京 market=2
- A 股筛选：深圳 `000/001/002/300` 开头，上海 `60/688` 开头，北交所 `8xxxxx` 或 `92xxxx` 开头
- 市场自动识别规则：6 开头 → 上海（sh），8 或 92 开头 → 北京（bj），其他 → 深圳（sz）

## 配置

通过 `.env` 文件配置：

| 变量 | 必填 | 说明 |
|------|------|------|
| `TDX_PATH` | 是 | 通达信安装目录 |
| `DB_TYPE` | 否 | `sqlite`（默认）/ `mysql` / `postgresql` |
| `DB_NAME` | 否 | 数据库名，SQLite 时为文件名（生成 `<name>.db`） |
| `DB_HOST` | MySQL/PG 必填 | 数据库主机 |
| `DB_USER` | MySQL/PG 必填 | 数据库用户名 |
| `DB_PASSWORD` | MySQL/PG 必填 | 数据库密码 |
| `DB_PORT` | 否 | 默认 `5432` |
| `DB_BATCH_SIZE` | 否 | 批量写入大小，默认 `10000` |
| `USE_TQDM` | 否 | 是否显示进度条，默认 `True` |

## sync 命令增量策略

`python main.py sync` 内部行为：

- 一次 SQL 查询获取所有股票最新日期（`SELECT code, MAX(date) FROM daily_data GROUP BY code`）
- 对每只股票：检查 gbbq 中是否有除权事件（category=1）发生在 last_date 之后
  - 有除权 → 删除该股旧数据，全量重写（保证复权价格正确）
  - 无除权 → 只写入 last_date 之后的新数据
