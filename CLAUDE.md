# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## 项目概述

tdx2db：从本地通达信(TDX)行情软件读取 A 股数据，增量同步到数据库。是量化分析工作站的数据入口。

本文件面向**贡献者**（改 tdx2db 代码）。如果你是在**消费数据库**（查询 daily_data 等表做分析），请读 `AGENTS.md`——schema、典型查询、陷阱清单都在那里，配套 skill 见 `.claude/skills/tdx2db-query/`。

## 常用命令

```bash
# 安装依赖（开发模式，含 postgres/mysql 驱动）
pip install -e '.[all]'

# 一键增量同步（日线 + 5/15/30/60 分钟线）— 日常使用这一个命令即可
tdx2db sync

# 单独同步
tdx2db daily --db-only --auto-start --incremental
tdx2db minutes --db-only --auto-start --incremental

# 同步股票列表
tdx2db stock-list --db-only

# 数据库状态一览（只读，不需要 TDX_PATH；--json 机器可读）
tdx2db status
```

`python main.py <子命令>` 与 `tdx2db <子命令>` 等价（main.py 是薄封装，保留老用户习惯）。包目录为 `tdx2db/`（v0.3.0 起从 `src/` 改名，发布到 PyPI）。psycopg2-binary/pymysql 为可选依赖（extras: postgres/mysql/all），默认安装仅支持 SQLite。

测试：`pytest tests/`（processor 纯函数 + 模块导入冒烟，CI 三矩阵 3.9/3.10/3.11 运行）。数据正确性验证仍以运行 `sync` 后检查数据库为准。

## 发布 PyPI（维护者）

版本号需同步 bump 两处：`pyproject.toml` 与 `tdx2db/__init__.py`（教训：v0.3.0 曾漏改后者）。发布 token 为项目级，存本地 `.env` 的 `PYPI_PROJECT_TOKEN`（gitignored，勿打印其值）：

```bash
rm -rf dist && uv build
UV_PUBLISH_TOKEN=$(grep '^PYPI_PROJECT_TOKEN=' .env | cut -d= -f2-) uv publish
```

发布后必须在全新 venv `pip install tdx2db==<版本>` 验证（含从工作目录读 `.env` 的脚本入口场景，教训见 v0.3.1）。增量/幂等类改动的冒烟必须跑两轮（首轮全量 + 次轮增量）。

## 架构

四层管道，单向数据流：

```
CLI (cli.py)  → Reader (reader.py) → Processor (processor.py) → Storage (storage.py)
    ↓                ↓                      ↓                        ↓
 argparse        pytdx 读取本地       校验 + 重采样 + 均线      SQLAlchemy 批量写库
 命令分发 +      .day/.lc5 文件       (OHLCV 校验, resample,   支持增量 ON CONFLICT
 同步编排                              MA5~MA250)               表名白名单保护
```

- **cli.py**: 除命令分发外，`sync_all_daily_data` / `sync_all_min_data` / `sync_single_stock_min_data` 编排逐股票流式同步
- **config.py**: 全局单例 `config`，从 `.env` 加载配置（TDX_PATH、DB_*）
- **logger.py**: 全局单例 `logger`

### 关键数据流

日线和分钟线均为**逐股票流式处理**，不全量加载到内存：

1. **日线**: 逐股票读取 `vipdoc/{sz,sh}/lday/*.day` → `process_daily_data()` 校验 OHLCV + 计算均线 → 增量写入 `daily_data` 表
2. **分钟线**: 逐股票读取 `.lc5`（5 分钟）→ `resample_ohlcv()` 重采样为 15/30/60 分钟 → `process_min_data()` 校验 + 均线 → 分别写入 `minute{5,15,30,60}_data` 表
3. **增量同步**: `save_incremental()` 使用批量 executemany + `ON CONFLICT DO NOTHING`（PostgreSQL）/ `INSERT IGNORE`（MySQL）跳过重复。分钟线按股票精确查询最新日期（`get_latest_datetime_by_code`），日线逐股票增量。

### 数据库表

| 表名 | 唯一约束 | 用途 |
|------|----------|------|
| `daily_data` | (code, date) | 日线数据 |
| `minute{5,15,30,60}_data` | (code, datetime) | 分钟线数据 |
| `stock_info` | code | 股票列表 |
| `block_stock_relation` | — | 板块关系（未完整实现） |

唯一约束已内建于 `tdx2db/storage.py` 的模型定义（`create_all` 自动创建）；仅老库（PR #17 之前建的表）需执行 `scripts/add_constraints.sql` / `add_constraints_mysql.sql` 迁移。

### 股票代码格式

代码带市场前缀：`sz000001`、`sh600000`。深圳 market=0，上海 market=1。
A 股筛选规则：深圳 `000/001/002/300/301` 开头，上海 `60xxxx/688xxx`。
北证（`vipdoc/bj/`）暂未纳入。历史教训：2026-06 之前上海正则误写为 `688\d{4}`（7 位），科创板被静默排除数年；同期深圳漏 `301`。

**表级差异（跨表查询/监控脚本必看）**：`stock_info.code` 带市场前缀（`sz000001`）；`daily_data` / `minute*_data` 的 code 为 **6 位纯数字**（reader 写入时截取）。已两次踩坑：commit 532ce76（分钟线增量 code 格式不匹配导致全量重处理）、20260612（监控脚本用 `LIKE 'sh688%'` 查 daily_data 静默零匹配）。

## 配置

通过 `.env` 文件配置，必填：`TDX_PATH`、`DB_TYPE`、`DB_HOST`、`DB_NAME`、`DB_USER`、`DB_PASSWORD`。
