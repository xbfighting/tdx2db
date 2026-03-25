# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## 项目概述

tdx2db：从本地通达信(TDX)行情软件读取 A 股数据，增量同步到数据库。是量化分析工作站的数据入口。

## 常用命令

```bash
# 安装依赖
pip install -r requirements.txt

# 一键增量同步（日线 + 5/15/30/60 分钟线）— 日常使用这一个命令即可
python main.py sync

# 单独同步
python main.py daily --db-only --auto-start --incremental
python main.py minutes --db-only --auto-start --incremental

# 同步股票列表
python main.py stock-list --db-only
```

无测试套件。验证方式是运行 `sync` 命令后检查数据库数据。

## 架构

四层管道，单向数据流：

```
CLI (cli.py) → Reader (reader.py) → Processor (processor.py) → Storage (storage.py)
    ↓               ↓                      ↓                        ↓
 argparse      pytdx 读取本地        清洗 + 计算均线           SQLAlchemy 写库
 命令分发      .day/.lc5 文件        (MA5~MA250)              支持增量 ON CONFLICT
```

- **config.py**: 全局单例 `config`，从 `.env` 加载配置（TDX_PATH、DB_*）
- **logger.py**: 全局单例 `logger`

### 关键数据流

1. **日线**: 读取 `vipdoc/{sz,sh}/lday/*.day` → `process_daily_data()` 添加 date 列和均线 → 写入 `daily_data` 表
2. **分钟线**: 读取 `vipdoc/{sz,sh}/fzline/*.lc5`（5 分钟原始数据）→ resample 为 15/30/60 分钟 → `process_min_data()` 计算均线 → 分别写入 `minute{5,15,30,60}_data` 表
3. **增量同步**: `save_incremental()` 使用 `ON CONFLICT DO NOTHING`（PostgreSQL）/ `INSERT IGNORE`（MySQL）跳过重复。分钟线按股票精确查询最新日期（`get_latest_datetime_by_code`），日线按全局最新日期。

### 数据库表

| 表名 | 唯一约束 | 用途 |
|------|----------|------|
| `daily_data` | (code, date) | 日线数据 |
| `minute{5,15,30,60}_data` | (code, datetime) | 分钟线数据 |
| `stock_info` | code | 股票列表 |
| `block_stock_relation` | — | 板块关系（未完整实现） |

唯一约束需通过 `scripts/add_constraints.sql` 手动添加。

### 股票代码格式

代码带市场前缀：`sz000001`、`sh600000`。深圳 market=0，上海 market=1。
A 股筛选规则：深圳 `000/001/002/300` 开头，上海 `60/688` 开头。

## 配置

通过 `.env` 文件配置，必填：`TDX_PATH`、`DB_TYPE`、`DB_HOST`、`DB_NAME`、`DB_USER`、`DB_PASSWORD`。
