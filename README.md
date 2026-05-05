# 通达信数据处理工具

读取本地通达信股票数据，增量同步到数据库。

## 快速开始

```bash
# 一键同步所有数据（日线 + 5/15/30/60分钟线）
python main.py sync
```

## 安装

```bash
# Python >= 3.10
pip install -r requirements.txt

# 复制并编辑配置文件
cp .env.example .env
```

**.env 必填配置**：
```
TDX_PATH=D:\通达信安装目录
DB_TYPE=postgresql
DB_HOST=localhost
DB_NAME=tdx_data
DB_USER=postgres
DB_PASSWORD=your_password
```

## 首次使用

1. 打开通达信 → 选项 → 盘后数据下载 → 下载日线和分钟线数据

2. 同步股票列表：
```bash
python main.py stock-list --db-only
```

3. 一键同步所有行情数据：
```bash
python main.py sync
```

## 启用增量同步（推荐）

> 增量同步可自动跳过重复数据，大幅提升每日更新效率。

**老用户**（已有数据库表）需执行一次约束脚本：
```bash
# PostgreSQL
psql -U your_user -d your_database -f scripts/add_constraints.sql
```

**新用户**同样建议执行，以启用增量同步功能。

脚本作用：为 `daily_data`、`minute*_data` 表添加唯一约束，确保 `(code, date/datetime)` 不重复。

## 每日更新

```bash
python main.py sync
```

程序会自动检测数据库最新日期，只同步新数据。

## 其他命令

<details>
<summary>单独同步日线/分钟线</summary>

```bash
# 日线增量同步
python main.py daily --db-only --auto-start --incremental

# 分钟线增量同步
python main.py minutes --db-only --auto-start --incremental
```
</details>

<details>
<summary>指定日期范围</summary>

```bash
python main.py daily --db-only --start_date 2025-01-01 --end_date 2025-01-31
python main.py minutes --db-only --start_date 2025-01-01
```
</details>

<details>
<summary>导出到 CSV</summary>

```bash
python main.py daily --csv-only
python main.py minutes --csv-only
```
</details>

<details>
<summary>修复历史空洞（minute 数据）</summary>

如果发现 minute5/15/30/60 表存在历史日期空洞（例如某些股票在某天缺数据），
可运行一次性回填脚本，从 TDX `.lc5` 全量重读 + ON CONFLICT 自动只补缺失：

```bash
python scripts/backfill_minute_gaps.py            # 全市场（约 1-2 小时）
python scripts/backfill_minute_gaps.py --code sz000001
python scripts/backfill_minute_gaps.py --dry-run  # 看流程不写库
```

> 自 2026-05 起，`sync` 默认回溯 30 天写入，新空洞不会再形成；本脚本仅用于修旧数据。
</details>

## 数据库支持

- PostgreSQL（推荐）
- MySQL
- SQLite

## 许可证

MIT
