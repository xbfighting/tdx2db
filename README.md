# 通达信数据处理工具

读取本地通达信股票数据，增量同步到数据库。

## 快速开始

```bash
# 一键同步所有数据（日线 + 5/15/30/60分钟线）
python main.py sync
```

## 安装

```bash
# Python >= 3.9
pip install -r requirements.txt
# 国内网络可选用镜像加速：pip install -r requirements.txt -i https://mirrors.aliyun.com/pypi/simple/

# 复制并编辑配置文件
cp .env.example .env

# 创建数据库（表结构会在首次运行时自动创建，但数据库本身需要先建好）
createdb tdx_data                          # PostgreSQL
# mysql -u root -p -e 'CREATE DATABASE tdx_data'   # MySQL
# SQLite 无需此步骤
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

## 增量同步与唯一约束

增量同步（自动跳过重复数据）依赖 `(code, date/datetime)` 唯一约束。

**新用户**：无需任何操作——表结构由程序自动创建，已内建唯一约束。

**老用户**（v0.2.0 之前建的表没有约束）需执行一次迁移脚本，**否则 PostgreSQL 下增量写入会全部失败、MySQL/SQLite 下会静默累积重复数据**。不确定的话可先自检：

```sql
-- PostgreSQL：有输出说明约束已存在，无需迁移
SELECT conname FROM pg_constraint WHERE conname LIKE 'uq_%';
-- MySQL
SELECT CONSTRAINT_NAME FROM information_schema.TABLE_CONSTRAINTS
WHERE CONSTRAINT_SCHEMA = DATABASE() AND CONSTRAINT_NAME LIKE 'uq_%';
```

迁移脚本：
```bash
# PostgreSQL
psql -U your_user -d your_database -f scripts/add_constraints.sql

# MySQL
mysql -u your_user -p your_database < scripts/add_constraints_mysql.sql
```

脚本会先清理已有重复数据再加约束，执行前请备份。

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

## 数据库支持

- PostgreSQL（推荐）
- MySQL
- SQLite

## 许可证

MIT
