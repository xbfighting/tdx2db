# tdx2db

> Import TDX (通达信) local A-share market data into PostgreSQL / MySQL / SQLite.

![CI](https://github.com/xbfighting/tdx2db/actions/workflows/python-package.yml/badge.svg)

读取本地通达信股票数据（日线 + 5/15/30/60 分钟线），增量同步到数据库。适合想用 SQL / pandas 做 A 股量化分析、又不想依赖收费行情 API 的人。

## 安装

```bash
# Python >= 3.9
pip install -r requirements.txt
# 国内网络可选用镜像加速：pip install -r requirements.txt -i https://mirrors.aliyun.com/pypi/simple/

# 复制并编辑配置文件
cp .env.example .env

# 创建数据库（表结构会在首次运行时自动创建，但数据库本身需要先建好）
createdb tdx_data                                  # PostgreSQL
# mysql -u root -p -e 'CREATE DATABASE tdx_data'   # MySQL
# SQLite 无需此步骤
```

## 配置（.env）

```ini
# 通达信安装目录（必填）。判断标准：该目录下应存在 vipdoc/sz/lday/*.day 文件
TDX_PATH=C:\new_tdx                # Windows
# TDX_PATH=/Volumes/share/new_tdx  # macOS（SMB 挂载 Windows 共享）
# TDX_PATH=/mnt/share/new_tdx     # Linux（CIFS 挂载）

DB_TYPE=postgresql                 # postgresql / mysql / sqlite
DB_HOST=localhost
DB_PORT=5432
DB_NAME=tdx_data                   # sqlite 时为文件名（生成 tdx_data.db）
DB_USER=postgres
DB_PASSWORD=your_password          # 密码只从 .env 读取，不提供命令行参数
```

可选项：`DB_BATCH_SIZE`（批量写入大小）、`CSV_OUTPUT_PATH`（CSV 导出目录）、`USE_TQDM`（进度条开关）。

## 首次使用

1. 打开通达信 → 选项 → **盘后数据下载** → 下载日线和分钟线数据（TDX 默认只缓存看过的股票，必须先做这一步）

2. 同步股票列表：
```bash
python main.py stock-list --db-only
```

3. 一键同步所有行情数据：
```bash
python main.py sync
```

## 每日更新

```bash
python main.py sync
```

程序按股票逐只检测数据库最新日期，只同步新数据。

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

## 其他命令

<details>
<summary>单独同步日线/分钟线</summary>

```bash
# 日线增量同步（逐股票精确增量）
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

## 数据表结构

| 表名 | 唯一约束 | 内容 |
|------|----------|------|
| `daily_data` | (code, date) | 日线 OHLCV + 均线 |
| `minute5_data` / `minute15_data` / `minute30_data` / `minute60_data` | (code, datetime) | 分钟线 OHLCV + 均线（15/30/60 由 5 分钟重采样） |
| `stock_info` | code | 股票列表 |

**均线列**：`ma5 / ma10 / ma60 / ma250` 为常规窗口，`ma13 / ma21 / ma34 / ma55 / ma89 / ma144 / ma233` 为斐波那契窗口（服务缠论类分析，不需要可忽略）。上市不足对应窗口天数的行为 NULL。

**⚠️ code 格式差异（跨表查询必读）**：`stock_info.code` 带市场前缀（`sz000001` / `sh600000`），而 `daily_data` / `minute*_data` 的 code 是 **6 位纯数字**（`000001`）。跨表 JOIN 需要 `RIGHT(stock_info.code, 6)` 或等价处理——这是最容易踩的坑。

**已知限制**：
- `stock_info.name` 目前是 `深A000001` 式占位符，不是真实股票名称
- 收录范围：深市 `000 / 001 / 002 / 300 / 301`，沪市 `60xxxx / 688xxx`；**北交所、ETF、指数暂未纳入**

## FAQ

**Q: 报"无法找到股票列表文件"或读到 0 只股票**
A: `TDX_PATH` 指向错误，或通达信还没下载数据。确认该目录下存在 `vipdoc/sz/lday/*.day`，并先在通达信里执行"盘后数据下载"。

**Q: 报 `database "tdx_data" does not exist`**
A: 表结构会自动建，但数据库本身要先创建，见"安装"一节的 `createdb`。

**Q: PostgreSQL 报 `no unique or exclusion constraint`，或 MySQL 数据越导越多**
A: 老库缺唯一约束，见"增量同步与唯一约束"一节的自检和迁移脚本。

**Q: 为什么没有北交所 / ETF / 指数数据？**
A: 当前 A 股筛选规则只收深市 000/001/002/300/301 和沪市 60/688。北交所（`vipdoc/bj/`）等扩展欢迎提 PR（见 CONTRIBUTING.md）。

**Q: 数据是否复权？**
A: 不复权，且默认口径不会改变（设计决策，见 issue #2）。复权请在消费端处理。

## 开发与贡献

四层管道，单向数据流：

```
CLI (cli.py)  → Reader (reader.py) → Processor (processor.py) → Storage (storage.py)
 argparse        pytdx 读取本地       校验 + 重采样 + 均线        SQLAlchemy 批量写库
 命令分发 +      .day/.lc5 文件       (OHLCV 校验, resample,     增量 ON CONFLICT +
 同步编排                             MA 计算)                   表名白名单
```

```bash
pip install -r requirements.txt
pytest tests/          # 单元测试（不需要真实 TDX 数据和数据库）
```

贡献前请读 [CONTRIBUTING.md](CONTRIBUTING.md)——特别是"不接受的改动"一节（数据契约）。

使用 AI 辅助开发的贡献者：仓库带 `CLAUDE.md`，包含架构细节与历史坑（code 格式差异、增量逻辑），能让 AI 产出符合数据契约的 PR。

## 许可证

[MIT](LICENSE)
