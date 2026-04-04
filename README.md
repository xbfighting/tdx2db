# tdx2db

从本地通达信（TDX）行情软件读取 A 股日线数据，增量同步到数据库。支持作为 Python 包被其他项目调用。

## 特性

- 同步深圳/上海全量 A 股日线数据（含科创板）
- 前复权 / 后复权 / 不复权，默认前复权
- 增量更新：有除权事件的个股自动全量重写，确保复权价格正确
- 日期格式：`YYYYMMDD` 整数（便于范围查询）
- 数据库：SQLite（默认）/ MySQL / PostgreSQL

## 安装

```bash
# 直接安装依赖
pip install -r requirements.txt

# 或作为包安装（支持被其他项目 import）
pip install -e .
```

## 配置

复制 `.env.example` 为 `.env` 并填写：

```
TDX_PATH=/path/to/tdx          # 通达信安装目录（必填）
DB_TYPE=sqlite                  # sqlite / mysql / postgresql
DB_NAME=tdx_data                # SQLite 时为文件名（生成 tdx_data.db）
DB_HOST=localhost               # MySQL/PostgreSQL 必填
DB_USER=postgres
DB_PASSWORD=your_password
DB_BATCH_SIZE=10000
USE_TQDM=True
```

## 命令行使用

```bash
# 同步股票列表
python main.py stock-list

# 一键增量同步所有股票日线（日常使用这一个命令）
python main.py sync

# 同步所有股票日线（全量）
python main.py daily

# 同步指定股票
python main.py daily --code sz000001

# 指定日期范围
python main.py daily --start 20240101 --end 20241231

# 指定复权类型
python main.py sync --adj backward
```

安装为包后也可直接使用 `tdx2db` 命令：

```bash
tdx2db sync
```

## 作为 Python 包调用

```python
from tdx2db import TdxDailySync

sync = TdxDailySync(
    tdx_path="/path/to/tdx",
    db_url="sqlite:///data.db",
)

# 同步所有股票
sync.sync_all(adj_type='forward')

# 同步单只股票
sync.sync_stock('sz000001', start_date=20240101)

# 查询数据
df = sync.get_daily('sz000001', start_date=20240101, end_date=20241231)
print(df.head())
```

## 数据表结构

**daily_data**

| 列 | 类型 | 说明 |
|----|------|------|
| code | String | 股票代码（6位，如 `000001`） |
| market | Integer | 市场（0=深圳，1=上海） |
| date | Integer | 日期 YYYYMMDD |
| open/high/low/close | Float | 复权后价格 |
| volume | Float | 成交量 |
| amount | Float | 成交额 |
| adj_factor | Float | 复权因子（1.0=无复权） |
| turnover_rate | Float | 换手率（%），待实现 |

唯一约束：`(code, date)`

## 运行测试

```bash
pip install pytest
python -m pytest tests/ -v
```

## 许可证

MIT
