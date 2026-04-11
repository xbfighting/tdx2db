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

## SMB 网络访问模式

如果通达信安装在另一台 Windows PC 上，可以通过 SMB 协议远程读取数据，无需把软件安装在运行本程序的机器上。

### 1. 在 Windows PC 上共享 TDX 目录

右键 TDX 安装目录（如 `D:\new_tdx64`）→ 属性 → 共享 → 高级共享：

- 勾选"共享此文件夹"
- 设置共享名，如 `new_tdx64`
- 权限 → 添加需要访问的账户，授予"读取"权限

### 2. 创建专用本地账户（推荐）

**强烈建议**新建一个 Windows 本地账户用于 SMB 访问，而不是使用微软账户。微软账户通过 NTLM 网络认证时兼容性较差，容易登录失败。

在 Windows PC 上以管理员身份打开 PowerShell：

```powershell
# 创建本地账户（替换为你想要的用户名和密码）
net user tdxread YourPassword123 /add
net localgroup Users tdxread /add
```

然后在共享权限中把 `tdxread` 加入，授予读取权限。

### 3. 配置 .env

```
SMB_ENABLED=true
SMB_HOST=192.168.1.100      # Windows PC 的 IP 或主机名
SMB_SHARE=new_tdx64         # 共享名
SMB_USER=tdxread            # 本地账户用户名
SMB_PASSWORD=YourPassword123
SMB_TDX_PATH=               # TDX 在共享内的相对路径，共享根目录就是 TDX 目录时留空
SMB_PORT=445
```

启用 SMB 模式后，`TDX_PATH` 可以不填。

## 命令行使用

```bash
# 同步股票列表
python main.py stock-list

# 一键增量同步所有股票日线（日常使用这一个命令）
python main.py sync

# 同步所有股票日线（全量）
python main.py daily

# 同步指定股票（6位代码，自动识别市场）
python main.py daily --code 000001

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
sync.sync_stock('000001', start_date=20240101)

# 查询数据
df = sync.get_daily('000001', start_date=20240101, end_date=20241231)
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
