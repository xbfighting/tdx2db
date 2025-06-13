# 通达信数据处理工具

一个使用pytdx读取本地通达信股票数据并存储到数据库或CSV的程序。

## 功能特点

- 读取通达信本地股票数据（日线、分钟线）
- 获取股票列表信息
- 数据处理与清洗
- 计算常用技术指标（MA5、MA10、MA60等）
- 支持多种存储方式（CSV、PostgreSQL、SQLite、MySQL）
- 提供命令行接口，方便批处理

## 安装

### python版本要求

python >= 3.10

### 使用pip安装

```bash
pip install -r requirements.txt
```

### 使用[Poetry](https://python-poetry.org/)安装

```bash
poetry install
```

### 虚拟环境创建

```bash
poetry shell
```

## 配置

1. 复制`.env.example`文件并重命名为`.env`
2. 编辑`.env`文件，设置通达信安装路径和其他配置项

## 通达信数据路径（必填）

- TDX_PATH=D:\通达信安装目录

## 数据库配置

- DB_TYPE=postgresql  # 支持postgresql, mysql, sqlite

## 使用方法

### 获取数据前务必确保本地已经安装了通达信软件，并且已经下载了所需的股票数据

1. 打开通达信软件
2. 点击“选项”->“盘后数据下载”
3. 在弹出的对话框中选择你要下载的日线和分钟数线数据
4. 点击“确定”开始下载

### 获取股票列表并入库(只获取A股个股)

```bash
# 数据入库
python main.py stock-list --db-only

# 数据保存为CSV
python main.py stock-list --csv-only

# 数据保存为CSV和入库
python main.py stock-list
```

### 获取日线数据

```bash
# 获取所有股票的日线数据
python main.py daily

# 获取指定股票的日线数据
python main.py daily --csv-only --code 000001

# 获取指定日期范围内的日线数据入库[包含开始日期]
python main.py daily --db-only  --start_date 2025-04-15 --end_date 2025-04-15

# 获取指定日期以后的日线数据入库[包含开始日期]
python main.py daily --db-only  --start_date 2025-04-15
```

### 获取分钟线数据

```bash
# 获取所有股票的分钟线数据
python main.py minutes

# 获取指定股票的分钟线数据
python main.py minutes --code 000001

# 获取指定日期范围内的分钟线数据入库[包含开始日期]
python main.py minutes --db-only  --start_date 2025-04-15 --end_date 2025-04-15

# 获取指定日期以后的分钟线数据入库[包含开始日期]
python main.py minutes --db-only  --start_date 2025-06-02
```

### 其他参数

```bash
# 指定通达信路径
python main.py daily --tdx-path "D:\通达信安装目录"

# 指定输出CSV路径
python main.py daily --output "./output"

# 仅保存到CSV
python main.py daily --csv-only

# 仅保存到数据库
python main.py daily --db-only

# 禁用进度条
python main.py daily --no-tqdm
```

## 数据库支持

支持以下数据库：

- PostgreSQL(推荐)
- MySQL
- SQLite

可以通过`.env`文件或命令行参数配置数据库连接信息。

## 许可证

MIT

## TODO

[ ] 数据自动增量同步功能 ：自动增加增量更新功能
[ ] 并行处理 ：使用多线程或多进程加速数据处理，特别是在处理大量股票数据时
[ ] 类型注解 ：添加Python类型注解，提高代码可读性和IDE支持
[ ] 代码测试 ：添加单元测试和集成测试，提高代码质量和可靠性
[ ] Docker支持 ：提供Docker镜像，简化部署过程
[ ] 允许用户自定义数据处理流程，例如增加均线参数，MACD等指标计算
[ ] API接口 ：提供REST API，便于与其他系统集成
[ ] 事件系统 ：实现事件驱动架构，支持自定义事件处理器

### 更新原则

优先考虑那些对用户最有价值的功能，逐步提升项目的实用性和易用性。
