"""数据存储模块

负责将处理后的数据保存到不同的存储介质，支持：
- CSV文件存储
- 数据库存储（PostgreSQL、MySQL、SQLite）
"""

import os
from datetime import datetime as dt
from pathlib import Path
from typing import Optional, Tuple

import pandas as pd
from sqlalchemy import create_engine, inspect, Column, Integer, Float, String, DateTime, MetaData, Table, UniqueConstraint, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from tqdm import tqdm

from .config import config
from .logger import logger

Base = declarative_base()

class BlockStockRelation(Base):
    """板块股票关系表模型（issue #39：全量替换式快照，无增量语义）

    唯一约束用 (block_type, block_name, code)：block_code 可空
    （沪深300 等指数板块无 880 码），且同名板块跨体系存在
    （880302 与 881002 都叫"煤炭开采"，type 不同）。
    """
    __tablename__ = 'block_stock_relation'
    __table_args__ = (UniqueConstraint('block_type', 'block_name', 'code', name='uq_block_name_code'),)

    id = Column(Integer, primary_key=True)
    block_type = Column(String(10), index=True)  # 行业/概念/指数/地区/风格/特殊
    block_code = Column(String(20), index=True, nullable=True)  # 880/881 板块代码，可空
    block_name = Column(String(50), index=True)  # 板块名称
    code = Column(String(10), index=True)  # 股票代码（6 位纯数字，与行情表口径一致）

class DailyData(Base):
    """日线数据表模型"""
    __tablename__ = 'daily_data'
    __table_args__ = (UniqueConstraint('code', 'date', name='uq_daily_code_date'),)

    id = Column(Integer, primary_key=True)
    code = Column(String(10), index=True)
    market = Column(Integer)
    datetime = Column(DateTime, index=True)
    date = Column(DateTime, index=True)
    open = Column(Float)
    high = Column(Float)
    low = Column(Float)
    close = Column(Float)
    volume = Column(Float)
    amount = Column(Float)
    ma13 = Column(Float)
    ma21 = Column(Float)
    ma34 = Column(Float)
    ma55 = Column(Float)
    ma89 = Column(Float)
    ma144 = Column(Float)
    ma233 = Column(Float)
    ma5 = Column(Float)
    ma10 = Column(Float)
    ma60 = Column(Float)
    ma250 = Column(Float)

class Minute5Data(Base):
    """5分钟线数据表模型"""
    __tablename__ = 'minute5_data'
    __table_args__ = (UniqueConstraint('code', 'datetime', name='uq_minute5_code_datetime'),)

    id = Column(Integer, primary_key=True, autoincrement=True)
    code = Column(String(10), nullable=False, index=True)
    market = Column(Integer, nullable=False)
    datetime = Column(DateTime, nullable=False, index=True)
    date = Column(DateTime, nullable=False, index=True)
    open = Column(Float, nullable=False)
    high = Column(Float, nullable=False)
    low = Column(Float, nullable=False)
    close = Column(Float, nullable=False)
    volume = Column(Float, nullable=False)
    amount = Column(Float, nullable=False)
    ma13 = Column(Float)
    ma21 = Column(Float)
    ma34 = Column(Float)
    ma55 = Column(Float)
    ma89 = Column(Float)
    ma144 = Column(Float)
    ma233 = Column(Float)
    ma5 = Column(Float)
    ma10 = Column(Float)
    ma60 = Column(Float)
    ma250 = Column(Float)

class Minute15Data(Base):
    """15分钟线数据表模型"""
    __tablename__ = 'minute15_data'
    __table_args__ = (UniqueConstraint('code', 'datetime', name='uq_minute15_code_datetime'),)

    id = Column(Integer, primary_key=True, autoincrement=True)
    code = Column(String(10), nullable=False, index=True)
    market = Column(Integer, nullable=False)
    datetime = Column(DateTime, nullable=False, index=True)
    date = Column(DateTime, nullable=False, index=True)
    open = Column(Float, nullable=False)
    high = Column(Float, nullable=False)
    low = Column(Float, nullable=False)
    close = Column(Float, nullable=False)
    volume = Column(Float, nullable=False)
    amount = Column(Float, nullable=False)
    # 添加技术指标列
    ma13 = Column(Float)
    ma21 = Column(Float)
    ma34 = Column(Float)
    ma55 = Column(Float)
    ma89 = Column(Float)
    ma144 = Column(Float)
    ma233 = Column(Float)
    ma5 = Column(Float)
    ma10 = Column(Float)
    ma60 = Column(Float)
    ma250 = Column(Float)
class Minute30Data(Base):
    """30分钟线数据表模型"""
    __tablename__ = 'minute30_data'
    __table_args__ = (UniqueConstraint('code', 'datetime', name='uq_minute30_code_datetime'),)

    id = Column(Integer, primary_key=True, autoincrement=True)
    code = Column(String(10), nullable=False, index=True)
    market = Column(Integer, nullable=False)
    datetime = Column(DateTime, nullable=False, index=True)
    date = Column(DateTime, nullable=False, index=True)
    open = Column(Float, nullable=False)
    high = Column(Float, nullable=False)
    low = Column(Float, nullable=False)
    close = Column(Float, nullable=False)
    volume = Column(Float, nullable=False)
    amount = Column(Float, nullable=False)
    # 添加技术指标列
    ma13 = Column(Float)
    ma21 = Column(Float)
    ma34 = Column(Float)
    ma55 = Column(Float)
    ma89 = Column(Float)
    ma144 = Column(Float)
    ma233 = Column(Float)
    ma5 = Column(Float)
    ma10 = Column(Float)
    ma60 = Column(Float)
    ma250 = Column(Float)

class Minute60Data(Base):
    """60分钟线数据表模型"""
    __tablename__ = 'minute60_data'
    __table_args__ = (UniqueConstraint('code', 'datetime', name='uq_minute60_code_datetime'),)

    id = Column(Integer, primary_key=True, autoincrement=True)
    code = Column(String(10), nullable=False, index=True)
    market = Column(Integer, nullable=False)
    datetime = Column(DateTime, nullable=False, index=True)
    date = Column(DateTime, nullable=False, index=True)
    open = Column(Float, nullable=False)
    high = Column(Float, nullable=False)
    low = Column(Float, nullable=False)
    close = Column(Float, nullable=False)
    volume = Column(Float, nullable=False)
    amount = Column(Float, nullable=False)
    # 添加技术指标列
    ma13 = Column(Float)
    ma21 = Column(Float)
    ma34 = Column(Float)
    ma55 = Column(Float)
    ma89 = Column(Float)
    ma144 = Column(Float)
    ma233 = Column(Float)
    ma5 = Column(Float)
    ma10 = Column(Float)
    ma60 = Column(Float)
    ma250 = Column(Float)

class StockInfo(Base):
    """股票信息表模型"""
    __tablename__ = 'stock_info'

    id = Column(Integer, primary_key=True)
    code = Column(String(10), unique=True, index=True)
    name = Column(String(50))
    market = Column(Integer)

# 允许写入的表名白名单
_VALID_TABLES = frozenset({
    'daily_data', 'minute5_data', 'minute15_data', 'minute30_data', 'minute60_data',
    'stock_info', 'block_stock_relation',
})

# status 命令统计的表及其日期列（stock_info / block_stock_relation 无日期列）
_STATS_TABLES = (
    ('stock_info', None),
    ('daily_data', 'date'),
    ('minute5_data', 'datetime'),
    ('minute15_data', 'datetime'),
    ('minute30_data', 'datetime'),
    ('minute60_data', 'datetime'),
    ('block_stock_relation', None),
)


class DataStorage:
    """数据存储类"""

    def __init__(
        self,
        db_url: Optional[str] = None,
        csv_path: Optional[str] = None,
        create_tables: bool = True
    ) -> None:
        """初始化数据存储

        Args:
            db_url: 数据库连接URL，如果为None则使用配置中的URL
            csv_path: CSV文件保存路径，如果为None则使用配置中的路径
            create_tables: 是否自动建表。status 等只读命令传 False，保证不产生任何写操作
        """
        self.db_url = db_url or config.database_url
        self.csv_path = csv_path or config.csv_output_path

        # 确保CSV输出目录存在
        if self.csv_path:
            os.makedirs(self.csv_path, exist_ok=True)

        # 初始化数据库连接
        if self.db_url:
            try:
                self.engine = create_engine(self.db_url)
            except ModuleNotFoundError as e:
                # 数据库驱动为可选依赖，缺失时给出对应 extras 安装提示
                extras = {'postgresql': 'postgres', 'mysql': 'mysql'}.get(config.db_type)
                if extras:
                    raise ValueError(
                        f"缺少 {config.db_type} 数据库驱动（{e.name}）。"
                        f"请安装: pip install 'tdx2db[{extras}]'"
                    ) from e
                raise
            if create_tables:
                Base.metadata.create_all(self.engine)
            self.Session = sessionmaker(bind=self.engine)

    def save_to_csv(self, df: pd.DataFrame, filename: str) -> Optional[str]:
        """保存数据到CSV文件

        Args:
            df: 要保存的DataFrame
            filename: 文件名（不包含路径和扩展名）

        Returns:
            str: 保存的文件路径，如果没有数据则返回None
        """
        if df.empty:
            logger.warning(f"没有数据可保存到 {filename}.csv")
            return None

        file_path = Path(self.csv_path) / f"{filename}.csv"
        df.to_csv(file_path, index=False, encoding='utf-8')
        logger.info(f"数据已保存到: {file_path}")
        return str(file_path)

    def get_table_stats(self) -> list:
        """统计每张表的行数、覆盖股票数、日期范围。只读，供 status 命令使用。

        Returns:
            list[dict]: 每表一项：table / exists / rows / codes / earliest / latest。
            表不存在时 exists=False（status 用 create_tables=False 初始化，不建表）
        """
        existing = set(inspect(self.engine).get_table_names())
        stats = []
        with self.engine.connect() as conn:
            for table, date_col in _STATS_TABLES:
                entry = {'table': table, 'exists': table in existing,
                         'rows': 0, 'codes': 0, 'earliest': None, 'latest': None}
                if not entry['exists']:
                    stats.append(entry)
                    continue
                entry['rows'] = conn.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar() or 0
                entry['codes'] = conn.execute(
                    text(f"SELECT COUNT(DISTINCT code) FROM {table}")
                ).scalar() or 0
                if date_col and entry['rows']:
                    mn, mx = conn.execute(
                        text(f"SELECT MIN({date_col}), MAX({date_col}) FROM {table}")
                    ).one()
                    fmt = '%Y-%m-%d' if date_col == 'date' else '%Y-%m-%d %H:%M'
                    entry['earliest'] = self._coerce_datetime(mn).strftime(fmt)
                    entry['latest'] = self._coerce_datetime(mx).strftime(fmt)
                stats.append(entry)
        return stats

    @staticmethod
    def _coerce_datetime(value) -> dt:
        """MAX(date) 的返回类型因数据库而异：SQLite 返回 TEXT，PG/MySQL 返回日期对象。
        统一转成 datetime，调用方才能安全做 + timedelta 运算。"""
        if isinstance(value, str):
            return pd.to_datetime(value).to_pydatetime()
        return value

    def get_latest_datetime(
        self,
        table_name: str,
        date_column: str = 'datetime'
    ) -> Optional[dt]:
        """获取表中最新的日期/时间

        Args:
            table_name: 表名
            date_column: 日期列名，默认为 'datetime'，日线数据应使用 'date'

        Returns:
            Optional[datetime]: 最新的日期/时间，如果表为空则返回 None
        """
        if table_name not in _VALID_TABLES:
            raise ValueError(f"不允许查询的表名: {table_name}")
        if date_column not in ('datetime', 'date'):
            raise ValueError(f"非法日期列名: {date_column}")
        try:
            with self.engine.connect() as conn:
                result = conn.execute(
                    text(f"SELECT MAX({date_column}) FROM {table_name}")
                )
                row = result.fetchone()
                if row and row[0]:
                    return self._coerce_datetime(row[0])
                return None
        except Exception as e:
            logger.debug(f"获取表 {table_name} 最新日期时出错: {e}")
            return None

    def get_latest_datetime_by_code(
        self,
        table_name: str,
        code: str,
        date_column: str = 'datetime'
    ) -> Optional[dt]:
        """获取指定股票的最新 datetime

        Args:
            table_name: 表名
            code: 股票代码
            date_column: 日期列名（分钟表 datetime，日线表 date）

        Returns:
            Optional[datetime]: 最新的 datetime，如果没有数据则返回 None
        """
        if table_name not in _VALID_TABLES:
            raise ValueError(f"不允许查询的表名: {table_name}")
        if date_column not in ('datetime', 'date'):
            raise ValueError(f"非法日期列名: {date_column}")
        try:
            with self.engine.connect() as conn:
                result = conn.execute(
                    text(f"SELECT MAX({date_column}) FROM {table_name} WHERE code = :code"),
                    {"code": code}
                )
                row = result.fetchone()
                if row and row[0]:
                    return self._coerce_datetime(row[0])
                return None
        except Exception as e:
            # warning 而非 debug：此查询挂掉会静默退化为全量重扫，用户必须可见
            logger.warning(f"获取表 {table_name} 股票 {code} 最新日期时出错（将全量重扫该股票）: {e}")
            return None

    def save_incremental(
        self,
        df: pd.DataFrame,
        table_name: str,
        conflict_columns: Tuple[str, ...] = ('code', 'datetime'),
        batch_size: int = 10000
    ) -> int:
        """增量保存数据，跳过重复记录

        使用 ON CONFLICT DO NOTHING 策略，需要先在数据库中添加唯一约束

        Args:
            df: 要保存的 DataFrame
            table_name: 表名
            conflict_columns: 唯一约束列，默认为 ('code', 'datetime')
            batch_size: 批处理大小

        Returns:
            int: 实际插入的行数
        """
        if table_name not in _VALID_TABLES:
            raise ValueError(f"不允许写入的表名: {table_name}")

        if df.empty:
            logger.warning(f"没有数据可保存到表 {table_name}")
            return 0

        total_rows = len(df)

        # 确保 datetime 不是索引而是列
        df_to_save = df.copy()
        if df_to_save.index.name == 'datetime' or isinstance(df_to_save.index, pd.DatetimeIndex):
            df_to_save = df_to_save.reset_index()

        # 获取列名
        columns = list(df_to_save.columns)
        columns_str = ', '.join(columns)
        db_type = config.db_type

        try:
            if db_type == 'postgresql':
                self._save_incremental_pg(
                    df_to_save, columns, columns_str,
                    table_name, conflict_columns, batch_size
                )
            else:
                # MySQL / SQLite: 走 SQLAlchemy executemany
                placeholders = ', '.join([f':{col}' for col in columns])
                if db_type == 'mysql':
                    sql = text(f"INSERT IGNORE INTO {table_name} ({columns_str}) VALUES ({placeholders})")
                elif db_type == 'sqlite':
                    sql = text(f"INSERT OR IGNORE INTO {table_name} ({columns_str}) VALUES ({placeholders})")
                else:
                    raise ValueError(f"不支持的数据库类型: {db_type}")

                with self.engine.connect() as conn:
                    for i in range(0, total_rows, batch_size):
                        batch_df = df_to_save.iloc[i:i + batch_size]
                        # sqlite3 适配器按精确类型匹配，不识别 pd.Timestamp（虽是
                        # datetime 子类）；先转 dict 再逐值转换，避开 pandas 类型推断
                        records = [
                            {
                                k: (None if pd.isna(v) else
                                    v.to_pydatetime() if isinstance(v, pd.Timestamp) else v)
                                for k, v in rec.items()
                            }
                            for rec in batch_df.to_dict('records')
                        ]
                        conn.execute(sql, records)
                        conn.commit()

            # per-call 日志降为 debug：全量同步时每股 ×4 表的 INFO 会淹没进度条
            logger.debug(f"增量保存完成: 共处理 {total_rows} 条到表 {table_name}（重复数据已跳过）")
            return total_rows

        except Exception as e:
            logger.error(f"增量保存数据到表 {table_name} 时出错: {e}")
            return 0

    def _save_incremental_pg(
        self,
        df: pd.DataFrame,
        columns: list,
        columns_str: str,
        table_name: str,
        conflict_columns: Tuple[str, ...],
        batch_size: int,
    ) -> None:
        """PostgreSQL 专用：使用 execute_values 真正批量插入

        一次网络往返插入整批数据，比 executemany 快 10-100x。
        """
        from psycopg2.extras import execute_values

        conflict_str = ', '.join(conflict_columns)
        sql = f"INSERT INTO {table_name} ({columns_str}) VALUES %s ON CONFLICT ({conflict_str}) DO NOTHING"

        # DataFrame → list of tuples，NaN/NaT → None（psycopg2 需要 None 表示 NULL）
        df_clean = df.astype(object).where(df.notna(), None)
        values = list(df_clean.itertuples(index=False, name=None))

        raw_conn = self.engine.raw_connection()
        try:
            cursor = raw_conn.cursor()
            execute_values(cursor, sql, values, page_size=batch_size)
            raw_conn.commit()
        finally:
            raw_conn.close()

    def save_to_database(
        self,
        df: pd.DataFrame,
        table_name: str,
        batch_size: int = 10000
    ) -> bool:
        """保存数据到数据库

        Args:
            df: 要保存的DataFrame
            table_name: 表名
            batch_size: 批处理大小，默认10000条记录

        Returns:
            bool: 是否保存成功
        """
        if table_name not in _VALID_TABLES:
            raise ValueError(f"不允许写入的表名: {table_name}")

        if df.empty:
            logger.warning(f"没有数据可保存到表 {table_name}")
            return False

        try:
            # 获取数据总量
            total_rows = len(df)

            logger.debug(f"开始保存数据到数据库表: {table_name}, 共 {total_rows} 条记录")

            # 如果数据量小于批处理大小，直接保存
            if total_rows <= batch_size:
                df.to_sql(table_name, self.engine, if_exists='append', index=False)
                logger.info(f"数据已保存到数据库表: {table_name}")
                return True

            # 数据量大，分批处理
            logger.info(f"数据量较大({total_rows}条)，开始分批保存到数据库表: {table_name}")

            # 计算批次数
            num_batches = (total_rows + batch_size - 1) // batch_size

            # 创建进度条
            iterator = tqdm(range(num_batches), desc="保存到数据库") if config.use_tqdm else range(num_batches)

            # 确保datetime不是索引而是列
            df_to_save = df.copy()
            if df_to_save.index.name == 'datetime' or isinstance(df_to_save.index, pd.DatetimeIndex):
                df_to_save = df_to_save.reset_index()

            # 分批保存
            for i in iterator:
                start_idx = i * batch_size
                end_idx = min((i + 1) * batch_size, total_rows)
                batch_df = df_to_save.iloc[start_idx:end_idx]

                # 保存当前批次
                # 使用正确的方法检查表是否存在
                from sqlalchemy import inspect
                inspector = inspect(self.engine)
                if_exists = 'append' if i > 0 or inspector.has_table(table_name) else 'replace'
                batch_df.to_sql(table_name, self.engine, if_exists=if_exists, index=False)

                if not config.use_tqdm:
                    logger.info(f"已保存 {end_idx}/{total_rows} 条记录到数据库表 {table_name}")

            logger.info(f"所有数据已成功保存到数据库表: {table_name}")
            return True
        except Exception as e:
            logger.error(f"保存数据到数据库表 {table_name} 时出错: {e}")
            return False

    def save_daily_data(
        self,
        df: pd.DataFrame,
        to_csv: bool = True,
        to_db: bool = True,
        batch_size: int = 10000
    ) -> Tuple[Optional[str], bool]:
        """保存日线数据

        Args:
            df: 日线数据DataFrame
            to_csv: 是否保存到CSV
            to_db: 是否保存到数据库
            batch_size: 批处理大小，默认10000条记录

        Returns:
            tuple: (csv_path, db_success)
        """
        csv_path = None
        db_success = False

        if to_csv:
            csv_path = self.save_to_csv(df, 'daily_data')

        if to_db:
            db_success = self.save_to_database(df, 'daily_data', batch_size=batch_size)

        return csv_path, db_success

    def save_minute_data(
        self,
        df: pd.DataFrame,
        freq: int = 1,
        to_csv: bool = True,
        to_db: bool = True,
        batch_size: int = 10000
    ) -> Tuple[Optional[str], bool]:
        """保存分钟线数据

        Args:
            df: 分钟线数据DataFrame
            freq: 分钟频率
            to_csv: 是否保存到CSV
            to_db: 是否保存到数据库
            batch_size: 批处理大小，默认10000条记录

        Returns:
            tuple: (csv_path, db_success)
        """
        csv_path = None
        db_success = False

        if to_csv:
            csv_path = self.save_to_csv(df, f'minute{freq}_data')

        if to_db:
            db_success = self.save_to_database(df, f'minute{freq}_data', batch_size=batch_size)

        return csv_path, db_success

    def save_stock_info(
        self,
        df: pd.DataFrame,
        to_csv: bool = True,
        to_db: bool = True,
        batch_size: int = 10000
    ) -> Tuple[Optional[str], bool]:
        """保存股票信息

        Args:
            df: 股票信息DataFrame
            to_csv: 是否保存到CSV
            to_db: 是否保存到数据库
            batch_size: 批处理大小，默认10000条记录

        Returns:
            tuple: (csv_path, db_success)
        """
        csv_path = None
        db_success = False

        if to_csv:
            csv_path = self.save_to_csv(df, 'stock_info')

        if to_db:
            db_success = self.save_to_database(df, 'stock_info', batch_size=batch_size)

        return csv_path, db_success

    def save_block_relation(
        self,
        df: pd.DataFrame,
        to_csv: bool = True,
        to_db: bool = True,
        batch_size: int = 10000
    ) -> Tuple[Optional[str], bool]:
        """保存板块与股票的对应关系（全量替换式快照）

        板块关系没有历史概念（成分调整由通达信盘后文件直接体现），
        每次同步以 DELETE + INSERT 反映当前快照——与行情表的增量语义不同。

        Args:
            df: 列 block_type/block_code/block_name/code
            to_csv: 是否保存到CSV
            to_db: 是否保存到数据库
            batch_size: 批处理大小

        Returns:
            tuple: (csv_path, db_success)
        """
        csv_path = None
        db_success = False

        if to_csv:
            csv_path = self.save_to_csv(df, 'block_stock_relation')

        if to_db:
            # DELETE + INSERT 必须同一事务：任一批次失败整体回滚，
            # 保住旧快照——否则失败会留下空表/半新半旧快照（PR #40 review）
            try:
                with self.engine.begin() as conn:
                    conn.execute(text("DELETE FROM block_stock_relation"))
                    df.to_sql(
                        'block_stock_relation', conn,
                        if_exists='append', index=False, chunksize=batch_size,
                    )
                db_success = True
            except Exception as e:
                logger.error(f"板块关系写入失败，已整体回滚、保留旧快照: {e}")
                db_success = False

        return csv_path, db_success
