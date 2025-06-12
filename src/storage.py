"""数据存储模块

负责将处理后的数据保存到不同的存储介质，支持：
- CSV文件存储
- 数据库存储（PostgreSQL、MySQL、SQLite）
"""

import os
from pathlib import Path
import pandas as pd
from sqlalchemy import create_engine, Column, Integer, Float, String, DateTime, MetaData, Table
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from tqdm import tqdm

from .config import config

Base = declarative_base()

class BlockStockRelation(Base):
    """板块股票关系表模型"""
    __tablename__ = 'block_stock_relation'

    id = Column(Integer, primary_key=True)
    block_code = Column(String(20), index=True)  # 板块代码
    block_name = Column(String(50))  # 板块名称
    code = Column(String(10), index=True)  # 股票代码
    name = Column(String(50))  # 股票名称

class DailyData(Base):
    """日线数据表模型"""
    __tablename__ = 'daily_data'

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
    ma5 = Column(Float)
    ma10 = Column(Float)
    ma60 = Column(Float)
    ma250 = Column(Float)

class Minute15Data(Base):
    """15分钟线数据表模型"""
    __tablename__ = 'minute15_data'

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
    ma5 = Column(Float)
    ma10 = Column(Float)
    ma60 = Column(Float)
    ma250 = Column(Float)

class Minute30Data(Base):
    """30分钟线数据表模型"""
    __tablename__ = 'minute30_data'

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
    ma5 = Column(Float)
    ma10 = Column(Float)
    ma60 = Column(Float)
    ma250 = Column(Float)

class Minute60Data(Base):
    """60分钟线数据表模型"""
    __tablename__ = 'minute60_data'

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

class DataStorage:
    """数据存储类"""

    def __init__(self, db_url=None, csv_path=None):
        """初始化数据存储

        Args:
            db_url: 数据库连接URL，如果为None则使用配置中的URL
            csv_path: CSV文件保存路径，如果为None则使用配置中的路径
        """
        self.db_url = db_url or config.database_url
        self.csv_path = csv_path or config.csv_output_path

        # 确保CSV输出目录存在
        if self.csv_path:
            os.makedirs(self.csv_path, exist_ok=True)

        # 初始化数据库连接
        if self.db_url:
            self.engine = create_engine(self.db_url)
            Base.metadata.create_all(self.engine)
            self.Session = sessionmaker(bind=self.engine)

    def save_to_csv(self, df, filename):
        """保存数据到CSV文件

        Args:
            df: 要保存的DataFrame
            filename: 文件名（不包含路径和扩展名）

        Returns:
            str: 保存的文件路径
        """
        if df.empty:
            print(f"警告: 没有数据可保存到{filename}.csv")
            return None

        file_path = Path(self.csv_path) / f"{filename}.csv"
        df.to_csv(file_path, index=False, encoding='utf-8')
        print(f"数据已保存到: {file_path}")
        return str(file_path)

    def save_to_database(self, df, table_name, batch_size=10000):
        """保存数据到数据库

        Args:
            df: 要保存的DataFrame
            table_name: 表名
            batch_size: 批处理大小，默认10000条记录

        Returns:
            bool: 是否保存成功
        """
        if df.empty:
            print(f"警告: 没有数据可保存到表{table_name}")
            return False

        try:
            # 获取数据总量
            total_rows = len(df)

            print(f"开始保存数据到数据库表df: {df}")

            # 如果数据量小于批处理大小，直接保存
            if total_rows <= batch_size:
                df.to_sql(table_name, self.engine, if_exists='append', index=False)
                print(f"数据已保存到数据库表: {table_name}")
                return True

            # 数据量大，分批处理
            print(f"数据量较大({total_rows}条)，开始分批保存到数据库表: {table_name}")

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
                    print(f"已保存 {end_idx}/{total_rows} 条记录到数据库表 {table_name}")

            print(f"所有数据已成功保存到数据库表: {table_name}")
            return True
        except Exception as e:
            print(f"保存数据到数据库表{table_name}时出错: {e}")
            return False

    def save_daily_data(self, df, to_csv=True, to_db=True, batch_size=10000):
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

    def save_minute_data(self, df, freq=1, to_csv=True, to_db=True, batch_size=10000):
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

    def save_stock_info(self, df, to_csv=True, to_db=True, batch_size=10000):
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

    def save_block_relation(self, df, to_csv=True, to_db=True, batch_size=10000):
        """保存板块与股票的对应关系

        Args:
            df: 板块与股票对应关系DataFrame
            to_csv: 是否保存到CSV
            to_db: 是否保存到数据库
            batch_size: 批处理大小，默认10000条记录

        Returns:
            tuple: (csv_path, db_success)
        """
        csv_path = None
        db_success = False

        if to_csv:
            csv_path = self.save_to_csv(df, 'block_stock_relation')

        if to_db:
            db_success = self.save_to_database(df, 'block_stock_relation', batch_size=batch_size)

        return csv_path, db_success
