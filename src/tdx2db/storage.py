import os
from pathlib import Path
from typing import Optional, Tuple

import pandas as pd
from sqlalchemy import create_engine, Column, Integer, Float, String, UniqueConstraint, text
from sqlalchemy.orm import declarative_base, sessionmaker

from .config import config
from .logger import logger

Base = declarative_base()


class DailyData(Base):
    __tablename__ = 'daily_data'
    __table_args__ = (UniqueConstraint('stock_code', 'date'),)

    id = Column(Integer, primary_key=True)
    stock_code = Column(String(10), index=True)
    market = Column(Integer)
    date = Column(String(8), index=True)   # YYYYMMDD 字符串
    open = Column(Float)
    high = Column(Float)
    low = Column(Float)
    close = Column(Float)
    volume = Column(Float)
    amount = Column(Float)
    adj_factor = Column(Float)
    turnover_rate = Column(Float)        # 换手率(%)，暂时为 NULL


class StockInfo(Base):
    __tablename__ = 'stock_info'
    __table_args__ = (UniqueConstraint('code'),)

    id = Column(Integer, primary_key=True)
    code = Column(String(10), index=True)
    name = Column(String(50))
    market = Column(Integer)


_VALID_TABLES = frozenset({'daily_data', 'stock_info'})


class DataStorage:
    def __init__(self, db_url: Optional[str] = None) -> None:
        self.db_url = db_url or config.database_url
        self._db_type = self.db_url.split('://')[0].split('+')[0]
        self.engine = create_engine(self.db_url)
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(bind=self.engine)

    def get_latest_date_by_code(self, code: str) -> Optional[str]:
        """返回指定股票在 daily_data 中最新的 YYYYMMDD 字符串日期，无数据返回 None。"""
        try:
            with self.engine.connect() as conn:
                row = conn.execute(
                    text("SELECT MAX(date) FROM daily_data WHERE stock_code = :code"),
                    {"code": code}
                ).fetchone()
                return row[0] if row and row[0] is not None else None
        except Exception as e:
            logger.debug(f"查询 {code} 最新日期出错: {e}")
            return None

    def get_all_latest_dates(self) -> dict:
        """一次查询返回所有股票最新日期 {code: YYYYMMDD str}。"""
        try:
            with self.engine.connect() as conn:
                rows = conn.execute(
                    text("SELECT stock_code, MAX(date) FROM daily_data GROUP BY stock_code")
                ).fetchall()
                return {r[0]: r[1] for r in rows if r[1] is not None}
        except Exception as e:
            logger.debug(f"查询所有股票最新日期出错: {e}")
            return {}

    def delete_stock_data(self, code: str) -> None:
        """删除某只股票全部日线记录（除权后全量重写前调用）。"""
        try:
            with self.engine.connect() as conn:
                conn.execute(
                    text("DELETE FROM daily_data WHERE stock_code = :code"),
                    {"code": code}
                )
                conn.commit()
        except Exception as e:
            logger.error(f"删除 {code} 数据出错: {e}")

    def save_incremental(
        self,
        df: pd.DataFrame,
        table_name: str,
        conflict_columns: Tuple[str, ...] = ('stock_code', 'date'),
        batch_size: int = 10000
    ) -> int:
        """增量保存，跳过重复记录（ON CONFLICT DO NOTHING / INSERT OR IGNORE / INSERT IGNORE）。"""
        if table_name not in _VALID_TABLES:
            raise ValueError(f"不允许写入的表名: {table_name}")
        if df.empty:
            return 0

        df_to_save = df.copy()
        if isinstance(df_to_save.index, pd.DatetimeIndex) or df_to_save.index.name in ('date', 'datetime'):
            df_to_save = df_to_save.reset_index(drop=True)

        columns = list(df_to_save.columns)
        columns_str = ', '.join(columns)
        total_rows = len(df_to_save)

        try:
            if self._db_type == 'postgresql':
                self._save_incremental_pg(df_to_save, columns, columns_str, table_name, conflict_columns, batch_size)
            else:
                placeholders = ', '.join([f':{c}' for c in columns])
                if self._db_type == 'mysql':
                    sql = text(f"INSERT IGNORE INTO {table_name} ({columns_str}) VALUES ({placeholders})")
                else:  # sqlite
                    sql = text(f"INSERT OR IGNORE INTO {table_name} ({columns_str}) VALUES ({placeholders})")

                with self.engine.connect() as conn:
                    for i in range(0, total_rows, batch_size):
                        batch = df_to_save.iloc[i:i + batch_size].astype(object).where(
                            df_to_save.iloc[i:i + batch_size].notna(), None
                        )
                        records = batch.to_dict('records')
                        for rec in records:
                            for k, v in rec.items():
                                if isinstance(v, pd.Timestamp):
                                    rec[k] = v.to_pydatetime()
                                elif v is pd.NaT:
                                    rec[k] = None
                        conn.execute(sql, records)
                        conn.commit()

            logger.debug(f"增量保存完成: {total_rows} 条 → {table_name}（重复已跳过）")
            return total_rows
        except Exception as e:
            logger.error(f"增量保存到 {table_name} 出错: {e}")
            return 0

    def _save_incremental_pg(self, df, columns, columns_str, table_name, conflict_columns, batch_size):
        from psycopg2.extras import execute_values
        conflict_str = ', '.join(conflict_columns)
        sql = f"INSERT INTO {table_name} ({columns_str}) VALUES %s ON CONFLICT ({conflict_str}) DO NOTHING"
        df_clean = df.astype(object).where(df.notna(), None)
        values = list(df_clean.itertuples(index=False, name=None))
        raw_conn = self.engine.raw_connection()
        try:
            cur = raw_conn.cursor()
            execute_values(cur, sql, values, page_size=batch_size)
            raw_conn.commit()
        finally:
            raw_conn.close()

    def save_stock_info(self, df: pd.DataFrame) -> bool:
        """保存股票列表到 stock_info 表（增量，跳过重复）。"""
        return self.save_incremental(df, 'stock_info', conflict_columns=('code',)) > 0

    def save_to_csv(self, df: pd.DataFrame, filename: str, csv_path: Optional[str] = None) -> Optional[str]:
        path = Path(csv_path or config.csv_output_path)
        os.makedirs(path, exist_ok=True)
        file_path = path / f"{filename}.csv"
        df.to_csv(file_path, index=False, encoding='utf-8')
        logger.info(f"数据已保存到: {file_path}")
        return str(file_path)
