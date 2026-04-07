"""tdx2db: 从通达信本地文件同步 A 股日线数据到数据库。

基本用法::

    from tdx2db import TdxDailySync

    sync = TdxDailySync(tdx_path="/opt/tdx", db_url="sqlite:///data.db")
    sync.sync_all(adj_type='forward', workers=4)
    sync.sync_stock('sz000001', start_date=20240101)
    df = sync.get_daily('sz000001', start_date=20240101)
"""

from typing import Optional
import pandas as pd

from .config import Config, config
from .reader import TdxDataReader
from .processor import DataProcessor
from .storage import DataStorage
from .logger import logger

__version__ = "0.2.0"
__all__ = ['TdxDailySync', 'TdxDataReader', 'DataProcessor', 'DataStorage', 'Config']


class TdxDailySync:
    """高层封装，供外部项目调用。"""

    def __init__(
        self,
        tdx_path: Optional[str] = None,
        db_url: Optional[str] = None,
    ) -> None:
        if tdx_path:
            config.tdx_path = tdx_path
        self.reader = TdxDataReader(tdx_path)
        self.processor = DataProcessor()
        self.storage = DataStorage(db_url)

    def sync_all(
        self,
        adj_type: str = 'forward',
        incremental: bool = True,
        start_date: Optional[int] = None,
        end_date: Optional[int] = None,
    ) -> dict:
        """同步所有 A 股日线数据。

        Returns:
            {'total': N, 'success': N, 'failed': N}
        """
        from .cli import sync_all_daily
        gbbq = self.reader.read_gbbq()
        return sync_all_daily(
            self.reader, self.processor, self.storage, gbbq,
            adj_type=adj_type, incremental=incremental,
            start_date=start_date, end_date=end_date,
        )

    def sync_stock(
        self,
        code: str,
        adj_type: str = 'forward',
        start_date: Optional[int] = None,
        end_date: Optional[int] = None,
    ) -> int:
        """同步单只股票日线数据，返回写入行数。"""
        market = 1 if code.startswith('sh') else 0
        gbbq = self.reader.read_gbbq()
        data = self.reader.read_daily_data(market, code)
        if isinstance(data.index, pd.DatetimeIndex) or data.index.name in ('date', 'datetime'):
            data = data.reset_index()
        processed = self.processor.process_daily_data(data, gbbq=gbbq, adj_type=adj_type)
        processed = self.processor.filter_data(processed, start_date=start_date, end_date=end_date)
        if processed.empty:
            return 0
        return self.storage.save_incremental(processed, 'daily_data', conflict_columns=('stock_code', 'date'))

    def sync_stock_list(self) -> int:
        """同步股票列表到 stock_info 表，返回股票数量。"""
        stocks = self.reader.get_stock_list()
        self.storage.save_stock_info(stocks)
        return len(stocks)

    def get_daily(
        self,
        code: str,
        start_date: Optional[int] = None,
        end_date: Optional[int] = None,
    ) -> pd.DataFrame:
        """从数据库查询日线数据，date 列为 YYYYMMDD 整数。"""
        from sqlalchemy import text
        if '.' in code:
            db_code = code.upper()
        else:
            pure = code[-6:] if len(code) > 6 else code
            if pure.startswith('6'):
                suffix = '.SH'
            elif pure.startswith('8') or pure.startswith('92'):
                suffix = '.BJ'
            else:
                suffix = '.SZ'
            db_code = pure + suffix
        conditions = ["stock_code = :code"]
        params: dict = {"code": db_code}
        if start_date:
            conditions.append("date >= :start_date")
            params["start_date"] = str(start_date)
        if end_date:
            conditions.append("date <= :end_date")
            params["end_date"] = str(end_date)
        where = " AND ".join(conditions)
        sql = text(f"SELECT * FROM daily_data WHERE {where} ORDER BY date")
        with self.storage.engine.connect() as conn:
            return pd.read_sql(sql, conn, params=params)
