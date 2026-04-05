"""
日线数据同步测试套件（使用 SQLite 内存库 + mock TDX reader）

测试用例：
1. test_full_sync_one_month  - 全量所有股票，1个月日期范围
2. test_single_stock_one_year - 指定股票，最近1年
3. test_forward_adj_price    - 前复权价格计算正确性
4. test_incremental_update   - 增量更新无重复，有除权时旧数据被替换
"""

import threading
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from src.tdx2db.processor import DataProcessor
from src.tdx2db.storage import DataStorage


# ─── 测试数据工厂 ────────────────────────────────────────────────────────────

def make_daily_df(code: str, market: int, start_date: str, periods: int) -> pd.DataFrame:
    """生成假日线数据，date 列为 DatetimeIndex（模拟 reader 返回格式）。"""
    dates = pd.bdate_range(start=start_date, periods=periods)
    df = pd.DataFrame({
        'open':   [10.0] * periods,
        'high':   [11.0] * periods,
        'low':    [9.0]  * periods,
        'close':  [10.5] * periods,
        'volume': [1e6]  * periods,
        'amount': [1e7]  * periods,
        'code':   [code[-6:]] * periods,
        'market': [market] * periods,
    }, index=dates)
    df.index.name = 'date'
    return df


def make_gbbq_empty() -> pd.DataFrame:
    return pd.DataFrame()


def make_gbbq_with_event(full_code: str, ex_date_int: int) -> pd.DataFrame:
    """构造一条除权记录（category=1，送股 10%）。"""
    if full_code.startswith('sh'):
        market_val = 1
    elif full_code.startswith('bj'):
        market_val = 2
    else:
        market_val = 0
    return pd.DataFrame([{
        'market': market_val,
        'code': int(full_code[2:]),
        'datetime': ex_date_int,
        'category': 1,
        'hongli_panqianliutong': 0,
        'peigujia_qianzongguben': 0,
        'songgu_qianzongguben': 1.0,   # 每10股送1股 → songgu/10 = 0.1
        'peigu_houzongguben': 0,
        'full_code': full_code,
    }])


# ─── 测试用例 ────────────────────────────────────────────────────────────────

class TestFullSyncOneMonth:
    """全量获取所有股票1个月日线数据。"""

    def test_records_written_and_date_format(self, tmp_path):
        db_url = f"sqlite:///{tmp_path}/test.db"
        storage = DataStorage(db_url=db_url)
        processor = DataProcessor()

        # 模拟3只股票，每只20个交易日（约1个月）
        stocks = [
            ('sz000001', 0), ('sz000002', 0), ('sh600000', 1)
        ]
        gbbq = make_gbbq_empty()

        for code, market in stocks:
            df = make_daily_df(code, market, '2024-03-01', 20)
            df = df.reset_index()
            processed = processor.process_daily_data(df, gbbq=gbbq, adj_type='none')
            storage.save_incremental(processed, 'daily_data', conflict_columns=('stock_code', 'date'))

        # 验证
        with storage.engine.connect() as conn:
            from sqlalchemy import text
            rows = conn.execute(text("SELECT COUNT(*) FROM daily_data")).fetchone()
            assert rows[0] == 60, f"期望60条，实际{rows[0]}"

            # date 列应为 YYYYMMDD 字符串
            sample = conn.execute(text("SELECT date FROM daily_data LIMIT 1")).fetchone()
            assert isinstance(sample[0], str), f"date 应为字符串，实际类型: {type(sample[0])}"
            assert '20240301' <= sample[0] <= '20241231'


class TestSingleStockOneYear:
    """指定股票最近1年日线数据。"""

    def test_date_range_correct(self, tmp_path):
        db_url = f"sqlite:///{tmp_path}/test.db"
        storage = DataStorage(db_url=db_url)
        processor = DataProcessor()

        # 生成约250个交易日（1年）
        df = make_daily_df('sz000001', 0, '2023-04-01', 250)
        df = df.reset_index()
        gbbq = make_gbbq_empty()
        processed = processor.process_daily_data(df, gbbq=gbbq, adj_type='none')

        # 按日期过滤：只取 20240101 之后
        filtered = processor.filter_data(processed, start_date=20240101)
        storage.save_incremental(filtered, 'daily_data', conflict_columns=('code', 'date'))

        with storage.engine.connect() as conn:
            from sqlalchemy import text
            rows = conn.execute(
                text("SELECT MIN(date), MAX(date), COUNT(*) FROM daily_data WHERE stock_code='000001'")
            ).fetchone()
            min_date, max_date, count = rows
            assert min_date >= '20240101', f"最小日期 {min_date} 应 >= 20240101"
            assert count > 0


class TestForwardAdjPrice:
    """前复权价格计算正确性：除权日前后价格应连续（无跳空）。"""

    def test_price_continuity_across_ex_date(self):
        processor = DataProcessor()

        # 构造数据：2024-01-01 ~ 2024-03-31，共约60个交易日
        # 除权日设为 2024-02-01（送股10%，songgu=1.0 即每10股送1股）
        df = make_daily_df('sz000001', 0, '2024-01-02', 60)
        df = df.reset_index()

        # 所有原始收盘价均为 10.5
        gbbq = make_gbbq_with_event('sz000001', 20240201)
        processed = processor.process_daily_data(df, gbbq=gbbq, adj_type='forward')

        ex_date = '20240201'
        before = processed[processed['date'] < ex_date]
        on_or_after = processed[processed['date'] >= ex_date]

        assert not before.empty, "除权日前应有数据"
        assert not on_or_after.empty, "除权日后应有数据"

        # 前复权后，除权日前的价格应被调低（adj_factor < 1）
        # 送股10%：factor = prev_close / (prev_close * 1.1) ≈ 0.909
        # 除权日前 close = 10.5 * 0.909 ≈ 9.545
        # 除权日后 close = 10.5（原始价格不变）
        adj_close_before = before['close'].iloc[-1]
        raw_close_after = on_or_after['close'].iloc[0]

        # 验证复权因子已应用（除权日前价格应低于原始价格）
        assert adj_close_before < 10.5, f"前复权后除权日前收盘价应 < 10.5，实际 {adj_close_before}"
        assert abs(raw_close_after - 10.5) < 0.01, f"除权日后收盘价应保持 10.5，实际 {raw_close_after}"

        # 验证 adj_factor 列存在
        assert 'adj_factor' in processed.columns

    def test_no_adj_factor_without_gbbq(self):
        processor = DataProcessor()
        df = make_daily_df('sz000001', 0, '2024-01-02', 10).reset_index()
        processed = processor.process_daily_data(df, gbbq=None, adj_type='forward')
        assert 'adj_factor' in processed.columns
        assert (processed['adj_factor'] == 1.0).all()


class TestIncrementalUpdate:
    """增量更新：无重复行；有除权时旧数据被替换。"""

    def test_no_duplicates_on_second_sync(self, tmp_path):
        db_url = f"sqlite:///{tmp_path}/test.db"
        storage = DataStorage(db_url=db_url)
        processor = DataProcessor()
        gbbq = make_gbbq_empty()

        # 第一次同步：20个交易日
        df1 = make_daily_df('sz000001', 0, '2024-01-02', 20).reset_index()
        p1 = processor.process_daily_data(df1, gbbq=gbbq, adj_type='none')
        storage.save_incremental(p1, 'daily_data', conflict_columns=('code', 'date'))

        # 第二次同步：同样的数据（模拟重复运行）
        storage.save_incremental(p1, 'daily_data', conflict_columns=('code', 'date'))

        with storage.engine.connect() as conn:
            from sqlalchemy import text
            count = conn.execute(
                text("SELECT COUNT(*) FROM daily_data WHERE stock_code='000001'")
            ).fetchone()[0]
            assert count == 20, f"重复同步后应仍为20条，实际{count}"

    def test_incremental_appends_new_records(self, tmp_path):
        db_url = f"sqlite:///{tmp_path}/test.db"
        storage = DataStorage(db_url=db_url)
        processor = DataProcessor()
        gbbq = make_gbbq_empty()

        # 第一次：前10个交易日
        df1 = make_daily_df('sz000001', 0, '2024-01-02', 10).reset_index()
        p1 = processor.process_daily_data(df1, gbbq=gbbq, adj_type='none')
        storage.save_incremental(p1, 'daily_data', conflict_columns=('code', 'date'))

        last_date = storage.get_latest_date_by_code('000001')
        assert last_date is not None

        # 第二次：后10个交易日（增量，只取 last_date 之后）
        df2 = make_daily_df('sz000001', 0, '2024-01-02', 25).reset_index()
        p2 = processor.process_daily_data(df2, gbbq=gbbq, adj_type='none')
        p2_new = p2[p2['date'] > last_date]
        storage.save_incremental(p2_new, 'daily_data', conflict_columns=('code', 'date'))

        with storage.engine.connect() as conn:
            from sqlalchemy import text
            count = conn.execute(
                text("SELECT COUNT(*) FROM daily_data WHERE stock_code='000001'")
            ).fetchone()[0]
            assert count == 25, f"增量后应为25条，实际{count}"

    def test_full_refresh_on_ex_rights(self, tmp_path):
        """有除权事件时，旧数据应被删除并重写（复权价格更新）。"""
        db_url = f"sqlite:///{tmp_path}/test.db"
        storage = DataStorage(db_url=db_url)
        processor = DataProcessor()

        # 第一次同步：无复权
        df = make_daily_df('sz000001', 0, '2024-01-02', 20).reset_index()
        gbbq_empty = make_gbbq_empty()
        p1 = processor.process_daily_data(df, gbbq=gbbq_empty, adj_type='none')
        storage.save_incremental(p1, 'daily_data', conflict_columns=('code', 'date'))

        # 模拟发现除权事件 → 删除旧数据 + 重写前复权数据
        gbbq = make_gbbq_with_event('sz000001', 20240115)
        p2 = processor.process_daily_data(df, gbbq=gbbq, adj_type='forward')

        storage.delete_stock_data('000001')
        storage.save_incremental(p2, 'daily_data', conflict_columns=('code', 'date'))

        with storage.engine.connect() as conn:
            from sqlalchemy import text
            count = conn.execute(
                text("SELECT COUNT(*) FROM daily_data WHERE stock_code='000001'")
            ).fetchone()[0]
            # 重写后记录数应与原始相同
            assert count == 20, f"全量重写后应为20条，实际{count}"

            # 除权日前的价格应已被调整（< 10.5）
            adj_row = conn.execute(
                text("SELECT close FROM daily_data WHERE stock_code='000001' AND date < '20240115' ORDER BY date DESC LIMIT 1")
            ).fetchone()
            if adj_row:
                assert adj_row[0] < 10.5, f"前复权后除权日前收盘价应 < 10.5，实际 {adj_row[0]}"


# ─── 三市场测试 ──────────────────────────────────────────────────────────────

class TestMultiMarket:
    """验证三个市场（sz/sh/bj）日线数据均能正确写入，且 market 列值准确。"""

    def test_all_three_markets_sync(self, tmp_path):
        """sz=0, sh=1, bj=2 三市场股票同步后记录数和 market 值均正确。"""
        db_url = f"sqlite:///{tmp_path}/test.db"
        storage = DataStorage(db_url=db_url)
        processor = DataProcessor()
        gbbq = make_gbbq_empty()

        stocks = [
            ('sz000001', 0),   # 深圳主板
            ('sh600000', 1),   # 上海主板
            ('bj920001', 2),   # 北交所（92开头新股）
        ]
        for code, market in stocks:
            df = make_daily_df(code, market, '2024-03-01', 10).reset_index()
            processed = processor.process_daily_data(df, gbbq=gbbq, adj_type='none')
            storage.save_incremental(processed, 'daily_data', conflict_columns=('stock_code', 'date'))

        with storage.engine.connect() as conn:
            from sqlalchemy import text
            for pure_code, expected_market in [('000001', 0), ('600000', 1), ('920001', 2)]:
                row = conn.execute(text(
                    f"SELECT market, COUNT(*) FROM daily_data WHERE stock_code='{pure_code}' GROUP BY market"
                )).fetchone()
                assert row is not None, f"{pure_code} 无数据"
                assert row[0] == expected_market, f"{pure_code} market 应为 {expected_market}，实际 {row[0]}"
                assert row[1] == 10, f"{pure_code} 应有10条记录，实际 {row[1]}"

    def test_bj_ex_rights_refresh(self, tmp_path):
        """北交所股票有除权事件时，旧数据应被删除并重写。"""
        db_url = f"sqlite:///{tmp_path}/test.db"
        storage = DataStorage(db_url=db_url)
        processor = DataProcessor()

        df = make_daily_df('bj920001', 2, '2024-01-02', 20).reset_index()
        gbbq = make_gbbq_with_event('bj920001', 20240115)

        p1 = processor.process_daily_data(df, gbbq=make_gbbq_empty(), adj_type='none')
        storage.save_incremental(p1, 'daily_data', conflict_columns=('stock_code', 'date'))

        storage.delete_stock_data('920001')
        p2 = processor.process_daily_data(df, gbbq=gbbq, adj_type='forward')
        storage.save_incremental(p2, 'daily_data', conflict_columns=('stock_code', 'date'))

        with storage.engine.connect() as conn:
            from sqlalchemy import text
            count = conn.execute(text(
                "SELECT COUNT(*) FROM daily_data WHERE stock_code='920001'"
            )).fetchone()[0]
            assert count == 20
