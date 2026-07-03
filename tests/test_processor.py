"""processor 纯函数单元测试（CI pytest 步骤的最小真实测试套件，issue #14）"""

import pandas as pd
import pytest

from src.processor import DataProcessor, MA_WINDOWS


@pytest.fixture
def daily_df():
    """两只股票各 6 天的最小日线数据"""
    rows = []
    for code in ['000001', '600000']:
        for i, day in enumerate(pd.date_range('2026-01-05', periods=6)):
            price = 10.0 + i if code == '000001' else 20.0 + i
            rows.append({
                'code': code, 'date': day,
                'open': price, 'high': price + 0.5, 'low': price - 0.5,
                'close': price, 'volume': 1000 + i, 'amount': price * (1000 + i),
            })
    return pd.DataFrame(rows)


class TestFilterData:
    def test_start_date_inclusive(self, daily_df):
        out = DataProcessor.filter_data(daily_df, start_date='2026-01-08')
        assert out['date'].min() == pd.Timestamp('2026-01-08')
        assert len(out) == 6  # 每只股票剩 3 天

    def test_end_date_inclusive(self, daily_df):
        out = DataProcessor.filter_data(daily_df, end_date='2026-01-06')
        assert out['date'].max() == pd.Timestamp('2026-01-06')
        assert len(out) == 4

    def test_codes_filter(self, daily_df):
        out = DataProcessor.filter_data(daily_df, codes=['600000'])
        assert set(out['code']) == {'600000'}

    def test_no_filter_returns_all(self, daily_df):
        assert len(DataProcessor.filter_data(daily_df)) == len(daily_df)

    def test_empty_df_passthrough(self):
        empty = pd.DataFrame()
        assert DataProcessor.filter_data(empty, start_date='2026-01-01').empty


class TestProcessDailyData:
    def test_ma_columns_added(self, daily_df):
        out = DataProcessor.process_daily_data(daily_df)
        for w in MA_WINDOWS:
            assert f'ma{w}' in out.columns

    def test_ma5_grouped_by_code(self, daily_df):
        """均线必须按股票分组计算，不能跨股票串窗口"""
        out = DataProcessor.process_daily_data(daily_df)
        row = out[(out['code'] == '000001') & (out['date'] == '2026-01-10')].iloc[0]
        # 000001 第 2-6 天 close = 11..15，ma5 = 13
        assert row['ma5'] == pytest.approx(13.0)
        # 窗口不足的行为 NaN
        first = out[(out['code'] == '600000') & (out['date'] == '2026-01-05')].iloc[0]
        assert pd.isna(first['ma5'])

    def test_empty_df_passthrough(self):
        assert DataProcessor.process_daily_data(pd.DataFrame()).empty

    def test_does_not_mutate_input(self, daily_df):
        before = daily_df.copy()
        DataProcessor.process_daily_data(daily_df)
        pd.testing.assert_frame_equal(daily_df, before)
