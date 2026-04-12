"""
float_cap_map 构建和换手率计算测试
"""
import pandas as pd
import pytest

from src.tdx2db.processor import DataProcessor


def make_gbbq_event(full_code: str, date_int: int, category: int, value: float) -> pd.DataFrame:
    market_val = 1 if full_code.startswith('sh') else 0
    return pd.DataFrame([{
        'market': market_val,
        'code': int(full_code[2:]),
        'datetime': date_int,
        'category': category,
        'hongli_panqianliutong': value,
        'songgu_qianzongguben': value if category == 1 else 0,
        'peigu_houzongguben': 0,
        'peigujia_qianzongguben': 0,
        'full_code': full_code,
    }])


class TestBuildFloatCapitalMap:

    def test_cat1_songgu(self):
        """category==1 送股：历史股本 = 当前 / (1 + ratio)"""
        # 当前 1100 万股，曾经 10 送 1（ratio=0.1）
        base_caps = {'000001': 1100.0}
        gbbq = make_gbbq_event('sz000001', 20240101, 1, 1.0)  # songgu=1.0 → ratio=0.1
        result = DataProcessor.build_float_capital_map(base_caps, gbbq)

        assert 'sz000001' in result
        snapshots = dict(result['sz000001'])
        # 事件日期 20240101 起生效的股本 = 1100（送股后）
        assert abs(snapshots[20240101] - 1100.0) < 0.01
        # 兜底（date=0）= 1100 / 1.1 = 1000
        assert abs(snapshots[0] - 1000.0) < 0.01

    def test_cat12_jiejin(self):
        """category==12 解禁：历史股本 = 当前 - N"""
        base_caps = {'000001': 1100.0}
        gbbq = make_gbbq_event('sz000001', 20240101, 12, 100.0)
        result = DataProcessor.build_float_capital_map(base_caps, gbbq)

        snapshots = dict(result['sz000001'])
        assert abs(snapshots[20240101] - 1100.0) < 0.01
        assert abs(snapshots[0] - 1000.0) < 0.01

    def test_cat10_zhuxiao(self):
        """category==10 回购注销：历史股本 = 当前 + N（注销后股本减少，回溯加回）"""
        base_caps = {'000001': 900.0}
        gbbq = make_gbbq_event('sz000001', 20240101, 10, 100.0)
        result = DataProcessor.build_float_capital_map(base_caps, gbbq)

        snapshots = dict(result['sz000001'])
        assert abs(snapshots[20240101] - 900.0) < 0.01
        assert abs(snapshots[0] - 1000.0) < 0.01

    def test_empty_base_caps(self):
        gbbq = make_gbbq_event('sz000001', 20240101, 1, 1.0)
        assert DataProcessor.build_float_capital_map({}, gbbq) == {}

    def test_empty_gbbq(self):
        assert DataProcessor.build_float_capital_map({'000001': 1000.0}, pd.DataFrame()) == {}

    def test_code_not_in_base_caps(self):
        """gbbq 有记录但 base_caps 没有该股票，应跳过"""
        base_caps = {'000002': 500.0}
        gbbq = make_gbbq_event('sz000001', 20240101, 1, 1.0)
        result = DataProcessor.build_float_capital_map(base_caps, gbbq)
        assert 'sz000001' not in result


class TestCalcTurnoverRateWithMap:

    def _make_df(self, code='000001', market=0, dates=None, volume=1e6):
        if dates is None:
            dates = ['20240101', '20240102', '20240103']
        return pd.DataFrame({
            'date': dates,
            'volume': [volume] * len(dates),
            'code': [code] * len(dates),
            'market': [market] * len(dates),
        })

    def test_priority_path_used(self):
        """float_cap_map 存在时走优先路径，换手率应有值"""
        df = self._make_df(volume=1000.0)
        # 流通股本 1000 万股 = 1000 * 10000 = 1e7 股
        # 换手率 = volume(手) * 10000 / 流通股本(股) = 1000 * 10000 / 1e7 = 1.0%
        float_cap_map = {'sz000001': [(0, 1000.0)]}
        gbbq = pd.DataFrame()

        result = DataProcessor._calc_turnover_rate(df, gbbq, float_cap_map=float_cap_map)
        assert result.notna().all()
        assert abs(result.iloc[0] - 1.0) < 0.001

    def test_fallback_when_not_in_map(self):
        """full_code 不在 float_cap_map 中时，降级到 gbbq category==5"""
        df = self._make_df()
        float_cap_map = {'sz000002': [(0, 1000.0)]}  # 不含 sz000001
        gbbq = pd.DataFrame()  # category==5 也无数据

        result = DataProcessor._calc_turnover_rate(df, gbbq, float_cap_map=float_cap_map)
        assert result.isna().all()

    def test_fallback_when_map_is_none(self):
        """float_cap_map=None 时走原有逻辑"""
        df = self._make_df()
        gbbq = pd.DataFrame()
        result = DataProcessor._calc_turnover_rate(df, gbbq, float_cap_map=None)
        assert result.isna().all()
