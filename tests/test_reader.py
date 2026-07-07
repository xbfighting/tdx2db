"""reader 二进制解析回归测试（issue #34）。

fixture 为确定性合成字节（见 tests/fixtures/make_fixtures.py），
断言值与生成参数一一对应。覆盖三条解析路径：
- 深市 .day → pytdx TdxDailyBarReader（A 股系数：价 ×0.01、量 ×0.01）
- 沪市 688 .day → 自研 _read_day_file_raw 回退（pytdx 不识别科创板）
- .lc5 → pytdx TdxLCMinBarReader
"""
import shutil
from pathlib import Path

import os

import pandas as pd
import pytest

from tdx2db.reader import TdxDataReader

FIXTURES = Path(__file__).parent / 'fixtures'


@pytest.fixture
def tdx_reader(tmp_path):
    """用 fixture 文件搭出最小 vipdoc 目录结构"""
    for market, sub, fname in [
        ('sz', 'lday', 'sz000001.day'),
        ('sh', 'lday', 'sh688001.day'),
        ('sz', 'fzline', 'sz000001.lc5'),
    ]:
        dst = tmp_path / 'vipdoc' / market / sub
        dst.mkdir(parents=True, exist_ok=True)
        shutil.copy(FIXTURES / fname, dst / fname)
    return TdxDataReader(tdx_path=str(tmp_path))


class TestDailyParsing:
    def test_sz_a_stock_via_pytdx(self, tdx_reader):
        df = tdx_reader.read_daily_data(0, 'sz000001')

        assert len(df) == 2
        assert list(df.index.strftime('%Y-%m-%d')) == ['2026-06-05', '2026-06-08']
        first = df.iloc[0]
        assert first['open'] == 10.50
        assert first['high'] == 10.75
        assert first['low'] == 10.25
        assert first['close'] == 10.50
        assert first['amount'] == 5250000.0
        assert first['volume'] == 5000.0          # 500000 × 0.01（pytdx A 股系数）
        assert first['code'] == '000001'          # 6 位纯数字，无前缀
        assert first['market'] == 0

    def test_688_via_raw_fallback(self, tdx_reader):
        """688 走 _read_day_file_raw，系数必须与 pytdx SH_A_STOCK 一致"""
        df = tdx_reader.read_daily_data(1, 'sh688001')

        assert len(df) == 1
        row = df.iloc[0]
        assert row['open'] == 50.00
        assert row['high'] == 51.50
        assert row['low'] == 49.50
        assert row['close'] == 50.75
        assert row['amount'] == 25375000.0
        assert row['volume'] == 3000.0            # 300000 × 0.01，与 pytdx 路径同系数
        assert row['code'] == '688001'

    def test_missing_file_raises(self, tdx_reader):
        with pytest.raises(FileNotFoundError):
            tdx_reader.read_daily_data(0, 'sz999999')


class TestStockListNames:
    def test_real_names_from_infoharbor_ex(self, tmp_path):
        """stock_info.name 用 infoharbor_ex.code 真名（issue #42），code 保持带前缀"""
        for m, f in [('sz', 'sz000001.day'), ('sh', 'sh688001.day')]:
            d = tmp_path / 'vipdoc' / m / 'lday'
            d.mkdir(parents=True)
            shutil.copy(FIXTURES / f, d / f)
        hq = tmp_path / 'T0002' / 'hq_cache'
        hq.mkdir(parents=True)
        (hq / 'infoharbor_ex.code').write_bytes(
            '000001|平安银行|平安保险,谢永林\n688001|华兴源创|陈文源\n'.encode('gbk')
        )
        df = TdxDataReader(tdx_path=str(tmp_path)).get_stock_list()
        names = dict(zip(df.code, df.name))
        assert names == {'sz000001': '平安银行', 'sh688001': '华兴源创'}

    def test_fallback_placeholder_when_file_missing(self, tdx_reader):
        """名称文件缺失回退占位符，不报错；股本/日期列为 NULL"""
        df = tdx_reader.get_stock_list()
        names = dict(zip(df.code, df.name))
        assert names['sz000001'] == '深Asz000001'
        assert names['sh688001'] == '上Ash688001'
        assert df['zgb'].isna().all() and df['list_date'].isna().all()


class TestStockListCapital:
    def test_capital_fields_from_base_dbf(self, tmp_path):
        """zgb/ltag（万股）与 capital_date/list_date 来自 base.dbf（issue #45）"""
        from tests.test_blocks import _make_dbf

        for m, f in [('sz', 'sz000001.day'), ('sh', 'sh688001.day')]:
            d = tmp_path / 'vipdoc' / m / 'lday'
            d.mkdir(parents=True)
            shutil.copy(FIXTURES / f, d / f)
        hq = tmp_path / 'T0002' / 'hq_cache'
        hq.mkdir(parents=True)
        _make_dbf(
            hq / 'base.dbf',
            [
                ('0', '000001', '20260425', '1940591.87', '1940560.12', '19910403'),
                ('1', '688001', '', '', '', ''),   # 空值 → NULL
            ],
            fields=[('SC', 2), ('GPDM', 6), ('GXRQ', 8), ('ZGB', 12), ('LTAG', 12), ('SSDATE', 8)],
        )

        df = TdxDataReader(tdx_path=str(tmp_path)).get_stock_list().set_index('code')
        pa = df.loc['sz000001']
        assert pa['zgb'] == pytest.approx(1940591.87)
        assert pa['ltag'] == pytest.approx(1940560.12)
        assert str(pa['capital_date']) == '2026-04-25'
        assert str(pa['list_date']) == '1991-04-03'
        hx = df.loc['sh688001']
        assert pd.isna(hx['zgb']) and hx['list_date'] is None

    def test_missing_base_dbf_degrades(self, tdx_reader):
        """base.dbf 缺失 → 四列 NULL，不报错（tdx_reader fixture 无 T0002）"""
        df = tdx_reader.get_stock_list()
        assert set(df.columns) == {'code', 'name', 'zgb', 'ltag', 'capital_date', 'list_date'}
        assert df['ltag'].isna().all()


@pytest.fixture
def real_tdx_reader(tmp_path):
    """真实文件切片（dd bs=32 count=3 自实际 vipdoc，2026-07-07）——
    防合成 fixture 与解析器共享同一格式误解"""
    for market, sub, src, dst in [
        ('sz', 'lday', 'real_sz000001.day', 'sz000001.day'),
        ('sh', 'lday', 'real_sh688001.day', 'sh688001.day'),
        ('sz', 'fzline', 'real_sz000001.lc5', 'sz000001.lc5'),
    ]:
        d = tmp_path / 'vipdoc' / market / sub
        d.mkdir(parents=True, exist_ok=True)
        shutil.copy(FIXTURES / src, d / dst)
    return TdxDataReader(tdx_path=str(tmp_path))


class TestRealFileSlices:
    """断言值为真实文件的观测值（历史数据永不变化，天然稳定）"""

    def test_sz000001_1991_head(self, real_tdx_reader):
        df = real_tdx_reader.read_daily_data(0, 'sz000001')
        assert list(df.index.strftime('%Y-%m-%d')) == ['1991-04-03', '1991-04-04', '1991-04-05']
        assert df.iloc[0]['close'] == pytest.approx(49.00, abs=1e-9)
        assert df.iloc[1]['close'] == pytest.approx(48.76, abs=1e-9)
        assert df.iloc[0]['amount'] == 5000.0
        assert df.iloc[0]['volume'] == 1.0

    def test_sh688001_2019_head_via_fallback(self, real_tdx_reader):
        """688001 华兴源创（科创板第一股）上市首日起 3 条，走 raw 回退路径"""
        df = real_tdx_reader.read_daily_data(1, 'sh688001')
        assert list(df.index.strftime('%Y-%m-%d')) == ['2019-07-22', '2019-07-23', '2019-07-24']
        first = df.iloc[0]
        assert first['open'] == pytest.approx(55.40, abs=1e-9)
        assert first['high'] == pytest.approx(72.02, abs=1e-9)
        assert first['low'] == pytest.approx(39.59, abs=1e-9)
        assert first['close'] == pytest.approx(55.50, abs=1e-9)
        assert first['amount'] == pytest.approx(1.507398e9, rel=1e-6)   # float32 精度
        assert first['volume'] == pytest.approx(290107.54, rel=1e-6)

    def test_lc5_real_head(self, real_tdx_reader):
        df = real_tdx_reader.read_5min_data(0, 'sz000001')
        assert list(df['datetime'].dt.strftime('%Y-%m-%d %H:%M')) == [
            '2025-01-21 09:35', '2025-01-21 09:40', '2025-01-21 09:45',
        ]
        first = df.iloc[0]
        assert first['open'] == pytest.approx(11.45, abs=1e-5)          # float32
        assert first['close'] == pytest.approx(11.38, abs=1e-5)
        assert first['volume'] == 9444100


class TestLc5Parsing:
    def test_fivemin_records(self, tdx_reader):
        df = tdx_reader.read_5min_data(0, 'sz000001')

        assert len(df) == 2
        assert list(df['datetime'].dt.strftime('%Y-%m-%d %H:%M')) == [
            '2026-06-05 09:35', '2026-06-05 09:40',
        ]
        first = df.iloc[0]
        assert first['open'] == 10.5
        assert first['high'] == 10.75
        assert first['low'] == 10.25
        assert first['close'] == 10.5
        assert first['amount'] == 2100000.0
        assert first['volume'] == 200000          # lc5 无系数换算
        assert first['code'] == '000001'
        assert first['market'] == 0


class TestStockListNamesUnreadable:
    @pytest.mark.skipif(os.name == 'nt', reason='chmod 权限位在 Windows 无效')
    def test_fallback_placeholder_when_file_unreadable(self, tmp_path):
        """名称文件存在但不可读（SMB 锁定 EPERM 等）回退占位符，不中断同步"""
        import shutil as _shutil
        d = tmp_path / 'vipdoc' / 'sz' / 'lday'
        d.mkdir(parents=True)
        _shutil.copy(FIXTURES / 'sz000001.day', d / 'sz000001.day')
        hq = tmp_path / 'T0002' / 'hq_cache'
        hq.mkdir(parents=True)
        f = hq / 'infoharbor_ex.code'
        f.write_bytes('000001|平安银行|x\n'.encode('gbk'))
        f.chmod(0o000)
        try:
            df = TdxDataReader(tdx_path=str(tmp_path)).get_stock_list()
        finally:
            f.chmod(0o644)
        names = dict(zip(df.code, df.name))
        assert names['sz000001'] == '深Asz000001'
