"""cli 纯函数测试（issue #24）+ status / 分钟线增量自愈测试（issue #28 / #11）"""

import json

import pandas as pd
import pytest
from sqlalchemy import text

from tdx2db.cli import infer_market, parse_args, run_status, sync_single_stock_min_data
from tdx2db.config import config
from tdx2db.processor import DataProcessor
from tdx2db.storage import DataStorage


class TestInferMarket:
    @pytest.mark.parametrize('code,expected', [
        ('sh600000', 1),
        ('sz000001', 0),
        ('SH688001', 1),
        ('600000', 1),
        ('688001', 1),
        ('000001', 0),
        ('002594', 0),
        ('300750', 0),
        ('301269', 0),
    ])
    def test_inference(self, code, expected):
        assert infer_market(code) == expected


class TestArgAliases:
    def test_hyphen_and_underscore_both_accepted(self, monkeypatch):
        """--start-date 与旧拼写 --start_date 都能解析到同一 dest"""
        monkeypatch.setattr('sys.argv', ['main.py', 'daily', '--start-date', '2026-01-01'])
        assert parse_args().start_date == '2026-01-01'
        monkeypatch.setattr('sys.argv', ['main.py', 'daily', '--start_date', '2026-01-02'])
        assert parse_args().start_date == '2026-01-02'

    def test_batch_size_default_none(self, monkeypatch):
        """--batch-size 默认 None，不覆盖 .env 的 DB_BATCH_SIZE"""
        monkeypatch.setattr('sys.argv', ['main.py', 'sync'])
        assert parse_args().batch_size is None

    def test_status_subcommand_parses(self, monkeypatch):
        monkeypatch.setattr('sys.argv', ['main.py', 'status', '--json'])
        args = parse_args()
        assert args.command == 'status' and args.json is True


@pytest.fixture
def sqlite_storage(tmp_path, monkeypatch):
    monkeypatch.setattr(config, 'db_type', 'sqlite')
    return DataStorage(db_url='sqlite://', csv_path=str(tmp_path))


def _fivemin_df():
    """一天 12 根 5 分钟 K 线（sz301088，2026-06-12）"""
    times = pd.date_range('2026-06-12 09:35', periods=12, freq='5min')
    return pd.DataFrame({
        'datetime': times,
        'open': 10.0, 'high': 10.5, 'low': 9.5, 'close': 10.2,
        'volume': 1000.0, 'amount': 10200.0,
        'code': '301088', 'market': 0,
    })


class _FakeReader:
    """替代 TdxDataReader：直接返回构造好的 5 分钟数据"""

    def __init__(self, df):
        self._df = df

    def read_5min_data(self, market, code):
        return self._df.copy()


class TestMinSyncDerivedTableHealing:
    def test_heals_missing_derived_tables(self, sqlite_storage):
        """issue #11 回归：minute5 已最新但衍生表被清空时，增量重跑必须补回，
        且 minute5 不产生重复"""
        reader = _FakeReader(_fivemin_df())
        processor = DataProcessor()

        assert sync_single_stock_min_data(reader, processor, sqlite_storage, 0, 'sz301088')

        # 模拟 #11 的历史状态：衍生表写入失败，minute5 完好
        with sqlite_storage.engine.connect() as conn:
            for tbl in ('minute15_data', 'minute30_data', 'minute60_data'):
                conn.execute(text(f"DELETE FROM {tbl}"))
            conn.commit()

        # 增量重跑（旧逻辑下起点取 minute5 的 latest，衍生表永远补不回）
        assert sync_single_stock_min_data(reader, processor, sqlite_storage, 0, 'sz301088')

        with sqlite_storage.engine.connect() as conn:
            counts = {
                tbl: conn.execute(text(f"SELECT COUNT(*) FROM {tbl}")).scalar()
                for tbl in ('minute5_data', 'minute15_data', 'minute30_data', 'minute60_data')
            }
        assert counts['minute5_data'] == 12          # 无重复
        assert counts['minute15_data'] > 0            # 已自愈
        assert counts['minute30_data'] > 0
        assert counts['minute60_data'] > 0


class TestRunStatus:
    def test_json_output_with_derived_gap_warning(self, sqlite_storage, capsys):
        """status --json：表统计 + 衍生表缺口警告"""
        reader = _FakeReader(_fivemin_df())
        sync_single_stock_min_data(reader, DataProcessor(), sqlite_storage, 0, 'sz301088')
        with sqlite_storage.engine.connect() as conn:
            conn.execute(text("DELETE FROM minute15_data"))
            conn.commit()

        assert run_status(sqlite_storage, as_json=True) == 0
        payload = json.loads(capsys.readouterr().out)

        tables = {t['table']: t for t in payload['tables']}
        assert tables['minute5_data']['rows'] == 12
        assert tables['minute5_data']['codes'] == 1
        assert tables['minute15_data']['rows'] == 0
        assert any('minute15_data' in w for w in payload['warnings'])

    def test_human_output_empty_db_no_crash(self, tmp_path, monkeypatch, capsys):
        """未建表的空库：不崩溃、提示未创建"""
        monkeypatch.setattr(config, 'db_type', 'sqlite')
        storage = DataStorage(db_url='sqlite://', csv_path=str(tmp_path), create_tables=False)
        assert run_status(storage) == 0
        assert '未创建' in capsys.readouterr().out
