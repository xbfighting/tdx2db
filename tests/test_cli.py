"""cli 纯函数测试（issue #24）+ status / 分钟线增量自愈测试（issue #28 / #11）"""

import json
import shutil
from pathlib import Path

import pandas as pd
import pytest
from sqlalchemy import text

from tdx2db.cli import (
    infer_market, main, parse_args, run_status, sync_all_daily_data, sync_single_stock_min_data,
)
from tdx2db.config import config
from tdx2db.processor import DataProcessor
from tdx2db.storage import DataStorage

FIXTURES = Path(__file__).parent / 'fixtures'


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

    def test_human_output_warning_encodable_in_gbk(self, sqlite_storage, capsys):
        """Windows 下 stdout 重定向时编码为 cp936(GBK)：输出必须可编码，
        否则 `tdx2db status > log.txt` 在有警告时 UnicodeEncodeError 崩溃"""
        reader = _FakeReader(_fivemin_df())
        sync_single_stock_min_data(reader, DataProcessor(), sqlite_storage, 0, 'sz301088')
        with sqlite_storage.engine.connect() as conn:
            conn.execute(text("DELETE FROM minute15_data"))
            conn.commit()

        assert run_status(sqlite_storage) == 0
        out = capsys.readouterr().out
        assert 'minute15_data' in out
        out.encode('gbk')  # 不抛 UnicodeEncodeError


class _FakeDailyReader:
    """替代 TdxDataReader：单只股票的日线"""

    def get_stock_list(self):
        return pd.DataFrame({'code': ['sz000001'], 'name': ['平安银行']})

    def read_daily_data(self, market, code):
        days = pd.date_range('2026-01-05', periods=3)
        return pd.DataFrame({
            'open': 10.0, 'high': 10.5, 'low': 9.5, 'close': 10.2,
            'amount': 10200.0, 'volume': 1000.0, 'code': '000001', 'market': 0,
        }, index=pd.DatetimeIndex(days, name='date'))


class TestDailySyncWriteFailure:
    def test_write_failure_is_counted_not_reported_as_up_to_date(self, sqlite_storage):
        """写库失败必须计入失败数、返回 False（退出码 1）。此前 save_incremental
        吞异常返回 0，全部写入失败时照样报"日线数据已是最新"、退出码 0"""
        with sqlite_storage.engine.connect() as conn:
            conn.execute(text("DROP TABLE daily_data"))
            conn.commit()

        assert sync_all_daily_data(_FakeDailyReader(), DataProcessor(), sqlite_storage) is False

    def test_success_path_still_returns_true(self, sqlite_storage):
        assert sync_all_daily_data(_FakeDailyReader(), DataProcessor(), sqlite_storage) is True
        with sqlite_storage.engine.connect() as conn:
            assert conn.execute(text("SELECT COUNT(*) FROM daily_data")).scalar() == 3


@pytest.fixture
def tdx_env(tmp_path, monkeypatch):
    """真实 fixture 文件组成的最小通达信目录 + SQLite 文件库，走完整 main() 路径。

    config 是全局单例、update_config 会改写它：先 monkeypatch 相关属性，
    测试结束自动还原，避免污染其他测试
    """
    tdx = tmp_path / 'tdx'
    for sub, fname in (('lday', 'sz000001.day'), ('fzline', 'sz000001.lc5')):
        d = tdx / 'vipdoc' / 'sz' / sub
        d.mkdir(parents=True)
        shutil.copy(FIXTURES / fname, d / fname)
    for attr in ('tdx_path', 'db_type', 'db_name', 'use_tqdm', 'csv_output_path'):
        monkeypatch.setattr(config, attr, getattr(config, attr))
    monkeypatch.chdir(tmp_path)
    db_name = str(tmp_path / 'tdx_test')

    def run(*argv):
        monkeypatch.setattr('sys.argv', [
            'tdx2db', '--no-tqdm', '--tdx-path', str(tdx),
            '--db-type', 'sqlite', '--db-name', db_name, *argv,
        ])
        return main()

    run.storage = lambda: DataStorage(db_url=f'sqlite:///{db_name}.db', csv_path=str(tmp_path / 'out'))
    return run


class TestMinutesAutoStart:
    def test_new_stock_history_not_filtered_by_other_stocks_latest(self, tdx_env):
        """minutes --auto-start 逐股票增量：库里其他股票更新到 06-10，
        新纳入的 000001（数据在 06-05）也必须全历史入库。
        旧逻辑取 minute15 全表 MAX+1 做统一起点，000001 被整段过滤（daily #12 同款）"""
        storage = tdx_env.storage()
        with storage.engine.connect() as conn:
            conn.execute(text(
                "INSERT INTO minute15_data (code, market, datetime, date, open, high, low, close, volume, amount) "
                "VALUES ('600000', 1, '2026-06-10 15:00:00', '2026-06-10 00:00:00', 1, 1, 1, 1, 1, 1)"
            ))
            conn.commit()

        assert tdx_env('minutes', '--db-only', '--auto-start', '--incremental') == 0

        with storage.engine.connect() as conn:
            n = conn.execute(text("SELECT COUNT(*) FROM minute5_data WHERE code = '000001'")).scalar()
        assert n == 2

    def test_auto_start_rerun_is_idempotent(self, tdx_env):
        assert tdx_env('minutes', '--db-only', '--auto-start') == 0
        assert tdx_env('minutes', '--db-only', '--auto-start') == 0
        with tdx_env.storage().engine.connect() as conn:
            assert conn.execute(text("SELECT COUNT(*) FROM minute5_data")).scalar() == 2

    def test_csv_only_rejected_instead_of_silently_writing_db(self, tdx_env):
        """minutes 没有 CSV 写出路径：此前 --csv-only 被忽略、照样写库、退出码 0"""
        assert tdx_env('minutes', '--csv-only') == 1
        with tdx_env.storage().engine.connect() as conn:
            assert conn.execute(text("SELECT COUNT(*) FROM minute5_data")).scalar() == 0


def test_db_only_commands_do_not_create_output_dir(tdx_env, tmp_path):
    """--db-only / status 不应在工作目录留下空 output/（status 承诺零写操作）"""
    assert tdx_env('status') == 0
    assert tdx_env('minutes', '--db-only', '--auto-start') == 0
    assert not (tmp_path / 'output').exists()
