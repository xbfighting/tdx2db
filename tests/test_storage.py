"""storage 约束与增量写入测试（issue #16：唯一约束内建进模型）"""

import pandas as pd
import pytest
from sqlalchemy import inspect, text

from tdx2db.config import config
from tdx2db.storage import DataStorage


@pytest.fixture
def sqlite_storage(tmp_path, monkeypatch):
    """SQLite 内存库，隔离于真实环境配置"""
    monkeypatch.setattr(config, 'db_type', 'sqlite')
    return DataStorage(db_url='sqlite://', csv_path=str(tmp_path))


def _daily_df():
    return pd.DataFrame([
        {
            'code': code, 'market': 0,
            'datetime': day, 'date': day,
            'open': 10.0, 'high': 10.5, 'low': 9.5, 'close': 10.2,
            'volume': 1000.0, 'amount': 10200.0,
        }
        for code in ['000001', '600000']
        for day in pd.date_range('2026-01-05', periods=3)
    ])


def test_create_all_builds_unique_constraints(sqlite_storage):
    """create_all 自动携带唯一约束，新用户无需手动跑迁移脚本"""
    insp = inspect(sqlite_storage.engine)
    daily_uqs = [set(u['column_names']) for u in insp.get_unique_constraints('daily_data')]
    assert {'code', 'date'} in daily_uqs
    for tbl in ['minute5_data', 'minute15_data', 'minute30_data', 'minute60_data']:
        uqs = [set(u['column_names']) for u in insp.get_unique_constraints(tbl)]
        assert {'code', 'datetime'} in uqs, tbl


def test_duplicate_incremental_insert_is_ignored(sqlite_storage):
    """重复写入被约束去重——无约束时会静默累积（issue #16 的核心回归）"""
    df = _daily_df()
    sqlite_storage.save_incremental(df, 'daily_data', conflict_columns=('code', 'date'))
    sqlite_storage.save_incremental(df, 'daily_data', conflict_columns=('code', 'date'))
    with sqlite_storage.engine.connect() as conn:
        n = conn.execute(text("SELECT COUNT(*) FROM daily_data")).scalar()
    assert n == len(df)


def test_save_incremental_rejects_unknown_table(sqlite_storage):
    with pytest.raises(ValueError):
        sqlite_storage.save_incremental(_daily_df(), 'evil_table', conflict_columns=('code', 'date'))


def test_get_latest_datetime_returns_datetime_on_sqlite(sqlite_storage):
    """SQLite 的 MAX() 返回 TEXT，必须归一化为 datetime，
    否则增量同步 latest + timedelta 崩溃（第二次 sync 全失败）"""
    from datetime import datetime, timedelta

    sqlite_storage.save_incremental(_daily_df(), 'daily_data', conflict_columns=('code', 'date'))

    latest = sqlite_storage.get_latest_datetime('daily_data', date_column='date')
    by_code = sqlite_storage.get_latest_datetime_by_code('daily_data', '000001', date_column='date')

    for value in (latest, by_code):
        assert isinstance(value, datetime), type(value)
        # 增量路径的实际用法必须可运算
        assert (value + timedelta(days=1)).strftime('%Y-%m-%d') == '2026-01-08'
