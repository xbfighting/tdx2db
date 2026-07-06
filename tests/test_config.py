"""config 数据库 URL 构造测试（issue #18）+ TDX 路径探测测试（issue #32）"""

import pytest
from sqlalchemy import URL

from tdx2db.config import config, detect_tdx_path
from tdx2db.reader import TdxDataReader


@pytest.fixture
def pg_config(monkeypatch):
    monkeypatch.setattr(config, 'db_type', 'postgresql')
    monkeypatch.setattr(config, 'db_host', 'db.example.com')
    monkeypatch.setattr(config, 'db_port', '5432')
    monkeypatch.setattr(config, 'db_name', 'tdx_data')
    monkeypatch.setattr(config, 'db_user', 'tdx')
    return config


def test_special_chars_in_password_survive(pg_config, monkeypatch):
    """密码含 @ : / 时 f-string 拼接会解析错乱，URL.create 必须原样保留"""
    monkeypatch.setattr(config, 'db_password', 'p@ss:w/rd')
    url = config.database_url
    assert isinstance(url, URL)
    assert url.password == 'p@ss:w/rd'
    assert url.host == 'db.example.com'
    assert url.database == 'tdx_data'


def test_password_masked_in_str(pg_config, monkeypatch):
    """URL 对象默认渲染掩码密码，避免异常链/日志泄漏"""
    monkeypatch.setattr(config, 'db_password', 'secret123')
    assert 'secret123' not in str(config.database_url)


def test_mysql_drivername(pg_config, monkeypatch):
    monkeypatch.setattr(config, 'db_type', 'mysql')
    monkeypatch.setattr(config, 'db_password', 'x')
    assert config.database_url.drivername == 'mysql+pymysql'


def test_sqlite_url(monkeypatch):
    monkeypatch.setattr(config, 'db_type', 'sqlite')
    monkeypatch.setattr(config, 'db_name', 'tdx_data')
    url = config.database_url
    assert url.drivername == 'sqlite'
    assert url.database == 'tdx_data.db'


def test_unknown_db_type_raises(monkeypatch):
    monkeypatch.setattr(config, 'db_type', 'oracle')
    with pytest.raises(ValueError):
        _ = config.database_url


def test_non_numeric_port_raises_clear_error(pg_config, monkeypatch):
    """DB_PORT 手滑配错时给明确配置错误，而非裸 traceback"""
    monkeypatch.setattr(config, 'db_password', 'x')
    monkeypatch.setattr(config, 'db_port', '5432"')
    with pytest.raises(ValueError, match='DB_PORT'):
        _ = config.database_url


class TestDetectTdxPath:
    def test_returns_first_candidate_with_vipdoc(self, tmp_path):
        bad = tmp_path / 'plain_dir'
        bad.mkdir()
        good = tmp_path / 'new_tdx'
        (good / 'vipdoc').mkdir(parents=True)
        good2 = tmp_path / 'zd_zsone'
        (good2 / 'vipdoc').mkdir(parents=True)

        hit = detect_tdx_path([str(bad), str(good), str(good2)])
        assert hit == str(good)  # 首个命中，且跳过无 vipdoc 的目录

    def test_no_valid_candidate_returns_none(self, tmp_path):
        assert detect_tdx_path([str(tmp_path / 'nope')]) is None

    def test_explicit_path_skips_detection(self, tmp_path, monkeypatch):
        """显式传入路径时完全不触发探测"""
        explicit = tmp_path / 'my_tdx'
        explicit.mkdir()
        monkeypatch.setattr(
            'tdx2db.reader.detect_tdx_path',
            lambda *a, **k: pytest.fail('显式配置下不应触发探测'),
        )
        reader = TdxDataReader(tdx_path=str(explicit))
        assert str(reader.tdx_path) == str(explicit)

    def test_empty_config_without_detection_raises(self, monkeypatch):
        """未配置且探测无果时报 ValueError 且带配置指引"""
        monkeypatch.setattr(config, 'tdx_path', '')
        monkeypatch.setattr('tdx2db.reader.detect_tdx_path', lambda *a, **k: None)
        with pytest.raises(ValueError, match='TDX_PATH'):
            TdxDataReader()

    def test_detected_path_is_used(self, tmp_path, monkeypatch):
        """未配置但探测命中时 reader 正常初始化"""
        detected = tmp_path / 'new_tdx'
        (detected / 'vipdoc').mkdir(parents=True)
        monkeypatch.setattr(config, 'tdx_path', '')
        monkeypatch.setattr('tdx2db.reader.detect_tdx_path', lambda *a, **k: str(detected))
        reader = TdxDataReader()
        assert str(reader.tdx_path) == str(detected)
