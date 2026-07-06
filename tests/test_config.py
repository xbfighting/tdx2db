"""config 数据库 URL 构造测试（issue #18：URL.create 替代 f-string 拼接）"""

import os
import subprocess
import sys
from pathlib import Path

import pytest
from sqlalchemy import URL

from tdx2db.config import config


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


def test_dotenv_loaded_from_cwd_for_script_entry(tmp_path):
    """.env 必须从当前工作目录向上查找（usecwd=True）。

    回归场景：裸 load_dotenv() 默认从 config.py 所在目录（pip 安装后为
    site-packages）向上找，pip 用户放在工作目录的 .env 被静默忽略。
    必须用脚本文件（__main__ 有 __file__）子进程复现——python -c 会命中
    dotenv 的交互模式特例（本来就用 cwd），测不到这个 bug。
    """
    (tmp_path / '.env').write_text('DB_NAME=probe_from_cwd\n')
    runner = tmp_path / 'runner.py'
    runner.write_text(
        'from tdx2db.config import config\n'
        'print(config.db_name)\n'
    )
    env = {k: v for k, v in os.environ.items() if k != 'DB_NAME'}
    env['PYTHONPATH'] = str(Path(__file__).resolve().parent.parent)
    result = subprocess.run(
        [sys.executable, str(runner)],
        cwd=tmp_path, env=env, capture_output=True, text=True, timeout=60,
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == 'probe_from_cwd'
