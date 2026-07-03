"""cli 纯函数测试（issue #24）"""

import pytest

from src.cli import infer_market, parse_args


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
