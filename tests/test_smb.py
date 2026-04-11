"""
SMB 访问模式测试套件（全部 mock，不发起真实网络请求）

测试用例：
1. TestSmbAccessorUncPaths  - UNC 路径构建逻辑
2. TestSmbAccessorIO        - I/O 方法（register_session, exists, list_files, download_to_tmp）
3. TestReaderSmbMode        - TdxDataReader 在 SMB 模式下的三个核心方法
4. TestCliSmbInit           - CLI SMB 参数解析与初始化
"""

import os
import struct
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch, call

import pandas as pd
import pytest

from src.tdx2db.smb_accessor import SmbAccessor
from src.tdx2db.reader import TdxDataReader


# ─── 工具函数 ────────────────────────────────────────────────────────────────

def _make_day_bytes(n: int = 3) -> bytes:
    fmt = '<IIIIIfII'
    return b''.join(
        struct.pack(fmt, 20240102 + i, 1000, 1100, 900, 1050, 1e7, 100000, 0)
        for i in range(n)
    )


def _write_tmp_side_effect(data: bytes):
    """返回 side_effect 函数：将 data 写入临时文件并返回路径。"""
    def _fn(unc, suffix='.day'):
        tmp = tempfile.NamedTemporaryFile(suffix=suffix, delete=False)
        tmp.write(data)
        tmp.flush()
        tmp.close()
        return tmp.name
    return _fn


# ─── TestSmbAccessorUncPaths ─────────────────────────────────────────────────

class TestSmbAccessorUncPaths:
    """UNC 路径构建逻辑。"""

    def test_unc_with_tdx_path(self):
        acc = SmbAccessor('192.168.1.1', 'share', tdx_path='TDX')
        assert acc._unc('vipdoc') == r'\\192.168.1.1\share\TDX\vipdoc'

    def test_unc_without_tdx_path(self):
        acc = SmbAccessor('192.168.1.1', 'share', tdx_path='')
        assert acc._unc('vipdoc') == r'\\192.168.1.1\share\vipdoc'

    def test_vipdoc_unc(self):
        acc = SmbAccessor('host', 'myshare', tdx_path='TDX')
        assert acc.vipdoc_unc == r'\\host\myshare\TDX\vipdoc'

    def test_gbbq_unc(self):
        acc = SmbAccessor('host', 'myshare', tdx_path='TDX')
        assert acc.gbbq_unc == r'\\host\myshare\TDX\T0002\hq_cache\gbbq'

    def test_lday_dir_unc(self):
        acc = SmbAccessor('host', 'myshare', tdx_path='TDX')
        assert acc.lday_dir_unc('sz') == r'\\host\myshare\TDX\vipdoc\sz\lday'

    def test_day_file_unc(self):
        acc = SmbAccessor('host', 'myshare', tdx_path='TDX')
        result = acc.day_file_unc('sz', 'sz000001.day')
        assert result == r'\\host\myshare\TDX\vipdoc\sz\lday\sz000001.day'

    def test_share_strips_slashes(self):
        acc = SmbAccessor('host', '\\share\\', tdx_path='')
        assert acc._unc('x') == r'\\host\share\x'

    def test_unc_multiple_parts(self):
        acc = SmbAccessor('host', 'share', tdx_path='')
        assert acc._unc('a', 'b', 'c') == r'\\host\share\a\b\c'


# ─── TestSmbAccessorIO ───────────────────────────────────────────────────────

class TestSmbAccessorIO:
    """I/O 方法的 mock 测试。"""

    @patch('smbclient.register_session')
    def test_context_manager_registers_session(self, mock_reg):
        acc = SmbAccessor('host', 'share', username='u', password='p', port=445)
        with acc:
            mock_reg.assert_called_once_with('host', username='u', password='p', port=445)

    @patch('smbclient.reset_connection_cache')
    @patch('smbclient.register_session')
    def test_context_manager_unregisters_on_exit(self, mock_reg, mock_reset):
        acc = SmbAccessor('host', 'share')
        with acc:
            pass
        mock_reset.assert_called_once()

    @patch('smbclient.register_session')
    def test_register_only_once(self, mock_reg):
        acc = SmbAccessor('host', 'share')
        acc._register()
        acc._register()
        assert mock_reg.call_count == 1

    @patch('smbclient.path.exists', return_value=True)
    def test_exists_returns_true(self, _):
        acc = SmbAccessor('host', 'share')
        assert acc.exists(r'\\host\share\file') is True

    @patch('smbclient.path.exists', side_effect=Exception('network error'))
    def test_exists_returns_false_on_error(self, _):
        acc = SmbAccessor('host', 'share')
        assert acc.exists(r'\\host\share\file') is False

    @patch('smbclient.listdir', return_value=['sz000001.day', 'sz000002.day', 'readme.txt'])
    def test_list_files_with_suffix_filter(self, _):
        acc = SmbAccessor('host', 'share')
        result = acc.list_files(r'\\host\share\lday', suffix='.day')
        assert result == ['sz000001.day', 'sz000002.day']

    @patch('smbclient.listdir', return_value=['a.day', 'b.day'])
    def test_list_files_no_filter(self, _):
        acc = SmbAccessor('host', 'share')
        result = acc.list_files(r'\\host\share\lday')
        assert len(result) == 2

    @patch('smbclient.listdir', side_effect=Exception('access denied'))
    def test_list_files_returns_empty_on_error(self, _):
        acc = SmbAccessor('host', 'share')
        result = acc.list_files(r'\\host\share\lday')
        assert result == []

    def test_download_to_tmp_writes_file(self):
        day_bytes = _make_day_bytes(3)
        mock_file = MagicMock()
        mock_file.__enter__ = MagicMock(return_value=mock_file)
        mock_file.__exit__ = MagicMock(return_value=False)
        mock_file.read = MagicMock(return_value=day_bytes)

        acc = SmbAccessor('host', 'share')
        with patch('smbclient.open_file', return_value=mock_file):
            tmp_path = acc.download_to_tmp(r'\\host\share\sz000001.day')
        try:
            assert os.path.exists(tmp_path)
            with open(tmp_path, 'rb') as f:
                assert f.read() == day_bytes
        finally:
            os.unlink(tmp_path)

    def test_download_to_tmp_cleans_up_on_read_error(self):
        acc = SmbAccessor('host', 'share')
        with patch.object(acc, 'read_bytes', side_effect=OSError('network error')):
            with pytest.raises(OSError):
                acc.download_to_tmp(r'\\host\share\file.day')


# ─── TestReaderSmbMode ───────────────────────────────────────────────────────

class TestReaderSmbMode:
    """TdxDataReader 在 SMB 模式下的行为。"""

    def _make_smb(self, day_bytes=None):
        smb = MagicMock(spec=SmbAccessor)
        smb.gbbq_unc = r'\\host\share\TDX\T0002\hq_cache\gbbq'
        smb.lday_dir_unc = MagicMock(side_effect=lambda m: rf'\\host\share\TDX\vipdoc\{m}\lday')
        smb.day_file_unc = MagicMock(
            side_effect=lambda m, f: rf'\\host\share\TDX\vipdoc\{m}\lday\{f}'
        )
        smb.exists = MagicMock(return_value=True)
        smb.list_files = MagicMock(return_value=[])
        if day_bytes is not None:
            smb.download_to_tmp = MagicMock(side_effect=_write_tmp_side_effect(day_bytes))
        return smb

    def test_init_smb_mode_no_path_check(self):
        """SMB 模式下不校验本地路径，不抛出异常。"""
        smb = self._make_smb()
        reader = TdxDataReader(smb=smb)
        assert reader.tdx_path is None
        assert reader._vipdoc_path is None
        assert reader._smb is smb

    def test_get_stock_list_smb_sz(self):
        """SMB 模式下 get_stock_list 正确解析深圳股票。"""
        smb = self._make_smb()
        smb.list_files = MagicMock(side_effect=lambda unc, suffix='': {
            r'\\host\share\TDX\vipdoc\sz\lday': ['sz000001.day', 'sz000002.day', 'sz399001.day'],
            r'\\host\share\TDX\vipdoc\sh\lday': [],
            r'\\host\share\TDX\vipdoc\bj\lday': [],
        }.get(unc, []))

        reader = TdxDataReader(smb=smb)
        stocks = reader.get_stock_list()

        assert '000001.SZ' in stocks
        assert '000002.SZ' in stocks
        # 399001 是指数，不应被包含
        assert '399001.SZ' not in stocks

    def test_get_stock_list_smb_sh(self):
        """SMB 模式下正确解析上海股票。"""
        smb = self._make_smb()
        smb.list_files = MagicMock(side_effect=lambda unc, suffix='': {
            r'\\host\share\TDX\vipdoc\sz\lday': [],
            r'\\host\share\TDX\vipdoc\sh\lday': ['sh600000.day', 'sh688001.day'],
            r'\\host\share\TDX\vipdoc\bj\lday': [],
        }.get(unc, []))

        reader = TdxDataReader(smb=smb)
        stocks = reader.get_stock_list()

        assert '600000.SH' in stocks
        assert '688001.SH' in stocks

    def test_get_stock_list_smb_raises_when_empty(self):
        """SMB 模式下无股票文件时应抛出 FileNotFoundError。"""
        smb = self._make_smb()
        smb.list_files = MagicMock(return_value=[])

        reader = TdxDataReader(smb=smb)
        with pytest.raises(FileNotFoundError, match="SMB"):
            reader.get_stock_list()

    def test_read_daily_data_smb(self):
        """SMB 模式下 read_daily_data 应返回正确的 DataFrame。"""
        day_bytes = _make_day_bytes(5)
        smb = self._make_smb(day_bytes=day_bytes)

        reader = TdxDataReader(smb=smb)
        df = reader.read_daily_data(market=0, code='sz000001')

        assert not df.empty
        assert 'code' in df.columns
        assert df['code'].iloc[0] == '000001'
        assert 'open' in df.columns
        assert len(df) == 5

    def test_read_daily_data_smb_raises_when_not_exists(self):
        """SMB 模式下文件不存在时应抛出 FileNotFoundError。"""
        smb = self._make_smb()
        smb.exists = MagicMock(return_value=False)

        reader = TdxDataReader(smb=smb)
        with pytest.raises(FileNotFoundError, match="SMB"):
            reader.read_daily_data(market=0, code='sz000001')

    def test_read_gbbq_smb_not_exists_returns_empty(self):
        """SMB 模式下 gbbq 文件不存在时返回空 DataFrame。"""
        smb = self._make_smb()
        smb.exists = MagicMock(return_value=False)

        reader = TdxDataReader(smb=smb)
        result = reader.read_gbbq()

        assert isinstance(result, pd.DataFrame)
        assert result.empty

    def test_read_daily_data_smb_cleans_tmp_file(self):
        """SMB 模式下读取完成后临时文件应被删除。"""
        day_bytes = _make_day_bytes(3)
        smb = self._make_smb(day_bytes=day_bytes)
        created_tmp = []

        original_side_effect = _write_tmp_side_effect(day_bytes)
        def tracking_side_effect(unc, suffix='.day'):
            path = original_side_effect(unc, suffix)
            created_tmp.append(path)
            return path

        smb.download_to_tmp = MagicMock(side_effect=tracking_side_effect)

        reader = TdxDataReader(smb=smb)
        reader.read_daily_data(market=0, code='sz000001')

        assert len(created_tmp) == 1
        assert not os.path.exists(created_tmp[0]), "临时文件应已被删除"


# ─── TestCliSmbInit ──────────────────────────────────────────────────────────

class TestCliSmbInit:
    """CLI SMB 参数解析与初始化。"""

    def test_smb_args_parsed_correctly(self):
        from src.tdx2db.cli import parse_args
        with patch('sys.argv', [
            'tdx2db', '--smb-host', '192.168.1.1',
            '--smb-share', 'tdx_share',
            '--smb-user', 'admin',
            '--smb-password', 'secret',
            '--smb-tdx-path', 'TDX',
            '--smb-port', '445',
            'sync',
        ]):
            args = parse_args()

        assert args.smb_host == '192.168.1.1'
        assert args.smb_share == 'tdx_share'
        assert args.smb_user == 'admin'
        assert args.smb_password == 'secret'
        assert args.smb_tdx_path == 'TDX'
        assert args.smb_port == 445

    def test_update_config_sets_smb_enabled(self):
        from src.tdx2db.cli import update_config, parse_args
        from src.tdx2db.config import Config

        with patch('sys.argv', ['tdx2db', '--smb-host', '10.0.0.1', '--smb-share', 'share', 'sync']):
            args = parse_args()

        cfg = Config()
        with patch('src.tdx2db.cli.config', cfg):
            update_config(args)

        assert cfg.smb_enabled is True
        assert cfg.smb_host == '10.0.0.1'
        assert cfg.smb_share == 'share'

    def test_create_reader_smb_mode(self):
        """_create_reader 在 SMB 模式下应创建 SmbAccessor 并注入 TdxDataReader。"""
        from src.tdx2db.cli import _create_reader
        from src.tdx2db.config import Config

        cfg = Config()
        cfg.smb_enabled = True
        cfg.smb_host = '192.168.1.1'
        cfg.smb_share = 'share'
        cfg.smb_user = 'user'
        cfg.smb_password = 'pass'
        cfg.smb_tdx_path = 'TDX'
        cfg.smb_port = 445

        with patch('src.tdx2db.cli.config', cfg):
            with patch('src.tdx2db.smb_accessor.smbclient.register_session'):
                reader, smb_acc = _create_reader()

        assert smb_acc is not None
        assert reader._smb is smb_acc

    def test_create_reader_smb_missing_host_raises(self):
        """SMB 模式下缺少 smb_host 应抛出 ValueError。"""
        from src.tdx2db.cli import _create_reader
        from src.tdx2db.config import Config

        cfg = Config()
        cfg.smb_enabled = True
        cfg.smb_host = ''
        cfg.smb_share = 'share'

        with patch('src.tdx2db.cli.config', cfg):
            with pytest.raises(ValueError, match="SMB_HOST"):
                _create_reader()

    def test_create_reader_local_mode(self, tmp_path):
        """非 SMB 模式下应创建本地 TdxDataReader。"""
        from src.tdx2db.cli import _create_reader
        from src.tdx2db.config import Config

        tdx_path = tmp_path / 'tdx'
        (tdx_path / 'vipdoc').mkdir(parents=True)

        cfg = Config()
        cfg.smb_enabled = False
        cfg.tdx_path = str(tdx_path)

        with patch('src.tdx2db.cli.config', cfg), \
             patch('src.tdx2db.reader.config', cfg):
            reader, smb_acc = _create_reader()

        assert smb_acc is None
        assert reader._smb is None
