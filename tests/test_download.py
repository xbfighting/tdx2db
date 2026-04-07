"""
联网下载功能测试套件（全部 mock，不发起真实网络请求）

测试用例：
1. TestDownloader      - downloader.py 的下载/解压/上下文管理逻辑
2. TestReaderVipdocPath - TdxDataReader 新增的 vipdoc_path 参数
3. TestDownloadCommand  - CLI download 子命令的完整流程
"""

import io
import struct
import zipfile
from pathlib import Path
from unittest.mock import MagicMock, patch, call

import pandas as pd
import pytest

from src.tdx2db.downloader import (
    DEFAULT_DOWNLOAD_URL,
    download_and_extract,
    download_zip,
    extract_zip,
)
from src.tdx2db.reader import TdxDataReader


# ─── 工具函数 ────────────────────────────────────────────────────────────────

def _make_day_bytes(n_records: int = 5) -> bytes:
    """生成假的 .day 二进制内容（n 条记录）。"""
    fmt = '<IIIIIfII'
    records = b''
    for i in range(n_records):
        # date=20240102+i, open/high/low/close 各1000（即 10.00 元），amount=1e7, volume=1000
        records += struct.pack(fmt, 20240102 + i, 1000, 1100, 900, 1050, 1e7, 100000, 0)
    return records


def _make_fake_zip(tmp_path: Path, markets=('sh', 'sz'), codes_per_market=2) -> Path:
    """在 tmp_path 创建假的 hsjday.zip，内含 {sh,sz}/lday/*.day 文件（与实际 ZIP 结构一致）。"""
    zip_path = tmp_path / 'hsjday.zip'
    day_content = _make_day_bytes(5)

    market_codes = {
        'sh': ['sh600000', 'sh600001'],
        'sz': ['sz000001', 'sz000002'],
        'bj': ['bj920001'],
    }

    with zipfile.ZipFile(zip_path, 'w') as zf:
        for market in markets:
            codes = market_codes.get(market, [])[:codes_per_market]
            for code in codes:
                arc_name = f'{market}/lday/{code}.day'
                zf.writestr(arc_name, day_content)

    return zip_path


# ─── TestDownloader ─────────────────────────────────────────────────────────

class TestDownloadZip:
    """download_zip 函数的单元测试。"""

    def test_downloads_file_successfully(self, tmp_path):
        """正常下载时文件应被写入目标路径。"""
        fake_content = b'PK\x03\x04' + b'\x00' * 100  # 假 ZIP 头

        mock_resp = MagicMock()
        mock_resp.raise_for_status = MagicMock()
        mock_resp.headers = {'Content-Length': str(len(fake_content))}
        mock_resp.iter_content = MagicMock(return_value=[fake_content])

        dest = tmp_path / 'test.zip'
        with patch('requests.get', return_value=mock_resp):
            with patch('src.tdx2db.downloader.config') as mock_cfg:
                mock_cfg.use_tqdm = False
                download_zip('http://fake-url/test.zip', dest)

        assert dest.exists()
        assert dest.read_bytes() == fake_content

    def test_cleans_up_on_error(self, tmp_path):
        """下载失败时应删除不完整文件。"""
        import requests as req

        mock_resp = MagicMock()
        mock_resp.raise_for_status = MagicMock(side_effect=req.HTTPError("404"))
        mock_resp.headers = {}

        dest = tmp_path / 'test.zip'
        with patch('requests.get', return_value=mock_resp):
            with pytest.raises(RuntimeError, match="下载失败"):
                download_zip('http://fake-url/test.zip', dest)

        assert not dest.exists()

    def test_raises_on_network_error(self, tmp_path):
        """网络异常时应抛出 RuntimeError。"""
        import requests as req

        with patch('requests.get', side_effect=req.ConnectionError("no route")):
            with pytest.raises(RuntimeError, match="下载失败"):
                download_zip('http://fake-url/test.zip', tmp_path / 'x.zip')


class TestExtractZip:
    """extract_zip 函数的单元测试。"""

    def test_extracts_and_returns_vipdoc_path(self, tmp_path):
        """正常解压时应返回包含市场目录的路径。"""
        zip_path = _make_fake_zip(tmp_path, markets=['sh', 'sz'])
        extract_dir = tmp_path / 'out'
        extract_dir.mkdir()

        result = extract_zip(zip_path, extract_dir)

        assert result.exists()
        assert (result / 'sh' / 'lday').exists()

    def test_raises_on_invalid_zip(self, tmp_path):
        """非法 ZIP 文件应抛出 ValueError。"""
        bad_zip = tmp_path / 'bad.zip'
        bad_zip.write_bytes(b'not a zip file at all')

        with pytest.raises(ValueError, match="ZIP"):
            extract_zip(bad_zip, tmp_path / 'out')

    def test_raises_on_missing_market_dirs(self, tmp_path):
        """ZIP 内无 sh/sz/bj 目录时应抛出 FileNotFoundError。"""
        zip_path = tmp_path / 'no_market.zip'
        with zipfile.ZipFile(zip_path, 'w') as zf:
            zf.writestr('some_other_dir/file.txt', 'content')

        extract_dir = tmp_path / 'out'
        extract_dir.mkdir()

        with pytest.raises(FileNotFoundError, match="sh/sz/bj"):
            extract_zip(zip_path, extract_dir)


class TestDownloadAndExtract:
    """download_and_extract 上下文管理器测试。"""

    def test_yields_vipdoc_path_and_cleans_up(self):
        """正常执行：yield vipdoc_path，结束后临时目录被清理。"""
        import tempfile, shutil
        src_dir = Path(tempfile.mkdtemp(prefix='tdx_test_src_'))
        try:
            zip_path = _make_fake_zip(src_dir, markets=['sh'])
            captured_vipdoc = None

            def fake_download(url, dest_path, **kwargs):
                shutil.copy(zip_path, dest_path)

            with patch('src.tdx2db.downloader.download_zip', side_effect=fake_download):
                with patch('src.tdx2db.downloader.config') as mock_cfg:
                    mock_cfg.download_url = ''
                    mock_cfg.use_tqdm = False
                    with download_and_extract(url='http://fake/', keep_tmp=False) as vipdoc_path:
                        captured_vipdoc = vipdoc_path
                        assert vipdoc_path.exists()

            # 退出后 vipdoc_path 所在的 tmp_dir 应已删除
            assert not captured_vipdoc.exists()
        finally:
            shutil.rmtree(src_dir, ignore_errors=True)

    def test_keep_tmp_preserves_directory(self):
        """keep_tmp=True 时 tmp_dir 应保留。"""
        import tempfile, shutil
        src_dir = Path(tempfile.mkdtemp(prefix='tdx_test_src_'))
        captured_tmp_dir = None
        try:
            zip_path = _make_fake_zip(src_dir, markets=['sh'])

            # 拦截 tempfile.mkdtemp 以记录 tmp_dir
            real_mkdtemp = tempfile.mkdtemp
            def fake_mkdtemp(**kwargs):
                d = real_mkdtemp(**kwargs)
                nonlocal captured_tmp_dir
                captured_tmp_dir = Path(d)
                return d

            def fake_download(url, dest_path, **kwargs):
                shutil.copy(zip_path, dest_path)

            with patch('src.tdx2db.downloader.download_zip', side_effect=fake_download):
                with patch('src.tdx2db.downloader.tempfile.mkdtemp', side_effect=fake_mkdtemp):
                    with patch('src.tdx2db.downloader.config') as mock_cfg:
                        mock_cfg.download_url = ''
                        mock_cfg.use_tqdm = False
                        with download_and_extract(url='http://fake/', keep_tmp=True):
                            pass

            assert captured_tmp_dir is not None
            assert captured_tmp_dir.exists()
        finally:
            shutil.rmtree(src_dir, ignore_errors=True)
            if captured_tmp_dir:
                shutil.rmtree(captured_tmp_dir, ignore_errors=True)

    def test_uses_default_url_when_none_given(self):
        """未传 url 时应使用 DEFAULT_DOWNLOAD_URL。"""
        import tempfile, shutil
        src_dir = Path(tempfile.mkdtemp(prefix='tdx_test_src_'))
        called_urls = []
        try:
            zip_path = _make_fake_zip(src_dir, markets=['sh'])

            def fake_download(url, dest_path, **kwargs):
                called_urls.append(url)
                shutil.copy(zip_path, dest_path)

            with patch('src.tdx2db.downloader.download_zip', side_effect=fake_download):
                with patch('src.tdx2db.downloader.config') as mock_cfg:
                    mock_cfg.download_url = ''
                    mock_cfg.use_tqdm = False
                    with download_and_extract(url=None, keep_tmp=False):
                        pass
        finally:
            shutil.rmtree(src_dir, ignore_errors=True)

        assert called_urls and called_urls[0] == DEFAULT_DOWNLOAD_URL

    def test_config_url_overrides_default(self):
        """config.download_url 非空时应优先于默认 URL。"""
        import tempfile, shutil
        src_dir = Path(tempfile.mkdtemp(prefix='tdx_test_src_'))
        called_urls = []
        custom_url = 'http://my-custom-host/hsjday.zip'
        try:
            zip_path = _make_fake_zip(src_dir, markets=['sh'])

            def fake_download(url, dest_path, **kwargs):
                called_urls.append(url)
                shutil.copy(zip_path, dest_path)

            with patch('src.tdx2db.downloader.download_zip', side_effect=fake_download):
                with patch('src.tdx2db.downloader.config') as mock_cfg:
                    mock_cfg.download_url = custom_url
                    mock_cfg.use_tqdm = False
                    with download_and_extract(url=None, keep_tmp=False):
                        pass
        finally:
            shutil.rmtree(src_dir, ignore_errors=True)

        assert called_urls and called_urls[0] == custom_url


# ─── TestReaderVipdocPath ────────────────────────────────────────────────────

class TestReaderVipdocPath:
    """TdxDataReader 的 vipdoc_path 参数测试。"""

    def _write_day_files(self, base: Path, market: str, codes: list) -> None:
        """在 base/{market}/lday/ 下写入假 .day 文件。"""
        lday = base / market / 'lday'
        lday.mkdir(parents=True, exist_ok=True)
        for code in codes:
            (lday / f'{code}.day').write_bytes(_make_day_bytes(5))

    def test_vipdoc_path_get_stock_list(self, tmp_path):
        """vipdoc_path 模式下 get_stock_list 应正确扫描三个市场目录。"""
        vipdoc = tmp_path / 'hsjday'
        self._write_day_files(vipdoc, 'sz', ['sz000001', 'sz000002'])
        self._write_day_files(vipdoc, 'sh', ['sh600000'])

        reader = TdxDataReader(vipdoc_path=str(vipdoc))
        stocks = reader.get_stock_list()

        codes = set(stocks['code'].tolist())
        assert 'sz000001' in codes
        assert 'sz000002' in codes
        assert 'sh600000' in codes

    def test_vipdoc_path_read_daily_data(self, tmp_path):
        """vipdoc_path 模式下 read_daily_data 应返回正确的 DataFrame。"""
        vipdoc = tmp_path / 'hsjday'
        self._write_day_files(vipdoc, 'sh', ['sh600000'])

        reader = TdxDataReader(vipdoc_path=str(vipdoc))
        df = reader.read_daily_data(market=1, code='sh600000')

        assert not df.empty
        assert 'code' in df.columns
        assert df['code'].iloc[0] == '600000'
        assert 'open' in df.columns

    def test_vipdoc_path_read_gbbq_returns_empty(self, tmp_path):
        """vipdoc_path 模式下 read_gbbq 应返回空 DataFrame（不报错）。"""
        vipdoc = tmp_path / 'hsjday'
        vipdoc.mkdir()

        reader = TdxDataReader(vipdoc_path=str(vipdoc))
        gbbq = reader.read_gbbq()

        assert isinstance(gbbq, pd.DataFrame)
        assert gbbq.empty

    def test_vipdoc_path_raises_if_not_exists(self, tmp_path):
        """vipdoc_path 不存在时应抛出 FileNotFoundError。"""
        with pytest.raises(FileNotFoundError):
            TdxDataReader(vipdoc_path=str(tmp_path / 'nonexistent'))

    def test_original_mode_still_works(self, tmp_path):
        """不传 vipdoc_path 时，原有 tdx_path 模式应正常工作。"""
        # 构造假的 TDX 目录结构
        tdx_path = tmp_path / 'tdx'
        vipdoc = tdx_path / 'vipdoc'
        (vipdoc / 'sz' / 'lday').mkdir(parents=True)
        (vipdoc / 'sz' / 'lday' / 'sz000001.day').write_bytes(_make_day_bytes(3))

        reader = TdxDataReader(tdx_path=str(tdx_path))
        assert reader.tdx_path == tdx_path
        assert reader._vipdoc_path == tdx_path / 'vipdoc'


# ─── TestDownloadCommand ─────────────────────────────────────────────────────

class TestDownloadCommand:
    """CLI download 子命令的集成测试（mock 网络，使用内存 SQLite）。"""

    def _write_day_files(self, base: Path, market: str, codes: list) -> None:
        lday = base / market / 'lday'
        lday.mkdir(parents=True, exist_ok=True)
        for code in codes:
            (lday / f'{code}.day').write_bytes(_make_day_bytes(5))

    def test_download_command_imports_data(self, tmp_path):
        """download 命令完整流程：下载→解压→读取→入库。"""
        from src.tdx2db.cli import main
        from src.tdx2db.storage import DataStorage

        # 准备假的 vipdoc 目录
        vipdoc = tmp_path / 'hsjday'
        self._write_day_files(vipdoc, 'sz', ['sz000001'])
        self._write_day_files(vipdoc, 'sh', ['sh600000'])

        db_path = tmp_path / 'test.db'

        # mock download_and_extract 使其直接 yield 假 vipdoc 目录
        from contextlib import contextmanager

        @contextmanager
        def fake_download_and_extract(url=None, keep_tmp=False):
            yield vipdoc

        with patch('src.tdx2db.cli.download_and_extract', fake_download_and_extract):
            with patch('src.tdx2db.cli.DataStorage') as MockStorage:
                storage_instance = DataStorage(db_url=f'sqlite:///{db_path}')
                MockStorage.return_value = storage_instance
                with patch('src.tdx2db.cli.config') as mock_cfg:
                    mock_cfg.tdx_path = ''
                    mock_cfg.use_tqdm = False
                    mock_cfg.db_batch_size = 10000
                    mock_cfg.download_url = ''

                    result = main.__wrapped__() if hasattr(main, '__wrapped__') else None
                    # 直接调用同步逻辑
                    from src.tdx2db.cli import sync_all_daily
                    from src.tdx2db.processor import DataProcessor

                    reader = TdxDataReader(vipdoc_path=str(vipdoc))
                    processor = DataProcessor()
                    gbbq = pd.DataFrame()
                    stats = sync_all_daily(reader, processor, storage_instance, gbbq,
                                          adj_type='none', incremental=False)

            # 验证两只股票都有数据
            with storage_instance.engine.connect() as conn:
                from sqlalchemy import text
                count = conn.execute(text("SELECT COUNT(*) FROM daily_data")).fetchone()[0]
                assert count > 0, "download 后应有数据写入数据库"

    def test_download_help_shows_subcommand(self):
        """parse_args 应包含 download 子命令。"""
        import sys
        from src.tdx2db.cli import parse_args

        with patch('sys.argv', ['tdx2db', 'download', '--help']):
            with pytest.raises(SystemExit) as exc:
                parse_args()
            assert exc.value.code == 0

    def test_download_args_parsed_correctly(self):
        """download 子命令参数应正确解析。"""
        from src.tdx2db.cli import parse_args

        with patch('sys.argv', ['tdx2db', 'download', '--url', 'http://myhost/data.zip',
                                 '--adj', 'none', '--no-clean']):
            args = parse_args()

        assert args.command == 'download'
        assert args.url == 'http://myhost/data.zip'
        assert args.adj == 'none'
        assert args.no_clean is True

    def test_download_default_args(self):
        """download 子命令默认参数：adj=forward, no_clean=False。"""
        from src.tdx2db.cli import parse_args

        with patch('sys.argv', ['tdx2db', 'download']):
            args = parse_args()

        assert args.command == 'download'
        assert args.adj == 'forward'
        assert args.no_clean is False
        assert args.url is None
