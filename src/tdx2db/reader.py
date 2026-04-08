import re
import struct
from pathlib import Path
from typing import Optional

import pandas as pd
from pytdx.reader import TdxDailyBarReader, GbbqReader

from .config import config
from .logger import logger


class TdxDataReader:
    def __init__(self, tdx_path: Optional[str] = None, vipdoc_path: Optional[str] = None) -> None:
        if vipdoc_path:
            self.tdx_path = None
            self._vipdoc_path = Path(vipdoc_path)
            if not self._vipdoc_path.exists():
                raise FileNotFoundError(f"vipdoc 目录不存在: {self._vipdoc_path}")
        else:
            self.tdx_path = Path(tdx_path or config.tdx_path)
            if not self.tdx_path:
                raise ValueError("通达信数据路径未设置，请在 .env 中设置 TDX_PATH")
            if not self.tdx_path.exists():
                raise FileNotFoundError(f"通达信数据路径不存在: {self.tdx_path}")
            self._vipdoc_path = self.tdx_path / 'vipdoc'
        self.daily_reader = TdxDailyBarReader()
        self.gbbq_reader = GbbqReader()

    def read_gbbq(self) -> pd.DataFrame:
        """读取权息文件，返回全量权息 DataFrame。文件不存在时返回空 DataFrame。"""
        if self.tdx_path is None:
            logger.warning("联网下载模式下不支持读取 gbbq，将跳过复权处理")
            return pd.DataFrame()
        gbbq_path = self.tdx_path / 'T0002' / 'hq_cache' / 'gbbq'
        if not gbbq_path.exists():
            logger.warning(f"权息文件不存在: {gbbq_path}，将跳过复权处理")
            return pd.DataFrame()
        try:
            df = self.gbbq_reader.get_df(str(gbbq_path))
            if df.empty:
                return pd.DataFrame()
            market_prefix = df['market'].map({0: 'sz', 1: 'sh'})
            df['full_code'] = market_prefix + df['code'].astype(str).str.zfill(6)
            return df
        except Exception as e:
            logger.warning(f"读取权息文件时出错: {e}，将跳过复权处理")
            return pd.DataFrame()

    def get_stock_list(self) -> list:
        """扫描本地 .day 文件，返回有数据的股票代码列表（000001.SZ 格式）。"""
        sz_path = self._vipdoc_path / 'sz' / 'lday'
        sh_path = self._vipdoc_path / 'sh' / 'lday'
        bj_path = self._vipdoc_path / 'bj' / 'lday'
        if not (sz_path.exists() or sh_path.exists() or bj_path.exists()):
            raise FileNotFoundError("无法找到股票数据目录")

        codes = []
        if sz_path.exists():
            for f in sz_path.glob('*.day'):
                pure = f.stem[-6:].zfill(6)
                if re.match(r'^(000|001|002|300|301)\d{3}$', pure):
                    codes.append(pure + '.SZ')

        if sh_path.exists():
            for f in sh_path.glob('*.day'):
                pure = f.stem[-6:].zfill(6)
                if re.match(r'^(60\d{4}|688\d{3})$', pure):
                    codes.append(pure + '.SH')

        if bj_path.exists():
            for f in bj_path.glob('*.day'):
                pure = f.stem[-6:].zfill(6)
                if re.match(r'^(8\d{5}|92\d{4})$', pure):
                    codes.append(pure + '.BJ')

        if not codes:
            raise FileNotFoundError("未找到任何股票数据文件")
        return codes

    def read_daily_data(self, market: int, code: str) -> pd.DataFrame:
        """读取单只股票日线数据，返回含 code/market 列的 DataFrame（date 为 DatetimeIndex）。"""
        market_map = {0: 'sz', 1: 'sh', 2: 'bj'}
        market_folder = market_map[market]
        pure_code = code[-6:] if len(code) > 6 else code
        file_path = self._vipdoc_path / market_folder / 'lday' / f"{market_folder}{pure_code}.day"

        if not file_path.exists():
            raise FileNotFoundError(f"日线数据文件不存在: {file_path}")

        import io
        from contextlib import redirect_stdout
        try:
            with redirect_stdout(io.StringIO()):
                sec_type = self.daily_reader.get_security_type(str(file_path))
            if sec_type in self.daily_reader.SECURITY_TYPE:
                data = self.daily_reader.get_df(str(file_path))
            else:
                data = self._read_day_file_raw(str(file_path))
        except Exception:
            data = self._read_day_file_raw(str(file_path))

        data['code'] = pure_code
        data['market'] = market
        return data

    @staticmethod
    def _read_day_file_raw(fname: str) -> pd.DataFrame:
        """直接解析 .day 二进制文件（用于科创板等 pytdx 不支持的证券类型）。"""
        rows = []
        with open(fname, 'rb') as f:
            content = f.read()
        record_size = struct.calcsize('<IIIIIfII')
        for i in range(0, len(content) - record_size + 1, record_size):
            row = struct.unpack_from('<IIIIIfII', content, i)
            t = str(row[0])
            date_str = f"{t[:4]}-{t[4:6]}-{t[6:]}"
            rows.append((date_str,
                         row[1] * 0.01, row[2] * 0.01, row[3] * 0.01, row[4] * 0.01,
                         row[5], row[6] * 0.01))
        df = pd.DataFrame(rows, columns=['date', 'open', 'high', 'low', 'close', 'amount', 'volume'])
        df.index = pd.to_datetime(df['date'])
        df.index.name = 'date'
        return df[['open', 'high', 'low', 'close', 'amount', 'volume']]
