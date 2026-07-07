"""数据读取模块

负责从通达信本地数据文件中读取股票数据，支持：
- 日线数据
- 分钟线数据
- 股票列表
"""

import os
import re
from pathlib import Path
from typing import Optional, List

import pandas as pd
from pytdx.reader import TdxDailyBarReader, TdxMinBarReader, TdxLCMinBarReader
from tqdm import tqdm

from .config import config, default_tdx_candidates, detect_tdx_path
from .logger import logger
from .processor import DataProcessor

class TdxDataReader:
    """通达信数据读取类"""

    def __init__(self, tdx_path: Optional[str] = None) -> None:
        """初始化数据读取器

        Args:
            tdx_path: 通达信安装目录，如果为None则使用配置中的路径
        """
        self.tdx_path = tdx_path or config.tdx_path
        if not self.tdx_path:
            # 未显式配置时探测常见安装路径（issue #32），显式配置永远优先
            detected = detect_tdx_path()
            if detected:
                logger.info(f"TDX_PATH 未配置，已自动探测到通达信目录: {detected}")
                self.tdx_path = detected
            else:
                hint = ""
                if os.name == 'nt':
                    hint = f"（已探测以下路径均无 vipdoc: {', '.join(default_tdx_candidates())}）"
                raise ValueError(
                    "通达信数据路径未设置，请在 .env 中设置 TDX_PATH 或使用 --tdx-path 指定" + hint
                )

        self.tdx_path = Path(self.tdx_path)
        if not self.tdx_path.exists():
            raise FileNotFoundError(f"通达信数据路径不存在: {self.tdx_path}")

        # 初始化读取器
        self.daily_reader = TdxDailyBarReader()
        self.min_reader = TdxMinBarReader()
        self.lc_min_reader = TdxLCMinBarReader()

    def _load_real_names(self) -> dict:
        """读取真实股票名称：T0002/hq_cache/infoharbor_ex.code（issue #42）

        格式：``6位code|股票名|关联人``，GBK，全 A 股含退市名，随盘后下载更新。
        文件缺失时返回空 dict，get_stock_list 回退占位符命名。

        Returns:
            dict: 6位纯数字 code -> 股票名
        """
        f = self.tdx_path / 'T0002' / 'hq_cache' / 'infoharbor_ex.code'
        try:
            if not f.exists():
                logger.warning(f"缺少 {f}，stock_info.name 回退为占位符（深A/上A + code）")
                return {}
            text = f.read_text(encoding='gbk', errors='replace')
        except OSError as e:
            # 文件存在但不可读（SMB 挂载下被运行中的通达信锁定等场景，
            # 表现为 EPERM）——名称是可选增强，不应中断 daily/minutes 同步
            logger.warning(f"读取 {f} 失败（{e}），stock_info.name 回退为占位符")
            return {}
        names = {}
        for line in text.splitlines():
            p = line.strip().split('|')
            if len(p) >= 2 and p[0] and p[1]:
                names[p[0]] = p[1]
        return names

    def _load_capital_info(self) -> dict:
        """读取股本/日期信息：base.dbf 的 ZGB/LTAG/GXRQ/SSDATE（issue #45）

        缺失/不可读时返回空 dict，对应列降级为 NULL。

        Returns:
            dict: 6位code -> {'zgb','ltag'(float 万股), 'capital_date','list_date'(date)}
        """
        from .blocks import parse_base_dbf

        f = self.tdx_path / 'T0002' / 'hq_cache' / 'base.dbf'
        try:
            if not f.exists():
                logger.warning(f"缺少 {f}，股本/上市日期列置 NULL")
                return {}
            raw = parse_base_dbf(f, ('ZGB', 'LTAG', 'GXRQ', 'SSDATE'))
        except (OSError, ValueError) as e:
            # 不可读（SMB 下被运行中的通达信锁定，#44 同款）/ 字段缺失均降级
            logger.warning(f"读取 {f} 失败（{e}），股本/上市日期列置 NULL")
            return {}

        def _num(v):
            try:
                return float(v)
            except (TypeError, ValueError):
                return None

        def _date(v):
            try:
                return pd.Timestamp(v).date() if v and len(v) == 8 else None
            except (ValueError, TypeError):
                return None

        return {
            code: {
                'zgb': _num(row['ZGB']),
                'ltag': _num(row['LTAG']),
                'capital_date': _date(row['GXRQ']),
                'list_date': _date(row['SSDATE']),
            }
            for code, row in raw.items()
        }

    def get_stock_list(self) -> pd.DataFrame:
        """获取股票列表

        Returns:
            DataFrame: A股代码/名称/股本/日期（不包含B股、基金等）。
            name 优先取 infoharbor_ex.code 真实名称，缺失时回退占位符；
            zgb/ltag（万股）与 capital_date/list_date 来自 base.dbf，缺失为 NULL
        """
        # 尝试查找通达信股票数据文件
        sz_path = self.tdx_path / 'vipdoc' / 'sz' / 'lday'
        sh_path = self.tdx_path / 'vipdoc' / 'sh' / 'lday'

        if not (sz_path.exists() or sh_path.exists()):
            raise FileNotFoundError(f"无法找到股票列表文件或股票数据目录")

        real_names = self._load_real_names()
        capital = self._load_capital_info()
        _empty_cap = {'zgb': None, 'ltag': None, 'capital_date': None, 'list_date': None}

        # 从目录中获取股票代码
        stocks = []

        # 处理深圳股票
        if sz_path.exists():
            for file in sz_path.glob('*.day'):
                code = file.stem
                pure_code = code[-6:]
                code_str = str(pure_code).zfill(6)  # 补齐为6位字符串
                name = real_names.get(code_str, f"深A{code}")
                # 深证A股：主板 000/001/002 + 创业板 300/301
                if re.match(r'^(000|001|002|300|301)\d{3}$', code_str):
                    stocks.append({'code': code, 'name': name, **capital.get(code_str, _empty_cap)})

        # 处理上海股票
        if sh_path.exists():
            for file in sh_path.glob('*.day'):
                code = file.stem
                pure_code = code[-6:]
                code_str = str(pure_code).zfill(6)  # 补齐为6位字符串
                name = real_names.get(code_str, f"上A{code}")
                # 上证A股：主板 60xxxx + 科创板 688xxx（旧正则 688\d{4} 共 7 位，永远匹配不上 6 位代码）
                if re.match(r'^(60\d{4}|688\d{3})$', code_str):
                    stocks.append({'code': code, 'name': name, **capital.get(code_str, _empty_cap)})

        if not stocks:
            raise FileNotFoundError(f"未找到任何股票数据文件")

        return pd.DataFrame(
            stocks,
            columns=['code', 'name', 'zgb', 'ltag', 'capital_date', 'list_date'],
        )

    def read_daily_data(self, market: int, code: str) -> pd.DataFrame:
        """读取日线数据

        Args:
            market: 市场代码，0表示深圳，1表示上海
            code: 股票代码

        Returns:
            DataFrame: 日线数据
        """
        # 构建日线数据文件路径
        market_folder = 'sz' if market == 0 else 'sh'
        data_path = self.tdx_path / 'vipdoc' / market_folder / 'lday'

        if (len(code)>6):
            code = code[-6:]
        file_path = data_path / f"{market_folder}{code}.day"

        if not file_path.exists():
            raise FileNotFoundError(f"日线数据文件不存在: {file_path}")

        # pytdx get_security_type 不识别科创板 688（code_head '68' 不在任何分支），
        # get_df 会 print 噪音 + raise NotImplementedError，需先检查再决定走原始解析
        sec_type = self.daily_reader.get_security_type(str(file_path))
        if sec_type in self.daily_reader.SECURITY_TYPE:
            data = self.daily_reader.get_df(str(file_path))
        else:
            data = self._read_day_file_raw(str(file_path))
        data['code'] = code
        data['market'] = market
        return data

    @staticmethod
    def _read_day_file_raw(fname: str) -> pd.DataFrame:
        """直接解析 .day 文件，绕过 pytdx 的证券类型检查（用于科创板 688 等）。
        系数与 pytdx SH_A_STOCK 路径一致：价格 ×0.01，volume ×0.01，amount 原样。"""
        import struct
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

    def read_min_data(self, market: int, code: str) -> List[pd.DataFrame]:
        """读取5分钟线数据并生成15分钟、30分钟和60分数据

        Args:
            market: 市场代码，0表示深圳，1表示上海
            code: 股票代码

        Returns:
            list: [15分钟数据, 30分钟数据, 60分钟数据]
        """
        # 构建分钟线数据文件路径
        market_folder = 'sz' if market == 0 else 'sh'
        freq_folder = 'fzline'
        data_path = self.tdx_path / 'vipdoc' / market_folder / freq_folder
        file_path = data_path /f"{market_folder}{code}.lc5"  # 只读取5分钟数据

        if not file_path.exists():
            raise FileNotFoundError(f"5分钟线数据文件不存在: {file_path}")

        # 读取5分钟数据（per-stock 日志降为 debug：全量同步时 5000+ 只的 INFO 会淹没进度条）
        logger.debug(f"正在读取 {code} 的5分钟线数据...")
        data = self.lc_min_reader.get_df(str(file_path))
        data['code'] = code
        data['market'] = market

        # 确保datetime列存在并且是日期时间类型
        if 'datetime' not in data.columns:
            # 如果没有datetime列，尝试从index创建
            if isinstance(data.index, pd.DatetimeIndex):
                data['datetime'] = data.index
            else:
                raise ValueError("数据中缺少datetime列且索引不是日期时间类型")
        elif not pd.api.types.is_datetime64_any_dtype(data['datetime']):
            data['datetime'] = pd.to_datetime(data['datetime'])

        # 设置datetime为索引，用于后续resample操作
        data.set_index('datetime', inplace=True)

        # 记得定期获取最新的数据，同步进TDX
        logger.debug(f"数据时间范围: {data.index[0]} ~ {data.index[-1]}")

        # 重采样生成多周期数据
        data_15min = DataProcessor.resample_ohlcv(data, '15min')
        data_30min = DataProcessor.resample_ohlcv(data, '30min')
        data_60min = DataProcessor.resample_ohlcv(data, '60min')

        data.reset_index(inplace=True)

        return [data_15min, data_30min, data_60min]

    def read_5min_data(self, market: int, code: str) -> pd.DataFrame:
        """读取5分钟线数据

        Args:
            market: 市场代码，0表示深圳，1表示上海
            code: 股票代码

        Returns:
            DataFrame: 5分钟数据
        """
        # 构建分钟线数据文件路径
        market_folder = 'sz' if market == 0 else 'sh'
        freq_folder = 'fzline'
        data_path = self.tdx_path / 'vipdoc' / market_folder / freq_folder

        if (len(code)>6):
            code = code[-6:]
        file_path = data_path /f"{market_folder}{code}.lc5"

        if not file_path.exists():
            raise FileNotFoundError(f"5分钟线数据文件不存在: {file_path}")

        # 读取5分钟数据（per-stock 日志降为 debug，理由同上）
        logger.debug(f"正在读取 {code} 的5分钟线数据...")
        data = self.lc_min_reader.get_df(str(file_path))
        data['code'] = code
        data['market'] = market

        # 确保datetime列存在并且是日期时间类型
        if 'datetime' not in data.columns:
            # 如果没有datetime列，尝试从index创建
            if isinstance(data.index, pd.DatetimeIndex):
                data['datetime'] = data.index
            else:
                raise ValueError("数据中缺少datetime列且索引不是日期时间类型")
        elif not pd.api.types.is_datetime64_any_dtype(data['datetime']):
            data['datetime'] = pd.to_datetime(data['datetime'])

        # 设置datetime为索引，用于后续resample操作
        data.set_index('datetime', inplace=True)

        # 记得定期获取最新的数据，同步进TDX
        logger.debug(f"数据时间范围: {data.index[0]} ~ {data.index[-1]}")

        # 重置索引，使datetime成为列
        data.reset_index(inplace=True)

        return data

    def read_all_daily_data(self) -> pd.DataFrame:
        """读取所有股票的日线数据

        Returns:
            DataFrame: 所有股票的日线数据
        """
        # 获取股票列表
        stocks = self.get_stock_list()
        logger.info(f"获取到 {len(stocks)} 只股票，开始读取日线数据...")

        all_data = []
        iterator = tqdm(stocks.iterrows(), total=len(stocks)) if config.use_tqdm else stocks.iterrows()

        for _, stock in iterator:
            code = stock['code']
            # 判断市场
            if code.startswith('sh'):
                market = 1  # 上海
            else:
                market = 0  # 深圳

            try:
                data = self.read_daily_data(market, code)
                # 确保datetime是列而不是索引
                if isinstance(data.index, pd.DatetimeIndex) or data.index.name == 'datetime':
                    data = data.reset_index()
                all_data.append(data)
            except FileNotFoundError:
                continue
            except Exception as e:
                logger.error(f"读取 {code} 日线数据时出错: {e}")
                continue

        if not all_data:
            return pd.DataFrame()

        # 合并数据时保留datetime列
        result_df = pd.concat(all_data, ignore_index=True)

        # 确保datetime列存在并且是正确的日期时间格式
        if 'datetime' in result_df.columns and not pd.api.types.is_datetime64_any_dtype(result_df['datetime']):
            try:
                result_df['datetime'] = pd.to_datetime(result_df['datetime'])
            except Exception as e:
                logger.warning(f"转换datetime列时出错: {e}")

        return result_df

