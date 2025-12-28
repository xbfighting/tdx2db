"""数据读取模块

负责从通达信本地数据文件中读取股票数据，支持：
- 日线数据
- 分钟线数据
- 股票列表
"""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import TYPE_CHECKING, Optional, List

import pandas as pd
from pytdx.reader import TdxDailyBarReader, TdxMinBarReader, TdxLCMinBarReader
from pytdx.reader import BlockReader
from tqdm import tqdm

from .config import config
from .logger import logger

if TYPE_CHECKING:
    from .storage import DataStorage
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
            raise ValueError("通达信数据路径未设置，请在.env文件中设置TDX_PATH或在初始化时提供")

        self.tdx_path = Path(self.tdx_path)
        if not self.tdx_path.exists():
            raise FileNotFoundError(f"通达信数据路径不存在: {self.tdx_path}")

        # 初始化读取器
        self.daily_reader = TdxDailyBarReader()
        self.min_reader = TdxMinBarReader()
        self.lc_min_reader = TdxLCMinBarReader()
        self.block_reader = BlockReader()

    def get_stock_list(self) -> pd.DataFrame:
        """获取股票列表

        Returns:
            DataFrame: 包含A股股票代码和名称的DataFrame（不包含B股、基金、等）
        """
        # 尝试查找通达信股票数据文件
        sz_path = self.tdx_path / 'vipdoc' / 'sz' / 'lday'
        sh_path = self.tdx_path / 'vipdoc' / 'sh' / 'lday'

        if not (sz_path.exists() or sh_path.exists()):
            raise FileNotFoundError(f"无法找到股票列表文件或股票数据目录")

        # 从目录中获取股票代码
        stocks = []

        # 处理深圳股票
        if sz_path.exists():
            for file in sz_path.glob('*.day'):
                code = file.stem
                name = f"深A{code}"

                pure_code = code[-6:]
                code_str = str(pure_code).zfill(6)  # 补齐为6位字符串
                # 匹配上证A股+深证A股
                if re.match(r'^(000|001|002|300)\d{3}$', code_str):
                    stocks.append({'code': code, 'name': name})

        # 处理上海股票
        if sh_path.exists():
            for file in sh_path.glob('*.day'):
                code = file.stem
                name = f"上A{code}"

                pure_code = code[-6:]
                code_str = str(pure_code).zfill(6)  # 补齐为6位字符串
                # 匹配上证A股+深证A股
                if re.match(r'^(60|688)\d{4}$', code_str):
                    stocks.append({'code': code, 'name': name})

        if not stocks:
            raise FileNotFoundError(f"未找到任何股票数据文件")

        return pd.DataFrame(stocks, columns=['code', 'name'])

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

        # 读取数据
        data = self.daily_reader.get_df(str(file_path))
        data['code'] = code
        data['market'] = market
        return data

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

        # 读取5分钟数据
        logger.info(f"正在读取 {code} 的5分钟线数据...")
        with tqdm(total=1, desc="读取进度") as pbar:
            data = self.lc_min_reader.get_df(str(file_path))
            data['code'] = code
            data['market'] = market
            pbar.update(1)

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

        # 生成15分钟数据
        data_15min = data.resample('15min').agg({
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'amount': 'sum',
            'volume': 'sum',
            'code': 'first',
            'market': 'first'
        }).dropna()

        # 生成30分钟数据
        data_30min = data.resample('30min').agg({
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'amount': 'sum',
            'volume': 'sum',
            'code': 'first',
            'market': 'first'
        }).dropna()

        # 生成60分钟数据
        data_60min = data.resample('60min').agg({
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'amount': 'sum',
            'volume': 'sum',
            'code': 'first',
            'market': 'first'
        }).dropna()

        # 重置索引，使datetime成为列
        data.reset_index(inplace=True)
        data_15min.reset_index(inplace=True)
        data_30min.reset_index(inplace=True)
        data_60min.reset_index(inplace=True)

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

        # 读取5分钟数据
        logger.info(f"正在读取 {code} 的5分钟线数据...")
        with tqdm(total=1, desc="读取进度") as pbar:
            data = self.lc_min_reader.get_df(str(file_path))
            data['code'] = code
            data['market'] = market
            pbar.update(1)

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

    def process_and_store_min_data(
        self,
        storage: DataStorage,
        processor: DataProcessor,
        start_date: Optional[str] = None
    ) -> bool:
        """处理所有股票的5分钟数据，转换为不同周期，计算技术指标并存入数据库

        Args:
            storage: 数据存储对象
            processor: 数据处理对象
            start_date: 开始日期，格式为'YYYY-MM-DD'

        Returns:
            bool: 是否处理成功
        """
        try:
            # 获取股票列表
            stocks = self.get_stock_list()
            logger.info(f"处理所有股票的分钟数据，共 {len(stocks)} 只股票")

            # 创建进度条
            iterator = tqdm(stocks.iterrows(), total=len(stocks)) if config.use_tqdm else stocks.iterrows()

            for _, stock in iterator:
                code = stock['code']
                # 判断市场
                if code.startswith('sh'):
                    market = 1  # 上海
                else:
                    market = 0  # 深圳

                try:
                    # 处理单只股票的分钟数据
                    self.process_single_stock_min_data(market, code, storage, processor, start_date)
                except FileNotFoundError:
                    continue
                except Exception as e:
                    logger.error(f"处理 {code} 分钟数据时出错: {e}")
                    continue

            return True
        except Exception as e:
            logger.error(f"处理分钟数据时出错: {e}")
            return False

    def process_single_stock_min_data(
        self,
        market: int,
        code: str,
        storage: DataStorage,
        processor: DataProcessor,
        start_date: Optional[str] = None
    ) -> bool:
        """处理单只股票的5分钟数据，转换为不同周期，计算技术指标并存入数据库

        Args:
            market: 市场代码
            code: 股票代码
            storage: 数据存储对象
            processor: 数据处理对象
            start_date: 开始日期，格式为'YYYY-MM-DD'

        Returns:
            bool: 是否处理成功
        """
        try:
            # 读取5分钟数据
            df_5min = self.read_5min_data(market, code)

            if df_5min.empty:
                logger.warning(f"{code} 无5分钟数据")
                return False

            # 确保datetime列存在且为datetime类型
            if 'datetime' not in df_5min.columns:
                if isinstance(df_5min.index, pd.DatetimeIndex):
                    df_5min['datetime'] = df_5min.index
                df_5min = df_5min.reset_index()

            # 转换datetime列为日期时间类型
            if not pd.api.types.is_datetime64_any_dtype(df_5min['datetime']):
                df_5min['datetime'] = pd.to_datetime(df_5min['datetime'])

            # 添加date列，用于按日期筛选
            df_5min['date'] = df_5min['datetime'].dt.date

            # 设置datetime为索引，用于后续resample操作
            df_5min = df_5min.set_index('datetime')

            # 转换为15分钟数据
            df_15min = df_5min.resample('15min').agg({
                'open': 'first',
                'high': 'max',
                'low': 'min',
                'close': 'last',
                'volume': 'sum',
                'amount': 'sum',
                'code': 'first',
                'market': 'first',
                'date': 'first'
            }).dropna()

            # 转换为30分钟数据
            df_30min = df_5min.resample('30min').agg({
                'open': 'first',
                'high': 'max',
                'low': 'min',
                'close': 'last',
                'volume': 'sum',
                'amount': 'sum',
                'code': 'first',
                'market': 'first',
                'date': 'first'
            }).dropna()

            # 转换为60分钟数据
            df_60min = df_5min.resample('60min').agg({
                'open': 'first',
                'high': 'max',
                'low': 'min',
                'close': 'last',
                'volume': 'sum',
                'amount': 'sum',
                'code': 'first',
                'market': 'first',
                'date': 'first'
            }).dropna()

            # 重置索引
            df_5min = df_5min.reset_index()
            df_15min = df_15min.reset_index()
            df_30min = df_30min.reset_index()
            df_60min = df_60min.reset_index()

            # 使用processor处理数据，计算技术指标
            processed_5min = processor.process_min_data(df_5min)
            processed_15min = processor.process_min_data(df_15min)
            processed_30min = processor.process_min_data(df_30min)
            processed_60min = processor.process_min_data(df_60min)

            if start_date:
                # 根据日期筛选
                filtered5min_data = processor.filter_data_min(
                    processed_5min,
                    start_date=start_date,
                    end_date=None,
                    codes=None
                )
                filtered15min_data = processor.filter_data_min(
                    processed_15min,
                    start_date=start_date,
                    end_date=None,
                    codes=None
                )
                filtered30min_data = processor.filter_data_min(
                    processed_30min,
                    start_date=start_date,
                    end_date=None,
                    codes=None
                )
                filtered60min_data = processor.filter_data_min(
                    processed_60min,
                    start_date=start_date,
                    end_date=None,
                    codes=None
                )

                storage.save_minute_data(filtered5min_data, freq=5, to_csv=False, to_db=True)
                storage.save_minute_data(filtered15min_data, freq=15, to_csv=False, to_db=True)
                storage.save_minute_data(filtered30min_data, freq=30, to_csv=False, to_db=True)
                storage.save_minute_data(filtered60min_data, freq=60, to_csv=False, to_db=True)
            else:

                # 存入数据库
                storage.save_minute_data(processed_5min, freq=5, to_csv=False, to_db=True)
                storage.save_minute_data(processed_15min, freq=15, to_csv=False, to_db=True)
                storage.save_minute_data(processed_30min, freq=30, to_csv=False, to_db=True)
                storage.save_minute_data(processed_60min, freq=60, to_csv=False, to_db=True)

            logger.info(f"{code} 分钟数据已处理并存入数据库")
            return True

        except Exception as e:
            logger.error(f"处理 {code} 分钟数据时出错: {e}")
            return False


    # 板块关系暂时未实现，由于板块文件未找到
    def get_block_stock_relation(self) -> pd.DataFrame:
        """获取通达信板块与股票的对应关系

        Returns:
            DataFrame: 包含板块代码、板块名称和对应股票代码的DataFrame
        """
        # 板块文件目录
        block_path = self.tdx_path / 'T0002' / 'hq_cache'

        if not block_path.exists():
            raise FileNotFoundError(f"板块文件目录不存在: {block_path}")

        blocks = self.block_reader.get_df(self.tdx_path / 'BlockMap' / 'TdxZLSelStock.dat')
        logger.debug(f"读取到板块数据: {len(blocks)} 条记录")

        # # 板块文件列表
        # block_files = list(block_path.glob('block*.dat'))
        # block_files.extend(list(block_path.glob('block*.blk')))

        # if not block_files:
        #     raise FileNotFoundError(f"未找到板块文件: {block_path}")

        # # 存储板块与股票的对应关系
        # block_stock_relations = []

        # # 遍历板块文件
        # for block_file in block_files:
        #     block_type = block_file.stem

        #     try:
        #         # 使用BlockReader读取板块文件
        #         block_data = self.block_reader.get_df(str(block_file))

        #         if block_data.empty:
        #             continue

        #         # 处理板块数据
        #         for _, row in block_data.iterrows():
        #             block_stock_relations.append({
        #                 'block_code': row.get('block_code', block_type),
        #                 'block_name': row.get('block_name', block_type),
        #                 'code': row.get('code', ''),
        #                 'name': row.get('name', '')
        #             })
        #     except Exception as e:
        #         print(f"读取板块文件{block_file}时出错: {e}")

        #         # 尝试直接读取文件内容
        #         try:
        #             with open(block_file, 'rb') as f:
        #                 content = f.read()

        #             # 解析板块文件内容
        #             block_name = block_file.stem

        #             # 尝试从文件名或内容中提取板块名称
        #             if block_file.suffix.lower() == '.dat':
        #                 # .dat文件通常是二进制格式
        #                 try:
        #                     # 尝试从文件头部提取板块名称
        #                     if len(content) > 50:
        #                         # 通达信板块文件格式可能不同，这里尝试几种常见格式
        #                         try:
        #                             name_bytes = content[0:50].split(b'\x00')[0]
        #                             block_name = name_bytes.decode('gbk', errors='ignore').strip()
        #                         except:
        #                             pass
        #                 except:
        #                     pass

        #             # 提取股票代码
        #             codes = []

        #             # 解析文件内容提取股票代码
        #             if block_file.suffix.lower() == '.blk':
        #                 # .blk文件通常是文本格式
        #                 try:
        #                     text_content = content.decode('gbk', errors='ignore')
        #                     for line in text_content.split('\n'):
        #                         line = line.strip()
        #                         if line and not line.startswith('#'):
        #                             # 通常格式为 1 000001 或 0 000001
        #                             parts = line.split()
        #                             if len(parts) >= 2:
        #                                 market = int(parts[0])
        #                                 code = parts[1]
        #                                 market_prefix = 'sh' if market == 1 else 'sz'
        #                                 codes.append(f"{market_prefix}{code}")
        #                             else:
        #                                 # 可能只有代码，没有市场标识
        #                                 code = line
        #                                 # 根据代码前缀判断市场
        #                                 if code.startswith(('6', '5', '9')):
        #                                     codes.append(f"sh{code}")
        #                                 else:
        #                                     codes.append(f"sz{code}")
        #                 except:
        #                     pass
        #             elif block_file.suffix.lower() == '.dat':
        #                 # .dat文件通常是二进制格式
        #                 try:
        #                     # 跳过文件头部，直接读取股票代码部分
        #                     offset = 384  # 通常板块文件头部大小
        #                     while offset < len(content):
        #                         if offset + 7 <= len(content):
        #                             market = content[offset]
        #                             code = content[offset+1:offset+7].decode('ascii', errors='ignore')
        #                             if code.isdigit():
        #                                 market_prefix = 'sh' if market == 1 else 'sz'
        #                                 codes.append(f"{market_prefix}{code}")
        #                         offset += 7
        #                 except:
        #                     pass

        #             # 添加到结果列表
        #             for code in codes:
        #                 block_stock_relations.append({
        #                     'block_code': block_type,
        #                     'block_name': block_name,
        #                     'code': code,
        #                     'name': ''
        #                 })
        #         except Exception as e:
        #             print(f"直接解析板块文件{block_file}时出错: {e}")

        # # 转换为DataFrame
        # if not block_stock_relations:
        #     return pd.DataFrame()

        # df = pd.DataFrame(block_stock_relations)

        # # 尝试补充股票名称
        # try:
        #     stocks = self.get_stock_list()
        #     stock_dict = dict(zip(stocks['code'], stocks['name']))
        #     df['name'] = df['code'].map(stock_dict)
        # except Exception as e:
        #     print(f"补充股票名称时出错: {e}")

        return df
