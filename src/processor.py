"""数据处理模块

负责对从通达信读取的原始数据进行清洗和转换，包括：
- 数据格式转换
- 缺失值处理
- 异常值检测
- 计算技术指标
- OHLCV 重采样
"""

from typing import Optional, List
import pandas as pd

from .logger import logger

# 重采样聚合规则
RESAMPLE_AGG = {
    'open': 'first',
    'high': 'max',
    'low': 'min',
    'close': 'last',
    'volume': 'sum',
    'amount': 'sum',
    'code': 'first',
    'market': 'first',
}

# 均线周期
MA_WINDOWS = [5, 10, 13, 21, 34, 55, 60, 89, 144, 233, 250]


class DataProcessor:
    """数据处理类"""

    @staticmethod
    def resample_ohlcv(df: pd.DataFrame, freq: str) -> pd.DataFrame:
        """将 OHLCV 数据重采样到目标频率

        Args:
            df: 带有 DatetimeIndex 的 DataFrame
            freq: pandas resample 频率字符串（'15min', '30min', '60min'）

        Returns:
            重采样后的 DataFrame（已 reset_index）
        """
        agg = dict(RESAMPLE_AGG)
        if 'date' in df.columns:
            agg['date'] = 'first'
        result = df.resample(freq).agg(agg).dropna()
        result.reset_index(inplace=True)
        return result

    @staticmethod
    def _calculate_ma(df: pd.DataFrame) -> pd.DataFrame:
        """计算均线指标，按股票分组

        Args:
            df: 包含 'close' 和 'code' 列的 DataFrame

        Returns:
            添加了均线列的 DataFrame
        """
        for w in MA_WINDOWS:
            df[f'ma{w}'] = df.groupby('code')['close'].transform(
                lambda x: x.rolling(window=w).mean()
            )
        return df

    @staticmethod
    def process_daily_data(df: pd.DataFrame) -> pd.DataFrame:
        """处理日线数据

        Args:
            df: 原始日线数据DataFrame

        Returns:
            DataFrame: 处理后的数据
        """
        if df.empty:
            return df

        # 复制数据，避免修改原始数据
        processed_df = df.copy()

        # 确保datetime列存在
        if 'datetime' not in processed_df.columns:
            # 检查是否有索引中包含日期时间信息
            if processed_df.index.name == 'datetime' or isinstance(processed_df.index, pd.DatetimeIndex):
                # 如果索引是日期时间类型，直接将索引转为列
                processed_df['datetime'] = processed_df.index
            # 如果索引不是日期时间类型但包含日期信息（如终端输出所示）
            elif hasattr(processed_df.iloc[-1], 'name') and isinstance(processed_df.iloc[-1].name, pd.Timestamp):
                # 从行索引名称中提取日期时间
                processed_df['datetime'] = processed_df.apply(lambda row: row.name if isinstance(row.name, pd.Timestamp) else None, axis=1)

        # 处理缺失值
        numeric_columns = ['open', 'high', 'low', 'close', 'volume', 'amount']
        for col in numeric_columns:
            if col in processed_df.columns:
                # 用前一个有效值填充缺失值
                processed_df[col] = processed_df[col].ffill()

        # 计算均线指标
        if all(col in processed_df.columns for col in ['close', 'volume']):
            processed_df = DataProcessor._calculate_ma(processed_df)

        return processed_df

    @staticmethod
    def process_min_data(df: pd.DataFrame) -> pd.DataFrame:
        """处理分钟线数据

        Args:
            df: 原始分钟线数据DataFrame

        Returns:
            DataFrame: 处理后的数据
        """
        if df.empty:
            return df

        # 复制数据，避免修改原始数据
        processed_df = df.copy()


        # 重命名列，使其更符合通用命名
        column_mapping = {
            'amount': 'amount',  # 成交额
            'close': 'close',    # 收盘价
            'open': 'open',      # 开盘价
            'high': 'high',      # 最高价
            'low': 'low',        # 最低价
            'vol': 'volume',     # 成交量
            'year': 'year',      # 年
            'month': 'month',    # 月
            'day': 'day',        # 日
            'hour': 'hour',      # 时
            'minute': 'minute',  # 分
            'datetime': 'datetime',  # 日期时间
            'code': 'code',      # 股票代码
            'market': 'market'   # 市场代码
        }
        processed_df.rename(columns={k: v for k, v in column_mapping.items() if k in processed_df.columns}, inplace=True)

        # 确保datetime列存在
        if 'datetime' not in processed_df.columns and all(col in processed_df.columns for col in ['year', 'month', 'day', 'hour', 'minute']):
            processed_df['datetime'] = pd.to_datetime(
                processed_df[['year', 'month', 'day']].astype(str).agg('-'.join, axis=1) + ' ' +
                processed_df[['hour', 'minute']].astype(str).agg(':'.join, axis=1)
            )

        # 处理缺失值
        numeric_columns = ['open', 'high', 'low', 'close', 'volume', 'amount']
        for col in numeric_columns:
            if col in processed_df.columns:
                # 用前一个有效值填充缺失值
                processed_df[col] = processed_df[col].ffill()

        # 计算均线指标
        if all(col in processed_df.columns for col in ['close', 'volume']):
            processed_df = DataProcessor._calculate_ma(processed_df)

        return processed_df

    @staticmethod
    def filter_data(
        df: pd.DataFrame,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        codes: Optional[List[str]] = None
    ) -> pd.DataFrame:
        """根据条件筛选数据

        Args:
            df: 原始数据DataFrame
            start_date: 开始日期，格式为'YYYY-MM-DD'
            end_date: 结束日期，格式为'YYYY-MM-DD'
            codes: 股票代码列表

        Returns:
            DataFrame: 筛选后的数据
        """
        if df.empty:
            return df

        filtered_df = df.copy()


        logger.debug(f"筛选日期范围: start_date={start_date}, end_date={end_date}")
        # 按日期筛选
        if 'date' in filtered_df.columns:
            if start_date:
                filtered_df = filtered_df[filtered_df['date'] >= pd.to_datetime(start_date)]
            if end_date:
                filtered_df = filtered_df[filtered_df['date'] <= pd.to_datetime(end_date)]

        # 按时间筛选
        if 'datetime' in filtered_df.columns:
            if start_date:
                filtered_df = filtered_df[filtered_df['datetime'] >= pd.to_datetime(start_date)]
            if end_date:
                filtered_df = filtered_df[filtered_df['datetime'] <= pd.to_datetime(end_date)]

        # 按股票代码筛选
        if codes and 'code' in filtered_df.columns:
            filtered_df = filtered_df[filtered_df['code'].isin(codes)]

        return filtered_df

    @staticmethod
    def filter_data_min(
        df: pd.DataFrame,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        codes: Optional[List[str]] = None
    ) -> pd.DataFrame:
        """根据条件筛选分钟线数据

        Args:
            df: 原始数据DataFrame
            start_date: 开始日期，格式为'YYYY-MM-DD'
            end_date: 结束日期，格式为'YYYY-MM-DD'
            codes: 股票代码列表

        Returns:
            DataFrame: 筛选后的数据
        """
        if df.empty:
            return df

        filtered_df = df.copy()

        # 按日期筛选
        if 'date' in filtered_df.columns:
            if start_date:
                filtered_df = filtered_df[filtered_df['datetime'] >= pd.to_datetime(start_date)]
            if end_date:
                filtered_df = filtered_df[filtered_df['datetime'] <= pd.to_datetime(end_date)]

        # 按股票代码筛选
        if codes and 'code' in filtered_df.columns:
            filtered_df = filtered_df[filtered_df['code'].isin(codes)]

        return filtered_df
