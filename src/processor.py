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
    def _validate_ohlcv(df: pd.DataFrame) -> pd.DataFrame:
        """校验 OHLCV 数据质量，丢弃不合格行

        校验规则：
        1. 价格列（open/high/low/close）必须 > 0
        2. OHLC 关系：high >= max(open, close), low <= min(open, close)

        Args:
            df: 包含 OHLCV 列的 DataFrame

        Returns:
            校验通过的 DataFrame
        """
        required = ['open', 'high', 'low', 'close']
        if not all(col in df.columns for col in required):
            return df

        before = len(df)

        # 价格必须为正
        positive_mask = (df[required] > 0).all(axis=1)

        # OHLC 关系校验
        ohlc_mask = (
            (df['high'] >= df[['open', 'close']].max(axis=1)) &
            (df['low'] <= df[['open', 'close']].min(axis=1))
        )

        valid_mask = positive_mask & ohlc_mask
        df = df[valid_mask]

        dropped = before - len(df)
        if dropped > 0:
            logger.warning(f"数据校验丢弃 {dropped} 条不合格记录（价格非正或 OHLC 关系异常）")

        return df

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
    def apply_forward_adj(df: pd.DataFrame, gbbq: pd.DataFrame) -> pd.DataFrame:
        """计算前复权价格并原地更新 open/high/low/close，新增 adj_factor 列

        前复权算法：对每个除权日，将该日之前的全部历史数据乘以当次复权因子，
        使价格序列在除权日前后保持连续。

        Args:
            df: 单只股票的日线 DataFrame，含 code(6位), market, date, open/high/low/close
            gbbq: 全量权息 DataFrame（来自 reader.read_gbbq()），含 full_code, category 等

        Returns:
            含 adj_factor 列、价格已前复权的 DataFrame
        """
        if gbbq.empty or df.empty:
            df = df.copy()
            df['adj_factor'] = 1.0
            return df

        # 构造带前缀的完整代码（market 0=sz, 1=sh）
        market_val = df['market'].iloc[0]
        prefix = 'sz' if market_val == 0 else 'sh'
        pure_code = str(df['code'].iloc[0]).zfill(6)
        full_code = prefix + pure_code

        events = gbbq[gbbq['full_code'] == full_code].copy()

        df = df.copy()
        if events.empty:
            df['adj_factor'] = 1.0
            return df

        # 转换除权日期为 Timestamp（与 df['date'] 的 datetime64[ns] 类型一致）
        events['ex_date'] = pd.to_datetime(
            events['datetime'].astype(str).str[:8], format='%Y%m%d'
        )
        # 仅处理 category==1（除权除息），且限制在 df 数据范围内（避免本地数据不完整时引入无效历史因子）
        data_start = df['date'].min()
        events = events[(events['category'] == 1) & (events['ex_date'] > data_start)].sort_values('ex_date')

        df = df.sort_values('date').copy()
        df['adj_factor'] = 1.0

        for _, ev in events.iterrows():
            ex_date = ev['ex_date']  # pd.Timestamp
            # gbbq 字段均以"每10股"为单位：
            # songgu: 每10股送股数，实际送股比例 = songgu / 10
            # hongli: 每10股红利（元），实际每股红利 = hongli / 10
            # peigujia: 配股价（元），单位无需换算
            # peigu: 每10股配股数，实际配股比例 = peigu / 10
            songgu  = float(ev.get('songgu_qianzongguben', 0) or 0) / 10.0
            hongli  = float(ev.get('hongli_panqianliutong', 0) or 0) / 10.0
            peigujia = float(ev.get('peigujia_qianzongguben', 0) or 0)
            peigu   = float(ev.get('peigu_houzongguben', 0) or 0) / 10.0

            before = df[df['date'] < ex_date]
            if before.empty:
                continue
            prev_close = float(before['close'].iloc[-1])
            if prev_close <= 0:
                continue

            denominator = prev_close * (1 + songgu + peigu)
            if denominator <= 0:
                continue
            factor = (prev_close - hongli + peigujia * peigu) / denominator

            if factor <= 0 or factor > 2:
                logger.warning(f"{full_code} 除权日 {ex_date} 复权因子异常({factor:.4f})，已跳过")
                continue

            mask = df['date'] < ex_date
            df.loc[mask, 'adj_factor'] *= factor

        # 应用前复权：历史价格 * adj_factor，当日及以后为原始价格（factor=1）
        for col in ['open', 'high', 'low', 'close']:
            df[col] = (df[col] * df['adj_factor']).round(3)

        return df

    @staticmethod
    def apply_backward_adj(df: pd.DataFrame, gbbq: pd.DataFrame) -> pd.DataFrame:
        """计算后复权价格并原地更新 open/high/low/close，新增 adj_factor 列

        后复权算法：对每个除权日，将该日及之后的全部数据乘以 1/factor，
        使价格序列以最早历史价格为基准保持连续。

        Args:
            df: 单只股票的日线 DataFrame，含 code(6位), market, date, open/high/low/close
            gbbq: 全量权息 DataFrame（来自 reader.read_gbbq()）

        Returns:
            含 adj_factor 列、价格已后复权的 DataFrame
        """
        if gbbq.empty or df.empty:
            df = df.copy()
            df['adj_factor'] = 1.0
            return df

        market_val = df['market'].iloc[0]
        prefix = 'sz' if market_val == 0 else 'sh'
        full_code = prefix + str(df['code'].iloc[0]).zfill(6)

        events = gbbq[gbbq['full_code'] == full_code].copy()
        df = df.copy()
        if events.empty:
            df['adj_factor'] = 1.0
            return df

        events['ex_date'] = pd.to_datetime(
            events['datetime'].astype(str).str[:8], format='%Y%m%d'
        )
        data_start = df['date'].min()
        events = events[
            (events['category'] == 1) & (events['ex_date'] > data_start)
        ].sort_values('ex_date')

        df = df.sort_values('date').copy()
        df['adj_factor'] = 1.0

        for _, ev in events.iterrows():
            ex_date = ev['ex_date']
            songgu   = float(ev.get('songgu_qianzongguben', 0) or 0) / 10.0
            hongli   = float(ev.get('hongli_panqianliutong', 0) or 0) / 10.0
            peigujia = float(ev.get('peigujia_qianzongguben', 0) or 0)
            peigu    = float(ev.get('peigu_houzongguben', 0) or 0) / 10.0

            before = df[df['date'] < ex_date]
            if before.empty:
                continue
            prev_close = float(before['close'].iloc[-1])
            if prev_close <= 0:
                continue

            denominator = prev_close * (1 + songgu + peigu)
            if denominator <= 0:
                continue
            factor = (prev_close - hongli + peigujia * peigu) / denominator

            if factor <= 0 or factor > 2:
                logger.warning(f"{full_code} 除权日 {ex_date} 复权因子异常({factor:.4f})，已跳过")
                continue

            # 后复权：除权日及之后乘以 1/factor（向上调整，历史价格不变）
            mask = df['date'] >= ex_date
            df.loc[mask, 'adj_factor'] *= (1.0 / factor)

        for col in ['open', 'high', 'low', 'close']:
            df[col] = (df[col] * df['adj_factor']).round(3)

        return df

    @staticmethod
    def process_daily_data(df: pd.DataFrame, gbbq: pd.DataFrame = None, adj_type: str = 'forward') -> pd.DataFrame:
        """处理日线数据

        Args:
            df: 原始日线数据DataFrame
            gbbq: 权息数据（来自 reader.read_gbbq()），传入时执行复权，None 时跳过复权
            adj_type: 复权类型，'forward'（前复权）/ 'backward'（后复权）/ 'none'（不复权），默认 'forward'

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

        # 数据质量校验
        processed_df = DataProcessor._validate_ohlcv(processed_df)

        # 复权处理（在均线计算之前，确保均线基于复权价格）
        if gbbq is not None and not gbbq.empty and 'date' in processed_df.columns:
            if adj_type == 'forward':
                processed_df = DataProcessor.apply_forward_adj(processed_df, gbbq)
            elif adj_type == 'backward':
                processed_df = DataProcessor.apply_backward_adj(processed_df, gbbq)
            else:  # 'none'
                processed_df['adj_factor'] = 1.0

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

        # 数据质量校验
        processed_df = DataProcessor._validate_ohlcv(processed_df)

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
