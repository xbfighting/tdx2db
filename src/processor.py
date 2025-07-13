"""数据处理模块

负责对从通达信读取的原始数据进行清洗和转换，包括：
- 数据格式转换
- 缺失值处理
- 异常值检测
- 计算技术指标
"""

import pandas as pd

class DataProcessor:
    """数据处理类"""

    @staticmethod
    def process_daily_data(df):
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

        # 计算一些基本的技术指标
        if all(col in processed_df.columns for col in ['close', 'volume']):
             # 计算13日均线
            processed_df['ma13'] = processed_df.groupby('code')['close'].transform(lambda x: x.rolling(window=13).mean())
            # 计算21日均线
            processed_df['ma21'] = processed_df.groupby('code')['close'].transform(lambda x: x.rolling(window=21).mean())
            # 计算34日均线
            processed_df['ma34'] = processed_df.groupby('code')['close'].transform(lambda x: x.rolling(window=34).mean())
            # 计算55日均线
            processed_df['ma55'] = processed_df.groupby('code')['close'].transform(lambda x: x.rolling(window=55).mean())
            # 计算89日均线
            processed_df['ma89'] = processed_df.groupby('code')['close'].transform(lambda x: x.rolling(window=89).mean())
            # 计算144日均线
            processed_df['ma144'] = processed_df.groupby('code')['close'].transform(lambda x: x.rolling(window=144).mean())
            # 计算233日均线
            processed_df['ma233'] = processed_df.groupby('code')['close'].transform(lambda x: x.rolling(window=233).mean())
            # 计算5日均线
            processed_df['ma5'] = processed_df.groupby('code')['close'].transform(lambda x: x.rolling(window=5).mean())
            # 计算10日均线
            processed_df['ma10'] = processed_df.groupby('code')['close'].transform(lambda x: x.rolling(window=10).mean())
            # 计算60日均线
            processed_df['ma60'] = processed_df.groupby('code')['close'].transform(lambda x: x.rolling(window=60).mean())
            # 计算250日均线
            processed_df['ma250'] = processed_df.groupby('code')['close'].transform(lambda x: x.rolling(window=250).mean())

        return processed_df

    @staticmethod
    def process_min_data(df):
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

         # 计算一些基本的技术指标
        if all(col in processed_df.columns for col in ['close', 'volume']):
             # 计算13日均线
            processed_df['ma13'] = processed_df.groupby('code')['close'].transform(lambda x: x.rolling(window=13).mean())
            # 计算21日均线
            processed_df['ma21'] = processed_df.groupby('code')['close'].transform(lambda x: x.rolling(window=21).mean())
            # 计算34日均线
            processed_df['ma34'] = processed_df.groupby('code')['close'].transform(lambda x: x.rolling(window=34).mean())
            # 计算55日均线
            processed_df['ma55'] = processed_df.groupby('code')['close'].transform(lambda x: x.rolling(window=55).mean())
            # 计算89日均线
            processed_df['ma89'] = processed_df.groupby('code')['close'].transform(lambda x: x.rolling(window=89).mean())
            # 计算144日均线
            processed_df['ma144'] = processed_df.groupby('code')['close'].transform(lambda x: x.rolling(window=144).mean())
            # 计算233日均线
            processed_df['ma233'] = processed_df.groupby('code')['close'].transform(lambda x: x.rolling(window=233).mean())
            # 计算5日均线
            processed_df['ma5'] = processed_df.groupby('code')['close'].transform(lambda x: x.rolling(window=5).mean())
            # 计算10日均线
            processed_df['ma10'] = processed_df.groupby('code')['close'].transform(lambda x: x.rolling(window=10).mean())
            # 计算60日均线
            processed_df['ma60'] = processed_df.groupby('code')['close'].transform(lambda x: x.rolling(window=60).mean())
            # 计算250日均线
            processed_df['ma250'] = processed_df.groupby('code')['close'].transform(lambda x: x.rolling(window=250).mean())

        return processed_df

    @staticmethod
    def filter_data(df, start_date=None, end_date=None, codes=None):
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


        print('start_date', start_date, pd.to_datetime(start_date))
        # print('end_date', end_date, pd.to_datetime(end_date))
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
    def filter_data_min(df, start_date=None, end_date=None, codes=None):
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
