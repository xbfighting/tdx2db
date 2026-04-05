from typing import Optional, List

import pandas as pd

from .logger import logger


class DataProcessor:

    @staticmethod
    def _validate_ohlcv(df: pd.DataFrame) -> pd.DataFrame:
        required = ['open', 'high', 'low', 'close']
        if not all(c in df.columns for c in required):
            return df
        before = len(df)
        positive = (df[required] > 0).all(axis=1)
        ohlc_ok = (
            (df['high'] >= df[['open', 'close']].max(axis=1)) &
            (df['low'] <= df[['open', 'close']].min(axis=1))
        )
        df = df[positive & ohlc_ok]
        dropped = before - len(df)
        if dropped > 0:
            logger.warning(f"数据校验丢弃 {dropped} 条不合格记录")
        return df

    @staticmethod
    def apply_forward_adj(df: pd.DataFrame, gbbq: pd.DataFrame) -> pd.DataFrame:
        """前复权：对每个除权日，将该日之前的历史价格乘以复权因子。"""
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
            df.loc[df['date'] < ex_date, 'adj_factor'] *= factor

        for col in ['open', 'high', 'low', 'close']:
            df[col] = (df[col] * df['adj_factor']).round(3)
        return df

    @staticmethod
    def apply_backward_adj(df: pd.DataFrame, gbbq: pd.DataFrame) -> pd.DataFrame:
        """后复权：对每个除权日，将该日及之后的价格乘以 1/factor。"""
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
            df.loc[df['date'] >= ex_date, 'adj_factor'] *= (1.0 / factor)

        for col in ['open', 'high', 'low', 'close']:
            df[col] = (df[col] * df['adj_factor']).round(3)
        return df

    @staticmethod
    def process_daily_data(
        df: pd.DataFrame,
        gbbq: pd.DataFrame = None,
        adj_type: str = 'forward'
    ) -> pd.DataFrame:
        """日线处理主流程：reset_index → 填充缺失值 → 校验 → 复权 → 日期转 YYYYMMDD 整数。"""
        if df.empty:
            return df

        processed = df.copy()

        # 确保 date 是列而非索引
        if isinstance(processed.index, pd.DatetimeIndex) or processed.index.name in ('date', 'datetime'):
            processed = processed.reset_index()

        # 统一列名：date 列
        if 'date' not in processed.columns and 'datetime' in processed.columns:
            processed.rename(columns={'datetime': 'date'}, inplace=True)

        # 确保 date 是 datetime 类型（复权逻辑依赖 Timestamp 比较）
        if 'date' in processed.columns and not pd.api.types.is_datetime64_any_dtype(processed['date']):
            processed['date'] = pd.to_datetime(processed['date'])

        # 填充缺失值
        for col in ['open', 'high', 'low', 'close', 'volume', 'amount']:
            if col in processed.columns:
                processed[col] = processed[col].ffill()

        # 数据校验
        processed = DataProcessor._validate_ohlcv(processed)

        # 复权
        if gbbq is not None and not gbbq.empty and 'date' in processed.columns:
            if adj_type == 'forward':
                processed = DataProcessor.apply_forward_adj(processed, gbbq)
            elif adj_type == 'backward':
                processed = DataProcessor.apply_backward_adj(processed, gbbq)
            else:
                processed['adj_factor'] = 1.0
        elif 'adj_factor' not in processed.columns:
            processed['adj_factor'] = 1.0

        # 日期转 YYYYMMDD 字符串
        processed['date'] = processed['date'].dt.strftime('%Y%m%d')

        # 预留 turnover_rate 列
        if 'turnover_rate' not in processed.columns:
            processed['turnover_rate'] = None

        # 重命名 code → stock_code 以对齐目标表结构
        processed = processed.rename(columns={'code': 'stock_code'})

        return processed

    @staticmethod
    def filter_data(
        df: pd.DataFrame,
        start_date: Optional[int] = None,
        end_date: Optional[int] = None,
        codes: Optional[List[str]] = None
    ) -> pd.DataFrame:
        """按 YYYYMMDD 整数日期和股票代码筛选。"""
        if df.empty:
            return df
        result = df.copy()
        if 'date' in result.columns:
            if start_date:
                result = result[result['date'] >= str(start_date)]
            if end_date:
                result = result[result['date'] <= str(end_date)]
        if codes and 'stock_code' in result.columns:
            result = result[result['stock_code'].isin(codes)]
        return result
