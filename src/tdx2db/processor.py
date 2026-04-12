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
    def build_float_capital_map(base_caps: dict, gbbq: pd.DataFrame) -> dict:
        """从 base.dbf 当前流通股本出发，逆向推算历史各时间点的流通股本。

        Returns: {full_code: [(date_int, cap_万股), ...]} 已按 date_int 升序排列，
        供 merge_asof 使用（date_int 表示"从该日期起生效的流通股本"）。
        """
        if gbbq.empty or not base_caps:
            logger.debug(f"[float_cap] 跳过：gbbq.empty={gbbq.empty} base_caps空={not base_caps}")
            return {}

        logger.debug(f"[float_cap] base_caps 共 {len(base_caps)} 条，gbbq 共 {len(gbbq)} 行")
        result = {}
        relevant_cats = {1, 10, 11, 12, 15}
        gbbq_filtered = gbbq[gbbq['category'].isin(relevant_cats)].copy()
        logger.debug(f"[float_cap] gbbq_filtered（cat∈{relevant_cats}）共 {len(gbbq_filtered)} 行，unique full_code={gbbq_filtered['full_code'].nunique()}")

        for full_code, group in gbbq_filtered.groupby('full_code'):
            pure_code = full_code[2:]  # 去掉 sz/sh/bj 前缀
            if pure_code not in base_caps:
                continue

            cap = float(base_caps[pure_code])
            logger.debug(f"[float_cap] {full_code} base_cap={cap:.2f} 万股，gbbq 事件数={len(group)}")
            events = group.sort_values('datetime', ascending=False)
            snapshots = []

            for _, ev in events.iterrows():
                date_int = int(ev['datetime'])
                cat = int(ev['category'])

                # 先记录：从 date_int 起生效的股本（事件发生后的值）
                snapshots.append((date_int, cap))

                # 再逆向推算事件发生前的 cap
                if cat == 1:
                    songgu = float(ev.get('songgu_qianzongguben', 0) or 0) / 10.0
                    peigu  = float(ev.get('peigu_houzongguben', 0) or 0) / 10.0
                    ratio  = songgu + peigu
                    if ratio > 0:
                        cap = cap / (1.0 + ratio)
                elif cat in (11, 12, 15):
                    # 增发/解禁/债转股：S_before = S_after - N
                    value = float(ev.get('hongli_panqianliutong', 0) or 0)
                    cap = cap - value
                    if cap <= 0:
                        cap = float(base_caps[pure_code])  # 异常保护
                elif cat == 10:
                    # 回购注销：注销后股本减少，回溯要加回来
                    value = float(ev.get('hongli_panqianliutong', 0) or 0)
                    cap = cap + value
                logger.debug(f"  [{full_code}] cat={cat} date={date_int} → cap_before={cap:.2f} 万股")

            # 兜底：最早历史数据使用推算到底的 cap
            snapshots.append((0, cap))
            snapshots.sort(key=lambda x: x[0])
            result[full_code] = snapshots

        logger.info(f"build_float_capital_map 完成，覆盖 {len(result)} 只股票")
        return result

    @staticmethod
    def _calc_turnover_rate(df: pd.DataFrame, gbbq: pd.DataFrame, float_cap_map: dict = None) -> pd.Series:
        """计算换手率(%)：volume(手) × 10000 / 流通股本(股)。

        优先使用 float_cap_map（base.dbf 锚点 + gbbq 逆向推算）；
        float_cap_map 为 None 时降级到原有 gbbq category==5 逻辑。
        """
        market_val = df['market'].iloc[0]
        prefix = {0: 'sz', 1: 'sh', 2: 'bj'}.get(market_val, 'sz')
        full_code = prefix + str(df['code'].iloc[0]).zfill(6)

        # ── 优先路径：float_cap_map ──────────────────────────────────────────
        if float_cap_map is not None and full_code in float_cap_map:
            snap_list = float_cap_map[full_code]  # [(date_int, cap_万股)]，升序
            logger.debug(f"[turnover] {full_code} 使用 float_cap_map，快照数={len(snap_list)}")
            if snap_list:
                snap_df = pd.DataFrame(snap_list, columns=['date_int', 'float_cap'])
                daily = df[['date', 'volume']].copy()
                daily['date_int'] = daily['date'].astype(int)
                daily = daily.sort_values('date_int').reset_index(drop=False)
                merged = pd.merge_asof(
                    daily[['index', 'date_int', 'volume']],
                    snap_df,
                    on='date_int'
                )
                cap = merged['float_cap'] * 10000  # 万股 → 股
                merged['turnover_rate'] = (
                    (merged['volume'] * 10000 / cap).where(cap > 0).round(4)
                )
                # 调试：打印最近几条
                sample = merged[['date_int', 'volume', 'float_cap', 'turnover_rate']].tail(3)
                for _, row in sample.iterrows():
                    logger.debug(
                        f"  [{full_code}] date={int(row['date_int'])} "
                        f"vol={row['volume']:.0f} cap={row['float_cap']:.2f}万股 "
                        f"turnover={row['turnover_rate']:.4f}%"
                    )
                return merged.set_index('index')['turnover_rate'].reindex(df.index)

        # ── 降级路径：原有 gbbq category==5 逻辑 ────────────────────────────
        if gbbq.empty or 'full_code' not in gbbq.columns:
            return pd.Series([None] * len(df), index=df.index, dtype=float)
        shares = gbbq[(gbbq['full_code'] == full_code) & (gbbq['category'] == 5)].copy()
        if shares.empty:
            return pd.Series([None] * len(df), index=df.index, dtype=float)

        # datetime 直接是 YYYYMMDD 整数
        shares = shares.rename(columns={'datetime': 'date_int'})
        shares = shares.sort_values('date_int').drop_duplicates('date_int', keep='last')

        daily = df[['date', 'volume']].copy()
        daily['date_int'] = daily['date'].astype(int)
        daily = daily.sort_values('date_int').reset_index(drop=False)

        merged = pd.merge_asof(
            daily[['index', 'date_int', 'volume']],
            shares[['date_int', 'hongli_panqianliutong']],
            on='date_int'
        )
        # 流通股本单位为万股，× 10000 换算为股
        cap = merged['hongli_panqianliutong'] * 10000
        # 换手率(%) = volume(手) × 100 / 流通股本(股) × 100 = volume × 10000 / 流通股本(股)
        merged['turnover_rate'] = (merged['volume'] * 10000 / cap).where(cap > 0).round(4)
        return merged.set_index('index')['turnover_rate'].reindex(df.index)

    @staticmethod
    def process_daily_data(
        df: pd.DataFrame,
        gbbq: pd.DataFrame = None,
        adj_type: str = 'forward',
        float_cap_map: dict = None,
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

        # 计算换手率
        if gbbq is not None and not gbbq.empty and 'code' in processed.columns:
            processed['turnover_rate'] = DataProcessor._calc_turnover_rate(
                processed, gbbq, float_cap_map=float_cap_map
            )
        else:
            processed['turnover_rate'] = None

        # 生成带市场后缀的 stock_code，如 000001.SZ / 600000.SH / 920001.BJ
        _suffix_map = {0: '.SZ', 1: '.SH', 2: '.BJ'}
        processed['stock_code'] = (
            processed['code'].astype(str).str.zfill(6)
            + processed['market'].map(_suffix_map).fillna('.SZ')
        )
        processed = processed.drop(columns=['code'])

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
