import argparse
import sys
from argparse import Namespace
from typing import Optional

import pandas as pd
from tqdm import tqdm

from .reader import TdxDataReader
from .processor import DataProcessor
from .storage import DataStorage
from .config import config
from .logger import logger


def _has_ex_rights_after(code: str, gbbq: pd.DataFrame, last_date: int) -> bool:
    """检查该股票在 last_date 之后是否有除权事件（category==1）。"""
    if gbbq is None or gbbq.empty:
        return False
    prefix = 'sh' if code.startswith('6') else 'sz'
    full_code = prefix + code.zfill(6)
    events = gbbq[
        (gbbq['full_code'] == full_code) &
        (gbbq['category'] == 1) &
        (gbbq['datetime'] > int(last_date))
    ]
    return not events.empty


def sync_all_daily(
    reader: TdxDataReader,
    processor: DataProcessor,
    storage: DataStorage,
    gbbq: pd.DataFrame,
    adj_type: str = 'forward',
    incremental: bool = True,
    start_date: Optional[int] = None,
    end_date: Optional[int] = None,
) -> dict:
    """逐股票流式同步日线数据，返回统计信息。"""
    stocks = reader.get_stock_list()
    logger.info(f"共 {len(stocks)} 只股票")

    latest_dates = storage.get_all_latest_dates() if incremental else {}
    stats = {'total': len(stocks), 'success': 0, 'failed': 0}

    iterator = tqdm(stocks.iterrows(), total=len(stocks), desc="同步日线") if config.use_tqdm else stocks.iterrows()

    for _, stock in iterator:
        code = stock['code']
        market = 1 if code.startswith('sh') else 0
        pure_code = code[-6:] if len(code) > 6 else code
        last_date = latest_dates.get(pure_code)

        try:
            data = reader.read_daily_data(market, code)
            if isinstance(data.index, pd.DatetimeIndex) or data.index.name in ('date', 'datetime'):
                data = data.reset_index()
            if data.empty:
                stats['success'] += 1
                continue

            needs_refresh = (
                incremental and last_date is not None and
                _has_ex_rights_after(pure_code, gbbq, last_date)
            )

            processed = processor.process_daily_data(data, gbbq=gbbq, adj_type=adj_type)

            if incremental and last_date and not needs_refresh:
                processed = processed[processed['date'] > last_date]
            if start_date:
                processed = processed[processed['date'] >= str(start_date)]
            if end_date:
                processed = processed[processed['date'] <= str(end_date)]

            if processed.empty:
                stats['success'] += 1
                continue

            if needs_refresh:
                storage.delete_stock_data(pure_code)
            storage.save_incremental(processed, 'daily_data', conflict_columns=('stock_code', 'date'),
                                     batch_size=config.db_batch_size)
            stats['success'] += 1

        except FileNotFoundError:
            stats['failed'] += 1
        except Exception as e:
            logger.error(f"处理 {code} 时出错: {e}")
            stats['failed'] += 1

    logger.info(f"同步完成: 成功 {stats['success']}，失败 {stats['failed']}")
    return stats


def parse_args() -> Namespace:
    parser = argparse.ArgumentParser(description='tdx2db 日线数据同步工具')

    parser.add_argument('--tdx-path', help='通达信安装目录')
    parser.add_argument('--db-type', choices=['sqlite', 'mysql', 'postgresql'])
    parser.add_argument('--db-host')
    parser.add_argument('--db-port')
    parser.add_argument('--db-name')
    parser.add_argument('--db-user')
    parser.add_argument('--db-password')
    parser.add_argument('--no-tqdm', action='store_true')
    parser.add_argument('--batch-size', type=int, default=10000)

    subparsers = parser.add_subparsers(dest='command')

    # stock-list
    subparsers.add_parser('stock-list', help='同步股票列表')

    # daily
    daily = subparsers.add_parser('daily', help='同步日线数据')
    daily.add_argument('--code', help='股票代码（6位数字，如 000001），不指定则全量，市场自动识别')
    daily.add_argument('--start', type=int, help='开始日期 YYYYMMDD')
    daily.add_argument('--end', type=int, help='结束日期 YYYYMMDD')
    daily.add_argument('--adj', choices=['forward', 'backward', 'none'], default='forward')
    daily.add_argument('--incremental', action='store_true', help='增量模式')

    # sync
    sync = subparsers.add_parser('sync', help='一键增量同步日线数据')
    sync.add_argument('--adj', choices=['forward', 'backward', 'none'], default='forward')

    return parser.parse_args()


def update_config(args: Namespace) -> None:
    if args.tdx_path:
        config.tdx_path = args.tdx_path
    if args.db_type:
        config.db_type = args.db_type
    if args.db_host:
        config.db_host = args.db_host
    if args.db_port:
        config.db_port = args.db_port
    if args.db_name:
        config.db_name = args.db_name
    if args.db_user:
        config.db_user = args.db_user
    if args.db_password:
        config.db_password = args.db_password
    if args.batch_size:
        config.db_batch_size = args.batch_size
    if args.no_tqdm:
        config.use_tqdm = False


def main() -> int:
    args = parse_args()
    update_config(args)

    try:
        reader = TdxDataReader()
    except (ValueError, FileNotFoundError) as e:
        logger.error(f"初始化失败: {e}")
        return 1

    storage = DataStorage()
    processor = DataProcessor()

    if args.command == 'stock-list':
        try:
            stocks = reader.get_stock_list()
            logger.info(f"获取到 {len(stocks)} 只股票")
            storage.save_stock_info(stocks)
        except Exception as e:
            logger.error(f"同步股票列表出错: {e}")
            return 1

    elif args.command == 'daily':
        adj_type = getattr(args, 'adj', 'forward')
        gbbq = reader.read_gbbq()

        if args.code:
            pure_code = args.code[-6:] if len(args.code) > 6 else args.code
            market = 1 if pure_code.startswith('6') else 0
            code = ('sh' if market == 1 else 'sz') + pure_code
            try:
                data = reader.read_daily_data(market, code)
                if isinstance(data.index, pd.DatetimeIndex) or data.index.name in ('date', 'datetime'):
                    data = data.reset_index()
                processed = processor.process_daily_data(data, gbbq=gbbq, adj_type=adj_type)
                processed = processor.filter_data(processed, start_date=args.start, end_date=args.end)
                if not processed.empty:
                    storage.save_incremental(processed, 'daily_data', conflict_columns=('stock_code', 'date'))
            except Exception as e:
                logger.error(f"同步 {code} 出错: {e}")
                return 1
        else:
            incremental = getattr(args, 'incremental', False)
            sync_all_daily(reader, processor, storage, gbbq,
                           adj_type=adj_type, incremental=incremental,
                           start_date=args.start, end_date=args.end)

    elif args.command == 'sync':
        adj_type = getattr(args, 'adj', 'forward')
        logger.info("=== 开始增量同步日线数据 ===")
        gbbq = reader.read_gbbq()
        sync_all_daily(reader, processor, storage, gbbq,
                       adj_type=adj_type, incremental=True)

    else:
        logger.error("请指定子命令，使用 -h 查看帮助")
        return 1

    return 0


if __name__ == '__main__':
    sys.exit(main())
