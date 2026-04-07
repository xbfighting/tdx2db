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
from .downloader import download_and_extract, DEFAULT_DOWNLOAD_URL


def _has_ex_rights_after(code: str, gbbq: pd.DataFrame, last_date: int) -> bool:
    """检查该股票在 last_date 之后是否有除权事件（category==1）。"""
    if gbbq is None or gbbq.empty:
        return False
    if code.startswith('6'):
        prefix = 'sh'
    elif code.startswith('8') or code.startswith('92'):
        prefix = 'bj'
    else:
        prefix = 'sz'
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

    iterator = tqdm(stocks, total=len(stocks), desc="同步日线") if config.use_tqdm else stocks

    for db_code in iterator:
        pure_code, suffix = db_code.split('.')
        market = {'SZ': 0, 'SH': 1, 'BJ': 2}[suffix]
        code = suffix.lower() + pure_code  # sz000001，供 read_daily_data 使用
        last_date = latest_dates.get(db_code)

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
                storage.delete_stock_data(db_code)
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

    # download
    download = subparsers.add_parser('download', help='联网下载 TDX 日线数据并导入数据库')
    download.add_argument('--url', help=f'下载地址（默认: {DEFAULT_DOWNLOAD_URL}）')
    download.add_argument('--adj', choices=['forward', 'backward', 'none'], default='forward')
    download.add_argument('--no-clean', action='store_true', dest='no_clean',
                          help='保留临时目录（用于调试）')

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

    storage = DataStorage()
    processor = DataProcessor()

    if args.command == 'download':
        adj_type = getattr(args, 'adj', 'forward')
        keep_tmp = getattr(args, 'no_clean', False)
        url = getattr(args, 'url', None)

        gbbq = pd.DataFrame()
        if config.tdx_path:
            try:
                local_reader = TdxDataReader()
                gbbq = local_reader.read_gbbq()
                logger.info("已从本地通达信读取权息文件")
            except Exception:
                logger.warning("本地权息文件读取失败，将跳过复权处理")

        logger.info("=== 开始联网下载 TDX 日线数据 ===")
        with download_and_extract(url=url, keep_tmp=keep_tmp) as vipdoc_path:
            dl_reader = TdxDataReader(vipdoc_path=str(vipdoc_path))
            stats = sync_all_daily(dl_reader, processor, storage, gbbq,
                           adj_type=adj_type, incremental=True)
        storage.save_sync_statistics(stats['success'])
        return 0

    try:
        reader = TdxDataReader()
    except (ValueError, FileNotFoundError) as e:
        logger.error(f"初始化失败: {e}")
        return 1

    if args.command == 'stock-list':
        try:
            import akshare as ak

            ak_df = ak.stock_info_a_code_name()  # columns: code, name（code 为纯6位）

            def _add_suffix(code: str) -> str:
                if code.startswith('6'):
                    return code + '.SH'
                elif code.startswith('8') or code.startswith('92'):
                    return code + '.BJ'
                return code + '.SZ'

            ak_map = {_add_suffix(row['code']): row['name'] for _, row in ak_df.iterrows()}

            local_codes = reader.get_stock_list()
            df = pd.DataFrame([
                {'stock_code': c, 'stock_name': ak_map.get(c, c)}
                for c in local_codes
            ])
            logger.info(f"获取到 {len(df)} 只股票")
            storage.save_stock_info(df)
        except Exception as e:
            logger.error(f"同步股票列表出错: {e}")
            return 1

    elif args.command == 'daily':
        adj_type = getattr(args, 'adj', 'forward')
        gbbq = reader.read_gbbq()

        if args.code:
            pure_code = args.code[-6:] if len(args.code) > 6 else args.code
            if pure_code.startswith('6'):
                market = 1
            elif pure_code.startswith('8') or pure_code.startswith('92'):
                market = 2
            else:
                market = 0
            prefix = {0: 'sz', 1: 'sh', 2: 'bj'}[market]
            code = prefix + pure_code
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
            stats = sync_all_daily(reader, processor, storage, gbbq,
                           adj_type=adj_type, incremental=incremental,
                           start_date=args.start, end_date=args.end)
            storage.save_sync_statistics(stats['success'])

    elif args.command == 'sync':
        adj_type = getattr(args, 'adj', 'forward')
        logger.info("=== 开始增量同步日线数据 ===")
        gbbq = reader.read_gbbq()
        stats = sync_all_daily(reader, processor, storage, gbbq,
                       adj_type=adj_type, incremental=True)
        storage.save_sync_statistics(stats['success'])

    else:
        logger.error("请指定子命令，使用 -h 查看帮助")
        return 1

    return 0


if __name__ == '__main__':
    sys.exit(main())
