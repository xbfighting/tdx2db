"""命令行接口模块

提供命令行接口，方便用户使用程序功能
"""

import argparse
import sys
from argparse import Namespace
from typing import Optional

from datetime import timedelta

import pandas as pd
from tqdm import tqdm

from .reader import TdxDataReader
from .processor import DataProcessor
from .storage import DataStorage
from .config import config
from .logger import logger


def sync_single_stock_min_data(
    reader: TdxDataReader,
    processor: DataProcessor,
    storage: DataStorage,
    market: int,
    code: str,
    start_date: Optional[str] = None,
    incremental: bool = True,
) -> bool:
    """处理并存储单只股票的分钟数据

    Args:
        reader: 数据读取器
        processor: 数据处理器
        storage: 数据存储器
        market: 市场代码
        code: 股票代码
        start_date: 开始日期
        incremental: 是否启用精确增量
    """
    # 精确增量：查询该股票的最新日期
    if incremental and not start_date:
        latest = storage.get_latest_datetime_by_code('minute5_data', code)
        if latest:
            start_date = (latest + timedelta(days=1)).strftime('%Y-%m-%d')
            logger.debug(f"{code} 增量起始日期: {start_date}")

    # 读取5分钟数据
    df_5min = reader.read_5min_data(market, code)
    if df_5min.empty:
        logger.warning(f"{code} 无5分钟数据")
        return False

    # 准备 datetime 索引
    if not pd.api.types.is_datetime64_any_dtype(df_5min['datetime']):
        df_5min['datetime'] = pd.to_datetime(df_5min['datetime'])
    df_5min['date'] = df_5min['datetime'].dt.date
    df_5min = df_5min.set_index('datetime')

    # 重采样为多周期
    df_15min = DataProcessor.resample_ohlcv(df_5min, '15min')
    df_30min = DataProcessor.resample_ohlcv(df_5min, '30min')
    df_60min = DataProcessor.resample_ohlcv(df_5min, '60min')
    df_5min = df_5min.reset_index()

    # 处理、筛选、存储各周期
    freq_data = [
        (df_5min, 5, 'minute5_data'),
        (df_15min, 15, 'minute15_data'),
        (df_30min, 30, 'minute30_data'),
        (df_60min, 60, 'minute60_data'),
    ]

    has_data = False
    for df, freq, table_name in freq_data:
        processed = processor.process_min_data(df)
        if start_date:
            processed = processor.filter_data_min(processed, start_date=start_date)
        if processed.empty:
            continue
        has_data = True
        if incremental:
            storage.save_incremental(processed, table_name)
        else:
            storage.save_minute_data(processed, freq=freq, to_csv=False, to_db=True)

    if has_data:
        logger.info(f"{code} 分钟数据已处理并存入数据库")
    else:
        logger.debug(f"{code} 无新数据需要同步")

    return True


def sync_all_min_data(
    reader: TdxDataReader,
    processor: DataProcessor,
    storage: DataStorage,
    start_date: Optional[str] = None,
) -> bool:
    """编排所有股票的分钟数据同步"""
    try:
        stocks = reader.get_stock_list()
        logger.info(f"处理所有股票的分钟数据，共 {len(stocks)} 只股票")

        iterator = tqdm(stocks.iterrows(), total=len(stocks)) if config.use_tqdm else stocks.iterrows()

        for _, stock in iterator:
            code = stock['code']
            market = 1 if code.startswith('sh') else 0
            try:
                sync_single_stock_min_data(reader, processor, storage, market, code, start_date)
            except FileNotFoundError:
                continue
            except Exception as e:
                logger.error(f"处理 {code} 分钟数据时出错: {e}")
                continue

        return True
    except Exception as e:
        logger.error(f"处理分钟数据时出错: {e}")
        return False

def parse_args() -> Namespace:
    """解析命令行参数

    Returns:
        Namespace: 解析后的命令行参数
    """
    parser = argparse.ArgumentParser(description='通达信数据处理工具')

    # 通用参数
    parser.add_argument('--tdx-path', help='通达信安装目录路径')
    parser.add_argument('--output', help='输出CSV文件的目录路径')
    parser.add_argument('--db-type', choices=['sqlite', 'mysql', 'postgresql'], help='数据库类型')
    parser.add_argument('--db-host', help='数据库主机')
    parser.add_argument('--db-port', help='数据库端口')
    parser.add_argument('--db-name', help='数据库名称')
    parser.add_argument('--db-user', help='数据库用户名')
    parser.add_argument('--db-password', help='数据库密码')
    parser.add_argument('--no-tqdm', action='store_true', help='禁用进度条')
    parser.add_argument('--batch-size', type=int, default=10000, help='数据库批量插入的批次大小，默认10000条')

    # 子命令
    subparsers = parser.add_subparsers(dest='command', help='子命令')

    # 获取股票列表
    stock_list_parser = subparsers.add_parser('stock-list', help='获取股票列表')
    stock_list_parser.add_argument('--csv-only', action='store_true', help='仅保存到CSV')
    stock_list_parser.add_argument('--db-only', action='store_true', help='仅保存到数据库')

    # 获取日线数据
    daily_parser = subparsers.add_parser('daily', help='获取日线数据')
    daily_parser.add_argument('--code', help='股票代码，不指定则获取所有股票')
    daily_parser.add_argument('--market', type=int, choices=[0, 1], help='市场代码，0表示深圳，1表示上海')
    daily_parser.add_argument('--start_date', help='开始日期，格式为YYYY-MM-DD')
    daily_parser.add_argument('--end_date', help='结束日期，格式为YYYY-MM-DD')
    daily_parser.add_argument('--csv-only', action='store_true', help='仅保存到CSV')
    daily_parser.add_argument('--db-only', action='store_true', help='仅保存到数据库')
    daily_parser.add_argument('--auto-start', action='store_true', help='自动检测起始日期（从数据库最新日期+1天开始）')
    daily_parser.add_argument('--incremental', action='store_true', help='增量同步模式，跳过重复数据')

    # 获取并计算分钟线数据
    min_parser = subparsers.add_parser('minutes', help='获取分钟线数据')
    min_parser.add_argument('--code', help='股票代码，不指定则获取所有股票')
    min_parser.add_argument('--market', type=int, choices=[0, 1], help='市场代码，0表示深圳，1表示上海')
    min_parser.add_argument('--start_date', help='开始日期，格式为YYYY-MM-DD')
    min_parser.add_argument('--end_date', help='结束日期，格式为YYYY-MM-DD')
    min_parser.add_argument('--csv-only', action='store_true', help='仅保存到CSV')
    min_parser.add_argument('--db-only', action='store_true', help='仅保存到数据库')
    min_parser.add_argument('--auto-start', action='store_true', help='自动检测起始日期（从数据库最新日期+1天开始）')
    min_parser.add_argument('--incremental', action='store_true', help='增量同步模式，跳过重复数据')

    # 获取板块与股票对应关系
    block_relation_parser = subparsers.add_parser('block-relation', help='获取板块与股票对应关系【未实现】')
    block_relation_parser.add_argument('--csv-only', action='store_true', help='仅保存到CSV')
    block_relation_parser.add_argument('--db-only', action='store_true', help='仅保存到数据库')

    # 一键同步（日线 + 分钟线增量同步到数据库）
    subparsers.add_parser('sync', help='一键增量同步所有数据到数据库（日线 + 5/15/30/60分钟线）')

    return parser.parse_args()

def update_config(args: Namespace) -> None:
    """根据命令行参数更新配置

    Args:
        args: 解析后的命令行参数
    """
    # 更新通达信路径
    if args.tdx_path:
        config.tdx_path = args.tdx_path

    # 更新CSV输出路径
    if args.output:
        config.csv_output_path = args.output

    # 更新数据库配置
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

    # 更新进度条配置
    if args.no_tqdm:
        config.use_tqdm = False

def main() -> int:
    """主函数

    Returns:
        int: 程序退出码，0表示成功，非0表示失败
    """
    args = parse_args()
    update_config(args)

    # 初始化数据读取器
    try:
        reader = TdxDataReader()
    except (ValueError, FileNotFoundError) as e:
        logger.error(f"初始化失败: {e}")
        return 1

    # 初始化数据存储器
    storage = DataStorage()

    # 处理子命令
    if args.command == 'stock-list':
        # 获取股票列表
        try:
            stocks = reader.get_stock_list()
            logger.info(f"获取到 {len(stocks)} 只股票信息")

            # 确定保存方式
            to_csv = not args.db_only
            to_db = not args.csv_only

            # 保存数据
            storage.save_stock_info(stocks, to_csv=to_csv, to_db=to_db, batch_size=config.db_batch_size)

        except Exception as e:
            logger.error(f"获取股票列表时出错: {e}")
            return 1

    elif args.command == 'daily':
        try:
            # 处理 --auto-start 参数
            start_date = args.start_date
            if hasattr(args, 'auto_start') and args.auto_start and not start_date:
                latest = storage.get_latest_datetime('daily_data', date_column='date')
                if latest:
                    start_date = (latest + timedelta(days=1)).strftime('%Y-%m-%d')
                    logger.info(f"自动检测起始日期: {start_date}")
                else:
                    logger.info("数据库中没有数据，将获取所有数据")

            # 获取日线数据
            if args.code and args.market is not None:
                # 获取单只股票的日线数据
                data = reader.read_daily_data(args.market, args.code)
            else:
                # 获取所有股票的日线数据
                data = reader.read_all_daily_data()

            if data.empty:
                logger.warning("未获取到任何数据")
                return 0

            logger.info(f"获取到 {len(data)} 条日线数据记录")

            # 处理数据
            processor = DataProcessor()
            processed_data = processor.process_daily_data(data)

            # 根据日期筛选
            filtered_data = processor.filter_data(
                processed_data,
                start_date=start_date,
                end_date=args.end_date,
                codes=[args.code] if args.code else None
            )

            if filtered_data.empty:
                logger.warning("筛选后没有数据")
                return 0

            logger.info(f"筛选后有 {len(filtered_data)} 条日线数据记录")

            # 确定保存方式
            to_csv = not args.db_only
            to_db = not args.csv_only
            incremental = hasattr(args, 'incremental') and args.incremental

            # 保存数据
            if to_csv:
                storage.save_to_csv(filtered_data, 'daily_data')
            if to_db:
                if incremental:
                    storage.save_incremental(
                        filtered_data, 'daily_data',
                        conflict_columns=('code', 'date'),
                        batch_size=config.db_batch_size
                    )
                else:
                    storage.save_to_database(filtered_data, 'daily_data', batch_size=config.db_batch_size)

        except Exception as e:
            logger.error(f"获取日线数据时出错: {e}")
            return 1

    elif args.command == 'minutes':
        try:
            # 处理 --auto-start 参数（使用15分钟线表作为参考）
            start_date = args.start_date
            if hasattr(args, 'auto_start') and args.auto_start and not start_date:
                latest = storage.get_latest_datetime('minute15_data')
                if latest:
                    start_date = (latest + timedelta(days=1)).strftime('%Y-%m-%d')
                    logger.info(f"自动检测起始日期: {start_date}")
                else:
                    logger.info("数据库中没有数据，将获取所有数据")

            incremental = hasattr(args, 'incremental') and args.incremental

            # 获取分钟线数据
            if args.code and args.market is not None:
                # 获取单只股票的分钟线数据
                data_list = reader.read_min_data(args.market, args.code)

                logger.info(f"获取到 {len(data_list)} 种分钟线数据记录")
                # 检查数据

                if data_list[0].empty:
                    logger.warning("未获取到任何数据")
                    return 0

                # [data_15min, data_30min, data_60min]
                logger.info(f"生成了 {len(data_list[0])} 条15分钟线数据记录")
                logger.info(f"生成了 {len(data_list[1])} 条30分钟线数据记录")
                logger.info(f"生成了 {len(data_list[2])} 条60分钟线数据记录")

                # 处理数据
                processor = DataProcessor()
                processed_data_list = []
                for i, data in enumerate(data_list):
                    freq = [15, 30, 60][i]  # 对应的分钟频率
                    processed_data = processor.process_min_data(data)

                    # 根据日期筛选
                    filtered_data = processor.filter_data(
                        processed_data,
                        start_date=start_date,
                        end_date=args.end_date
                    )

                    if not filtered_data.empty:
                        processed_data_list.append((filtered_data, freq))
                        logger.info(f"筛选后有 {len(filtered_data)} 条 {freq} 分钟线数据记录")
                    else:
                        logger.warning(f"筛选后 {freq} 分钟线没有数据")

                if not processed_data_list:
                    logger.warning("筛选后所有周期都没有数据")
                    return 0

                # 确定保存方式
                to_csv = not args.db_only
                to_db = not args.csv_only

                # 保存数据
                for filtered_data, freq in processed_data_list:
                    table_name = f'minute{freq}_data'
                    if to_csv:
                        storage.save_to_csv(filtered_data, table_name)
                    if to_db:
                        if incremental:
                            storage.save_incremental(filtered_data, table_name, batch_size=config.db_batch_size)
                        else:
                            storage.save_to_database(filtered_data, table_name, batch_size=config.db_batch_size)
            else:
                # 获取所有股票的分钟线数据
                logger.info("开始处理所有股票的分钟线数据...")
                processor = DataProcessor()
                success = sync_all_min_data(reader, processor, storage, start_date)
                if success:
                    logger.info("所有股票的分钟线数据处理完成")
                else:
                    logger.error("处理分钟线数据时出错")
                    return 1

        except Exception as e:
            logger.error(f"获取分钟线数据时出错: {e}")
            return 1

    elif args.command == 'block-relation':
        # 获取板块与股票对应关系
        try:
            block_relations = reader.get_block_stock_relation()
            logger.info(f"获取到 {len(block_relations)} 条板块与股票对应关系记录")

            # 确定保存方式
            to_csv = not args.db_only
            to_db = not args.csv_only

            # 保存数据
            storage.save_block_relation(block_relations, to_csv=to_csv, to_db=to_db, batch_size=config.db_batch_size)

        except Exception as e:
            logger.error(f"获取板块与股票对应关系时出错: {e}")
            return 1

    elif args.command == 'sync':
        # 一键增量同步所有数据
        logger.info("开始一键增量同步...")
        processor = DataProcessor()
        has_error = False

        # 1. 同步日线数据
        try:
            logger.info("=== 同步日线数据 ===")
            latest = storage.get_latest_datetime('daily_data', date_column='date')
            start_date = None
            if latest:
                start_date = (latest + timedelta(days=1)).strftime('%Y-%m-%d')
                logger.info(f"日线起始日期: {start_date}")

            data = reader.read_all_daily_data()
            if not data.empty:
                processed_data = processor.process_daily_data(data)
                filtered_data = processor.filter_data(processed_data, start_date=start_date)
                if not filtered_data.empty:
                    storage.save_incremental(
                        filtered_data, 'daily_data',
                        conflict_columns=('code', 'date'),
                        batch_size=config.db_batch_size
                    )
                else:
                    logger.info("日线数据已是最新")
            else:
                logger.warning("未获取到日线数据")
        except Exception as e:
            logger.error(f"同步日线数据时出错: {e}")
            has_error = True

        # 2. 同步分钟线数据
        try:
            logger.info("=== 同步分钟线数据 ===")
            latest = storage.get_latest_datetime('minute15_data')
            start_date = None
            if latest:
                start_date = (latest + timedelta(days=1)).strftime('%Y-%m-%d')
                logger.info(f"分钟线起始日期: {start_date}")

            success = sync_all_min_data(reader, processor, storage, start_date)
            if not success:
                logger.error("同步分钟线数据时出错")
                has_error = True
        except Exception as e:
            logger.error(f"同步分钟线数据时出错: {e}")
            has_error = True

        if has_error:
            logger.warning("同步完成，但有部分错误")
            return 1
        else:
            logger.info("一键增量同步完成！")

    else:
        logger.error("请指定子命令，使用 -h 查看帮助信息")
        return 1

    return 0

if __name__ == '__main__':
    sys.exit(main())
