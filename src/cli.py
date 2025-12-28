"""命令行接口模块

提供命令行接口，方便用户使用程序功能
"""

import argparse
import sys
from argparse import Namespace

from .reader import TdxDataReader
from .processor import DataProcessor
from .storage import DataStorage
from .config import config
from .logger import logger

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

    # 获取并计算分钟线数据
    min_parser = subparsers.add_parser('minutes', help='获取分钟线数据')
    min_parser.add_argument('--code', help='股票代码，不指定则获取所有股票')
    min_parser.add_argument('--market', type=int, choices=[0, 1], help='市场代码，0表示深圳，1表示上海')
    min_parser.add_argument('--start_date', help='开始日期，格式为YYYY-MM-DD')
    min_parser.add_argument('--end_date', help='结束日期，格式为YYYY-MM-DD')
    min_parser.add_argument('--csv-only', action='store_true', help='仅保存到CSV')
    min_parser.add_argument('--db-only', action='store_true', help='仅保存到数据库')

    # 获取板块与股票对应关系
    block_relation_parser = subparsers.add_parser('block-relation', help='获取板块与股票对应关系【未实现】')
    block_relation_parser.add_argument('--csv-only', action='store_true', help='仅保存到CSV')
    block_relation_parser.add_argument('--db-only', action='store_true', help='仅保存到数据库')

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
                start_date=args.start_date,
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

            # 保存数据
            storage.save_daily_data(filtered_data, to_csv=to_csv, to_db=to_db, batch_size=config.db_batch_size)

        except Exception as e:
            logger.error(f"获取日线数据时出错: {e}")
            return 1

    elif args.command == 'minutes':
        try:
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
                        start_date=args.start_date,
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
                    storage.save_minute_data(filtered_data, freq=freq, to_csv=to_csv, to_db=to_db, batch_size=config.db_batch_size)
            else:
                # 获取所有股票的分钟线数据
                logger.info("开始处理所有股票的分钟线数据...")
                processor = DataProcessor()
                success = reader.process_and_store_min_data(storage, processor, args.start_date)
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

    else:
        logger.error("请指定子命令，使用 -h 查看帮助信息")
        return 1

    return 0

if __name__ == '__main__':
    sys.exit(main())
