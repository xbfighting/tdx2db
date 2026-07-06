"""命令行接口模块

提供命令行接口，方便用户使用程序功能
"""

import argparse
import json
import sys
from argparse import Namespace
from typing import Optional

from datetime import timedelta

import pandas as pd
from sqlalchemy.exc import OperationalError
from tqdm import tqdm

from .reader import TdxDataReader
from .processor import DataProcessor
from .storage import DataStorage
from .config import config
from .logger import logger


def infer_market(code: str) -> int:
    """从股票代码推断市场：sh/sz 前缀直接判定；6 位纯数字按首位（6/68 沪，其余深）"""
    c = code.lower()
    if c.startswith('sh'):
        return 1
    if c.startswith('sz'):
        return 0
    return 1 if c.startswith('6') else 0


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
    # DB 中 code 为纯 6 位数字（reader 写入时会截取），查询时需匹配
    db_code = code[-6:] if len(code) > 6 else code

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
        # 增量起点按表分别计算（issue #11）：只查 minute5 的话，
        # 衍生表（15/30/60）一旦落后或缺失，重采样结果会被统一起点过滤掉，永远补不回
        table_start = start_date
        if incremental and not start_date:
            latest = storage.get_latest_datetime_by_code(table_name, db_code)
            if latest:
                table_start = (latest + timedelta(days=1)).strftime('%Y-%m-%d')
        if table_start:
            processed = processor.filter_data_min(processed, start_date=table_start)
        if processed.empty:
            continue
        has_data = True
        if incremental:
            storage.save_incremental(processed, table_name)
        else:
            storage.save_minute_data(processed, freq=freq, to_csv=False, to_db=True)

    if has_data:
        logger.debug(f"{code} 分钟数据已处理并存入数据库")
    else:
        logger.debug(f"{code} 无新数据需要同步")

    return True


def sync_all_daily_data(
    reader: TdxDataReader,
    processor: DataProcessor,
    storage: DataStorage,
    start_date: Optional[str] = None,
) -> bool:
    """逐股票流式同步日线数据，避免全量加载到内存

    start_date=None 时按股票精确增量（查各自的 MAX(date)）。
    全表 MAX(date) 做起点会把库里没有的新股全历史过滤掉（2026-06-12 科创/301 首次同步踩坑）。
    """
    try:
        stocks = reader.get_stock_list()
        logger.info(f"同步日线数据，共 {len(stocks)} 只股票")

        iterator = tqdm(stocks.iterrows(), total=len(stocks)) if config.use_tqdm else stocks.iterrows()
        total_inserted = 0
        failed = 0

        for _, stock in iterator:
            code = stock['code']
            market = 1 if code.startswith('sh') else 0
            try:
                # 精确增量：该股票自己的最新日期；新股 latest=None → 全历史
                stock_start = start_date
                if stock_start is None:
                    db_code = code[-6:] if len(code) > 6 else code
                    latest = storage.get_latest_datetime_by_code(
                        'daily_data', db_code, date_column='date')
                    if latest:
                        stock_start = (latest + timedelta(days=1)).strftime('%Y-%m-%d')

                data = reader.read_daily_data(market, code)
                if isinstance(data.index, pd.DatetimeIndex) or data.index.name == 'datetime':
                    data = data.reset_index()
                if data.empty:
                    continue

                processed = processor.process_daily_data(data)
                filtered = processor.filter_data(processed, start_date=stock_start)
                if filtered.empty:
                    continue

                inserted = storage.save_incremental(
                    filtered, 'daily_data',
                    conflict_columns=('code', 'date'),
                    batch_size=config.db_batch_size
                )
                total_inserted += inserted
            except FileNotFoundError:
                continue
            except Exception as e:
                failed += 1
                logger.error(f"同步 {code} 日线数据时出错: {e}")
                continue

        if total_inserted > 0:
            logger.info(f"日线数据同步完成，共处理 {total_inserted} 条（重复已跳过）"
                        + (f"，{failed} 只股票失败" if failed else ""))
        else:
            logger.info("日线数据已是最新" + (f"（{failed} 只股票失败）" if failed else ""))

        # 失败率超 1% 视为整体失败——cron 用户靠退出码监控
        if len(stocks) and failed / len(stocks) > 0.01:
            logger.error(f"日线同步失败率过高: {failed}/{len(stocks)}")
            return False
        return True
    except Exception as e:
        logger.error(f"同步日线数据时出错: {e}")
        return False


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
        synced = 0
        nodata = 0
        failed = 0

        for _, stock in iterator:
            code = stock['code']
            market = 1 if code.startswith('sh') else 0
            try:
                if sync_single_stock_min_data(reader, processor, storage, market, code, start_date):
                    synced += 1
                else:
                    nodata += 1
            except FileNotFoundError:
                nodata += 1
                continue
            except Exception as e:
                failed += 1
                logger.error(f"处理 {code} 分钟数据时出错: {e}")
                continue

        logger.info(f"分钟数据同步完成：{synced} 只已处理，{nodata} 只无数据"
                    + (f"，{failed} 只失败" if failed else ""))

        # 失败率超 1% 视为整体失败——cron 用户靠退出码监控
        if len(stocks) and failed / len(stocks) > 0.01:
            logger.error(f"分钟同步失败率过高: {failed}/{len(stocks)}")
            return False
        return True
    except Exception as e:
        logger.error(f"处理分钟数据时出错: {e}")
        return False

def parse_args() -> Namespace:
    """解析命令行参数

    Returns:
        Namespace: 解析后的命令行参数
    """
    parser = argparse.ArgumentParser(
        description='通达信数据处理工具',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            '典型用法:\n'
            '  python main.py sync                                        # 日常一键增量同步（日线+分钟线）\n'
            '  python main.py stock-list --db-only                        # 首次使用先同步股票列表\n'
            '  python main.py daily --db-only --code 600000 --incremental # 单股票补数（market 自动推断）\n'
            '\n'
            '注意: --no-tqdm 等全局参数需放在子命令之前，如 python main.py --no-tqdm sync'
        ),
    )

    # 通用参数
    parser.add_argument('--tdx-path', help='通达信安装目录路径')
    parser.add_argument('--output', help='输出CSV文件的目录路径')
    parser.add_argument('--db-type', choices=['sqlite', 'mysql', 'postgresql'], help='数据库类型')
    parser.add_argument('--db-host', help='数据库主机')
    parser.add_argument('--db-port', help='数据库端口')
    parser.add_argument('--db-name', help='数据库名称')
    parser.add_argument('--db-user', help='数据库用户名')
    # 不提供 --db-password：argv 中的密码会暴露于进程列表和 shell 历史，密码只从 .env 读取
    parser.add_argument('--no-tqdm', action='store_true', help='禁用进度条')
    # 默认 None：不覆盖 .env 的 DB_BATCH_SIZE（此前默认 10000 恒为真值，.env 配置永远不生效）
    parser.add_argument('--batch-size', type=int, default=None, help='数据库批量插入的批次大小（默认取 .env 的 DB_BATCH_SIZE，缺省 10000）')

    # 子命令
    subparsers = parser.add_subparsers(dest='command', help='子命令')

    # 获取股票列表
    stock_list_parser = subparsers.add_parser('stock-list', help='获取股票列表')
    stock_list_parser.add_argument('--csv-only', action='store_true', help='仅保存到CSV')
    stock_list_parser.add_argument('--db-only', action='store_true', help='仅保存到数据库')

    # 获取日线数据
    daily_parser = subparsers.add_parser('daily', help='获取日线数据')
    daily_parser.add_argument('--code', help='股票代码，不指定则获取所有股票')
    daily_parser.add_argument('--market', type=int, choices=[0, 1], help='市场代码，0深圳 1上海（不指定则从 code 自动推断）')
    daily_parser.add_argument('--start-date', '--start_date', dest='start_date', help='开始日期，格式为YYYY-MM-DD')
    daily_parser.add_argument('--end-date', '--end_date', dest='end_date', help='结束日期，格式为YYYY-MM-DD')
    daily_parser.add_argument('--csv-only', action='store_true', help='仅保存到CSV')
    daily_parser.add_argument('--db-only', action='store_true', help='仅保存到数据库')
    daily_parser.add_argument('--auto-start', action='store_true', help='自动检测起始日期（逐股票精确增量，各股票从自己的最新日期+1天开始）')
    daily_parser.add_argument('--incremental', action='store_true', help='增量同步模式，跳过重复数据')

    # 获取并计算分钟线数据
    min_parser = subparsers.add_parser('minutes', help='获取分钟线数据')
    min_parser.add_argument('--code', help='股票代码，不指定则获取所有股票')
    min_parser.add_argument('--market', type=int, choices=[0, 1], help='市场代码，0深圳 1上海（不指定则从 code 自动推断）')
    min_parser.add_argument('--start-date', '--start_date', dest='start_date', help='开始日期，格式为YYYY-MM-DD')
    min_parser.add_argument('--end-date', '--end_date', dest='end_date', help='结束日期，格式为YYYY-MM-DD')
    min_parser.add_argument('--csv-only', action='store_true', help='仅保存到CSV')
    min_parser.add_argument('--db-only', action='store_true', help='仅保存到数据库')
    min_parser.add_argument('--auto-start', action='store_true', help='自动检测起始日期（从数据库最新日期+1天开始）')
    min_parser.add_argument('--incremental', action='store_true', help='增量同步模式，跳过重复数据')


    # 一键同步（日线 + 分钟线增量同步到数据库）
    subparsers.add_parser('sync', help='一键增量同步所有数据到数据库（日线 + 5/15/30/60分钟线）')

    # 数据库状态一览（只读，不需要 TDX_PATH）
    status_parser = subparsers.add_parser('status', help='数据库状态一览：每表行数/覆盖股票数/日期范围（只读）')
    status_parser.add_argument('--json', action='store_true', help='输出机器可读 JSON')

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
    if args.batch_size is not None:
        config.db_batch_size = args.batch_size

    # 更新进度条配置
    if args.no_tqdm:
        config.use_tqdm = False

def _init_storage(create_tables: bool = True) -> Optional[DataStorage]:
    """初始化 DataStorage，失败时打印指引并返回 None"""
    try:
        return DataStorage(create_tables=create_tables)
    except ValueError as e:
        logger.error(f"数据库配置错误: {e}")
        return None
    except OperationalError as e:
        logger.error(f"数据库连接失败: {e.orig if e.orig else e}")
        logger.error(
            "请依次检查：\n"
            f"  1) 数据库服务是否已启动（{config.db_type} @ {config.db_host}:{config.db_port}）\n"
            f"  2) 数据库 {config.db_name} 是否已创建（PostgreSQL: createdb {config.db_name}；"
            f"MySQL: CREATE DATABASE {config.db_name}）\n"
            "  3) .env 中 DB_USER / DB_PASSWORD 等配置是否正确"
        )
        return None


def run_status(storage: DataStorage, as_json: bool = False) -> int:
    """status 子命令：数据库状态一览（只读）

    数据走 stdout print（可管道/重定向），错误走 logger。
    """
    stats = storage.get_table_stats()
    by_name = {s['table']: s for s in stats}

    # 衍生分钟表覆盖检查：15/30/60 覆盖股票数少于 minute5 说明存在缺口（issue #11 场景）
    warnings = []
    m5 = by_name.get('minute5_data', {})
    if m5.get('codes'):
        for tbl in ('minute15_data', 'minute30_data', 'minute60_data'):
            t = by_name.get(tbl, {})
            if t.get('exists') and t.get('codes', 0) < m5['codes']:
                warnings.append(
                    f"{tbl} 覆盖 {t['codes']} 只股票，少于 minute5_data 的 {m5['codes']} 只，"
                    "存在衍生表缺口；重跑 `tdx2db minutes --db-only --incremental` 可自动补齐"
                )

    if as_json:
        print(json.dumps({'tables': stats, 'warnings': warnings}, ensure_ascii=False, indent=2))
        return 0

    print(f"{'表名':<18}{'行数':>14}{'股票数':>10}  {'最早':<18}{'最新':<18}")
    print('-' * 80)
    for s in stats:
        if not s['exists']:
            print(f"{s['table']:<18}{'（未创建，运行 sync 后自动建表）':<40}")
            continue
        print(
            f"{s['table']:<18}{s['rows']:>14,}{s['codes']:>10}  "
            f"{s['earliest'] or '-':<18}{s['latest'] or '-':<18}"
        )
    for w in warnings:
        print(f"\n⚠️  {w}")
    return 0


def main() -> int:
    """主函数

    Returns:
        int: 程序退出码，0表示成功，非0表示失败
    """
    args = parse_args()
    update_config(args)

    # status 是纯数据库只读命令：不需要 TDX_PATH，也不建表
    if args.command == 'status':
        storage = _init_storage(create_tables=False)
        if storage is None:
            return 1
        return run_status(storage, as_json=args.json)

    # 初始化数据读取器
    try:
        reader = TdxDataReader()
    except (ValueError, FileNotFoundError) as e:
        logger.error(f"初始化失败: {e}")
        return 1

    # 初始化数据存储器
    storage = _init_storage()
    if storage is None:
        return 1

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
            start_date = args.start_date

            # --auto-start 全股票模式：逐股票精确增量（与 sync 命令同路径）。
            # 全表 MAX(date)+1 做起点会在同步中断后把未完成股票的数据静默过滤掉（issue #12）
            if hasattr(args, 'auto_start') and args.auto_start and not start_date and not args.code:
                if args.csv_only:
                    logger.error("--auto-start 逐股票增量模式仅写入数据库，与 --csv-only 冲突；如需 CSV 请显式指定 --start_date")
                    return 1
                if not args.db_only:
                    logger.warning("--auto-start 逐股票增量模式仅写入数据库，跳过 CSV 输出；如需 CSV 请显式指定 --start_date")
                if args.end_date:
                    logger.warning(f"--auto-start 逐股票增量模式忽略 --end_date {args.end_date}")
                processor = DataProcessor()
                success = sync_all_daily_data(reader, processor, storage)
                return 0 if success else 1

            # --auto-start 单股票模式：查该股票自己的最新日期，而非全表 MAX
            if hasattr(args, 'auto_start') and args.auto_start and not start_date and args.code:
                db_code = args.code[-6:] if len(args.code) > 6 else args.code
                latest = storage.get_latest_datetime_by_code('daily_data', db_code, date_column='date')
                if latest:
                    start_date = (latest + timedelta(days=1)).strftime('%Y-%m-%d')
                    logger.info(f"自动检测 {args.code} 起始日期: {start_date}")
                else:
                    logger.info(f"数据库中没有 {args.code} 的数据，将获取全部历史")

            # 获取日线数据
            if args.code:
                # 获取单只股票的日线数据（market 未指定时从 code 推断）
                market = args.market if args.market is not None else infer_market(args.code)
                data = reader.read_daily_data(market, args.code)
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
            if args.code:
                # 单只股票：统一走 sync_single_stock_min_data，覆盖 5/15/30/60 全部周期
                # market 未指定时从 code 推断（此前漏 --market 会静默进入全市场同步）
                market = args.market if args.market is not None else infer_market(args.code)
                processor = DataProcessor()
                success = sync_single_stock_min_data(
                    reader, processor, storage,
                    market, args.code,
                    start_date=start_date,
                    incremental=incremental,
                )
                if not success:
                    logger.warning(f"股票 {args.code} 无数据可同步")
                    return 0
                logger.info(f"{args.code} 分钟数据同步完成")
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

    elif args.command == 'sync':
        # 一键增量同步所有数据
        import time
        t0 = time.monotonic()
        logger.info("开始一键增量同步...")
        processor = DataProcessor()
        has_error = False

        # 1. 同步日线数据（逐股票精确增量，不传全局 start_date——
        #    全表 MAX(date) 会把新上市/新纳入股票的全历史过滤掉）
        try:
            logger.info("=== 同步日线数据 ===")
            success = sync_all_daily_data(reader, processor, storage)
            if not success:
                logger.error("同步日线数据时出错")
                has_error = True
        except Exception as e:
            logger.error(f"同步日线数据时出错: {e}")
            has_error = True

        # 2. 同步分钟线数据（逐股票精确增量，不传全局 start_date）
        try:
            logger.info("=== 同步分钟线数据 ===")
            success = sync_all_min_data(reader, processor, storage)
            if not success:
                logger.error("同步分钟线数据时出错")
                has_error = True
        except Exception as e:
            logger.error(f"同步分钟线数据时出错: {e}")
            has_error = True

        elapsed = time.monotonic() - t0
        if has_error:
            logger.warning(f"同步完成但有错误（耗时 {elapsed/60:.1f} 分钟），请回看上方 ERROR 日志")
            return 1
        else:
            logger.info(f"一键增量同步完成！耗时 {elapsed/60:.1f} 分钟")

    else:
        logger.error("请指定子命令，使用 -h 查看帮助信息")
        return 1

    return 0

def entry() -> None:
    """console script 入口：统一处理中断与未捕获异常的退出码"""
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("\n程序被用户中断")
        sys.exit(1)
    except Exception as e:
        print(f"\n程序运行出错: {e}")
        sys.exit(1)

if __name__ == '__main__':
    entry()
