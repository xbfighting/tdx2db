"""一次性回填脚本：补齐 minute5/15/30/60 数据的历史空洞

背景：在引入回溯窗口（cli.py LOOKBACK_DAYS）之前，旧的「latest+1」增量逻辑
会把 TDX .lc5 滞后导致的空洞永久跳过。本脚本对每只股票从 .lc5 全量重读，
依靠数据库 (code, datetime) 唯一约束 + ON CONFLICT DO NOTHING 自动只补缺失。

用法：
    # 全市场回填（默认）
    python scripts/backfill_minute_gaps.py

    # 单只股票
    python scripts/backfill_minute_gaps.py --code sz000001

    # dry-run（只跑流程不写库）
    python scripts/backfill_minute_gaps.py --dry-run --code sz000001

注意：回填走完整 .lc5，单只股票约 1-3 秒，全市场约 1-2 小时。可中断重跑。
"""

import argparse
import sys
from pathlib import Path

# 允许 `python scripts/backfill_minute_gaps.py` 直接跑
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from tqdm import tqdm

from src.cli import sync_single_stock_min_data
from src.config import config
from src.logger import logger
from src.processor import DataProcessor
from src.reader import TdxDataReader
from src.storage import DataStorage

# 远早于任何 A 股数据的日期，传入后等价于不过滤
EARLIEST_DATE = '1990-01-01'


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='回填 minute5/15/30/60 数据空洞')
    parser.add_argument('--code', help='仅处理指定股票，例如 sz000001 / sh600000；不指定则全市场')
    parser.add_argument('--dry-run', action='store_true', help='不实际写库，只打印流程')
    parser.add_argument('--no-tqdm', action='store_true', help='禁用进度条')
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.no_tqdm:
        config.use_tqdm = False

    try:
        reader = TdxDataReader()
    except (ValueError, FileNotFoundError) as e:
        logger.error(f"初始化 reader 失败: {e}")
        return 1

    storage = DataStorage()
    processor = DataProcessor()

    # 收集要处理的股票
    if args.code:
        code = args.code
        market = 1 if code.startswith('sh') else 0
        stocks = [(market, code)]
    else:
        try:
            df = reader.get_stock_list()
        except FileNotFoundError as e:
            logger.error(f"获取股票列表失败: {e}")
            return 1
        stocks = [
            (1 if row['code'].startswith('sh') else 0, row['code'])
            for _, row in df.iterrows()
        ]

    logger.info(f"准备回填 {len(stocks)} 只股票（dry_run={args.dry_run}）")

    if args.dry_run:
        for market, code in stocks[:5]:
            logger.info(f"[dry-run] 将回填 market={market} code={code} from {EARLIEST_DATE}")
        if len(stocks) > 5:
            logger.info(f"[dry-run] ...（省略 {len(stocks) - 5} 只）")
        logger.info("[dry-run] 未实际写库")
        return 0

    iterator = tqdm(stocks) if config.use_tqdm else stocks
    success_cnt = 0
    skip_cnt = 0
    err_cnt = 0

    for market, code in iterator:
        try:
            ok = sync_single_stock_min_data(
                reader, processor, storage,
                market, code,
                start_date=EARLIEST_DATE,  # 跳过 cli.py 的 lookback 分支，过滤等价于全量
                incremental=True,           # 走 save_incremental → ON CONFLICT DO NOTHING
            )
            if ok:
                success_cnt += 1
            else:
                skip_cnt += 1
        except FileNotFoundError:
            skip_cnt += 1
        except Exception as e:
            logger.error(f"回填 {code} 失败: {e}")
            err_cnt += 1

    logger.info(
        f"回填完成: success={success_cnt} skip={skip_cnt} error={err_cnt}"
    )
    return 0 if err_cnt == 0 else 1


if __name__ == '__main__':
    sys.exit(main())
