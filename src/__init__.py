"""TDX数据处理器 - 用于读取通达信本地股票数据并存储到数据库或CSV"""

__version__ = "0.1.0"

from .logger import logger, setup_logger

__all__ = ["logger", "setup_logger", "__version__"]
