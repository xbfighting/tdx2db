"""日志模块

提供统一的日志配置和输出格式
"""

import logging
import os
import sys
from typing import Optional

from dotenv import load_dotenv

# 确保 .env 已加载（无论 logger 与 config 的导入先后），load_dotenv 幂等
load_dotenv()


def _level_from_env() -> int:
    """从 LOG_LEVEL 环境变量（.env）读取日志级别，非法值回退 INFO"""
    level = getattr(logging, os.getenv('LOG_LEVEL', 'INFO').upper(), None)
    return level if isinstance(level, int) else logging.INFO


def setup_logger(
    name: str = "tdx2db",
    level: Optional[int] = None
) -> logging.Logger:
    """配置并返回logger实例

    Args:
        name: logger名称
        level: 日志级别，None 时取 .env 的 LOG_LEVEL（缺省 INFO）

    Returns:
        配置好的Logger实例
    """
    logger = logging.getLogger(name)

    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    logger.setLevel(level if level is not None else _level_from_env())
    return logger


# 全局logger实例
logger = setup_logger()
