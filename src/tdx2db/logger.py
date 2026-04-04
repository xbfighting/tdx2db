"""日志模块

提供统一的日志配置和输出格式
"""

import logging
import sys
from typing import Optional


def setup_logger(
    name: str = "tdx2db",
    level: int = logging.INFO
) -> logging.Logger:
    """配置并返回logger实例

    Args:
        name: logger名称
        level: 日志级别

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

    logger.setLevel(level)
    return logger


# 全局logger实例
logger = setup_logger()
