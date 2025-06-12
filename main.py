#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
通达信数据处理工具入口点

此脚本是程序的主入口点，用于启动通达信数据处理工具。
支持以下功能：
- 获取股票列表：python main.py stock-list
- 获取日线数据：python main.py daily [--code CODE] [--market MARKET]
- 获取分钟线数据：python main.py minute --code CODE --market MARKET [--freq {1,5}]
- 获取分钟线数据：python main.py minutes --code CODE --market MARKET [--freq {1,5}]

使用 python main.py -h 查看完整的帮助信息。
"""

import sys
from src.cli import main

if __name__ == '__main__':
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("\n程序被用户中断")
        sys.exit(1)
    except Exception as e:
        print(f"\n程序运行出错: {e}")
        sys.exit(1)
