#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
通达信数据处理工具入口点

常用命令：
- 一键增量同步（日线 + 5/15/30/60 分钟线，日常用这一个即可）：python main.py sync
- 同步股票列表：python main.py stock-list --db-only
- 单独同步日线：python main.py daily --db-only --auto-start --incremental
- 单独同步分钟线：python main.py minutes --db-only --auto-start --incremental

使用 python main.py -h 查看完整的帮助信息。
"""

from tdx2db.cli import entry

if __name__ == '__main__':
    entry()
