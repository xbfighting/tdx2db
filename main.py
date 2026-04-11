#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""tdx2db 命令行入口"""

import sys
from src.tdx2db.cli import main

if __name__ == '__main__':
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("\n程序被用户中断")
        sys.exit(1)
    except Exception as e:
        print(f"\n程序运行出错: {e}")
        sys.exit(1)
