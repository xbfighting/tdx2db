"""模块导入冒烟测试——捕捉 F821 类漏网错误和 import 期崩溃（issue #14）"""

import importlib

import pytest


@pytest.mark.parametrize('module', [
    'src.config',
    'src.logger',
    'src.processor',
    'src.reader',
    'src.storage',
    'src.cli',
])
def test_module_imports(module):
    importlib.import_module(module)
