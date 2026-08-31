# -*- coding: utf-8 -*-
"""
OguraTopicRouter 测试入口
========================

用法（仓库任意位置）：
    python3 plugins.v2/oguratopicrouter/tests/run_tests.py

先装载假 MP 环境（sys.modules），再导入插件与测试模块，
确保插件绑定的是假环境而非兜底桩。
"""
import os
import sys
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
PLUGINS_V2 = os.path.dirname(os.path.dirname(HERE))
for p in (HERE, PLUGINS_V2):
    if p not in sys.path:
        sys.path.insert(0, p)

# 1) 先装假环境
import fake_mp  # noqa: E402

fake_mp.install_fake_mp()
fake_mp.install_fake_tg_module()

# 2) 再导入插件（绑定假环境）
import oguratopicrouter  # noqa: E402, F401

# 3) 发现并运行测试
TEST_MODULES = [
    "test_routing",
    "test_patches",
    "test_pages",
    "test_marker_pydantic",
    "test_source_layout",
]

if __name__ == "__main__":
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    for name in TEST_MODULES:
        module = __import__(name)
        suite.addTests(loader.loadTestsFromModule(module))
    result = unittest.TextTestRunner(verbosity=1).run(suite)
    failed = len(result.failures) + len(result.errors)
    print(f"\n===== OguraTopicRouter 测试：共 {result.testsRun}，"
          f"失败 {len(result.failures)}，错误 {len(result.errors)} =====")
    sys.exit(0 if result.wasSuccessful() else 1)
