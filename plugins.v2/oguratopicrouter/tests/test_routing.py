# -*- coding: utf-8 -*-
"""路由决策与配置解析测试"""
import sys
import unittest
from types import SimpleNamespace

from fake_mp import (
    FakeNotification, FakeNotificationType, install_fake_mp,
)

_restore = install_fake_mp()
from oguratopicrouter import (  # noqa: E402
    MARKER, OguraTopicRouter,
)
from fake_helpers import build_router, make_message  # noqa: E402


class ConfigParseTest(unittest.TestCase):
    """配置解析：路由规则行、群ID、默认话题"""

    def _router(self, **config):
        return build_router(config)

    def test_valid_routes(self):
        r = self._router(
            enabled=True, group_id="-100123",
            routes="SeedWatch=123\n# 注释\nOguraSubscribePlus=456\ntype:Plugin=789",
        )
        self.assertEqual(r._plugin_routes, {"SeedWatch": 123, "OguraSubscribePlus": 456})
        self.assertEqual(r._type_routes, {"Plugin": 789})
        self.assertEqual(r._group_int, -100123)
        self.assertEqual(r._config_errors, [])

    def test_bad_lines_reported(self):
        r = self._router(
            enabled=True, group_id="-100123",
            routes="没有等号\n=123\nSeedWatch=abc\nSeedWatch=\n  \n",
        )
        self.assertEqual(len(r._config_errors), 4)
        self.assertNotIn("SeedWatch", r._plugin_routes)
        for err in r._config_errors:
            self.assertIn("第", err)  # 报错带行号

    def test_group_id_validation(self):
        r = self._router(enabled=True, group_id="abc")
        self.assertIsNone(r._group_int)
        self.assertTrue(any("不是数字" in e for e in r._config_errors))

        r = self._router(enabled=True, group_id="100123")
        self.assertIsNone(r._group_int)
        self.assertTrue(any("负数" in e for e in r._config_errors))

        r = self._router(enabled=True, group_id="-100123")
        self.assertEqual(r._group_int, -100123)
        self.assertEqual(r._config_errors, [])

    def test_default_thread_validation(self):
        r = self._router(enabled=True, group_id="-100123", default_thread_id="xyz")
        self.assertIsNone(r._default_tid)
        r = self._router(enabled=True, group_id="-100123", default_thread_id="-5")
        self.assertIsNone(r._default_tid)
        r = self._router(enabled=True, group_id="-100123", default_thread_id="42")
        self.assertEqual(r._default_tid, 42)

    def test_disabled_no_error_noise(self):
        r = self._router(enabled=False, group_id="", routes="")
        self.assertFalse(r._enabled)
        self.assertEqual(r._config_errors, [])


class ChatMatchTest(unittest.TestCase):
    def setUp(self):
        self.r = build_router({"enabled": True, "group_id": "-100123"})

    def test_int_str_equivalent(self):
        self.assertTrue(self.r._chat_matches(-100123))
        self.assertTrue(self.r._chat_matches("-100123"))

    def test_mismatch(self):
        self.assertFalse(self.r._chat_matches(-100999))
        self.assertFalse(self.r._chat_matches("@mychannel"))
        self.assertFalse(self.r._chat_matches(None))
        self.assertFalse(self.r._chat_matches(""))

    def test_no_group_no_match(self):
        r = build_router({"enabled": True, "group_id": ""})
        self.assertFalse(r._chat_matches(-100123))


class ResolveTest(unittest.TestCase):
    """_resolve_thread 决策矩阵"""

    def test_no_marker_passthrough(self):
        r = build_router({"enabled": True, "group_id": "-100123",
                          "routes": "SeedWatch=1"})
        msg = make_message()  # 无标记
        self.assertIsNone(r._resolve_thread(msg))

    def test_disabled_passthrough(self):
        r = build_router({"enabled": False, "group_id": "-100123",
                          "routes": "SeedWatch=1"})
        msg = make_message(owner="SeedWatch")
        self.assertIsNone(r._resolve_thread(msg))

    def test_plugin_route_hit(self):
        r = build_router({"enabled": True, "group_id": "-100123",
                          "routes": "SeedWatch=11\ntype:Plugin=99"})
        msg = make_message(owner="SeedWatch")
        self.assertEqual(r._resolve_thread(msg), 11)

    def test_priority_plugin_over_type_over_default(self):
        r = build_router({
            "enabled": True, "group_id": "-100123",
            "routes": "SeedWatch=11\ntype:Plugin=99",
            "default_thread_id": "55",
        })
        self.assertEqual(r._resolve_thread(make_message(owner="SeedWatch")), 11)
        self.assertEqual(
            r._resolve_thread(make_message(owner="Other", mtype="Plugin")), 99)
        self.assertEqual(
            r._resolve_thread(make_message(owner="Other", mtype="Download")), 55)

    def test_no_rule_no_default_passthrough(self):
        r = build_router({"enabled": True, "group_id": "-100123"})
        self.assertIsNone(r._resolve_thread(make_message(owner="SeedWatch")))

    def test_directed_private_skip(self):
        r = build_router({"enabled": True, "group_id": "-100123",
                          "routes": "SeedWatch=11"})
        msg = make_message(owner="SeedWatch", userid="86023")
        self.assertIsNone(r._resolve_thread(msg))

    def test_interaction_skip(self):
        r = build_router({"enabled": True, "group_id": "-100123",
                          "routes": "SeedWatch=11"})
        self.assertIsNone(r._resolve_thread(
            make_message(owner="SeedWatch", buttons=[[{"text": "ok"}]])))
        self.assertIsNone(r._resolve_thread(
            make_message(owner="SeedWatch", force_reply=True)))

    def test_group_missing_skip(self):
        r = build_router({"enabled": True, "routes": "SeedWatch=11"})
        self.assertIsNone(r._resolve_thread(make_message(owner="SeedWatch")))

    def test_route_log_entries(self):
        r = build_router({"enabled": True, "group_id": "-100123",
                          "routes": "SeedWatch=11"})
        r._resolve_thread(make_message(owner="SeedWatch"))
        r._resolve_thread(make_message(owner="Unkown"))
        self.assertEqual(len(r._route_log), 2)
        # insert(0) 新条目在前：后调用的 Unkown(原样) 在 [0]，先调用的 SeedWatch(改道) 在 [1]
        self.assertEqual(r._route_log[0]["result"], "原样")
        self.assertEqual(r._route_log[1]["result"], "改道")


if __name__ == "__main__":
    unittest.main()
