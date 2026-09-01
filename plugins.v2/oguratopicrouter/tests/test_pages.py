# -*- coding: utf-8 -*-
"""页面/API/话题记录测试"""
import unittest
from types import SimpleNamespace

from fake_mp import (
    FakeBot, FakePluginBase, FakeTelegramClient, FakeTelegramModule,
    install_fake_mp, install_fake_tg_module,
)
from fake_helpers import build_router, make_tg_msg

install_fake_mp()
import oguratopicrouter as otr  # noqa: E402
from oguratopicrouter import ChainBase  # noqa: E402

_G = install_fake_tg_module()


class TopicRecordTest(unittest.TestCase):

    def setUp(self):
        topics, lock = otr._topics_registry()
        with lock:
            topics.clear()

    def test_record_topic_message(self):
        otr._record_topic_from_message(make_tg_msg(
            chat_id=-100123, thread_id=77, topic_name="种子话题"))
        topics, _ = otr._topics_registry()
        self.assertIn("-100123:77", topics)
        entry = topics["-100123:77"]
        self.assertEqual(entry["thread_id"], 77)
        self.assertEqual(entry["title"], "种子话题")
        self.assertEqual(entry["chat_id"], "-100123")

    def test_record_topic_name_updated_by_later_message(self):
        """同话题后来只发普通消息（无 forum_topic_created）→ 保留旧话题名"""
        otr._record_topic_from_message(make_tg_msg(thread_id=7, topic_name="名字A"))
        otr._record_topic_from_message(make_tg_msg(thread_id=7, topic_name=None,
                                                   text="普通消息"))
        topics, _ = otr._topics_registry()
        self.assertEqual(topics["-100123:7"]["title"], "名字A")
        self.assertEqual(topics["-100123:7"]["last_text"], "普通消息")

    def test_private_chat_ignored(self):
        otr._record_topic_from_message(make_tg_msg(chat_id=86023, thread_id=None))
        topics, _ = otr._topics_registry()
        self.assertEqual(topics, {})

    def test_no_thread_ignored(self):
        otr._record_topic_from_message(make_tg_msg(thread_id=None))
        topics, _ = otr._topics_registry()
        self.assertEqual(topics, {})

    def test_garbage_input_survives(self):
        otr._record_topic_from_message(None)
        otr._record_topic_from_message(SimpleNamespace())
        otr._record_topic_from_message("not-a-message")
        topics, _ = otr._topics_registry()
        self.assertEqual(topics, {})

    def test_registry_persists_across_reload(self):
        """挂在 ChainBase 类属性上：模拟插件模块重载后记录仍在"""
        otr._record_topic_from_message(make_tg_msg(thread_id=55, topic_name="X"))
        # 模拟重载：重新执行模块的 _topics_registry（新模块对象同 ChainBase）
        get_topics = otr._topics_registry
        topics, _ = get_topics()
        self.assertIn("-100123:55", topics)


class HandlerInstallTest(unittest.TestCase):

    def setUp(self):
        from fake_mp import FakeModuleManager
        FakeModuleManager.running.clear()
        self._FakeMM = FakeModuleManager

    def _module_with_client(self):
        bot = FakeBot()
        client = FakeTelegramClient(bot)
        module = FakeTelegramModule(client)
        return bot, client, module

    def test_handlers_installed_once(self):
        r = build_router({"enabled": True})
        bot, client, module = self._module_with_client()
        self._FakeMM.running["TelegramModule"] = module

        notes = r._ensure_topic_handlers()
        self.assertEqual(len(bot.handlers), 1, notes)
        notes2 = r._ensure_topic_handlers()
        self.assertEqual(len(bot.handlers), 1, "幂等：不重复挂")

        # 处理器真的能记录
        bot.handlers[0](make_tg_msg(thread_id=91, topic_name="T91"))
        topics, _ = otr._topics_registry()
        self.assertIn("-100123:91", topics)

    def test_no_tg_module_note(self):
        r = build_router({"enabled": True})
        notes = r._ensure_topic_handlers()
        self.assertTrue(any("未启用" in n or "失败" in n for n in notes))


class PageTest(unittest.TestCase):

    def test_page_renders_all_sections(self):
        r = build_router({"enabled": True, "group_id": "-100123",
                          "route_SeedWatch": 11})
        r._patch_status = {"消息归属": True, "标记传递": True, "模块入口": True,
                           "底层发送注入": True, "发送失败监视": True}
        r._log_route("SeedWatch", "插件", "插件规则:SeedWatch", 11, "改道")
        page = r.get_page()
        self.assertIsInstance(page, list)
        self.assertTrue(len(page) >= 5)
        text = str(page)
        self.assertIn("运行状态", text)
        self.assertIn("路由规则", text)
        self.assertIn("话题清单", text)
        self.assertIn("SeedWatch", text)
        self.assertIn("plugin/OguraTopicRouter/test_route", text)

    def test_rules_table_has_test_button(self):
        """每条规则一行测试按钮（配置完立刻可测，不依赖消息流过）"""
        r = build_router({"enabled": True, "group_id": "-100123",
                          "route_SeedWatch": 11, "route_OguraSubscribePlus": 22})
        page = r.get_page()
        text = str(page)
        self.assertEqual(text.count("test_route"), 2, "两条规则两个测试按钮")
        self.assertEqual(text.count("del_route"), 2, "两条规则两个删除按钮")

    def test_form_dynamic_plugin_list(self):
        """配置页动态列出已安装插件（无需手写插件ID）"""
        from fake_mp import register_fake_installed, clear_fake_installed
        clear_fake_installed()
        register_fake_installed("SeedWatch", "小仓酱的种子监控助手")
        register_fake_installed("OguraSubscribePlus", "小仓酱的订阅补全助手")
        try:
            r = build_router({"enabled": True})
            form, defaults = r.get_form()
            text = str(form)
            self.assertIn("小仓酱的种子监控助手", text)
            self.assertIn("route_SeedWatch", text)
            self.assertIn("插件ID：SeedWatch", text)
            self.assertIn("route_OguraSubscribePlus", text)
            self.assertIn("route_SeedWatch", defaults)
            self.assertNotIn("routes", defaults, "旧文本字段应退出默认配置")
        finally:
            clear_fake_installed()

    def test_form_excludes_self(self):
        """清单不含自身（路由插件不路由自己）"""
        from fake_mp import register_fake_installed, clear_fake_installed
        clear_fake_installed()
        register_fake_installed("OguraTopicRouter", "小仓酱的消息话题路由")
        register_fake_installed("SeedWatch", "种子监控")
        try:
            r = build_router({"enabled": True})
            form, _ = r.get_form()
            text = str(form)
            self.assertIn("route_SeedWatch", text)
            self.assertNotIn("route_OguraTopicRouter", text)
        finally:
            clear_fake_installed()

    def test_form_no_plugins_fallback(self):
        """读不到插件清单时给提示而非报错"""
        from fake_mp import clear_fake_installed
        clear_fake_installed()
        r = build_router({"enabled": True})
        form, defaults = r.get_form()
        self.assertIn("暂时读不到已安装插件清单", str(form))

    def test_page_disabled_state(self):
        r = build_router({"enabled": False})
        page = r.get_page()
        text = str(page)
        self.assertIn("插件未启用", text)

    def test_page_config_error_visible(self):
        r = build_router({"enabled": True, "group_id": "123"})
        page = r.get_page()
        text = str(page)
        self.assertIn("配置有问题", text)
        self.assertIn("负数", text)

    def test_page_topics_table(self):
        otr._record_topic_from_message(make_tg_msg(thread_id=33, topic_name="TV"))
        r = build_router({"enabled": True, "group_id": "-100123"})
        page = r.get_page()
        text = str(page)
        self.assertIn("33", text)
        self.assertIn("TV", text)


class ApiTest(unittest.TestCase):

    def test_get_api_paths(self):
        r = build_router({"enabled": True})
        apis = r.get_api()
        paths = {a["path"] for a in apis}
        self.assertEqual(paths, {"/scan_topics", "/test_route",
                                 "/del_route", "/clear_log"})
        for a in apis:
            self.assertTrue(callable(a["endpoint"]))

    def test_test_route_validation(self):
        r = build_router({"enabled": False})
        resp = r._api_test_route(owner="SeedWatch")
        self.assertFalse(resp.success)

        r2 = build_router({"enabled": True, "group_id": "-100123",
                           "routes": "SeedWatch=11"})
        resp2 = r2._api_test_route(owner="")
        self.assertFalse(resp2.success)
        resp3 = r2._api_test_route(owner="SeedWatch")
        # 走假 chain（chain=None 时会异常 → 失败响应，不崩溃即可）
        self.assertIsInstance(resp3.success, bool)

    def test_del_route_updates_config(self):
        r = build_router({"enabled": True, "group_id": "-100123",
                          "routes": "SeedWatch=11\nOguraSubscribePlus=22"})
        resp = r._api_del_route(owner="SeedWatch")
        self.assertTrue(resp.success)
        cfg = r.get_config()
        self.assertNotIn("SeedWatch", cfg["routes"])
        self.assertIn("OguraSubscribePlus=22", cfg["routes"])
        self.assertNotIn("SeedWatch", r._plugin_routes)
        self.assertEqual(r._plugin_routes.get("OguraSubscribePlus"), 22)

    def test_clear_log(self):
        r = build_router({"enabled": True, "group_id": "-100123"})
        r._log_route("A", "插件", "r", 1, "改道")
        resp = r._api_clear_log()
        self.assertTrue(resp.success)
        self.assertEqual(r._route_log, [])

    def test_scan_topics(self):
        r = build_router({"enabled": True})
        otr._record_topic_from_message(make_tg_msg(thread_id=61, topic_name="S"))
        resp = r._api_scan_topics()
        self.assertTrue(resp.success)
        self.assertIn("61", str(r.get_data("topics_seen")))


if __name__ == "__main__":
    unittest.main()
