import unittest
from types import SimpleNamespace
from unittest.mock import patch

from ogurasubscribeplus import OguraSubscribePlus
from ogurasubscribeplus.models import PluginConfig


class SchedulerServiceTest(unittest.TestCase):
    def setUp(self):
        self.plugin = OguraSubscribePlus()
        self.plugin._plugin_config = PluginConfig(enabled=True, cron="*/5 * * * *")

    def test_service_passes_source_to_function_not_scheduler(self):
        service = self.plugin.get_service()[0]

        self.assertEqual(service["func"], self.plugin.run_scan)
        self.assertEqual(service.get("kwargs"), None)
        self.assertEqual(service["func_kwargs"], {"source": "schedule"})

    def test_service_can_be_registered_without_function_kwargs_leaking(self):
        service = self.plugin.get_service()[0]
        captured = {}

        def add_job(func, trigger, **kwargs):
            captured.update(kwargs)

        add_job(service["func"], service["trigger"], **(service.get("kwargs") or {}),
                kwargs={"job_id": "OguraSubscribePlus_ogurasubscribeplus_scan"},
                replace_existing=True)

        self.assertNotIn("source", captured)
        self.assertEqual(captured["kwargs"], {"job_id": "OguraSubscribePlus_ogurasubscribeplus_scan"})


class ConfigSaveSchedulerTest(unittest.TestCase):
    def test_save_config_refreshes_scheduler_after_reinitializing(self):
        plugin = OguraSubscribePlus()
        plugin._plugin_config = PluginConfig(enabled=False, cron="0 9 * * *")
        plugin.update_config = lambda _config: True
        plugin.init_plugin = lambda config: setattr(plugin, "_plugin_config", PluginConfig.from_dict(config))
        calls = []
        fake_scheduler = SimpleNamespace(update_plugin_job=lambda plugin_id: calls.append(plugin_id))

        with patch.dict("sys.modules", {"app.scheduler": SimpleNamespace(Scheduler=lambda: fake_scheduler)}):
            result = plugin.save_config_api({"enabled": True, "cron": "*/6 * * * *"})

        self.assertTrue(result["success"])
        self.assertEqual(calls, ["OguraSubscribePlus"])
        self.assertTrue(plugin._plugin_config.enabled)
        self.assertEqual(plugin._plugin_config.cron, "*/6 * * * *")


class NotificationTest(unittest.TestCase):
    def make_plugin(self, **config):
        plugin = OguraSubscribePlus()
        plugin._plugin_config = PluginConfig.from_dict({"enabled": True, **config})
        return plugin

    def test_plugin_notification_has_telegram_channel_and_plugin_type(self):
        plugin = self.make_plugin()
        calls = []
        plugin.post_message = lambda **kwargs: calls.append(kwargs)

        plugin._post_plugin_notification("标题", "正文", save_history=False)

        self.assertEqual(len(calls), 1)
        self.assertEqual(getattr(calls[0]["channel"], "value", calls[0]["channel"]), "Telegram")
        self.assertEqual(getattr(calls[0]["mtype"], "value", calls[0]["mtype"]), "插件")
        self.assertEqual(calls[0]["title"], "标题")

    def test_test_notification_is_blocked_when_notifications_are_disabled(self):
        plugin = self.make_plugin(notifications_enabled=False)
        calls = []
        plugin.post_message = lambda **kwargs: calls.append(kwargs)

        result = plugin.test_notify_api()

        self.assertFalse(result["success"])
        self.assertEqual(calls, [])

    def test_test_notification_does_not_touch_business_data(self):
        plugin = self.make_plugin()
        calls = []
        plugin.post_message = lambda **kwargs: calls.append(kwargs)

        result = plugin.test_notify_api()

        self.assertTrue(result["success"])
        self.assertEqual(len(calls), 1)
        self.assertIn("测试通知发送成功", calls[0]["text"])

    def test_scan_complete_notification_contains_scan_summary(self):
        plugin = self.make_plugin(notify_scan_complete=True)
        calls = []
        plugin._post_plugin_notification = lambda **kwargs: calls.append(kwargs)

        plugin._notify_scan_complete("schedule", 12, 5, 3)

        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0]["title"], "小仓酱的订阅补全助手扫描完成")
        self.assertIn("扫描方式：定时", calls[0]["text"])
        self.assertIn("发现候选订阅：12 部", calls[0]["text"])
        self.assertIn("本轮处理：5 部", calls[0]["text"])
        self.assertIn("生成诊断：3 部", calls[0]["text"])


class StartupNotificationTest(unittest.TestCase):
    def test_enabled_init_posts_startup_notification_once(self):
        plugin = OguraSubscribePlus()
        calls = []
        plugin._post_plugin_notification = lambda **kwargs: calls.append(kwargs)

        plugin.init_plugin({"enabled": True, "notifications_enabled": True})
        plugin.init_plugin({"enabled": True, "notifications_enabled": True, "cron": "0 12 * * *"})

        self.assertEqual(len(calls), 1)
        self.assertIn("已启动", calls[0]["title"])

    def test_reenable_posts_startup_notification_again(self):
        plugin = OguraSubscribePlus()
        calls = []
        plugin._post_plugin_notification = lambda **kwargs: calls.append(kwargs)

        plugin.init_plugin({"enabled": True, "notifications_enabled": True})
        plugin.init_plugin({"enabled": False, "notifications_enabled": True})
        plugin.init_plugin({"enabled": True, "notifications_enabled": True})

        self.assertEqual(len(calls), 2)

    def test_disabled_notifications_suppress_startup_notification(self):
        plugin = OguraSubscribePlus()
        calls = []
        plugin._post_plugin_notification = lambda **kwargs: calls.append(kwargs)

        plugin.init_plugin({"enabled": True, "notifications_enabled": False})

        self.assertEqual(calls, [])


class RuntimeDiagnosisLogTest(unittest.TestCase):
    def test_init_logs_loaded_version_and_frontend_assets(self):
        plugin = OguraSubscribePlus()
        plugin._post_plugin_notification = lambda **kwargs: None

        with patch("ogurasubscribeplus.logger.info") as info:
            plugin.init_plugin({"enabled": True, "notifications_enabled": True})

        messages = [str(call.args[0]) for call in info.call_args_list if call.args]
        self.assertTrue(any("1.0.5 已加载" in message for message in messages))
        self.assertTrue(any("frontend=dist/assets-v104" in message for message in messages))
        self.assertTrue(any("启动通知已提交到 Telegram 插件通道" in message for message in messages))


if __name__ == "__main__":
    unittest.main()
