import unittest
from types import SimpleNamespace
from unittest.mock import patch

from ogurasubscribeplus import OguraSubscribePlus
from ogurasubscribeplus.models import PluginConfig


class SchedulerServiceTest(unittest.TestCase):
    def setUp(self):
        self.plugin = OguraSubscribePlus()
        self.plugin._plugin_config = PluginConfig(enabled=True, scan_times=["07:15", "19:45"])

    def test_service_registers_one_job_per_explicit_time(self):
        # 本地纯测试环境可能没有 APScheduler；模拟宿主可用的最小触发器。
        class FakeCronTrigger:
            def __init__(self, **kwargs):
                self.kwargs = kwargs

        with patch("ogurasubscribeplus.CronTrigger", FakeCronTrigger):
            services = self.plugin.get_service()

        self.assertEqual([service["id"] for service in services], ["scan_0715", "scan_1945"])
        self.assertEqual([service["name"] for service in services], [
            "小仓酱的订阅补全助手扫描（07:15）",
            "小仓酱的订阅补全助手扫描（19:45）",
        ])
        self.assertTrue(all(service["func"] == self.plugin.run_scheduled_scan for service in services))
        self.assertEqual([service["trigger"].kwargs for service in services], [
            {"hour": 7, "minute": 15}, {"hour": 19, "minute": 45},
        ])

    def test_disabled_plugin_registers_no_jobs(self):
        self.plugin._plugin_config.enabled = False
        self.assertEqual(self.plugin.get_service(), [])

    def test_scheduled_scan_defers_five_minutes_then_runs(self):
        self.plugin._plugin_config = PluginConfig(enabled=True, system_refresh_retry_minutes=5)
        checks = [True, False]
        self.plugin._scan_is_running = lambda: checks.pop(0)
        calls = []
        self.plugin._run_scan_unlocked = lambda source: calls.append(source) or {"success": True, "source": source}

        with patch("ogurasubscribeplus.time.sleep") as sleep:
            result = self.plugin.run_scheduled_scan()

        sleep.assert_called_once_with(300)
        self.assertEqual(calls, ["schedule"])
        self.assertTrue(result["success"])

    def test_scheduled_scan_skips_when_plugin_is_already_busy(self):
        self.assertTrue(self.plugin._scan_lock.acquire(blocking=False))
        try:
            result = self.plugin.run_scheduled_scan()
        finally:
            self.plugin._scan_lock.release()

        self.assertTrue(result["skipped"])
        self.assertEqual(result["reason"], "plugin_scan_running")

    def test_manual_scan_skips_when_scheduled_scan_or_wait_is_busy(self):
        self.assertTrue(self.plugin._scan_lock.acquire(blocking=False))
        try:
            result = self.plugin.run_scan()
        finally:
            self.plugin._scan_lock.release()

        self.assertTrue(result["skipped"])
        self.assertEqual(result["reason"], "plugin_scan_running")


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

        plugin._notify_to_allowed_users(title="标题", text="正文", save_history=False)

        self.assertEqual(len(calls), 1)
        self.assertEqual(getattr(calls[0]["channel"], "value", calls[0]["channel"]), "Telegram")
        self.assertEqual(getattr(calls[0]["mtype"], "value", calls[0]["mtype"]), "插件")
        self.assertEqual(calls[0]["title"], "标题")

    def test_whitelist_direct_sends_to_each_user(self):
        plugin = self.make_plugin(tg_user_ids="123, 456")
        calls = []
        plugin.post_message = lambda **kwargs: calls.append(kwargs)

        ok = plugin._notify_to_allowed_users(title="标题", text="正文", save_history=False)

        self.assertTrue(ok)
        self.assertEqual(len(calls), 2)
        self.assertEqual({c["userid"] for c in calls}, {"123", "456"})
        for call in calls:
            self.assertEqual(getattr(call["channel"], "value", call["channel"]), "Telegram")
            self.assertEqual(getattr(call["mtype"], "value", call["mtype"]), "插件")

    def test_whitelist_parses_commas_spaces_and_filters_garbage(self):
        plugin = self.make_plugin(tg_user_ids="123, 456　789，abc")
        self.assertEqual(plugin._parse_tg_user_ids(), [123, 456, 789])

    def test_empty_whitelist_broadcasts_once_without_userid(self):
        plugin = self.make_plugin()
        calls = []
        plugin.post_message = lambda **kwargs: calls.append(kwargs)

        ok = plugin._notify_to_allowed_users(title="标题", text="正文", save_history=False)

        self.assertTrue(ok)
        self.assertEqual(len(calls), 1)
        self.assertIsNone(calls[0].get("userid"))

    def test_notify_failure_returns_false(self):
        plugin = self.make_plugin()

        def boom(**kwargs):
            raise RuntimeError("send failed")

        plugin.post_message = boom
        self.assertFalse(plugin._notify_to_allowed_users(title="标题", text="正文"))

    def test_whitelist_partial_failure_returns_false_but_sends_others(self):
        plugin = self.make_plugin(tg_user_ids="123, 456")
        calls = []

        def flaky(**kwargs):
            if kwargs.get("userid") == "123":
                raise RuntimeError("first user failed")
            calls.append(kwargs)

        plugin.post_message = flaky
        ok = plugin._notify_to_allowed_users(title="标题", text="正文")
        self.assertFalse(ok)
        self.assertEqual([c["userid"] for c in calls], ["456"])

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

    def test_test_notification_reports_send_failure(self):
        plugin = self.make_plugin()
        plugin._notify_to_allowed_users = lambda **kwargs: False

        result = plugin.test_notify_api()

        self.assertFalse(result["success"])
        self.assertIn("发送失败", result["message"])

    def test_scan_complete_notification_contains_scan_summary(self):
        plugin = self.make_plugin(notify_scan_complete=True)
        calls = []
        plugin._notify_to_allowed_users = lambda **kwargs: calls.append(kwargs)

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
        plugin._notify_to_allowed_users = lambda **kwargs: calls.append(kwargs)

        plugin.init_plugin({"enabled": True, "notifications_enabled": True})
        plugin.init_plugin({"enabled": True, "notifications_enabled": True, "cron": "0 12 * * *"})

        self.assertEqual(len(calls), 1)
        self.assertIn("已启动", calls[0]["title"])

    def test_reenable_posts_startup_notification_again(self):
        plugin = OguraSubscribePlus()
        calls = []
        plugin._notify_to_allowed_users = lambda **kwargs: calls.append(kwargs)

        plugin.init_plugin({"enabled": True, "notifications_enabled": True})
        plugin.init_plugin({"enabled": False, "notifications_enabled": True})
        plugin.init_plugin({"enabled": True, "notifications_enabled": True})

        self.assertEqual(len(calls), 2)

    def test_disabled_notifications_suppress_startup_notification(self):
        plugin = OguraSubscribePlus()
        calls = []
        plugin._notify_to_allowed_users = lambda **kwargs: calls.append(kwargs)

        plugin.init_plugin({"enabled": True, "notifications_enabled": False})

        self.assertEqual(calls, [])


class RuntimeDiagnosisLogTest(unittest.TestCase):
    def test_init_logs_loaded_version_and_frontend_assets(self):
        plugin = OguraSubscribePlus()
        plugin._notify_to_allowed_users = lambda **kwargs: True

        with patch("ogurasubscribeplus.logger.info") as info:
            plugin.init_plugin({"enabled": True, "notifications_enabled": True})

        messages = [str(call.args[0]) for call in info.call_args_list if call.args]
        self.assertTrue(any("已加载" in message for message in messages))
        self.assertTrue(any("frontend=dist/assets-v110" in message for message in messages))
        self.assertTrue(any("启动通知已提交到 Telegram 插件通道" in message for message in messages))


if __name__ == "__main__":
    unittest.main()
