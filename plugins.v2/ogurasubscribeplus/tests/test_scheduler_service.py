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


if __name__ == "__main__":
    unittest.main()
