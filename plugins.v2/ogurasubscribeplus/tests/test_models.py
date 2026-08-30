import unittest
import inspect
from types import SimpleNamespace

from ogurasubscribeplus import OguraSubscribePlus
from ogurasubscribeplus.models import PluginConfig


class PluginConfigTest(unittest.TestCase):
    def test_plugin_metadata_and_vue_render_mode(self):
        plugin = OguraSubscribePlus()

        self.assertEqual(plugin.plugin_name, "小仓酱的订阅补全助手")
        self.assertEqual(plugin.plugin_config_prefix, "ogurasubscribeplus_")
        self.assertEqual(plugin.get_render_mode(), ("vue", "dist/assets"))

    def test_config_defaults_select_all_categories_and_mp_sites(self):
        config = PluginConfig.from_dict({})

        self.assertFalse(config.enabled)
        self.assertEqual(config.delay_days, 1)
        self.assertEqual(config.selected_categories, [])
        self.assertTrue(config.notify_tg)
        self.assertFalse(config.allow_tg_rule_update)

    def test_config_normalizes_numeric_bounds(self):
        config = PluginConfig.from_dict({"delay_days": "-1", "max_scan_subscribes": "0", "notify_tg": False})

        self.assertEqual(config.delay_days, 0)
        self.assertEqual(config.max_scan_subscribes, 1)
        self.assertEqual(config.notifications_enabled, False)

    def test_new_notification_switch_overrides_legacy_notify_tg(self):
        config = PluginConfig.from_dict({"notify_tg": False, "notifications_enabled": True})

        self.assertEqual(config.notifications_enabled, True)

    def test_default_notification_switches_are_enabled_and_scan_summary_disabled(self):
        config = PluginConfig.from_dict({})

        self.assertTrue(config.notifications_enabled)
        self.assertFalse(config.notify_scan_complete)

    def test_post_api_endpoints_do_not_require_var_kwargs(self):
        plugin = OguraSubscribePlus()
        post_apis = [api for api in plugin.get_api() if "POST" in api.get("methods", [])]

        self.assertTrue(post_apis)
        for api in post_apis:
            signature = inspect.signature(api["endpoint"])
            self.assertNotIn(
                inspect.Parameter.VAR_KEYWORD,
                {parameter.kind for parameter in signature.parameters.values()},
                msg=f"{api['path']} should use an explicit optional body parameter",
            )

    def test_subscribe_log_label_includes_title_and_ids(self):
        plugin = OguraSubscribePlus()
        subscribe = SimpleNamespace(id=102, name="一念永恒", tmdbid=107371)

        self.assertEqual(plugin._describe_subscribe(subscribe), "一念永恒 ID=102 TMDB=107371")


if __name__ == "__main__":
    unittest.main()
