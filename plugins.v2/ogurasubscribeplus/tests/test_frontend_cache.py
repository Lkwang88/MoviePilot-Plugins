import re
import unittest
from pathlib import Path

from ogurasubscribeplus import OguraSubscribePlus


ROOT = Path(__file__).resolve().parents[1]
OLD_REMOTE_ENTRY = ROOT / "dist" / "assets" / "remoteEntry.js"
OLD_CONFIG_NAME = "__federation_expose_Config-37sn52oG.js"


def current_assets() -> Path:
    _mode, relative = OguraSubscribePlus().get_render_mode()
    return ROOT / relative


class FrontendCacheBustTest(unittest.TestCase):
    def test_render_mode_uses_new_versioned_assets_directory(self):
        assets = current_assets()
        self.assertNotEqual(assets, OLD_REMOTE_ENTRY.parent)
        self.assertTrue((assets / "remoteEntry.js").is_file())

    def test_remote_entry_uses_new_config_asset_name(self):
        assets = current_assets()
        text = (assets / "remoteEntry.js").read_text(encoding="utf-8")
        match = re.search(r"__federation_expose_Config-[A-Za-z0-9_-]+\.js", text)
        self.assertIsNotNone(match)
        config_name = match.group(0)
        self.assertNotEqual(config_name, OLD_CONFIG_NAME)
        self.assertTrue((assets / config_name).is_file())

    def test_referenced_config_asset_contains_notification_ui(self):
        assets = current_assets()
        text = (assets / "remoteEntry.js").read_text(encoding="utf-8")
        config_name = re.search(r"__federation_expose_Config-[A-Za-z0-9_-]+\.js", text).group(0)
        config_text = (assets / config_name).read_text(encoding="utf-8")
        self.assertIn("plugin/OguraSubscribePlus/test_notify", config_text)
        self.assertIn("key: 'notifications_enabled'", config_text)
        self.assertIn("key: 'notify_scan_complete'", config_text)
        self.assertIn("key: 'tg_user_ids'", config_text)
        self.assertIn("Telegram 白名单用户 ID", config_text)
        self.assertIn("tg_user_ids: ''", config_text)
        self.assertIn('测试通知', config_text)


class FrontendAssetClosureTest(unittest.TestCase):
    def test_all_relative_asset_references_exist(self):
        assets = current_assets()
        patterns = [
            r"(?:from|import)\s*['\"]\./([^'\"]+)['\"]",
            r"__federation_import\(['\"]\./([^'\"]+)['\"]\)",
            r"dynamicLoadingCss\(\[(.*?)\]",
        ]
        missing = []
        for file in assets.glob("*.js"):
            text = file.read_text(encoding="utf-8")
            refs = []
            refs.extend(re.findall(patterns[0], text))
            refs.extend(re.findall(patterns[1], text))
            for block in re.findall(patterns[2], text):
                refs.extend(re.findall(r"['\"]([^'\"]+)['\"]", block))
            for ref in refs:
                if not (assets / ref).is_file():
                    missing.append(f"{file.name} -> {ref}")
        self.assertEqual(missing, [])


if __name__ == "__main__":
    unittest.main()
