# -*- coding: utf-8 -*-
"""静态门禁：结构、版本同步、打包索引、危险模式扫描"""
import ast
import json
import re
import unittest
from pathlib import Path

PLUGIN_DIR = Path(__file__).resolve().parents[1]
REPO_DIR = PLUGIN_DIR.parents[1]
MAIN_FILE = PLUGIN_DIR / "__init__.py"
PACKAGE_FILE = REPO_DIR / "package.v2.json"

SOURCE = MAIN_FILE.read_text(encoding="utf-8")
TREE = ast.parse(SOURCE)


class StructureTest(unittest.TestCase):

    def test_single_plugin_class(self):
        classes = [n for n in ast.walk(TREE)
                   if isinstance(n, ast.ClassDef) and any(
                       isinstance(b, ast.Name) and b.id == "_PluginBase"
                       for b in n.bases)]
        self.assertEqual([c.name for c in classes], ["OguraTopicRouter"])

    def test_meta_fields_present(self):
        for field in ["plugin_name", "plugin_desc", "plugin_icon",
                      "plugin_version", "plugin_author", "plugin_config_prefix",
                      "plugin_order", "auth_level"]:
            self.assertRegex(
                SOURCE, rf"{field}\s*=\s*", msg=f"缺少插件元信息 {field}")

    def test_no_shell_out(self):
        """安全扫描：不允许执行外部命令/eval"""
        for bad in ["os.system", "subprocess", "eval(", "exec(", "__import__("]:
            self.assertNotIn(bad, SOURCE, msg=f"出现危险调用 {bad}")

    def test_patch_points_are_guarded(self):
        """所有 setattr 补丁必须包在 try/except 里（_patch_one 内部）"""
        self.assertIn("def _patch_one", SOURCE)
        # 三个包装器工厂用字面量标记，两个 key 工厂用变量标记
        self.assertIn('wrapper.__otr_patched__ = "chain_post"', SOURCE)
        self.assertIn('wrapper.__otr_patched__ = "chain_norm"', SOURCE)
        self.assertIn("__otr_patched__ = key", SOURCE)  # 入口工厂+注入器工厂
        self.assertIn('wrapper.__otr_patched__ = "tg_request"', SOURCE)
        # 三个模块入口共用入口工厂，各自独立标记
        self.assertIn('_make_tg_entry_wrapper(key)', SOURCE)
        self.assertIn('("tg_module", "post_message")', SOURCE)
        self.assertIn('("tg_module_medias", "post_medias_message")', SOURCE)
        self.assertIn('("tg_module_torrents", "post_torrents_message")', SOURCE)

    def test_scope_guards_present(self):
        """铁律守卫必须存在：私聊/交互/无标记/未启用"""
        self.assertIn("if getattr(message, \"userid\", None):", SOURCE)
        self.assertIn("getattr(message, \"force_reply\", False)", SOURCE)
        self.assertIn("if not self._enabled or message is None:", SOURCE)
        self.assertIn("if not owner:\n            return None", SOURCE)


class VersionSyncTest(unittest.TestCase):

    def test_package_v2_entry_exists(self):
        data = json.loads(PACKAGE_FILE.read_text(encoding="utf-8"))
        self.assertIn("OguraTopicRouter", data)
        entry = data["OguraTopicRouter"]
        self.assertEqual(entry["name"], "小仓酱的消息话题路由")
        self.assertIn("version", entry)
        self.assertIn("history", entry)

    def test_version_consistent(self):
        m = re.search(r'PLUGIN_VERSION\s*=\s*"([^"]+)"', SOURCE)
        plugin_version = m.group(1)
        data = json.loads(PACKAGE_FILE.read_text(encoding="utf-8"))
        self.assertEqual(data["OguraTopicRouter"]["version"], plugin_version)


class PageContractTest(unittest.TestCase):

    def test_page_apis_use_bear_auth(self):
        """页面按钮 API 必须用登录态鉴权（verify_apikey 不认前端 Bearer，会 401 静默失败）"""
        self.assertEqual(SOURCE.count('"auth": "bear"'), 4)

    def test_page_buttons_no_token_param(self):
        """bear 模式下前端自动带 Authorization，无需 token/apikey 参数"""
        self.assertNotIn("settings.API_TOKEN", SOURCE)

    def test_api_paths_prefixed_by_plugin_id_automatically(self):
        """get_api 返回的 path 不应自带插件ID前缀（MP 会自动加）"""
        self.assertNotIn('"/OguraTopicRouter/', SOURCE)


if __name__ == "__main__":
    unittest.main()
