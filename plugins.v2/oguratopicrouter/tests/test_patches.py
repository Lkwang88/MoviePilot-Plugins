# -*- coding: utf-8 -*-
"""补丁行为测试：归属、传递、注入、监视、幂等、降级、端到端"""
import copy
import queue
import sys
import threading
import unittest
from types import SimpleNamespace

from fake_mp import (
    FakeBot, FakeChainBase, FakeNotification, FakePluginBase,
    FakeTelegramClient, FakeTelegramModule, install_fake_mp,
    install_fake_tg_module,
)
from fake_helpers import build_router, make_message

install_fake_mp()
import oguratopicrouter as otr  # noqa: E402
from oguratopicrouter import MARKER, OguraTopicRouter  # noqa: E402

_G = install_fake_tg_module()  # (restore, TelegramModule, Telegram)


def full_router(config=None, group="-100123", routes="SeedWatch=11\ntype:Plugin=99"):
    """带真实补丁安装的完整路由实例（挂在假 TG 类上）"""
    cfg = {
        "enabled": True, "group_id": group, "routes": routes,
        "default_thread_id": "55",
    }
    if config:
        cfg.update(config)
    r = otr.OguraTopicRouter.__new__(otr.OguraTopicRouter)
    FakePluginBase.__init__(r)
    import threading
    r._lock = threading.Lock()
    r._log_lock = threading.Lock()
    r._route_log = []
    r._last_log_flush = 0.0
    # 保留真实 _install_patches
    r.init_plugin(dict(cfg))
    r.update_config(dict(cfg))
    return r


def fresh_client(default_chat="-100999"):
    """用「已被补丁的当前 TG 类」（sys.modules 里的）造客户端，
    镜像真实 MP：类被补丁后实例调用才有注入行为"""
    import sys as _sys
    tg_pkg = _sys.modules.get("app.modules.telegram")
    client_cls = tg_pkg.Telegram if tg_pkg else FakeTelegramClient
    module_cls = tg_pkg.TelegramModule if tg_pkg else FakeTelegramModule
    bot = FakeBot()
    client = client_cls(bot, default_chat_id=default_chat)
    module = module_cls(client)
    return bot, client, module


class FakeSeedWatchPlugin(FakePluginBase):
    """模拟一个普通插件（类名=插件ID）：拥有真实调用链方法"""

    def post_message(self, channel=None, mtype=None, title=None, text=None, **kwargs):
        # 镜像 _PluginBase.post_message：构造 Notification → self.chain.post_message
        n = FakeNotification(channel=channel, mtype=mtype, title=title,
                             text=text, **kwargs)
        self.chain.post_message(message=n)


class PatchInstallTest(unittest.TestCase):

    def test_install_all_patches(self):
        r = full_router()
        status = r._patch_status
        self.assertTrue(all(status.values()), msg=f"补丁状态：{status}")
        self.assertGreaterEqual(len(status), 5)

    def test_idempotent_no_double_wrap(self):
        """重复 init 不会二次包装：原函数只被包一层"""
        r = full_router()
        import sys as _sys
        tg_cls = _sys.modules["app.modules.telegram"].Telegram
        first = tg_cls._Telegram__send_short_message
        r2 = full_router()
        second = tg_cls._Telegram__send_short_message
        self.assertIs(first, second)  # 幂等：对象不变
        self.assertEqual(getattr(second, "__otr_patched__", None), "tg_short")

    def test_import_failure_degrades(self):
        """TG 模块导入失败：全部补丁放弃挂载，不留半成品"""
        saved = {k: sys.modules.pop(k) for k in list(sys.modules)
                 if k.startswith("app.modules")}
        try:
            r = otr.OguraTopicRouter.__new__(otr.OguraTopicRouter)
            FakePluginBase.__init__(r)
            status = r._install_patches()
            self.assertFalse(any(status.values()))
        finally:
            sys.modules.update(saved)


class OwnerMarkTest(unittest.TestCase):

    def _chain(self):
        return FakeChainBase()

    def test_plugin_caller_marked(self):
        r = full_router()
        plugin = FakeSeedWatchPlugin()
        msg = FakeNotification(mtype=otr.NotificationType.Plugin)
        chain = FakeChainBase()
        plugin.chain = chain
        # 真实链路：插件方法（self=插件）→ chain.post_message → 包装器
        plugin.post_message(channel=None, mtype=otr.NotificationType.Plugin,
                            title="t", text="x")
        # 插件的 post_message 调 self.chain.post_message(Notification(...))
        marked = getattr(chain._last_message, MARKER, None)
        self.assertEqual(marked, "FakeSeedWatchPlugin")

    def test_system_caller_unmarked(self):
        r = full_router()
        msg = FakeNotification(mtype=otr.NotificationType.Plugin)
        chain = self._chain()
        # 顶层调用，栈里没有插件实例
        chain.post_message(message=msg)
        self.assertIsNone(getattr(msg, MARKER, None))

    def test_disabled_no_mark(self):
        r = full_router({"enabled": False})
        plugin = FakeSeedWatchPlugin()
        msg = FakeNotification()
        plugin.chain = self._chain()
        plugin.chain.post_message(message=msg)
        self.assertIsNone(getattr(msg, MARKER, None))

    def test_normalize_propagates_marker(self):
        r = full_router()
        src = make_message(owner="SeedWatch")
        chain = self._chain()
        dispatch = chain._normalize_notification_for_dispatch(src)
        self.assertEqual(getattr(dispatch, MARKER, None), "SeedWatch")

    def test_normalize_without_marker_clean(self):
        r = full_router()
        src = FakeNotification()
        chain = self._chain()
        dispatch = chain._normalize_notification_for_dispatch(src)
        self.assertIsNone(getattr(dispatch, MARKER, None))


class InjectTest(unittest.TestCase):

    def _setup(self, routes="SeedWatch=11\ntype:Plugin=99", **cfg):
        cfg.setdefault("group_id", "-100999")  # 默认与假客户端默认群一致
        r = full_router(cfg, routes=routes)
        bot, client, module = fresh_client()
        return r, bot, client, module

    def _dispatch(self, module, message):
        """模拟 MP 队列消费端：run_module → 模块 post_message"""
        module.post_message(message=message)

    def test_injection_hits_topic(self):
        r, bot, client, module = self._setup()
        msg = make_message(owner="SeedWatch")
        self._dispatch(module, msg)
        self.assertEqual(len(bot.calls), 1, "必须只发一次，不重复")
        kind, chat_id, text, kwargs = bot.calls[0]
        self.assertEqual(kwargs.get("message_thread_id"), 11)
        self.assertEqual(chat_id, "-100999")  # 群ID=MP默认目标群（假客户端）

    def test_no_rule_no_injection(self):
        r, bot, client, module = self._setup(routes="", default_thread_id="")
        msg = make_message(owner="UnkonwnPlugin")
        self._dispatch(module, msg)
        self.assertEqual(len(bot.calls), 1)
        self.assertNotIn("message_thread_id", bot.calls[0][3])

    def test_private_chat_no_injection(self):
        r, bot, client, module = self._setup()
        msg = make_message(owner="SeedWatch", userid="86023")
        self._dispatch(module, msg)
        kind, chat_id, text, kwargs = bot.calls[0]
        self.assertEqual(chat_id, "86023")
        self.assertNotIn("message_thread_id", kwargs)

    def test_type_fallback_route(self):
        r, bot, client, module = self._setup()
        msg = make_message(owner="Whatever", mtype="Plugin")
        self._dispatch(module, msg)
        self.assertEqual(bot.calls[0][3].get("message_thread_id"), 99)

    def test_chat_mismatch_no_injection(self):
        """目标群不是配置的话题群时绝不注入"""
        r, bot, client, module = self._setup(group_id="-100123")
        msg = make_message(owner="SeedWatch")
        self._dispatch(module, msg)
        # 注意：消息应该到了 -100999（原生行为），thread 注入被群匹配拦下
        kind, chat_id, text, kwargs = bot.calls[0]
        self.assertNotIn("message_thread_id", kwargs)

    def test_group_match_by_config(self):
        """群ID配成客户端默认群时注入成功"""
        r, bot, client, module = self._setup(group_id="-100999")
        msg = make_message(owner="SeedWatch")
        self._dispatch(module, msg)
        self.assertEqual(bot.calls[0][3].get("message_thread_id"), 11)

    def test_long_message_path_injects(self):
        r, bot, client, module = self._setup(group="-100999")
        msg = make_message(owner="SeedWatch")
        msg.text = "长" * 5000
        self._dispatch(module, msg)
        kind, chat_id, text, kwargs = bot.calls[0]
        self.assertEqual(kwargs.get("message_thread_id"), 11)

    def test_thread_context_cleared_after(self):
        r, bot, client, module = self._setup()
        self._dispatch(module, make_message(owner="SeedWatch"))
        self.assertIsNone(getattr(otr._TLS, "tid", None))

    def test_marker_survives_thread_hop(self):
        """标记必须跨线程存活（真实 MP 消息队列在独立线程消费）"""
        r, bot, client, module = self._setup(group="-100999")
        msg = make_message(owner="SeedWatch")
        q = queue.Queue()
        q.put({"args": ("post_message",), "kwargs": {"message": msg}})
        errors = []

        def consumer():
            try:
                item = q.get()
                module.post_message(message=item["kwargs"]["message"])
            except Exception as e:  # pragma: no cover
                errors.append(e)

        t = threading.Thread(target=consumer)
        t.start()
        t.join(5)
        self.assertFalse(errors)
        self.assertEqual(bot.calls[0][3].get("message_thread_id"), 11)


class SendMonitorTest(unittest.TestCase):

    def test_failure_logged_with_hint(self):
        r = full_router()
        tg_cls = _G[2]
        client = SimpleNamespace()
        # 让 __send_request 原函数返回 None（模拟发送失败）
        client._Telegram__send_request = lambda *a, **k: None
        # 手动套监视器（镜像补丁行为）
        wrapper = otr._wrap_send_request_monitor(
            lambda inst, *a, **k: inst._Telegram__send_request(*a, **k)
        )
        otr._TLS.tid = 11
        otr._TLS.owner = "SeedWatch"
        try:
            ret = wrapper(client)
        finally:
            otr._TLS.tid = None
            otr._TLS.owner = None
        self.assertIsNone(ret)
        # 验证路由日志里有失败记录（页面与日志都会显示）
        failed = [e for e in r._route_log if e["result"] == "发送失败"]
        self.assertEqual(len(failed), 1)


if __name__ == "__main__":
    unittest.main()
