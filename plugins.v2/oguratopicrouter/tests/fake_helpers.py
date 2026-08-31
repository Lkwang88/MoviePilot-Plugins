# -*- coding: utf-8 -*-
"""测试辅助：构建路由插件实例与假消息"""
from types import SimpleNamespace

from fake_mp import (
    FakeNotification, FakePluginBase, install_fake_mp,
)

# 确保插件模块已可导入（fake_mp 装载假环境）
install_fake_mp()
import oguratopicrouter as otr  # noqa: E402


def build_router(config: dict, plugin_id: str = "TestPlugin") -> otr.OguraTopicRouter:
    """
    构建一个跳过真实补丁安装的路由插件实例：
    init_plugin 走完整配置解析（含报错收集），但 _install_patches 被替换为全成功假象。
    """
    FakePluginBase._store.clear()  # 测试隔离：清空类级存储
    r = otr.OguraTopicRouter.__new__(otr.OguraTopicRouter)
    FakePluginBase.__init__(r)
    import threading
    r._lock = threading.Lock()
    r._log_lock = threading.Lock()
    r._route_log = []
    r._patch_status = {}
    r._last_log_flush = 0.0
    patches = {"消息归属": True, "标记传递": True, "模块入口": True,
               "底层发送注入": True, "发送失败监视": True}
    r._install_patches = lambda: dict(patches)
    r.init_plugin(dict(config))
    # 镜像 MP 行为：配置保存后才会带配置 init_plugin
    r.update_config(dict(config))
    return r


_NT = otr.NotificationType


def make_message(owner=None, mtype=None, userid=None, buttons=None,
                 force_reply=False, **extra):
    """造一条假 Notification；owner 非 None 时打归属标记"""
    mtype_map = {
        "Plugin": _NT.Plugin,
        "Download": _NT.Download,
        "Organize": _NT.Organize,
        "Subscribe": _NT.Subscribe,
        "Manual": _NT.Manual,
    }
    msg = FakeNotification(
        mtype=mtype_map.get(mtype, _NT.Plugin),
        userid=userid, buttons=buttons, force_reply=force_reply,
        title="t", text="x",
    )
    if owner:
        object.__setattr__(msg, otr.MARKER, owner)
    for k, v in extra.items():
        setattr(msg, k, v)
    return msg


def make_tg_msg(chat_id=-100123, thread_id=77, text="hi", topic_name="种子话题"):
    """造一条假 telebot 群消息（供话题记录测试）"""
    msg = SimpleNamespace()
    msg.chat = SimpleNamespace(id=chat_id)
    msg.message_thread_id = thread_id
    msg.text = text
    msg.forum_topic_created = (
        SimpleNamespace(name=topic_name) if topic_name else None
    )
    return msg
