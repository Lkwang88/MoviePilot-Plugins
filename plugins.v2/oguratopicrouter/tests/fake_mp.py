# -*- coding: utf-8 -*-
"""
OguraTopicRouter 测试共享夹具：用假 MP 模块替换 sys.modules，
让插件可以脱离 MoviePilot 宿主独立加载与测试。
"""
import copy
import logging
import sys
import types
from datetime import datetime
from enum import Enum
from types import SimpleNamespace


# ---------------------------------------------------------------------- 假 MP 构件
class FakeChainBase:
    """假 ChainBase：镜像 MP v2 的 post_message 主干，供补丁替换"""

    def post_message(self, message=None, meta=None, mediainfo=None,
                     torrentinfo=None, transferinfo=None, **kwargs):
        # 镜像 MP：规范化（deepcopy）→ 交给模块分发
        self._last_message = message
        dispatch = self._normalize_notification_for_dispatch(message)
        self._last_dispatch = dispatch
        run = getattr(self, "run_module", None)
        if run is not None and dispatch is not None:
            run("post_message", message=dispatch)
        return dispatch

    @staticmethod
    def _normalize_notification_for_dispatch(message):
        return copy.deepcopy(message) if message is not None else None


class FakePluginBase:
    """假 _PluginBase：内存版数据/配置存储"""

    _store: dict = {}

    def __init__(self):
        self.chain = None

    # ---- _PluginBase 数据接口（内存实现）
    def get_data(self, key=None, plugin_id=None):
        return type(self)._store.get(f"data:{key}")

    def save_data(self, key, value, plugin_id=None):
        type(self)._store[f"data:{key}"] = copy.deepcopy(value)

    def get_config(self, plugin_id=None):
        return type(self)._store.get("_config")

    def update_config(self, config: dict, plugin_id=None):
        type(self)._store["_config"] = copy.deepcopy(config)


class FakeNotification:
    """假 Notification：够用的字段容器（deepcopy 安全）"""

    def __init__(self, **kwargs):
        defaults = dict(
            channel=None, source=None, mtype=None, title=None, text=None,
            image=None, link=None, userid=None, username=None, targets=None,
            buttons=None, force_reply=False, save_history=True,
            original_message_id=None, original_chat_id=None, parse_mode=None,
        )
        defaults.update(kwargs)
        self.__dict__.update(defaults)

    def model_dump(self, **kwargs):
        return dict(self.__dict__)


class FakeResponse:
    def __init__(self, success=True, message=""):
        self.success = success
        self.message = message


class FakeMessageChannel(Enum):
    Telegram = "telegram"
    Wechat = "wechat"


class FakeNotificationType(Enum):
    Plugin = "插件"
    Download = "资源下载"
    Organize = "整理入库"
    Subscribe = "订阅"
    Manual = "手动处理"
    Other = "其它"


def _capture_logger(name="mp-fake"):
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    if not logger.handlers:
        class _ListHandler(logging.Handler):
            records = []

            def emit(self, record):
                self.records.append(record.getMessage())

        handler = _ListHandler()
        logger.addHandler(handler)
        logger._list_handler = handler
    return logger


def make_fake_settings():
    return SimpleNamespace(API_TOKEN="test-token", PROXY="")


# ---------------------------------------------------------------------- 假 TG 模块
class FakeBot:
    """假 telebot Bot：记录 send_message/send_photo 调用与注册的处理器"""

    def __init__(self):
        self.calls = []
        self.handlers = []

    def send_message(self, chat_id=None, text=None, **kwargs):
        self.calls.append(("send_message", chat_id, text, kwargs))
        return SimpleNamespace(message_id=len(self.calls))

    def send_photo(self, photo=None, caption=None, **kwargs):
        self.calls.append(("send_photo", chat_id_of(photo), caption, kwargs))
        return SimpleNamespace(message_id=len(self.calls))

    def send_document(self, document=None, **kwargs):
        self.calls.append(("send_document", chat_id_of(document), None, kwargs))
        return SimpleNamespace(message_id=len(self.calls))

    def register_message_handler(self, handler, func=None, content_types=None):
        self.handlers.append(handler)


def chat_id_of(*_args):
    return None


def register_fake_installed(plugin_id: str, plugin_name: str):
    """往假 PluginManager 注册一个已安装插件类（供配置页动态清单测试）"""
    import sys as _sys
    pm = _sys.modules.get("app.core.plugin")
    if pm is None:
        return None
    cls = type(plugin_id, (), {"plugin_name": plugin_name})
    pm.PluginManager._plugins[plugin_id] = cls
    return cls


def clear_fake_installed():
    import sys as _sys
    pm = _sys.modules.get("app.core.plugin")
    if pm is not None:
        pm.PluginManager._plugins.clear()


class FakeModuleManager:
    """假 ModuleManager：_running_modules 可由测试注入"""
    running: dict = {}

    def __init__(self):
        self._running_modules = FakeModuleManager.running


class FakeTelegramClient:
    """假 TG 客户端：完全镜像 MP v2 的内部调用链

    send_msg → __send_request（组 kwargs：chat_id/parse_mode/reply_markup）
             → __send_short_message / __send_long_plain_message / __send_long_message
             → self._bot.send_message(...)
    """

    def __init__(self, bot, default_chat_id="-100999", token="fake-token"):
        self._bot = bot
        self._telegram_chat_id = default_chat_id
        self._telegram_token = token

    # 对齐 MP：send_msg 组装 caption 后交给 __send_request
    def send_msg(self, title, text=None, image=None, userid=None, link=None,
                 buttons=None, force_reply=False, original_message_id=None,
                 original_chat_id=None, disable_web_page_preview=None,
                 stop_typing=False, parse_mode=None):
        if original_message_id and original_chat_id:
            return self._Telegram__send_request(
                userid=original_chat_id, image=image,
                caption=f"{title}\n{text}", parse_mode=parse_mode,
                reply_to_message_id=original_message_id,
            )
        chat_id = userid or self._telegram_chat_id
        caption = f"{title}\n{text}" if text else title
        return self._Telegram__send_request(
            userid=chat_id, image=image, caption=caption, parse_mode=parse_mode,
        )

    def _Telegram__send_request(self, userid=None, image="", caption="",
                                reply_markup=None, disable_web_page_preview=None,
                                parse_mode=None, reply_to_message_id=None):
        kwargs = {
            "chat_id": userid or self._telegram_chat_id,
            "parse_mode": parse_mode,
            "reply_markup": reply_markup,
        }
        if reply_to_message_id:
            kwargs["reply_to_message_id"] = reply_to_message_id
        if len(caption) < 4096:
            return self._Telegram__send_short_message(
                image, caption,
                disable_web_page_preview=disable_web_page_preview, **kwargs,
            )
        return self._Telegram__send_long_plain_message(
            image, caption, 4096,
            disable_web_page_preview=disable_web_page_preview, **kwargs,
        )

    def _Telegram__send_short_message(self, image=None, caption="",
                                      disable_web_page_preview=None, **kwargs):
        if image:
            return self._bot.send_photo(photo=image, caption=caption, **kwargs)
        return self._bot.send_message(
            text=caption,
            disable_web_page_preview=disable_web_page_preview, **kwargs,
        )

    def _Telegram__send_long_plain_message(self, image=None, caption="",
                                           caption_limit=4096,
                                           disable_web_page_preview=None, **kwargs):
        reply_markup = kwargs.pop("reply_markup", None)
        return self._bot.send_message(
            **kwargs, text=caption[:caption_limit], reply_markup=reply_markup,
        )

    def _Telegram__send_long_message(self, image=None, caption="", sent_idx=None,
                                     disable_web_page_preview=None, **kwargs):
        kwargs.pop("reply_markup", None)
        return self._bot.send_document(document=None, **kwargs)


class FakeTelegramModule:
    """假 TelegramModule：镜像 MP v2 的 post_message 目标解析"""

    def __init__(self, client):
        self.client = client
        self._channel = FakeMessageChannel.Telegram

    def get_configs(self):
        return {"Telegram": SimpleNamespace(name="Telegram", enabled=True)}

    def get_instance(self, name):
        return self.client

    def check_message(self, message, source=None):
        return True

    def post_message(self, message, **kwargs):
        if not self.check_message(message):
            return
        userid = message.userid
        targets = message.targets
        if not userid and targets is not None:
            userid = targets.get("telegram_userid")
            if not userid:
                return
        self.client.send_msg(
            title=message.title, text=message.text, image=message.image,
            userid=userid, link=message.link, buttons=message.buttons,
            force_reply=message.force_reply,
            original_message_id=message.original_message_id,
            original_chat_id=message.original_chat_id,
            parse_mode=message.parse_mode,
        )

    def post_medias_message(self, message, medias):
        self.client.send_msg(title=message.title or "媒体列表")

    def post_torrents_message(self, message, torrents):
        self.client.send_msg(title=message.title or "种子列表")


# ---------------------------------------------------------------------- 环境安装
def install_fake_mp():
    """
    把假 MP 模块塞进 sys.modules，返回 restore()。
    幂等：已安装时直接返回空 restore，避免插件模块与假环境错位。
    必须在 import 插件模块之前调用。
    """
    if sys.modules.get("app") is not None and hasattr(
            sys.modules.get("app"), "_otr_fake"):
        return lambda: None

    app_module = types.ModuleType("app")
    app_module.__path__ = []
    app_module._otr_fake = True

    chain_module = types.ModuleType("app.chain")
    chain_module.ChainBase = FakeChainBase
    sys.modules["app.chain"] = chain_module
    app_module.chain = chain_module

    config_module = types.ModuleType("app.core.config")
    config_module.settings = make_fake_settings()
    sys.modules["app.core.config"] = config_module

    log_module = types.ModuleType("app.log")
    log_module.logger = _capture_logger()
    sys.modules["app.log"] = log_module

    core_pkg = types.ModuleType("app.core")
    core_pkg.__path__ = []
    sys.modules["app.core"] = core_pkg
    core_pkg.config = config_module

    # 假 ModuleManager（插件通过 ModuleManager()._running_modules 找 TG 模块）
    module_module = types.ModuleType("app.core.module")
    module_module.ModuleManager = FakeModuleManager
    sys.modules["app.core.module"] = module_module
    core_pkg.module = module_module

    # 假 PluginManager（插件配置页动态枚举已安装插件用）
    plugin_module = types.ModuleType("app.core.plugin")

    class FakePluginManager:
        # {插件ID: 类(需带 plugin_name 属性)}，测试用 register_fake_installed 注入
        _plugins = {}

        def __init__(self):
            self.plugins = type(self)._plugins

    plugin_module.PluginManager = FakePluginManager
    sys.modules["app.core.plugin"] = plugin_module
    core_pkg.plugin = plugin_module

    plugins_module = types.ModuleType("app.plugins")
    plugins_module._PluginBase = FakePluginBase
    sys.modules["app.plugins"] = plugins_module
    app_module.plugins = plugins_module

    schemas_module = types.ModuleType("app.schemas")
    schemas_module.Notification = FakeNotification
    schemas_module.Response = FakeResponse
    sys.modules["app.schemas"] = schemas_module
    app_module.schemas = schemas_module

    schemas_types_module = types.ModuleType("app.schemas.types")
    schemas_types_module.MessageChannel = FakeMessageChannel
    schemas_types_module.NotificationType = FakeNotificationType
    sys.modules["app.schemas.types"] = schemas_types_module
    schemas_module.types = schemas_types_module
    schemas_pkg = types.ModuleType("app.schemas_pkg")
    # 让 app.schemas.types 解析为子模块
    schemas_module.__path__ = []

    # 假 PluginManager（插件配置页动态枚举已安装插件用）
    plugin_module = types.ModuleType("app.core.plugin")

    class FakePluginManager:
        # {插件ID: 类(需带 plugin_name 属性)}
        _plugins = {}

        def __init__(self):
            self.plugins = type(self)._plugins

    plugin_module.PluginManager = FakePluginManager
    sys.modules["app.core.plugin"] = plugin_module
    core_pkg.plugin = plugin_module

    sys.modules["app"] = app_module

    def restore():
        for name in [
            "app", "app.chain", "app.core", "app.core.config", "app.core.module",
            "app.core.plugin", "app.log",
            "app.plugins", "app.schemas", "app.schemas.types",
        ]:
            sys.modules.pop(name, None)

    return restore


def install_fake_tg_module():
    """安装假的 app.modules.telegram（供 _install_patches 导入）。
    幂等：只安装一次，后续调用返回已安装的类，保证补丁目标与测试实例一致。
    直接复用带方法镜像的 FakeTelegramModule / FakeTelegramClient。"""
    existing = sys.modules.get("app.modules.telegram")
    if existing is not None and getattr(existing, "_otr_fake", False):
        return (lambda: None), existing.TelegramModule, existing.Telegram

    tg_pkg = types.ModuleType("app.modules")
    tg_pkg.__path__ = []
    telegram_pkg = types.ModuleType("app.modules.telegram")
    telegram_pkg.__path__ = []
    telegram_pkg._otr_fake = True

    # 动态生成全新子类（带方法镜像；首次生成后固定复用）
    module_cls = type("TelegramModule", (FakeTelegramModule,), {})
    client_cls = type("Telegram", (FakeTelegramClient,), {})
    telegram_pkg.TelegramModule = module_cls
    telegram_pkg.Telegram = client_cls
    telegram_module_file = types.ModuleType("app.modules.telegram.telegram")
    telegram_module_file.Telegram = client_cls
    sys.modules["app.modules"] = tg_pkg
    sys.modules["app.modules.telegram"] = telegram_pkg
    sys.modules["app.modules.telegram.telegram"] = telegram_module_file

    def restore():
        for name in [
            "app.modules", "app.modules.telegram", "app.modules.telegram.telegram",
        ]:
            sys.modules.pop(name, None)

    return restore, module_cls, client_cls


def make_topic_message(chat_id=-100123, thread_id=77, text="hi",
                       topic_name="种子话题"):
    """造一条假 telebot 群消息"""
    msg = SimpleNamespace()
    msg.chat = SimpleNamespace(id=chat_id)
    msg.message_thread_id = thread_id
    msg.text = text
    if topic_name:
        msg.forum_topic_created = SimpleNamespace(name=topic_name)
    else:
        msg.forum_topic_created = None
    return msg
