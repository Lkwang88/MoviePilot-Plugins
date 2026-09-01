# -*- coding: utf-8 -*-
"""
小仓酱的消息话题路由 (MoviePilot V2)
====================================

把 MP 里「插件发出的通知」重定向到 Telegram 群组的话题（Topics）里按插件隔离显示。
不改任何一个插件、不改 MP 核心，只在本插件内挂三个受保护的运行时补丁。

原理（任何一环失效自动回退原生行为，绝不丢消息）：
1. 归属：包装 ChainBase.post_message，用调用栈帧识别消息来自哪个插件实例，
   把插件 ID 写进消息对象的私有属性（不碰任何现有字段，不影响入库/渠道过滤）
2. 传递：包装 ChainBase._normalize_notification_for_dispatch，
   确保标记在 MP 内部 deepcopy 后仍然存活
3. 注入：包装 Telegram 模块 post_message 与底层 send 系列，
   命中路由规则且目标群是话题群时，给 Telegram API 加 message_thread_id 参数

范围（铁律）：
- 只拦截「插件发出的消息」；MP 系统消息（整理/订阅/下载等）不经过归属识别，
  物理隔离，绝不触碰
- 定向私聊消息（带 userid）不进话题；带按钮/强制回复的交互消息不进话题
- 未命中任何规则的消息保持原样（发到群 General / 原目标）
- 通知不重复发送：是「改道」不是「复制」

话题 ID 获取：机器人收到的每条群消息都会被动记录其话题信息（不主动调
getUpdates，与 MP 自身的轮询零冲突），详情页「扫描话题」即可查看清单；
机器人看不到消息的话题，可转发消息给 @getidsbot 查询。
"""
import re
import sys
import threading
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

# MP 宿主导入（测试环境缺失时走兜底，保证本包任何地方都能 import）
try:
    from app.chain import ChainBase
    from app.core.config import settings
    from app.core.plugin import PluginManager
    from app.log import logger
    from app.plugins import _PluginBase
    from app.schemas import Notification, Response
    from app.schemas.types import MessageChannel, NotificationType
except Exception:  # pragma: no cover - 本地单元测试环境无 MP 宿主
    ChainBase = type("ChainBase", (), {})
    settings = None
    PluginManager = None

    class _FallbackLogger:
        @staticmethod
        def _fmt(*args):
            return " ".join(str(a) for a in args)

        def debug(self, *a, **k):
            pass

        def info(self, *a, **k):
            print("[INFO]", self._fmt(*a))

        def warning(self, *a, **k):
            print("[WARN]", self._fmt(*a))

        def error(self, *a, **k):
            print("[ERROR]", self._fmt(*a))

    logger = _FallbackLogger()

    class _PluginBase:
        def __init__(self):
            self.chain = None

        def get_data(self, key=None, plugin_id=None):
            return None

        def save_data(self, key, value, plugin_id=None):
            pass

        def get_config(self, plugin_id=None):
            return None

        def update_config(self, config: dict, plugin_id=None):
            return True

    class Notification:
        def __init__(self, **kwargs):
            self.__dict__.update(kwargs)

    class Response:
        def __init__(self, success=True, message=""):
            self.success = success
            self.message = message

    from enum import Enum as _Enum

    class MessageChannel(_Enum):
        Telegram = "telegram"

    class NotificationType(_Enum):
        Plugin = "插件"

# 版本
PLUGIN_VERSION = "1.5"

# 消息对象上的归属标记（私有属性，Pydantic 字段之外，不参与序列化/入库）
MARKER = "_otr_owner"

# 线程上下文：路由命中后在本线程内传递话题 ID（队列消费线程内设置与消费）。
# 注意：包装器在 MP 进程内只会安装一次（插件热重载不会二次包装），因此
# _TLS 及包装器闭包都来自首次加载的模块；跨重载共享的状态一律挂在
# ChainBase 类属性上（app.chain 不会被重载，类对象恒定）。
_TLS = threading.local()


def _get_router():
    """拿当前运行的路由插件实例（挂在 ChainBase 类属性上，跨插件热重载稳定）"""
    return getattr(ChainBase, "_otr_router_instance", None)


def _topics_registry() -> Tuple[Dict[str, Dict[str, Any]], threading.Lock]:
    """话题记录注册表（挂在 ChainBase 类属性上，跨热重载稳定）"""
    if not hasattr(ChainBase, "_otr_topics"):
        ChainBase._otr_topics = {}
        ChainBase._otr_topics_lock = threading.Lock()
    return ChainBase._otr_topics, ChainBase._otr_topics_lock


# 原始函数仓库：key -> (目标类, 属性名, 原始函数)。只在首次安装时捕获一次，
# 插件热重载（MP 重新 import 本模块）后也不会二次包装。
_ORIGINALS: Dict[str, Tuple[type, str, Any]] = {}

# 机器人事件处理器的内容类型清单（含话题服务消息）
_HANDLER_CONTENT_TYPES = [
    "text", "photo", "document", "video", "video_note", "voice", "audio",
    "sticker", "location", "contact", "pinned_message", "new_chat_members",
    "forum_topic_created", "forum_topic_edited", "forum_topic_closed",
    "forum_topic_reopened", "general_forum_topic_hidden",
    "general_forum_topic_unhidden",
]


def _norm_chat_int(value: Any) -> Optional[int]:
    """把 chat_id/话题 ID 统一成 int，失败返回 None"""
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return None


def _parse_tme_link(text: Any) -> Tuple[Optional[int], Optional[int]]:
    """
    解析 Telegram 链接 → (群chat_id, 话题ID)。
    支持：t.me/c/<chat>/<topic>（话题分享链接，两段式）与
         t.me/c/<chat>/<topic>/<msg>（话题内消息链接，三段式）。
    群ID = -100 前缀 + 第一段；话题ID = 第二段。
    """
    m = re.search(r"t\.me/c/(\d+)(?:/(\d+))?(?:/(\d+))?", str(text or ""))
    if not m:
        return None, None
    try:
        chat_id = int(f"-100{m.group(1)}")
    except ValueError:
        return None, None
    topic_id = int(m.group(2)) if m.group(2) else None
    return chat_id, topic_id


def _norm_topic_value(raw: Any) -> Optional[int]:
    """话题值：支持纯数字，或直接粘贴 t.me 话题链接（自动提取话题ID）"""
    text = str(raw or "").strip()
    if not text:
        return None
    if "t.me/" in text:
        return _parse_tme_link(text)[1]
    return _norm_chat_int(text)


def _find_plugin_owner() -> Optional[str]:
    """
    调用栈帧识别：向上找第一个 self 为插件实例的帧，返回插件 ID（=类名）。
    找到自己（路由插件）则返回 None，避免路由自己的消息造成回环。
    纯只读操作，任何异常都不外抛。
    """
    try:
        frame = sys._getframe(2)
    except ValueError:
        return None
    depth = 0
    while frame is not None and depth < 40:
        try:
            obj = frame.f_locals.get("self")
            if obj is not None:
                cls = obj.__class__
                name = cls.__name__
                if name == "OguraTopicRouter":
                    return None
                if isinstance(obj, _PluginBase):
                    return name
        except Exception:
            pass
        frame = frame.f_back
        depth += 1
    return None


def _record_topic_from_message(msg: Any) -> None:
    """机器人消息处理器：被动记录话题信息（尽力而为，绝不打扰 MP 轮询）"""
    try:
        chat_id = getattr(getattr(msg, "chat", None), "id", None)
        if chat_id is None:
            return
        chat_int = _norm_chat_int(chat_id)
        # 只记录超级群（话题只存在于超级群）
        if chat_int is None or chat_int > 0:
            return
        thread_id = getattr(msg, "message_thread_id", None)
        if thread_id is None:
            # 不记录无话题的普通消息
            return
        title = ""
        topic_created = getattr(msg, "forum_topic_created", None)
        if topic_created is not None:
            title = getattr(topic_created, "name", "") or ""
        text = (getattr(msg, "text", None) or getattr(msg, "caption", None) or "")
        topics, lock = _topics_registry()
        with lock:
            key = f"{chat_int}:{thread_id}"
            old = topics.get(key) or {}
            topics[key] = {
                "chat_id": str(chat_int),
                "thread_id": int(thread_id),
                "title": title or old.get("title") or "",
                "last_text": (text[:40] if text else old.get("last_text") or ""),
                "last_ts": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            }
    except Exception:
        # 处理器运行在 MP 轮询线程里，任何异常都吞掉，绝不影响收消息
        pass


class OguraTopicRouter(_PluginBase):
    """小仓酱的消息话题路由"""

    # 插件元信息
    plugin_name = "小仓酱的消息话题路由"
    plugin_desc = (
        "把插件发出的通知自动改道到 Telegram 群组话题（Topics）里按插件隔离显示。"
        "不改任何插件、不改 MP 核心；系统消息/私聊/交互消息不受影响，未配置规则的插件照常发送。"
    )
    plugin_icon = "https://raw.githubusercontent.com/Lkwang88/MoviePilot-Plugins/main/icons/OguraTopicRouter.png"
    plugin_version = PLUGIN_VERSION
    plugin_author = "Lkwang88"
    author_url = "https://github.com/Lkwang88"
    plugin_config_prefix = "oguratopicrouter."
    plugin_order = 36
    auth_level = 1

    def __init__(self):
        super().__init__()
        self._lock = threading.Lock()
        self._enabled = False
        self._debug = False
        self._group_id = ""
        self._group_int: Optional[int] = None
        self._default_tid: Optional[int] = None
        self._plugin_routes: Dict[str, int] = {}
        self._type_routes: Dict[str, int] = {}
        self._config_errors: List[str] = []
        self._patch_status: Dict[str, bool] = {}
        self._route_log: List[Dict[str, Any]] = []
        self._log_lock = threading.Lock()

    # ------------------------------------------------------------------ 配置
    def init_plugin(self, config: dict = None):
        """生效配置并安装补丁"""
        self._enabled = False
        self._debug = False
        self._group_id = ""
        self._default_tid = None
        self._plugin_routes = {}
        self._type_routes = {}
        self._config_errors = []
        routes_text = ""
        if config:
            self._enabled = bool(config.get("enabled"))
            self._debug = bool(config.get("debug"))
            self._group_id = str(config.get("group_id") or "").strip()
            # 群ID 支持直接粘贴 t.me 链接（群链接或话题链接均可，取群段）
            if "t.me/" in self._group_id:
                gid, _ = _parse_tme_link(self._group_id)
                if gid:
                    logger.info(f"【话题路由】已从链接解析群ID：{gid}")
                    self._group_id = str(gid)
            default_raw = str(config.get("default_thread_id") or "").strip()
            # 默认话题也支持链接
            default_tid = _norm_topic_value(default_raw)
            if default_raw and default_tid is None:
                self._config_errors.append(
                    f"默认话题「{default_raw}」无法识别（填数字或 t.me 话题链接），已忽略"
                )
                default_raw = ""
            if default_tid is not None and default_tid <= 0:
                self._config_errors.append(f"默认话题ID「{default_raw}」应为正整数，已忽略")
                default_tid = None
            self._default_tid = default_tid
            self._group_int = _norm_chat_int(self._group_id)
            if self._group_id and self._group_int is None:
                self._config_errors.append(f"话题群ID「{self._group_id}」不是数字，路由不会生效")
            elif self._group_int is not None and self._group_int > 0:
                self._config_errors.append(
                    f"话题群ID「{self._group_id}」应为负数超级群ID（-100 开头），当前格式不对"
                )
                self._group_int = None

        # 新版配置：每插件一键 route_<插件ID> = 话题ID（配置页动态清单写入）
        for key, raw in (config or {}).items():
            if not key.startswith("route_"):
                continue
            pid = key[6:].strip()
            raw = str(raw or "").strip()
            if not pid or not raw:
                continue  # 空 = 不接管
            tid = _norm_topic_value(raw)
            if tid is None or tid <= 0:
                self._config_errors.append(
                    f"插件 {pid} 的话题「{raw}」无法识别（填数字或 t.me 话题链接），已忽略"
                )
                continue
            self._plugin_routes[pid] = tid

        # 旧版兼容：routes 文本行（v0.1.0 手写格式），只补位不覆盖新版键
        routes_text = str((config or {}).get("routes") or "")
        for idx, line in enumerate(routes_text.splitlines(), start=1):
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" not in line:
                self._config_errors.append(f"路由规则第{idx}行格式错误（缺 =）：「{line}」")
                continue
            key, _, tid_raw = line.partition("=")
            key = key.strip()
            tid = _norm_chat_int(tid_raw)
            if not key:
                self._config_errors.append(f"路由规则第{idx}行缺少插件ID/类型：「{line}」")
                continue
            if tid is None or tid <= 0:
                self._config_errors.append(
                    f"路由规则第{idx}行话题ID「{tid_raw.strip()}」应为正整数：「{line}」"
                )
                continue
            if key.lower().startswith("type:"):
                self._type_routes[key[5:].strip()] = tid
            else:
                # 新版 route_ 键优先，旧文本只补位
                self._plugin_routes.setdefault(key, tid)

        # 装载历史路由日志（详情页展示）
        try:
            saved_log = self.get_data("route_log")
            if isinstance(saved_log, list):
                self._route_log = saved_log
        except Exception:
            self._route_log = []

        # 恢复话题扫描记录
        self._load_topics()

        # 注册为当前实例（挂在 ChainBase 类属性上，包装器跨热重载可取）
        ChainBase._otr_router_instance = self
        # 安补丁（幂等：重复 init 不会二次包装）
        self._patch_status = self._install_patches()
        ok = sum(1 for v in self._patch_status.values() if v)
        state = "已启用" if self._enabled else "未启用"
        logger.info(
            f"【话题路由】插件{state}，补丁 {ok}/{len(self._patch_status)} 挂载成功，"
            f"插件规则 {len(self._plugin_routes)} 条，类型规则 {len(self._type_routes)} 条"
        )
        for err in self._config_errors:
            logger.warning(f"【话题路由】配置问题：{err}")

    @staticmethod
    def get_command() -> list:
        return []

    def get_state(self) -> bool:
        return self._enabled

    def get_service(self) -> List[Dict[str, Any]]:
        """无定时服务"""
        return []

    def stop_service(self):
        """补丁常驻（未启用时包装器自动透传），无需卸载"""
        pass

    # ------------------------------------------------------------------ 补丁
    def _install_patches(self) -> Dict[str, bool]:
        """
        安装补丁点（幂等）。返回 {名称: 是否挂载成功}。
        任一失败只记日志不抛异常，其余补丁照常。
        """
        status = {}
        try:
            from app.modules.telegram import TelegramModule
            from app.modules.telegram.telegram import Telegram
        except Exception as e:
            logger.error(f"【话题路由】导入 MP 模块失败，补丁全部未挂载：{e}")
            return {
                "消息归属": False, "标记传递": False,
                "模块入口": False, "底层发送注入": False, "发送失败监视": False,
            }

        # 1) 归属：ChainBase.post_message
        status["消息归属"] = self._patch_one(
            "chain_post", ChainBase, "post_message", _wrap_chain_post_message
        )
        # 2) 传递：ChainBase._normalize_notification_for_dispatch（静态方法）
        status["标记传递"] = self._patch_one(
            "chain_norm", ChainBase, "_normalize_notification_for_dispatch",
            _wrap_chain_normalize,
        )
        # 3) 模块入口：TelegramModule 三个消息入口（各自独立补丁标记）
        entry_ok = True
        for key, attr in [
            ("tg_module", "post_message"),
            ("tg_module_medias", "post_medias_message"),
            ("tg_module_torrents", "post_torrents_message"),
        ]:
            ok = self._patch_one(key, TelegramModule, attr, _make_tg_entry_wrapper(key))
            entry_ok = entry_ok and ok
        status["模块入口"] = entry_ok
        # 4) 底层发送注入：三个 send 分支都要能透传 message_thread_id
        inject_ok = True
        for key, attr in [
            ("tg_short", "_Telegram__send_short_message"),
            ("tg_long_plain", "_Telegram__send_long_plain_message"),
            ("tg_long", "_Telegram__send_long_message"),
        ]:
            ok = self._patch_one(key, Telegram, attr, _make_send_injector(key))
            inject_ok = inject_ok and ok
        status["底层发送注入"] = inject_ok
        # 5) 发送失败监视：__send_request 返回空时给出配置问题提示
        status["发送失败监视"] = self._patch_one(
            "tg_request", Telegram, "_Telegram__send_request", _wrap_send_request_monitor
        )
        return status

    def _patch_one(self, key: str, target_class: type, attr: str, factory) -> bool:
        """
        打补丁（每次 init 都用最新代码重建，杜绝热更新后旧包装器驻留）：
        - 同类同键已捕获过真身 → 直接复用；
        - 目标是自家旧版包装器（升级/重载场景，标记命中 _KNOWN_KEYS）→
          取其 __otr_original__ 真身重建（旧版无该属性时退化为包一层）；
        - 目标被陌生补丁占用 → 跳过并报错，绝不覆盖别人。
        """
        try:
            current = getattr(target_class, attr, None)
            if current is None:
                logger.error(f"【话题路由】补丁点不存在：{target_class.__name__}.{attr}，跳过")
                return False
            saved = _ORIGINALS.get(key)
            mark = getattr(current, "__otr_patched__", None)
            if saved and saved[0] is target_class and saved[1] == attr:
                original = saved[2]  # 本进程内已捕获过的真身
            elif mark in _KNOWN_KEYS:
                original = getattr(current, "__otr_original__", None) or current
                _ORIGINALS[key] = (target_class, attr, original)
                if getattr(current, "__otr_original__", None) is None:
                    logger.info(
                        f"【话题路由】接管旧版包装器：{attr}（旧版无真身引用，本次嵌套接管；"
                        f"重启 MP 后恢复单层，日志可能重复一次，功能不受影响）"
                    )
                else:
                    logger.info(f"【话题路由】已用最新代码重建包装器：{attr}")
            elif mark:
                logger.error(f"【话题路由】补丁点 {attr} 已被其它补丁占用，跳过")
                return False
            else:
                original = current
                _ORIGINALS[key] = (target_class, attr, current)
            wrapper = factory(original)
            # 保留原描述符类型：staticmethod 换回 staticmethod，否则实例调用
            # 会把 self 塞进第一个参数导致签名错位
            raw = target_class.__dict__.get(attr)
            if isinstance(raw, staticmethod):
                wrapper = staticmethod(wrapper)
            setattr(target_class, attr, wrapper)
            return True
        except Exception as e:
            logger.error(f"【话题路由】安装补丁 {attr} 失败：{e}")
            return False

    # ------------------------------------------------------------------ 路由决策
    def _chat_matches(self, chat_id: Any) -> bool:
        """目标群匹配（容忍 int/str 差异；@频道名 一律不匹配）"""
        if self._group_int is None:
            return False
        cid = _norm_chat_int(chat_id)
        return cid is not None and cid == self._group_int

    def _resolve_thread(self, message) -> Optional[int]:
        """
        决定该消息应发往的话题 ID；返回 None 表示不干预（保持原生行为）。
        只处理带归属标记的插件消息；私聊/交互/未命中规则一律 None。
        """
        if not self._enabled or message is None:
            return None
        try:
            owner = getattr(message, MARKER, None)
        except Exception:
            owner = None
        if not owner:
            return None
        mtype = getattr(message, "mtype", None)
        mtype_name = getattr(mtype, "name", "") if mtype else ""
        configured = owner in self._plugin_routes

        # 铁律一：定向私聊消息不进话题（仅对已配置插件留痕，未配置的完全静默）
        if getattr(message, "userid", None):
            if configured:
                self._log_route(owner, mtype_name, "定向私聊", None, "私聊跳过")
            return None
        # 铁律二：交互消息（按钮/强制回复）不进话题
        if getattr(message, "buttons", None) or getattr(message, "force_reply", False):
            if configured:
                self._log_route(owner, mtype_name, "交互消息", None, "交互跳过")
            return None
        # 话题群未配置/配置错 → 不干预
        if self._group_int is None:
            if configured:
                self._log_route(owner, mtype_name, "群未配置", None, "原样")
            return None

        tid: Optional[int] = None
        rule = ""
        if owner in self._plugin_routes:
            tid = self._plugin_routes[owner]
            rule = f"插件规则:{owner}"
        elif mtype_name and mtype_name in self._type_routes:
            tid = self._type_routes[mtype_name]
            rule = f"类型兜底:{mtype_name}"
        elif self._default_tid:
            tid = self._default_tid
            rule = "默认话题"
        if tid is None:
            # 未命中任何规则 → 原样。未配置的插件零日志零记录（降噪）
            return None

        self._log_route(owner, mtype_name, rule, tid, "改道")
        logger.info(
            f"【话题路由】{owner} 的{mtype_name or '插件'}消息 → 话题 {tid}（{rule}）"
        )
        return tid

    # ------------------------------------------------------------------ 日志与记录
    _ROUTE_LOG_MAX = 200

    def _log_route(self, owner: str, mtype_name: str, rule: str,
                   tid: Optional[int], result: str) -> None:
        """路由日志（环形缓冲 + 持久化），供详情页展示与排查"""
        entry = {
            "ts": datetime.now().strftime("%m-%d %H:%M:%S"),
            "owner": owner,
            "mtype": mtype_name,
            "rule": rule,
            "tid": tid,
            "result": result,
        }
        persist = False
        with self._log_lock:
            self._route_log.insert(0, entry)
            if len(self._route_log) > self._ROUTE_LOG_MAX:
                self._route_log = self._route_log[: self._ROUTE_LOG_MAX]
            # 同一秒内多条只落盘一次，避免高频写盘
            now = time.time()
            if now - getattr(self, "_last_log_flush", 0) > 5 or result == "改道":
                self._last_log_flush = now
                persist = True
            if len(self._route_log) > 0 and persist:
                try:
                    self.save_data("route_log", self._route_log)
                except Exception:
                    pass

    # ------------------------------------------------------------------ 话题记录
    def _load_topics(self) -> None:
        """从插件数据恢复话题扫描记录"""
        try:
            saved = self.get_data("topics_seen")
            if isinstance(saved, dict):
                topics, lock = _topics_registry()
                with lock:
                    for k, v in saved.items():
                        topics.setdefault(k, v)
        except Exception:
            pass

    def _flush_topics(self) -> None:
        """把话题记录落盘"""
        try:
            topics, lock = _topics_registry()
            with lock:
                snapshot = dict(topics)
            self.save_data("topics_seen", snapshot)
        except Exception:
            pass

    def _ensure_topic_handlers(self) -> List[str]:
        """
        给所有已启用的 Telegram 客户端挂被动消息记录处理器（幂等）。
        返回处理结果说明列表。
        """
        notes: List[str] = []
        try:
            from app.core.module import ModuleManager
            tg_module = ModuleManager()._running_modules.get("TelegramModule")
        except Exception as e:
            notes.append(f"读取 Telegram 模块失败：{e}")
            return notes
        if tg_module is None:
            notes.append("Telegram 模块未启用，无法记录话题")
            return notes
        try:
            confs = tg_module.get_configs() or {}
        except Exception as e:
            notes.append(f"读取 Telegram 配置失败：{e}")
            return notes
        if not confs:
            notes.append("未找到已启用的 Telegram 通知配置")
            return notes
        for conf in confs.values():
            try:
                client = tg_module.get_instance(conf.name)
                bot = getattr(client, "_bot", None)
                if bot is None:
                    notes.append(f"配置「{conf.name}」机器人未就绪")
                    continue
                if getattr(bot, "_otr_handler_installed", False):
                    notes.append(f"配置「{conf.name}」记录器已在监听")
                    continue

                def _handler(message, _conf_name=conf.name):
                    _record_topic_from_message(message)

                bot.register_message_handler(
                    _handler, func=lambda m: True,
                    content_types=_HANDLER_CONTENT_TYPES,
                )
                try:
                    object.__setattr__(bot, "_otr_handler_installed", True)
                except Exception:
                    pass
                notes.append(f"配置「{conf.name}」话题记录器已挂载，等待机器人收到群消息")
            except Exception as e:
                notes.append(f"配置「{conf.name}」挂载记录器失败：{e}")
        return notes

    # ------------------------------------------------------------------ API
    def get_api(self) -> List[Dict[str, Any]]:
        return [
            {
                "path": "/scan_topics",
                "endpoint": self._api_scan_topics,
                "methods": ["GET"],
                "summary": "扫描/刷新话题记录",
                "auth": "bear",
            },
            {
                "path": "/test_route",
                "endpoint": self._api_test_route,
                "methods": ["GET"],
                "summary": "向指定插件的路由发送测试通知",
                "auth": "bear",
            },
            {
                "path": "/del_route",
                "endpoint": self._api_del_route,
                "methods": ["GET"],
                "summary": "删除指定插件的路由规则",
                "auth": "bear",
            },
            {
                "path": "/clear_log",
                "endpoint": self._api_clear_log,
                "methods": ["GET"],
                "summary": "清空路由日志",
                "auth": "bear",
            },
        ]

    def _api_scan_topics(self, **kwargs) -> Response:
        notes = self._ensure_topic_handlers()
        self._flush_topics()
        topics, topics_lock = _topics_registry()
        with topics_lock:
            count = len(topics)
        msg = "；".join(notes) if notes else "完成"
        logger.info(f"【话题路由】扫描话题记录：共 {count} 个话题。{msg}")
        return Response(success=True, message=f"已记录 {count} 个话题。{msg}")

    def _api_test_route(self, owner: str = "", **kwargs) -> Response:
        """
        端到端测试：构造一条归属为 owner 的插件通知，走真实的完整消息链。
        页面点击后请到 Telegram 对应话题查看是否收到。
        """
        owner = (owner or "").strip()
        if not self._enabled:
            return Response(success=False, message="插件未启用，请先在配置里打开开关")
        if not owner:
            return Response(success=False, message="缺少插件ID参数")
        # 预检，给出更明确的失败原因
        if self._group_int is None:
            return Response(success=False, message="话题群ID未配置或格式不对，请检查配置")
        if owner not in self._plugin_routes and not self._default_tid:
            return Response(
                success=False,
                message=f"{owner} 未配置路由规则且无默认话题，测试消息将保持原样发到群 General",
            )
        try:
            message = Notification(
                channel=MessageChannel.Telegram,
                mtype=NotificationType.Plugin,
                title=f"【话题路由测试】{owner}",
                text=(
                    f"这是一条来自「{owner}」路由通道的测试通知。\n"
                    f"如果你在对应话题里看到它，说明路由已生效；"
                    f"如果出现在群 General，请检查话题 ID 是否填对。"
                ),
                save_history=False,
            )
            object.__setattr__(message, MARKER, owner)
            self.chain.post_message(message=message)
            logger.info(f"【话题路由】已提交 {owner} 的测试通知（走完整消息链）")
            return Response(success=True, message=f"测试通知已提交，请到 Telegram 对应话题查看")
        except Exception as e:
            logger.error(f"【话题路由】测试通知发送失败：{e}")
            return Response(success=False, message=f"测试通知发送失败：{e}")

    def _api_del_route(self, owner: str = "", **kwargs) -> Response:
        owner = (owner or "").strip()
        if not owner:
            return Response(success=False, message="缺少插件ID参数")
        try:
            config = dict(self.get_config() or {})
            removed = False
            # 新版：route_<插件ID> 键
            if f"route_{owner}" in config:
                config.pop(f"route_{owner}")
                removed = True
            # 旧版兼容：routes 文本行
            routes_text = str(config.get("routes") or "")
            if routes_text:
                lines = [
                    ln for ln in routes_text.splitlines()
                    if ln.strip() and not ln.strip().startswith("#")
                    and ln.split("=", 1)[0].strip() != owner
                ]
                if "\n".join(lines) != routes_text:
                    config["routes"] = "\n".join(lines)
                    removed = True
            if not removed:
                return Response(success=False, message=f"{owner} 没有配置路由规则")
            self.update_config(config)
            self.init_plugin(config)
            logger.info(f"【话题路由】已删除 {owner} 的路由规则")
            return Response(success=True, message=f"已删除 {owner} 的路由规则")
        except Exception as e:
            logger.error(f"【话题路由】删除路由规则失败：{e}")
            return Response(success=False, message=f"删除失败：{e}")

    def _api_clear_log(self, **kwargs) -> Response:
        with self._log_lock:
            self._route_log = []
        try:
            self.save_data("route_log", [])
        except Exception:
            pass
        logger.info("【话题路由】路由日志已清空")
        return Response(success=True, message="路由日志已清空")

    # ------------------------------------------------------------------ 配置页
    def _installed_plugins(self) -> List[Tuple[str, str]]:
        """
        已启用插件清单 [(插件ID, 显示名)]，按显示名排序，排除自身。
        只列运行中（已启用）的插件：停用的插件不进清单、不进日志。
        任何失败返回空清单（配置页会给出提示，不阻塞表单渲染）。
        """
        try:
            pm = PluginManager()
            plugins = getattr(pm, "_running_plugins", None) or {}
            items: List[Tuple[str, str]] = []
            for pid, obj in plugins.items():
                if pid == self.__class__.__name__:
                    continue
                name = getattr(obj, "plugin_name", "") or pid
                items.append((pid, name))
            return sorted(items, key=lambda x: x[1])
        except Exception:
            return []

    def _plugin_display_map(self) -> Dict[str, str]:
        """插件ID → 显示名（用于详情页展示）"""
        return {pid: name for pid, name in self._installed_plugins()}

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        """
        插件配置页面（Vuetify JSON，动态生成）：
        每个已安装插件一行，填话题 ID 即接管该插件，无需手写插件 ID。
        """
        saved = {}
        try:
            saved = self.get_config() or {}
        except Exception:
            saved = {}
        defaults: Dict[str, Any] = {
            "enabled": False,
            "debug": False,
            "group_id": "",
            "default_thread_id": "",
        }
        # 已安装插件都给 defaults：已配置的回显话题ID，未配置的空值
        for pid in [pid for pid, _ in self._installed_plugins()]:
            defaults[f"route_{pid}"] = str(self._plugin_routes.get(pid, ""))
        # 未安装但历史配置过的插件也回显（插件停用时规则不丢）
        for pid, tid in self._plugin_routes.items():
            defaults.setdefault(f"route_{pid}", str(tid))

        plugins = self._installed_plugins()
        if plugins:
            # 每行两个插件，紧凑排布
            plugin_rows: List[dict] = []
            for i in range(0, len(plugins), 2):
                cols = []
                for pid, name in plugins[i:i + 2]:
                    cols.append({
                        "component": "VCol",
                        "props": {"cols": 12, "md": 6},
                        "content": [{
                            "component": "VTextField",
                            "props": {
                                "model": f"route_{pid}",
                                "label": name,
                                "placeholder": "话题ID，留空不接管",
                                "hint": f"插件ID：{pid}",
                                "persistent-hint": True,
                                "type": "number",
                            },
                        }],
                    })
                plugin_rows.append({"component": "VRow", "content": cols})
        else:
            plugin_rows = [{
                "component": "VRow",
                "content": [{
                    "component": "VCol",
                    "props": {"cols": 12},
                    "content": [{
                        "component": "VAlert",
                        "props": {
                            "type": "warning",
                            "variant": "tonal",
                            "text": (
                                "暂时读不到已安装插件清单（可能是 MP 正在启动或插件尚未加载）。"
                                "保存后重新打开配置页即可看到插件清单。"
                            ),
                        },
                    }],
                }],
            }]

        form = [
            {
                "component": "VForm",
                "content": [
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12},
                                "content": [
                                    {
                                        "component": "VAlert",
                                        "props": {
                                            "type": "info",
                                            "variant": "tonal",
                                            "title": "🔀 小仓酱的消息话题路由",
                                            "text": (
                                                "在下方插件清单里给需要的插件填话题 ID，即接管它的通知路由（留空不接管）。"
                                                "话题 ID 支持直接粘贴 t.me 话题分享链接自动提取。"
                                                "只拦截插件消息：系统通知、私聊、带按钮的交互消息一律不动；"
                                                "未配置的插件照常发送，不会丢消息也不会重复发。"
                                                "清单只显示已启用的插件；停用插件的历史规则保留，重新启用即恢复。"
                                            ),
                                        },
                                    }
                                ],
                            }
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {"model": "enabled", "label": "启用插件"},
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {"model": "debug", "label": "调试日志"},
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "group_id",
                                            "label": "话题群ID（-100 开头）",
                                            "placeholder": "-1001234567890",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "default_thread_id",
                                            "label": "默认话题ID（未配置插件的兜底）",
                                            "placeholder": "留空=未配置的插件保持原样",
                                        },
                                    }
                                ],
                            },
                        ],
                    },
                    {"component": "VDivider", "props": {"class": "my-2"}},
                    {
                        "component": "VRow",
                        "content": [{
                            "component": "VCol",
                            "props": {"cols": 12},
                            "content": [{
                                "component": "p",
                                "props": {"class": "text-h6 mb-0"},
                                "text": "插件路由清单（填话题 ID 即接管，留空不接管）",
                            }],
                        }],
                    },
                    *plugin_rows,
                ],
            }
        ]
        return form, defaults

    # ------------------------------------------------------------------ 详情页
    def get_page(self) -> Optional[List[dict]]:
        """插件详情页：状态卡 + 路由规则 + 话题清单 + 路由日志"""
        sections: List[dict] = []
        sections.append(self._page_status_card())
        sections.append(self._page_action_row())
        sections.append(self._page_rules_table())
        sections.append(self._page_topics_table())
        sections.append(self._page_route_log_table())
        return sections

    def _page_rules_table(self) -> dict:
        """路由规则表：每条规则一行，带测试/删除按钮（配置完立刻可测）"""
        if not self._plugin_routes:
            return self._page_alert(
                "info", "还没有路由规则",
                "在配置页的「插件路由清单」里给需要的插件填话题 ID 并保存，"
                "这里就会出现规则和对应的测试按钮。",
            )
        display = self._plugin_display_map()
        rows = []
        for pid, tid in sorted(self._plugin_routes.items(), key=lambda x: x[1]):
            rows.append({
                "component": "tr",
                "content": [
                    {"component": "td", "text": display.get(pid) or pid},
                    {"component": "td", "text": pid},
                    {"component": "td", "text": str(tid)},
                    {"component": "td", "content": [
                        self._page_btn("测试", "test_route",
                                       {"owner": pid},
                                       color="primary"),
                        self._page_btn("删规则", "del_route",
                                       {"owner": pid},
                                       color="error"),
                    ]},
                ],
            })
        return {
            "component": "div",
            "content": [
                {
                    "component": "p",
                    "props": {"class": "text-h6 mt-2 mb-1"},
                    "text": "路由规则（点「测试」发一条测试通知到对应话题）",
                },
                {
                    "component": "VTable",
                    "props": {"hover": True, "density": "compact"},
                    "content": [
                        {
                            "component": "thead",
                            "content": [{
                                "component": "tr",
                                "content": [
                                    {"component": "th", "text": "插件"},
                                    {"component": "th", "text": "插件ID"},
                                    {"component": "th", "text": "话题ID"},
                                    {"component": "th", "text": "操作"},
                                ],
                            }],
                        },
                        {"component": "tbody", "content": rows},
                    ],
                },
            ],
        }

    def _page_alert(self, level: str, title: str, text: str) -> dict:
        return {
            "component": "VAlert",
            "props": {
                "type": level,
                "variant": "tonal",
                "title": title,
                "text": text,
                "class": "mb-3",
            },
        }

    def _page_status_card(self) -> dict:
        """补丁与配置健康状态"""
        if not self._enabled:
            head = self._page_alert(
                "info", "插件未启用",
                "在配置页打开「启用插件」后开始接管插件通知的话题路由。",
            )
        else:
            patch_items = "、".join(
                f"{'✅' if ok else '❌'}{name}"
                for name, ok in self._patch_status.items()
            )
            all_ok = all(self._patch_status.values()) and len(self._patch_status) > 0
            head = self._page_alert(
                "success" if all_ok else "warning",
                "运行状态",
                f"补丁挂载：{patch_items or '未知'}。"
                f"插件规则 {len(self._plugin_routes)} 条，类型规则 {len(self._type_routes)} 条，"
                f"话题群：{self._group_id or '未配置'}。",
            )
        errors = []
        for err in self._config_errors:
            errors.append(self._page_alert("error", "配置有问题", err))
        content = [head] + errors
        return {"component": "div", "content": content}

    def _page_btn(self, text: str, api_path: str, params: Optional[dict],
                  color: str = "primary", size: str = "small") -> dict:
        # API 已声明 auth="bear"（登录态校验），前端 axios 自动带 Authorization 头，
        # 无需再传 token/apikey 参数
        event = {
            "api": f"plugin/OguraTopicRouter/{api_path}",
            "method": "get",
        }
        if params:
            event["params"] = params
        return {
            "component": "VBtn",
            "props": {"color": color, "variant": "tonal", "size": size, "class": "mr-2"},
            "text": text,
            "events": {"click": event},
        }

    def _page_action_row(self) -> dict:
        return {
            "component": "VRow",
            "content": [
                {
                    "component": "VCol",
                    "props": {"cols": 12},
                    "content": [
                        self._page_btn("🔄 扫描/刷新话题", "scan_topics", None),
                        self._page_btn("🧹 清空路由日志", "clear_log", None, color="warning"),
                        {
                            "component": "span",
                            "props": {"class": "text-body-2 text-medium-emphasis"},
                            "text": "话题记录来自机器人收到的群消息（被动监听，不与 MP 抢消息）；"
                                    "在需要的话题里随便发条消息再点扫描即可收录。",
                        },
                    ],
                }
            ],
        }

    def _page_topics_table(self) -> dict:
        """话题清单：照抄 thread_id 到路由规则里"""
        topics_map, topics_lock = _topics_registry()
        with topics_lock:
            topics = sorted(
                topics_map.values(),
                key=lambda x: x.get("last_ts", ""),
                reverse=True,
            )[:30]
        if not topics:
            return self._page_alert(
                "info", "还没有话题记录",
                "两种方式获取话题 ID：① 在需要的话题里随便发一条消息（机器人可见），"
                "点「扫描/刷新话题」即可收录；② 直接复制话题分享链接"
                "（长按话题 → 复制链接，形如 https://t.me/c/4423340207/17），"
                "粘贴到配置页对应插件的输入框，自动提取话题 ID——链接第二段就是话题 ID。",
            )
        rows = []
        for t in topics:
            rows.append({
                "component": "tr",
                "content": [
                    {"component": "td", "text": str(t.get("thread_id", ""))},
                    {"component": "td", "text": t.get("title") or "（未知话题名）"},
                    {"component": "td", "text": t.get("chat_id", "")},
                    {"component": "td", "text": t.get("last_text") or "-"},
                    {"component": "td", "text": t.get("last_ts", "")},
                ],
            })
        return {
            "component": "div",
            "content": [
                {
                    "component": "p",
                    "props": {"class": "text-h6 mt-2 mb-1"},
                    "text": "话题清单",
                },
                {
                    "component": "VTable",
                    "props": {"hover": True, "density": "compact"},
                    "content": [
                        {
                            "component": "thead",
                            "content": [{
                                "component": "tr",
                                "content": [
                                    {"component": "th", "text": "话题ID"},
                                    {"component": "th", "text": "话题名"},
                                    {"component": "th", "text": "群ID"},
                                    {"component": "th", "text": "最近消息"},
                                    {"component": "th", "text": "时间"},
                                ],
                            }],
                        },
                        {"component": "tbody", "content": rows},
                    ],
                },
            ],
        }

    def _page_route_log_table(self) -> dict:
        with self._log_lock:
            logs = list(self._route_log[:20])
        if not logs:
            return {"component": "div", "content": []}
        rows = []
        for entry in logs:
            rows.append({
                "component": "tr",
                "content": [
                    {"component": "td", "text": entry.get("ts", "")},
                    {"component": "td", "text": entry.get("owner", "")},
                    {"component": "td", "text": entry.get("mtype", "") or "-"},
                    {"component": "td", "text": entry.get("rule", "") or "-"},
                    {"component": "td",
                     "text": str(entry.get("tid")) if entry.get("tid") else "-"},
                    {"component": "td", "text": entry.get("result", "")},
                ],
            })
        return {
            "component": "div",
            "content": [
                {
                    "component": "p",
                    "props": {"class": "text-h6 mt-3 mb-1"},
                    "text": "路由日志（最近 20 条）",
                },
                {
                    "component": "VTable",
                    "props": {"density": "compact"},
                    "content": [
                        {
                            "component": "thead",
                            "content": [{
                                "component": "tr",
                                "content": [
                                    {"component": "th", "text": "时间"},
                                    {"component": "th", "text": "插件"},
                                    {"component": "th", "text": "类型"},
                                    {"component": "th", "text": "规则"},
                                    {"component": "th", "text": "话题"},
                                    {"component": "th", "text": "结果"},
                                ],
                            }],
                        },
                        {"component": "tbody", "content": rows},
                    ],
                },
            ],
        }


# ---------------------------------------------------------------------- 包装器
# 已知补丁键集合（含历史版本键）：用于识别「自家旧版包装器」并接管重建
_KNOWN_KEYS = {
    "chain_post", "chain_norm",
    "tg_module", "tg_module_medias", "tg_module_torrents",
    "tg_short", "tg_long_plain", "tg_long", "tg_request",
}


def _wrap_chain_post_message(original):
    """ChainBase.post_message 包装：识别插件归属并打标记（系统消息零影响）"""

    def wrapper(inner_self, message=None, *args, **kwargs):
        try:
            router = _get_router()
            if router is not None and message is not None:
                marked = getattr(message, MARKER, None)
                if not marked:
                    if router._enabled:
                        owner = _find_plugin_owner()
                        if owner:
                            object.__setattr__(message, MARKER, owner)
                            if router._debug:
                                logger.debug(f"【话题路由】归属识别：{owner}")
        except Exception:
            pass
        return original(inner_self, message, *args, **kwargs)

    wrapper.__otr_patched__ = "chain_post"
    wrapper.__otr_original__ = original
    return wrapper


def _wrap_chain_normalize(original):
    """_normalize_notification_for_dispatch 包装：deepcopy 后补标记，保证不丢"""

    def wrapper(*args, **kwargs):
        dispatch = original(*args, **kwargs)
        try:
            source = args[0] if args else kwargs.get("message")
            if dispatch is not None and source is not None:
                owner = getattr(source, MARKER, None)
                if owner and not getattr(dispatch, MARKER, None):
                    object.__setattr__(dispatch, MARKER, owner)
        except Exception:
            pass
        return dispatch

    wrapper.__otr_patched__ = "chain_norm"
    wrapper.__otr_original__ = original
    return wrapper


def _make_tg_entry_wrapper(key: str):
    """
    TelegramModule 消息入口包装器工厂（post_message / post_medias_message /
    post_torrents_message 三个入口共用，各自带独立补丁标记避免幂等检查串号）：
    查路由 → 设置线程话题上下文 → 调原入口 → finally 清理。
    """

    def _factory(original):
        def wrapper(inner_self, message, *args, **kwargs):
            try:
                _TLS.tid = None
                _TLS.owner = None
                router = _get_router()
                if router is not None:
                    tid = router._resolve_thread(message)
                    if tid:
                        _TLS.tid = tid
                        try:
                            _TLS.owner = getattr(message, MARKER, None)
                        except Exception:
                            _TLS.owner = None
            except Exception:
                _TLS.tid = None
                _TLS.owner = None
            try:
                return original(inner_self, message, *args, **kwargs)
            finally:
                _TLS.tid = None
                _TLS.owner = None

        wrapper.__otr_patched__ = key
        wrapper.__otr_original__ = original
        return wrapper

    return _factory


def _make_send_injector(key: str):
    """
    Telegram 底层 send 分支包装：话题改道的最终执行点。
    - 目标群已是配置的话题群 → 只注入 message_thread_id；
    - 目标是 MP 默认通知目标（可能是旧频道/别的聊天）→ 强制改道：
      chat_id 改写为配置的话题群 + 注入 message_thread_id。
      （命中路由的插件消息是广播消息，chat_id 一定是客户端默认目标；
        私聊/交互消息在路由决策处已被排除，不会走到这里。）
    """

    def _factory(original):
        def wrapper(inner_self, *args, **kwargs):
            try:
                tid = getattr(_TLS, "tid", None)
                if tid:
                    router = _get_router()
                    if router is not None and router._group_int is not None:
                        chat_id = kwargs.get("chat_id")
                        if router._chat_matches(chat_id):
                            if "message_thread_id" not in kwargs:
                                kwargs = dict(kwargs)
                                kwargs["message_thread_id"] = int(tid)
                        else:
                            # 只接管"默认广播目标"：与客户端默认 chat_id 一致才改道
                            default_id = getattr(inner_self, "_telegram_chat_id", None)
                            same_target = (
                                str(chat_id) == str(default_id) if default_id else False
                            )
                            if same_target:
                                kwargs = dict(kwargs)
                                kwargs["chat_id"] = router._group_int
                                kwargs["message_thread_id"] = int(tid)
                                logger.info(
                                    f"【话题路由】已强制改道：默认目标 {chat_id} → "
                                    f"话题群 {router._group_int} 话题 {tid}"
                                )
            except Exception:
                pass
            return original(inner_self, *args, **kwargs)

        wrapper.__otr_patched__ = key
        wrapper.__otr_original__ = original
        return wrapper

    return _factory


def _wrap_send_request_monitor(original):
    """__send_request 包装：话题改道发送失败时给出明确的排查提示"""

    def wrapper(inner_self, *args, **kwargs):
        result = original(inner_self, *args, **kwargs)
        try:
            tid = getattr(_TLS, "tid", None)
            if tid and result is None:
                router = _get_router()
                if router is not None:
                    logger.error(
                        f"【话题路由】话题 {tid} 发送失败（API 返回空）。常见原因："
                        f"话题ID不存在、机器人不是群管理员/无发言权限、该群未开启话题模式。"
                        f"请用「扫描话题」核对话题ID。"
                    )
                    router._log_route(
                        owner=getattr(_TLS, "owner", "") or "?",
                        mtype_name="", rule=f"话题{tid}", tid=tid, result="发送失败",
                    )
        except Exception:
            pass
        return result

    wrapper.__otr_patched__ = "tg_request"
    wrapper.__otr_original__ = original
    return wrapper
