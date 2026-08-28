# -*- coding: utf-8 -*-
"""
订阅优先整理插件 (MoviePilot V2)
================================

让订阅下载的媒体在整理队列中享有最高优先级：
- 订阅任务（下载历史 note.source 以 "Subscribe|" 开头）→ 优先级 0，插队到队首；
- 手动下载 / 其他来源 / 无法识别来源 → 优先级 1，保持原有先来后到顺序；
- 同一优先级内部严格按入队顺序（FIFO），绝不乱序。

实现方式（零破坏设计）：
- 对 TransferChain 单例的 _queue 做「原地升级」：对象引用直接替换为
  SubscribePriorityQueue（queue.Queue 的接口兼容实现，锁结构与语义对齐），
  迁移期间先装新引用、再排空旧队列，任务零丢失；
- 消费线程每次循环都通过 self._queue.get(...) 取任务，引用替换后最多
  一个取数周期（15s 超时）内自动切到新队列，无需打断正在整理的任务；
- 所有判定路径全部 try/except 兜底，任何异常一律按普通优先级处理，
  插件自身异常绝不影响整理主流程；
- 停用插件时把队列还原为原生 queue.Queue（按当前优先级顺序回填）。
"""
import heapq
import importlib
import threading
import time
import traceback
import weakref
from typing import Any, Optional

from app.log import logger
from app.plugins import _PluginBase
from app.schemas import NotificationType

# 原生 queue.Queue 类型引用（用于类型判断与还原）
import queue as _native_queue


# ---------------------------------------------------------------------------
# 类级协调注册表（插件分身场景）：记录当前处于「已生效」状态的实例。
# 只有关联到队列的实例才有权降级队列；其他分身停用不影响队列。
# 用 WeakSet 存实例引用，实例被回收后自动移除，不泄漏。
# ---------------------------------------------------------------------------
_active_plugin_refs: "weakref.WeakSet" = weakref.WeakSet()
_registry_lock = threading.Lock()


class SubscribePriorityQueue:
    """
    queue.Queue 的优先级替换实现（订阅任务优先，同级 FIFO）。

    与 queue.Queue 的兼容点：
    - 接口：put / get(block, timeout) / task_done / empty / qsize / join
    - 锁结构：mutex + not_empty + not_full + all_tasks_done，
      字段名与 queue.Queue 一致，外部若直接访问 .mutex 依然可用；
    - 阻塞语义：get(block=True) 在队列为空时挂起等待，被 put 唤醒；
      get(block=True, timeout=N) 超时抛 queue.Empty。

    排序键：(priority, seq, item)，priority 0=订阅 1=普通；
    seq 为全局递增序号，堆比较在第二字段必分胜负，item 永不参与比较。
    """

    def __init__(self, priority_fn=None, logger_=None, source_tag="", jump_notify_fn=None):
        self._priority_fn = priority_fn
        self._logger = logger_
        self._source_tag = source_tag
        self._jump_notify_fn = jump_notify_fn
        # 降级开关：True 时按纯 FIFO 工作（停用插件用，杜绝引用交换丢任务）
        self._disabled = False
        # 队列所有权：当前绑定到该队列的插件实例（弱引用，防循环引用泄漏）
        self._owner_ref = None
        self.mutex = threading.Lock()
        self.not_empty = threading.Condition(self.mutex)
        self.not_full = threading.Condition(self.mutex)  # 无界队列，仅占位保接口
        self.all_tasks_done = threading.Condition(self.mutex)
        self._heap = []           # [(priority, seq, item), ...]
        self._seq = 0
        self.unfinished_tasks = 0

    # ------------------------------------------------------------------ 内部
    def _log(self, level: str, msg: str):
        try:
            if self._logger is not None:
                getattr(self._logger, level)(f"【{self._source_tag}】{msg}")
        except Exception:
            pass

    def _priority_of(self, item) -> int:
        """判定任务优先级：任何异常一律按普通（1）处理。"""
        try:
            if self._priority_fn is not None and self._priority_fn(item):
                return 0
        except Exception as e:
            self._log("warning", f"优先级判定异常，按普通处理：{e}")
        return 1

    # ------------------------------------------------------------------ put
    def put(self, item, block=True, timeout=None):
        priority = self._priority_of(item)
        with self.mutex:
            # 降级模式（插件停用）：一律按 P1 追加，纯 FIFO，不丢引用不换对象
            if self._disabled:
                priority = 1
            heapq.heappush(self._heap, (priority, self._seq, item))
            self._seq += 1
            self.unfinished_tasks += 1
            # 插队通知：订阅任务排到了普通任务前面（在锁内统计，避免竞态）
            jump_ahead = 0
            jump_key = None
            jump_title = None
            if priority == 0 and self._jump_notify_fn is not None:
                normal_cnt = sum(1 for p, _, _ in self._heap if p == 1)
                if normal_cnt > 0:
                    jump_ahead = normal_cnt
                    jump_key = self._extract_hash(item)
                    jump_title = self._extract_title(item)
            self.not_empty.notify()
        if jump_ahead and jump_key is not None:
            self._notify_jump(jump_ahead, jump_key, jump_title)

    @staticmethod
    def _extract_hash(item) -> str:
        """从队列元素提取 download_hash（用于插队通知去重），失败返回空串。"""
        try:
            return str(getattr(getattr(item, "task", None), "download_hash", "") or "")
        except Exception:
            return ""

    @staticmethod
    def _extract_title(item) -> str:
        """从队列元素提取标题（优先 mediainfo.title，兜底文件名）。"""
        try:
            task = getattr(item, "task", None)
            media = getattr(task, "mediainfo", None)
            title = getattr(media, "title", None)
            if title:
                year = getattr(media, "year", None)
                return f"{title} ({year})" if year else str(title)
            return str(getattr(getattr(task, "fileitem", None), "name", "") or "未知")
        except Exception:
            return "未知"

    def _notify_jump(self, ahead_of: int, key: str, title: str):
        try:
            if self._jump_notify_fn is not None:
                self._jump_notify_fn(ahead_of=ahead_of, download_hash=key, title=title)
        except Exception:
            pass

    # ------------------------------------------------------------------ get
    def get(self, block=True, timeout=None):
        with self.not_empty:
            if not block:
                if not self._heap:
                    raise _native_queue.Empty()
                return self._pop()
            if timeout is None:
                while not self._heap:
                    self.not_empty.wait()
                return self._pop()
            deadline = time.monotonic() + timeout
            while not self._heap:
                remaining = deadline - time.monotonic()
                if remaining <= 0.0:
                    raise _native_queue.Empty()
                self.not_empty.wait(remaining)
            return self._pop()

    def get_nowait(self):
        """get(block=False) 的标准别名（queue.Queue 兼容接口）。"""
        return self.get(block=False)

    def put_nowait(self, item):
        """put(block=False) 的标准别名（无界队列，等价 put）。"""
        return self.put(item, block=False)

    def _pop(self):
        # heapq 弹出堆顶 (priority, seq, item)；比较仅发生在前两个字段
        priority, seq, item = heapq.heappop(self._heap)
        return item

    # ------------------------------------------------------------------ 查询
    def qsize(self) -> int:
        with self.mutex:
            return len(self._heap)

    def empty(self) -> bool:
        with self.mutex:
            return not self._heap

    def snapshot(self):
        """返回 [(priority, seq)] 概览（仅诊断用，不含 item）。"""
        with self.mutex:
            return [(p, s) for p, s, _ in self._heap]

    # ------------------------------------------------------------------ 计数
    def task_done(self):
        with self.mutex:
            unfinished = self.unfinished_tasks - 1
            if unfinished <= 0:
                if unfinished < 0:
                    self._log("warning", "task_done() 调用次数超过队列项数")
                unfinished = 0
                self.all_tasks_done.notify_all()
            self.unfinished_tasks = unfinished

    def join(self):
        with self.all_tasks_done:
            while self.unfinished_tasks:
                self.all_tasks_done.wait()

    # ------------------------------------------------------------------ 迁移
    def absorb(self, old_queue) -> int:
        """
        吸收旧队列的存量任务（调用前应已把宿主引用替换为本实例，
        确保吸收期间新生产者写入本队列、不写旧队列）。
        存量任务按「订阅优先、同级保持原相对顺序」重新排列。
        返回吸收的任务数。
        """
        absorbed = 0
        while True:
            try:
                item = old_queue.get_nowait()
            except Exception:
                break
            if item is None:
                continue
            with self.mutex:
                priority = self._priority_of(item)
                heapq.heappush(self._heap, (priority, self._seq, item))
                self._seq += 1
                self.unfinished_tasks += 1
                self.not_empty.notify()
            absorbed += 1
        if absorbed:
            self._log("info", f"已吸收原队列 {absorbed} 个存量任务")
        return absorbed


class OguraTransferPriority(_PluginBase):
    """
    订阅优先整理：订阅任务在整理队列中插队到队首，其余保持原顺序。
    """

    # 插件元信息
    plugin_name = "订阅优先整理"
    plugin_desc = "订阅下载的媒体优先整理：订阅任务插队到整理队列队首，手动及其他任务保持原有顺序，适合 HDD 等慢速存储环境。"
    plugin_icon = "https://raw.githubusercontent.com/Lkwang88/MoviePilot-Plugins/main/icons/SpeedLimiter.jpg"
    plugin_version = "1.0.1"
    plugin_author = "Lkwang88"
    author_url = "https://github.com/Lkwang88"
    plugin_config_prefix = "oguratransferpriority."
    plugin_order = 34
    auth_level = 1

    # 配置
    _enabled = False
    _notify = True
    _notify_jump = True

    # 运行状态
    _patched = False
    _chain = None
    _pqueue = None
    # 实例级状态（不设类级可变对象，避免插件分身间共享污染）
    _jump_notified_hash = None
    _jump_notify_lock = None

    def init_plugin(self, config: dict = None):
        """
        生效配置：启用时改造整理队列，停用时还原。
        MP 重启后队列恢复原生 FIFO，本方法会被再次调用自动重新生效。
        """
        # 实例级状态初始化（每次 init 重建，隔离分身/残留）
        self._jump_notified_hash = set()
        self._jump_notify_lock = threading.Lock()
        self._enabled = False
        self._notify = True
        self._notify_jump = True
        if config:
            self._enabled = bool(config.get("enabled"))
            self._notify = bool(config.get("notify", True))
            self._notify_jump = bool(config.get("notify_jump", True))
        if self._enabled:
            try:
                self._patch_queue()
            except Exception as e:
                logger.error(f"【订阅优先整理】队列改造失败，插件自动停用：{e}")
                logger.error(traceback.format_exc())
                self._patched = False
                self._enabled = False
                if self._notify:
                    self._post(
                        "❌ 订阅优先整理生效失败",
                        f"队列改造遇到异常，已自动停用以保护正常整理流程。\n{e}",
                    )
                return
            logger.info("【订阅优先整理】插件已生效")
            if self._notify:
                self._post(
                    "🚀 订阅优先整理已生效",
                    "订阅任务的整理将插队到队首，手动及其他任务保持原有顺序。",
                )
        else:
            self._restore_queue()
            logger.info("【订阅优先整理】插件未启用")

    # ------------------------------------------------------------------ 改造
    def _patch_queue(self):
        """
        将 TransferChain 单例的 _queue 原地升级为订阅优先队列。
        顺序：先装新引用（新生产者写入新队列），再吸收旧队列存量（零丢失）。
        """
        transfer_module = importlib.import_module("app.chain.transfer")
        TransferChain = getattr(transfer_module, "TransferChain", None)
        if TransferChain is None:
            raise RuntimeError("无法定位 TransferChain（MoviePilot 版本不兼容）")

        chain = TransferChain()
        self._chain = chain

        old_queue = getattr(chain, "_queue", None)
        if old_queue is None:
            raise RuntimeError("TransferChain 没有 _queue 属性（版本不兼容）")

        if isinstance(old_queue, SubscribePriorityQueue):
            # 已改造过（重复保存配置 / 重新启用 / 其他分身改造过）：
            # 更新回调引用、解除降级，并转移所有权给当前实例
            old_queue._priority_fn = self._detect_subscribe
            old_queue._logger = logger
            old_queue._jump_notify_fn = self._queue_jump_notify
            with old_queue.mutex:
                old_queue._disabled = False
                old_queue._owner_ref = weakref.ref(self)
            with _registry_lock:
                _active_plugin_refs.add(self)
            self._pqueue = old_queue
            self._patched = True
            logger.info("【订阅优先整理】队列已是优先级队列，跳过重复改造")
            return

        if not isinstance(old_queue, _native_queue.Queue):
            logger.warning(
                f"【订阅优先整理】未知队列类型 {type(old_queue).__name__}，放弃改造"
            )
            self._patched = False
            return

        pqueue = SubscribePriorityQueue(
            priority_fn=self._detect_subscribe,
            logger_=logger,
            source_tag="订阅优先整理",
            jump_notify_fn=self._queue_jump_notify,
        )
        # 先替换引用：此后新任务一律写入优先队列
        chain._queue = pqueue
        # 认领队列所有权 + 注册生效实例（分身协调）
        pqueue._owner_ref = weakref.ref(self)
        with _registry_lock:
            _active_plugin_refs.add(self)
        # 再吸收存量：旧队列中等待的任务全部迁入（订阅置顶，同级保序）
        absorbed = pqueue.absorb(old_queue)
        self._pqueue = pqueue
        self._patched = True
        logger.info(f"【订阅优先整理】队列改造完成，迁移存量任务 {absorbed} 个")
        if absorbed and self._notify:
            self._post(
                "📦 整理队列已切换为订阅优先",
                f"切换时队列中有 {absorbed} 个待整理任务，已按「订阅优先、同级先来后到」"
                f"重新排序，无任务丢失。",
            )

    def _restore_queue(self):
        """
        停用插件：把优先队列切到降级模式（纯 FIFO），不交换对象引用。
        分身协调：只有「队列所有者」才有权降级——其他分身停用不影响队列；
        所有权在 _patch_queue 时转移给最后启用的实例。
        """
        with _registry_lock:
            _active_plugin_refs.discard(self)
        if self._pqueue is not None:
            try:
                with self._pqueue.mutex:
                    owner_ref = self._pqueue._owner_ref
                    owner = owner_ref() if owner_ref else None
                    # 仅当自己是所有者（或所有者已消失）时才降级
                    if owner is None or owner is self:
                        self._pqueue._disabled = True
                        self._pqueue._owner_ref = None
                        logger.info("【订阅优先整理】队列已切换为降级模式（纯FIFO），后续任务按原顺序整理")
                    else:
                        logger.info("【订阅优先整理】另一分身仍在生效，队列保持优先级模式")
            except Exception as e:
                logger.warning(f"【订阅优先整理】切换降级模式失败：{e}")
        self._patched = False
        self._pqueue = None
        self._chain = None

    # ------------------------------------------------------------------ 判定
    def _detect_subscribe(self, item) -> bool:
        """
        判定队列元素是否订阅任务。
        item 为 TransferQueue（含 .task），判定链：
        task.download_history.note.source 以 "Subscribe|" 开头 → 订阅。
        任何一环缺失/异常 → False（按普通优先级）。
        """
        try:
            task = getattr(item, "task", None)
            if task is None:
                return False
            history = getattr(task, "download_history", None)
            if history is None:
                return False
            note = getattr(history, "note", None)
            if not isinstance(note, dict):
                return False
            src = note.get("source")
            return bool(src) and str(src).startswith("Subscribe|")
        except Exception:
            return False

    # ------------------------------------------------------------------ 通知
    def _post(self, title: str, text: str):
        """走插件消息通道发送通知，失败只记日志不影响功能。"""
        try:
            self.post_message(mtype=NotificationType.Plugin, title=title, text=text)
        except Exception as e:
            logger.warning(f"【订阅优先整理】通知发送失败：{e}")

    def notify_subscribe_jump(self, download_hash: str, title: str, ahead_of: int):
        """
        插队通知：同一 download_hash 只提醒一次，避免多集种子刷屏。
        """
        if not (self._enabled and self._notify and self._notify_jump):
            return
        if not ahead_of or ahead_of <= 0:
            return
        key = download_hash or f"n:{title}"
        # 懒初始化（防御未经 init_plugin 的调用路径）
        if self._jump_notify_lock is None or self._jump_notified_hash is None:
            self._jump_notify_lock = threading.Lock()
            self._jump_notified_hash = set()
        with self._jump_notify_lock:
            if key in self._jump_notified_hash:
                return
            self._jump_notified_hash.add(key)
            # 防无限增长：超上限丢弃最旧的一半
            if len(self._jump_notified_hash) > 200:
                for old_key in list(self._jump_notified_hash)[:100]:
                    self._jump_notified_hash.discard(old_key)
        self._post(
            "⏫ 订阅任务已插队整理",
            f"《{title}》排到了 {ahead_of} 个手动/其他任务前面，HDD 正在优先伺候订阅～",
        )

    def _queue_jump_notify(self, ahead_of: int, download_hash: str, title: str):
        """队列回调入口（签名与队列侧解耦，任何异常不外抛）。"""
        try:
            self.notify_subscribe_jump(
                download_hash=download_hash, title=title, ahead_of=ahead_of
            )
        except Exception as e:
            logger.debug(f"【订阅优先整理】插队通知异常（忽略）：{e}")

    # ------------------------------------------------------------------ 接口
    def get_state(self) -> bool:
        return self._enabled

    @staticmethod
    def get_command() -> list:
        return []

    @staticmethod
    def get_api() -> list:
        return []

    @staticmethod
    def get_service() -> list:
        return []

    def get_form(self):
        """
        插件配置页面（Vuetify JSON）。
        """
        return [
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
                                            "title": "🚀 订阅优先整理",
                                            "text": (
                                                "订阅下载的媒体插队到整理队列队首优先整理，"
                                                "手动下载及其他来源保持原有先来后到顺序。"
                                                "适合 HDD 大盘机：订阅想尽快看，手动任务慢慢搬。"
                                                "插件不影响正在整理中的任务，任务零丢失，停用后自动还原。"
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
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {"model": "enabled", "label": "启用插件"},
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "notify",
                                            "label": "生效/迁移通知",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "notify_jump",
                                            "label": "插队通知（每种子一次）",
                                        },
                                    }
                                ],
                            }
                        ],
                    },
                ],
            }
        ], {
            "enabled": False,
            "notify": True,
            "notify_jump": True,
        }

    @staticmethod
    def get_page() -> Optional[list]:
        return None

    def stop_service(self):
        """
        插件停止：还原原生队列（按当前优先级顺序回填，不丢任务）。
        """
        self._restore_queue()
        if self._jump_notify_lock and self._jump_notified_hash:
            with self._jump_notify_lock:
                self._jump_notified_hash.clear()
