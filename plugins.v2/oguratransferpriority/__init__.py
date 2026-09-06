# -*- coding: utf-8 -*-
"""
订阅优先整理插件 (MoviePilot V2)
================================

让订阅下载的媒体在整理队列中享有最高优先级：
- **P0**：订阅任务（下载历史 `note.source` 以 `Subscribe|` 开头）——置顶；
- **P0.5**：通过数据页手动插队的普通任务——排在订阅之后；
- **P1**：手动下载 / 其他来源 / 识别失败的兜底；
- 同一优先级内部严格按入队顺序（FIFO），绝不乱序。

实现方式（零破坏设计）：
- 对 TransferChain 单例的 _queue 做「原地升级」：对象引用直接替换为
  SubscribePriorityQueue（queue.Queue 的接口兼容实现，锁结构与语义对齐），
  迁移期间先装新引用、再排空旧队列，任务零丢失；
- 消费线程每次循环都通过 self._queue.get(...) 取任务，引用替换后最多
  一个取数周期（15s 超时）内自动切到新队列，无需打断正在整理的任务；
- 所有判定路径全部 try/except 兜底，任何异常一律按普通优先级处理，
  插件自身异常绝不影响整理主流程；
- 停用插件时把队列切换为降级模式（纯 FIFO），不交换对象引用，避免生产者并发入队丢任务；
- 切换时保留 MP 当前活跃任务计数，保证 `task_done/join` 语义不因迁移少算；
"""
import hashlib
import heapq
import importlib
import re
import threading
import time
import traceback
import weakref
from pathlib import Path
from typing import Any, Dict, List, Optional

from app.log import logger
from app.plugins import _PluginBase
from app.schemas import NotificationType

# 原生 queue.Queue 类型引用（用于类型判断）
import queue as _native_queue


# ---------------------------------------------------------------------------
# 类级协调注册表（插件分身场景）：记录当前处于「已生效」状态的实例。
# 只有关联到队列的实例才有权降级队列；其他分身停用不影响队列。
# 用 WeakSet 存实例引用，实例被回收后自动移除，不泄漏。
# ---------------------------------------------------------------------------
_active_plugin_refs: "weakref.WeakSet" = weakref.WeakSet()
_registry_lock = threading.Lock()

# 队列优先级：订阅最高；手动插队只超过普通任务，不越过订阅。
_SUBSCRIBE_PRIORITY = 0
_MANUAL_PRIORITY = 0.5
_NORMAL_PRIORITY = 1
_TERMINAL_STATES = {"completed", "failed", "cancelled", "success"}


class SubscribePriorityQueue:
    """
    queue.Queue 的优先级替换实现（订阅任务优先，同级 FIFO）。

    与 queue.Queue 的兼容点：
    - 接口：put / get(block, timeout) / task_done / empty / qsize / join
    - 锁结构：mutex + not_empty + not_full + all_tasks_done，
      字段名与 queue.Queue 一致，外部若直接访问 .mutex 依然可用；
    - 阻塞语义：get(block=True) 在队列为空时挂起等待，被 put 唤醒；
      get(block=True, timeout=N) 超时抛 queue.Empty。

    排序键：(priority, seq, item)，priority 0=订阅、0.5=手动插队、1=普通；
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

    def _priority_of(self, item) -> float:
        """判定任务优先级：任何异常一律按普通（1）处理。"""
        try:
            if self._priority_fn is not None and self._priority_fn(item):
                return _SUBSCRIBE_PRIORITY
        except Exception as e:
            self._log("warning", f"优先级判定异常，按普通处理：{e}")
        return _NORMAL_PRIORITY

    # ------------------------------------------------------------------ put
    def put(self, item, block=True, timeout=None):
        priority = self._priority_of(item)
        with self.mutex:
            # 降级模式（插件停用）：一律按 P1 追加，纯 FIFO，不丢引用不换对象
            if self._disabled:
                priority = _NORMAL_PRIORITY
            heapq.heappush(self._heap, (priority, self._seq, item))
            self._seq += 1
            self.unfinished_tasks += 1
            # 插队通知：订阅任务排到了普通任务前面（在锁内统计，避免竞态）
            jump_ahead = 0
            jump_key = None
            jump_title = None
            if priority == _SUBSCRIBE_PRIORITY and self._jump_notify_fn is not None:
                normal_cnt = sum(1 for p, _, _ in self._heap if p == _NORMAL_PRIORITY)
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

    def snapshot_items(self):
        """在队列锁内复制等待任务快照（不取出、不改变计数）。"""
        with self.mutex:
            return list(self._heap)

    def promote(self, predicate) -> int:
        """在锁内把命中的普通任务提升为中间优先级，不改变任务计数。"""
        changed = 0
        with self.mutex:
            updated = []
            for priority, seq, item in self._heap:
                if priority == _NORMAL_PRIORITY and predicate(item):
                    priority = _MANUAL_PRIORITY
                    changed += 1
                updated.append((priority, seq, item))
            if changed:
                self._heap[:] = updated
                heapq.heapify(self._heap)
        return changed

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
    plugin_name = "小仓酱的订阅优先整理"
    plugin_desc = (
        "订阅下载的媒体优先整理：数据页合并显示等待中与正在整理的项目，"
        "支持同剧同季合并及普通任务手动插队；订阅仍排在最前，适合 HDD 等慢速存储环境。"
    )
    plugin_icon = "https://raw.githubusercontent.com/Lkwang88/MoviePilot-Plugins/main/icons/SpeedLimiter.jpg"
    plugin_version = "1.1.0"
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
        生效配置：启用时改造整理队列，停用时切换为降级 FIFO。
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
            if not self._patched:
                # 放弃改造（如完全未知的队列类型）：不算生效，别误报
                logger.warning("【订阅优先整理】队列未改造，插件保持观察模式（不影响整理）")
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

        # ① 同代优先队列（重复保存配置 / 重新启用 / 分身改造过）：直接接管
        if isinstance(old_queue, SubscribePriorityQueue):
            self._adopt_queue(old_queue, "队列已是优先级队列，跳过重复改造")
            return

        # ② 跨代优先队列：插件更新会重载模块，旧实例的类身份与新代码不匹配
        #    （isinstance 必然 False）。按结构签名识别旧版队列并直接接管——
        #    不换对象、不动存量，解除降级即恢复插队，无需重启 MP
        if self._looks_like_priority_queue(old_queue):
            logger.info("【订阅优先整理】检测到插件更新前的旧版优先队列，接管中...")
            try:
                self._adopt_queue(old_queue, "已接管旧版优先级队列（插件更新场景）")
                return
            except Exception as e:
                logger.warning(f"【订阅优先整理】接管旧队列失败：{e}，改为重建迁移")

        # ③ 可排空队列（原生 queue.Queue / 更早版本优先队列）：全新改造+迁移存量
        if isinstance(old_queue, _native_queue.Queue) or callable(
                getattr(old_queue, "get_nowait", None)):
            self._install_fresh_queue(chain, old_queue)
            return

        # ④ 彻底未知的队列类型：放弃（不动现有队列，不影响整理）
        logger.warning(
            f"【订阅优先整理】未知队列类型 {type(old_queue).__name__}，放弃改造"
        )
        self._patched = False

    @staticmethod
    def _looks_like_priority_queue(queue) -> bool:
        """
        结构签名识别跨代旧版优先队列（插件更新后模块重载导致类身份失配）。
        """
        return (
                type(queue).__name__ == "SubscribePriorityQueue"
                and hasattr(queue, "_heap")
                and hasattr(queue, "_disabled")
                and callable(getattr(queue, "get_nowait", None))
                and callable(getattr(queue, "absorb", None))
                and callable(getattr(queue, "task_done", None))
        )

    def _adopt_queue(self, queue, note: str = ""):
        """
        接管现有优先队列（同代或跨代）：刷新回调、解除降级、转移所有权。
        不换对象引用、不动队列存量，正在整理/排队的任务零影响。
        """
        queue._priority_fn = self._detect_subscribe
        queue._logger = logger
        queue._jump_notify_fn = self._queue_jump_notify
        with queue.mutex:
            was_disabled = bool(getattr(queue, "_disabled", False))
            queue._disabled = False
            queue._owner_ref = weakref.ref(self)
        with _registry_lock:
            _active_plugin_refs.add(self)
        self._pqueue = queue
        self._patched = True
        msg = note or "队列已是优先级队列，跳过重复改造"
        if was_disabled:
            msg += "（已解除降级模式，插队恢复）"
        logger.info(f"【订阅优先整理】{msg}")

    def _install_fresh_queue(self, chain, old_queue) -> None:
        """
        全新安装优先队列并吸收旧队列存量（引用先换、数据后迁，零丢失）。
        """
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

    # ------------------------------------------------------------------ 队列查看与手动插队
    @staticmethod
    def _normalize_text(value: Any) -> str:
        """规范化用于兜底分组的文本，不改变页面展示原文。"""
        try:
            return re.sub(r"\s+", " ", str(value or "").strip()).casefold()
        except Exception:
            return ""

    @classmethod
    def _media_identity_key(cls, media=None, meta=None, history=None) -> str:
        """
        生成稳定的媒体身份键。

        优先复用 MP v2 官方 ``resolve_media_identity``（来源+原生 ID），
        再做兼容兜底；没有任何 ID 时才使用标题+年份+类型。不把标题作为有
        ID 媒体的主键，避免同名剧误合并。
        """
        def value(obj, name):
            try:
                return getattr(obj, name, None) if obj is not None else None
            except Exception:
                return None

        # MP v2.15.6 的正式媒体身份算法，避免把辅助 TMDB ID 错配到豆瓣等主源。
        try:
            from app.utils.media import resolve_media_identity
            for obj in (media, meta, history):
                if obj is None:
                    continue
                source, media_id = resolve_media_identity(media=obj)
                if source and media_id:
                    return f"id:{str(source).strip().casefold()}:{str(media_id).strip()}"
        except Exception:
            pass

        # 兼容极老/测试环境没有官方 helper 的情况。
        aliases = {
            "tmdb": "themoviedb",
            "themoviedb": "themoviedb",
            "douban": "douban",
            "bangumi": "bangumi",
            "anilist": "anilist",
        }
        source = value(media, "source") or value(media, "media_source")
        media_id = value(media, "media_id")
        if not source:
            source = value(meta, "media_source")
        if media_id is None:
            media_id = value(meta, "media_id")
        if history is not None:
            if not source:
                source = value(history, "media_source")
            if media_id is None:
                media_id = value(history, "media_id")
        source = aliases.get(cls._normalize_text(source), cls._normalize_text(source))
        if source and media_id is not None and str(media_id).strip():
            return f"id:{source}:{str(media_id).strip()}"

        id_fields = (
            ("themoviedb", ("tmdb_id", "tmdbid")),
            ("douban", ("douban_id", "doubanid")),
            ("bangumi", ("bangumi_id", "bangumiid")),
            ("anilist", ("anilist_id", "anilistid")),
        )
        for identity_source, fields in id_fields:
            for obj in (media, meta):
                for field in fields:
                    candidate = value(obj, field)
                    if candidate is not None and str(candidate).strip():
                        return f"id:{identity_source}:{str(candidate).strip()}"

        title = (
            value(media, "title")
            or value(media, "original_title")
            or value(meta, "name")
            or value(meta, "title")
            or value(history, "title")
            or "未知媒体"
        )
        year = value(media, "year") or value(meta, "year") or value(history, "year") or ""
        media_type = value(media, "type") or value(meta, "type") or value(history, "type") or ""
        return (
            f"title:{cls._normalize_text(title)}|year:{cls._normalize_text(year)}"
            f"|type:{cls._normalize_text(media_type)}"
        )

    @classmethod
    def _record_season(cls, record: dict):
        """从队列任务/作业视图记录解析季号。"""
        season = record.get("season")
        if season is None:
            meta = record.get("meta")
            media = record.get("media")
            for obj, fields in (
                (meta, ("begin_season", "season")),
                (media, ("season",)),
            ):
                for field in fields:
                    try:
                        candidate = getattr(obj, field, None) if obj is not None else None
                    except Exception:
                        candidate = None
                    if candidate is not None:
                        season = candidate
                        break
                if season is not None:
                    break
        try:
            return int(season) if season is not None else None
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _file_key(fileitem) -> Optional[tuple]:
        """生成与 MP v2 JobManager 同口径的文件键。"""
        try:
            if fileitem is None:
                return None
            raw_path = getattr(fileitem, "path", None)
            if not raw_path:
                return None
            path = str(raw_path).replace("\\", "/").rstrip("/") or "/"
            path = Path(path).as_posix()
            return str(getattr(fileitem, "storage", None) or "local"), path
        except Exception:
            return None

    @classmethod
    def _record_title(cls, record: dict) -> str:
        """提取项目展示标题，优先已识别媒体标题。"""
        media = record.get("media")
        meta = record.get("meta")
        for obj, fields in (
            (media, ("title_year", "title", "original_title")),
            (meta, ("name", "title")),
        ):
            for field in fields:
                try:
                    value = getattr(obj, field, None) if obj is not None else None
                except Exception:
                    value = None
                if value:
                    title = str(value).strip()
                    if title:
                        if field not in ("title_year",) and getattr(obj, "year", None):
                            title = f"{title} ({getattr(obj, 'year')})"
                        return title
        return "未知媒体"

    @staticmethod
    def _record_episodes(record: dict) -> set[int]:
        """提取单个任务的集数，供同剧同季合并展示。"""
        meta = record.get("meta")
        if meta is None:
            return set()
        try:
            episodes = getattr(meta, "episode_list", None) or []
            result = {int(ep) for ep in episodes if str(ep).strip().isdigit()}
            if result:
                return result
            begin = getattr(meta, "begin_episode", None)
            end = getattr(meta, "end_episode", None)
            if begin is not None:
                begin, end = int(begin), int(end or begin)
                if 0 <= end - begin <= 200:
                    return set(range(begin, end + 1))
        except (TypeError, ValueError, AttributeError):
            pass
        return set()

    @staticmethod
    def _format_episodes(episodes: set[int]) -> str:
        """将集数集合压缩为 E01-E03、E05 的可读文本。"""
        if not episodes:
            return "集数未知"
        values = sorted(episodes)
        ranges = []
        start = previous = values[0]
        for value in values[1:]:
            if value == previous + 1:
                previous = value
                continue
            ranges.append((start, previous))
            start = previous = value
        ranges.append((start, previous))
        return ", ".join(
            f"E{begin:02d}" if begin == end else f"E{begin:02d}-E{end:02d}"
            for begin, end in ranges
        )

    @staticmethod
    def _history_is_subscribe(history) -> bool:
        """判断 MP v2 DownloadHistory 是否来自订阅。"""
        try:
            note = getattr(history, "note", None)
            source = note.get("source") if isinstance(note, dict) else None
            return bool(source) and str(source).startswith("Subscribe|")
        except Exception:
            return False

    @staticmethod
    def _queue_supported(queue) -> bool:
        """判断当前队列是否具备本插件安全读写所需的结构。"""
        return bool(
            queue is not None
            and type(queue).__name__ == "SubscribePriorityQueue"
            and getattr(queue, "mutex", None) is not None
            and isinstance(getattr(queue, "_heap", None), list)
            and callable(getattr(queue, "get_nowait", None))
            and callable(getattr(queue, "task_done", None))
        )

    @classmethod
    def _snapshot_priority_items(cls, queue) -> list:
        """锁内复制优先队列堆，不 pop、不改变 unfinished_tasks。"""
        snapshot = getattr(queue, "snapshot_items", None)
        if callable(snapshot):
            return list(snapshot())
        # 插件更新后的旧代队列没有新方法，但结构签名仍兼容。
        with queue.mutex:
            return list(queue._heap)

    def _promote_queue_items(self, queue, file_keys: set[tuple]) -> int:
        """将目标普通任务原地提升到 0.5，兼容跨代旧队列对象。"""
        if not file_keys:
            return 0

        def matches(item) -> bool:
            try:
                task = getattr(item, "task", None)
                return (
                    self._file_key(getattr(task, "fileitem", None)) in file_keys
                    and not self._detect_subscribe(item)
                )
            except Exception:
                return False

        promote = getattr(queue, "promote", None)
        if callable(promote):
            return int(promote(matches) or 0)

        changed = 0
        with queue.mutex:
            updated = []
            for priority, seq, item in queue._heap:
                if priority == _NORMAL_PRIORITY and matches(item):
                    priority = _MANUAL_PRIORITY
                    changed += 1
                updated.append((priority, seq, item))
            if changed:
                queue._heap[:] = updated
                heapq.heapify(queue._heap)
        return changed

    def _load_history_map(self, hashes: set[str]) -> dict:
        """一次性补查运行中任务的下载历史，避免页面请求形成 N+1 查询。"""
        if not hashes:
            return {}
        try:
            from app.db.downloadhistory_oper import DownloadHistoryOper
            return DownloadHistoryOper().get_by_hashes(list(hashes)) or {}
        except Exception as e:
            logger.warning(f"【订阅优先整理】读取运行中任务下载来源失败：{e}")
            return {}

    def _collect_transfer_records(self) -> tuple[list, bool]:
        """
        合并等待队列与 MP v2 JobManager 的当前作业视图。

        队列提供真实等待顺序；JobManager 补足已被消费者取走的运行中任务。
        以 storage+path 去重，页面只显示请求瞬间快照。
        """
        chain = self._chain
        if chain is None:
            try:
                transfer_module = importlib.import_module("app.chain.transfer")
                chain = getattr(transfer_module, "TransferChain")()
            except Exception as e:
                logger.warning(f"【订阅优先整理】读取整理链失败：{e}")
                return [], False
        self._chain = chain
        queue = getattr(chain, "_queue", None)
        if not self._queue_supported(queue):
            return [], False

        records: dict[tuple, dict] = {}
        try:
            queue_items = sorted(
                self._snapshot_priority_items(queue),
                key=lambda entry: (entry[0], entry[1]),
            )
        except Exception as e:
            logger.warning(f"【订阅优先整理】读取等待队列快照失败：{e}")
            return [], False

        for queue_index, entry in enumerate(queue_items):
            try:
                priority, seq, item = entry
                task = getattr(item, "task", None)
                fileitem = getattr(task, "fileitem", None)
                file_key = self._file_key(fileitem)
                if not task or not file_key:
                    continue
                records[file_key] = {
                    "file_key": file_key,
                    "media": getattr(task, "mediainfo", None),
                    "meta": getattr(task, "meta", None),
                    "season": None,
                    "fileitem": fileitem,
                    "download_hash": getattr(task, "download_hash", None),
                    "history": getattr(task, "download_history", None),
                    "subscribe": self._detect_subscribe(item),
                    "priority": priority,
                    "state": "waiting",
                    "order": (1, priority, seq, queue_index),
                }
            except Exception:
                continue

        try:
            jobs = chain.get_queue_tasks() or []
        except Exception as e:
            logger.warning(f"【订阅优先整理】读取 MP v2 作业视图失败：{e}")
            jobs = []

        hashes = {
            str(getattr(job_task, "download_hash", "") or "")
            for job in jobs
            for job_task in (getattr(job, "tasks", None) or [])
            if getattr(job_task, "download_hash", None)
        }
        history_map = self._load_history_map(hashes)

        for job_index, job in enumerate(jobs):
            job_media = getattr(job, "media", None)
            job_season = getattr(job, "season", None)
            for task_index, job_task in enumerate(getattr(job, "tasks", None) or []):
                state = str(getattr(job_task, "state", None) or "waiting").lower()
                if state in _TERMINAL_STATES:
                    continue
                fileitem = getattr(job_task, "fileitem", None)
                file_key = self._file_key(fileitem)
                if not file_key:
                    continue
                download_hash = str(getattr(job_task, "download_hash", "") or "")
                is_subscribe = self._history_is_subscribe(history_map.get(download_hash))
                record = records.get(file_key)
                if record is not None:
                    # 同一文件在等待队列和作业视图中只算一次；作业视图可补全媒体身份。
                    if not record.get("media") and job_media:
                        record["media"] = job_media
                    if not record.get("meta") and getattr(job_task, "meta", None):
                        record["meta"] = job_task.meta
                    if record.get("season") is None and job_season is not None:
                        record["season"] = job_season
                    record["subscribe"] = bool(record["subscribe"] or is_subscribe)
                    if state == "running":
                        record["state"] = "running"
                        record["order"] = (0, job_index, task_index)
                    continue
                records[file_key] = {
                    "file_key": file_key,
                    "media": job_media,
                    "meta": getattr(job_task, "meta", None),
                    "season": job_season,
                    "fileitem": fileitem,
                    "download_hash": download_hash,
                    "history": history_map.get(download_hash),
                    "subscribe": is_subscribe,
                    "priority": None,
                    "state": "running" if state == "running" else "waiting",
                    "order": (
                        (0, job_index, task_index)
                        if state == "running"
                        else (1, _NORMAL_PRIORITY, 10**12 + job_index, task_index)
                    ),
                }

        return sorted(records.values(), key=lambda record: record["order"]), True

    def _build_queue_view(self) -> dict:
        """构造页面用的合并项目视图，并保留插队所需文件映射。"""
        records, supported = self._collect_transfer_records()
        groups = {}
        for record in records:
            season = self._record_season(record)
            identity = self._media_identity_key(
                media=record.get("media"),
                meta=record.get("meta"),
                history=record.get("history"),
            )
            raw_project_key = f"{identity}|season:{season if season is not None else '-'}"
            project_key = hashlib.sha256(raw_project_key.encode("utf-8")).hexdigest()[:24]
            group = groups.get(project_key)
            if group is None:
                group = {
                    "project_key": project_key,
                    "title": self._record_title(record),
                    "season": season,
                    "episodes": set(),
                    "running": 0,
                    "waiting": 0,
                    "subscribe": 0,
                    "normal": 0,
                    "manual": 0,
                    "promotable": 0,
                    "order": record["order"],
                    "file_keys": set(),
                }
                groups[project_key] = group
            group["file_keys"].add(record["file_key"])
            group["episodes"].update(self._record_episodes(record))
            if record["state"] == "running":
                group["running"] += 1
            else:
                group["waiting"] += 1
            if record["subscribe"]:
                group["subscribe"] += 1
            else:
                group["normal"] += 1
                if record["priority"] == _MANUAL_PRIORITY:
                    group["manual"] += 1
                if (
                    record["state"] != "running"
                    and record["priority"] == _NORMAL_PRIORITY
                ):
                    group["promotable"] += 1
            record["project_key"] = project_key

        return {
            "supported": supported,
            "degraded": bool(
                supported
                and self._chain
                and getattr(getattr(self._chain, "_queue", None), "_disabled", False)
            ),
            "records": records,
            "groups": sorted(groups.values(), key=lambda group: group["order"]),
        }

    @staticmethod
    def _api_event(path: str, method: str = "get", params: Optional[dict] = None) -> dict:
        """生成 MP v2 Vuetify 页面按钮事件。"""
        api = f"plugin/OguraTransferPriority{path}"
        try:
            from app.core.config import settings
            token = getattr(settings, "API_TOKEN", None)
            if token:
                from urllib.parse import quote
                api += f"?apikey={quote(str(token), safe='')}"
        except Exception:
            pass
        event = {"api": api, "method": method}
        if params:
            event["params"] = params
        return event

    def api_queue(self) -> dict:
        """页面刷新接口：只返回快照摘要，具体内容由 get_page 重载。"""
        view = self._build_queue_view()
        return {
            "success": True,
            "supported": view["supported"],
            "degraded": bool(
                self._chain
                and getattr(getattr(self._chain, "_queue", None), "_disabled", False)
            ),
            "projects": len(view["groups"]),
            "tasks": len(view["records"]),
        }

    def api_promote(self, project: str = "") -> dict:
        """将指定项目中仍在等待的非订阅任务原地提升到 P0.5。"""
        if not self._enabled:
            return {"success": False, "message": "插件未启用"}
        view = self._build_queue_view()
        if not view["supported"]:
            return {"success": False, "message": "当前队列结构不支持手动插队"}
        target_keys = {
            record["file_key"]
            for record in view["records"]
            if (
                record.get("project_key") == str(project)
                and record["state"] != "running"
                and record["priority"] == _NORMAL_PRIORITY
                and not record["subscribe"]
            )
        }
        queue = getattr(self._chain, "_queue", None) if self._chain else None
        if not self._queue_supported(queue):
            return {"success": False, "message": "当前队列结构不支持手动插队"}
        if bool(getattr(queue, "_disabled", False)):
            return {"success": False, "message": "插件当前处于降级模式，暂不可手动插队"}
        promoted = self._promote_queue_items(queue, target_keys)
        if promoted:
            logger.info(
                f"【订阅优先整理】手动插队项目 {str(project)[:24]}：已提升 {promoted} 个普通任务"
            )
        return {
            "success": True,
            "promoted": promoted,
            "message": f"已将 {promoted} 个等待中的普通任务排到订阅之后",
        }

    def get_state(self) -> bool:
        return self._enabled

    @staticmethod
    def get_command() -> list:
        return []

    def get_api(self) -> List[Dict[str, Any]]:
        """注册 MP v2 页面刷新与手动插队 API。"""
        return [
            {
                "path": "/queue",
                "endpoint": self.api_queue,
                "methods": ["GET"],
                "summary": "查看当前整理项目",
                "description": "返回当前整理队列快照摘要，页面按钮用其触发刷新",
            },
            {
                "path": "/promote",
                "endpoint": self.api_promote,
                "methods": ["POST"],
                "summary": "手动插队整理项目",
                "description": "将指定项目中等待的非订阅任务提升到订阅之后",
            },
        ]

    @staticmethod
    def get_service() -> list:
        return []

    def get_page(self) -> List[dict]:
        """MP v2 Vuetify 详情页：合并显示当前项目，并提供手动插队。"""
        if not self._enabled:
            return [{
                "component": "VAlert",
                "props": {"type": "info", "variant": "tonal"},
                "text": "插件未启用，请先在设置中开启。",
            }]

        try:
            view = self._build_queue_view()
        except Exception as e:
            logger.warning(f"【订阅优先整理】构造队列页面失败：{e}")
            return [{
                "component": "VAlert",
                "props": {"type": "error", "variant": "tonal"},
                "text": "读取当前整理队列失败，请稍后刷新。",
            }]

        header = {
            "component": "div",
            "props": {"class": "d-flex justify-space-between align-center flex-wrap"},
            "content": [
                {
                    "component": "p",
                    "props": {"class": "text-h6 mb-0"},
                    "text": (
                        f"当前整理项目：{len(view['groups'])} 个｜"
                        f"任务：{len(view['records'])} 个"
                    ),
                },
                {
                    "component": "VBtn",
                    "props": {
                        "size": "small",
                        "variant": "tonal",
                        "prepend-icon": "mdi-refresh",
                    },
                    "text": "刷新队列",
                    "events": {"click": self._api_event("/queue")},
                },
            ],
        }
        content = [header]

        if not view["supported"]:
            content.append({
                "component": "VAlert",
                "props": {"type": "warning", "variant": "tonal", "class": "mt-3"},
                "text": (
                    "当前整理队列结构不支持安全查看/手动插队；"
                    "插件核心整理功能不受影响。"
                ),
            })
            return [{"component": "div", "content": content}]

        if view.get("degraded"):
            content.append({
                "component": "VAlert",
                "props": {"type": "warning", "variant": "tonal", "class": "mt-3"},
                "text": "插件当前处于降级模式，暂不提供手动插队；正在整理与队列查看不受影响。",
            })

        if not view["groups"]:
            content.append({
                "component": "VAlert",
                "props": {"type": "info", "variant": "tonal", "class": "mt-3"},
                "text": "当前没有正在等待或执行的整理任务。",
            })
            return [{"component": "div", "content": content}]

        for group in view["groups"]:
            season_text = (
                f"S{int(group['season']):02d}" if group["season"] is not None else "电影/季号未知"
            )
            state_text = f"正在整理 {group['running']}｜等待 {group['waiting']}"
            source_text = f"订阅 {group['subscribe']}｜普通 {group['normal']}"
            if group["manual"]:
                source_text += f"｜已手动插队 {group['manual']}"
            episode_text = self._format_episodes(group["episodes"])

            card_content = [
                {
                    "component": "VCardTitle",
                    "text": group["title"],
                },
                {
                    "component": "VCardText",
                    "props": {"class": "text-body-2", "style": "white-space:pre-wrap;"},
                    "text": f"{season_text} · {episode_text}\n{state_text}\n{source_text}",
                },
            ]
            if group["promotable"] and not view.get("degraded"):
                card_content.append({
                    "component": "VCardActions",
                    "content": [{
                        "component": "VBtn",
                        "props": {
                            "size": "small",
                            "variant": "tonal",
                            "color": "primary",
                            "prepend-icon": "mdi-fast-forward",
                        },
                        "text": f"手动插队（{group['promotable']} 个普通任务）",
                        "events": {
                            "click": self._api_event(
                                "/promote", method="post",
                                params={"project": group["project_key"]},
                            )
                        },
                    }],
                })
            elif group["manual"]:
                card_content.append({
                    "component": "VCardText",
                    "props": {"class": "text-caption text-primary pt-0"},
                    "text": "已手动插队；订阅任务仍然排在它前面。",
                })

            content.append({
                "component": "VCard",
                "props": {"variant": "tonal", "class": "mt-3"},
                "content": card_content,
            })

        return [{"component": "div", "content": content}]

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
                                                "订阅任务仍排在整理队列最前；数据页会合并显示等待中与正在整理的项目，"
                                                "同剧同季自动合并，并支持将等待中的普通任务手动插队到订阅之后。"
                                                "适合 HDD 大盘机：订阅想尽快看，手动任务慢慢搬。"
                                                "插件不影响正在整理中的任务，任务零丢失，停用后自动降级为 FIFO。"
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

    def stop_service(self):
        """
        插件停止：切换为降级 FIFO 模式，不交换队列引用，不丢任务。
        """
        self._restore_queue()
        if self._jump_notify_lock and self._jump_notified_hash:
            with self._jump_notify_lock:
                self._jump_notified_hash.clear()
