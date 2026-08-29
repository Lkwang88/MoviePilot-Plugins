# -*- coding: utf-8 -*-
"""
小仓酱的种子监控助手 (MoviePilot V2)
===================================

读取 MoviePilot V2 种子缓存，发现「新发布且媒体库/下载历史没有」的影视资源，通过 MP 通知通道提醒。

核心场景：不错过新剧集发布（漏订/新剧）。
- 数据源：TorrentsChain().get_torrents()（MP 订阅刷新抓取后落盘的缓存，零额外站点请求）
- 判定：首次看到（签名去重）+ 发布年龄 ≤ 窗口 + 媒体库没有 + 下载历史没有
- 电影洗版：库内有旧版 + 新种带 DV/HDR/4K → 提醒（v1 保守策略，不解析库内画质）
- 通知：聚合（上限可配），走 MessageType.Plugin 通道，无图无链接，中文名优先用缓存里的

设计原则：轻量、低负担、补漏。允许漏报，不允许打扰。
兼容：MoviePilot V2（app/chain/torrents.py 的 TorrentsChain、app/schemas/context.py 的 Context/TorrentInfo/MediaInfo）
"""
import re
import threading
import time
import uuid
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Set, Tuple

from app.chain.torrents import TorrentsChain
from app.core.event import Event, eventmanager
from app.log import logger
from app.plugins import _PluginBase
from app.schemas.types import EventType, MediaType, NotificationType

# 版本
PLUGIN_VERSION = "0.1.0"

# 画质等级（数字越大越好；v1 仅用于通知文案展示）
# 4=DV+HDR(P8) 3=DV 2=HDR10+ 1=HDR 0=SDR/未知
DV_HDR_RE = re.compile(r"(?i)\bdv\b[._+ ]?hdr|\bhdr\b[._+ ]?\bdv\b|dolby[._ ]?vision[._ ]?hdr|hdr[._ ]?dolby[._ ]?vision")
DV_RE = re.compile(r"(?i)\bdv\b|dolby[._ ]?vision|dovi")
HDR10P_RE = re.compile(r"(?i)hdr10\+|hdr[._ ]?plus")
HDR_RE = re.compile(r"(?i)\bhdr(10)?\b|high[._ ]?dynamic[._ ]?range")
# 洗版信号：DV+HDR / DV / HDR / 4K
_UPGRADE_SIGNAL_RE = re.compile(
    r"(?i)\bdv\b|dolby[._ ]?vision|dovi|"
    r"\bhdr(10\+?)?\b|2160p|4k"
)


def _quality_level(title: str, description: str = "") -> int:
    """从种子标题/描述解析画质等级（仅用于文案展示）"""
    text = f"{title} {description or ''}"
    has_dv = bool(DV_RE.search(text))
    has_hdr = bool(HDR_RE.search(text))
    if has_dv and has_hdr:
        return 4
    if has_dv:
        return 3
    if HDR10P_RE.search(text):
        return 2
    if has_hdr:
        return 1
    return 0


def _pub_minutes_of(pubdate: Optional[str]) -> Optional[int]:
    """解析 v2 的 pubdate 字符串（YYYY-MM-DD HH:MM:SS）为发布距今分钟数；无法解析返回 None"""
    if not pubdate:
        return None
    try:
        pub = datetime.strptime(str(pubdate).strip(), "%Y-%m-%d %H:%M:%S")
        return int((datetime.now() - pub).total_seconds() // 60)
    except Exception:
        return None


class SeedWatch(_PluginBase):
    """小仓酱的种子监控助手"""

    # 插件元信息
    plugin_name = "小仓酱的种子监控助手"
    plugin_desc = "监控 MP 种子缓存：新发布的剧集/电影（漏订或新剧）及时提醒，电影出更好版本（DV+HDR 优先）也提醒。轻量低负担，纯读缓存零抓取。"
    plugin_icon = "https://raw.githubusercontent.com/Lkwang88/MoviePilot-Plugins/main/icons/SeedWatch.png"
    plugin_version = PLUGIN_VERSION
    plugin_author = "Lkwang88"
    author_url = "https://github.com/Lkwang88"
    plugin_config_prefix = "seedwatch."
    plugin_order = 35
    auth_level = 1

    # 数据键
    _SEEN_KEY = "seen"
    _NOTIFY_LOG_KEY = "notify_log"
    _QUEUE_KEY = "pending_queue"
    _RUN_KEY = "last_run"

    # 上限
    _MAX_SEEN = 5000
    _MAX_NOTIFY_LOG = 100
    _MAX_PENDING = 200

    # 运行状态（实例级）
    _enabled = False
    _scan_interval = 20
    _pub_window = 120
    _site_filter = ""
    _notify_new_episode = True
    _notify_new_movie = True
    _notify_upgrade = True
    _aggregate_max = 10
    _exclude_downloaded = True
    _was_enabled = False

    _lock = None
    _running = False

    def init_plugin(self, config: dict = None):
        """生效配置"""
        was_enabled = bool(getattr(self, "_was_enabled", False))
        self._lock = threading.Lock()
        self._enabled = False
        self._scan_interval = 20
        self._pub_window = 120
        self._site_filter = ""
        self._notify_new_episode = True
        self._notify_new_movie = True
        self._notify_upgrade = True
        self._aggregate_max = 10
        self._exclude_downloaded = True
        run_once = False
        clear_seen = False
        if config:
            self._enabled = bool(config.get("enabled"))
            self._scan_interval = int(config.get("scan_interval") or 20)
            self._pub_window = int(config.get("pub_window") or 120)
            self._site_filter = str(config.get("site_filter") or "").strip()
            self._notify_new_episode = bool(config.get("notify_new_episode", True))
            self._notify_new_movie = bool(config.get("notify_new_movie", True))
            self._notify_upgrade = bool(config.get("notify_upgrade", True))
            self._aggregate_max = int(config.get("aggregate_max") or 10)
            self._exclude_downloaded = bool(config.get("exclude_downloaded", True))
            # 一次性开关（保存后执行并自动复位）
            run_once = bool(config.get("run_once"))
            clear_seen = bool(config.get("clear_seen"))
        self._was_enabled = self._enabled
        logger.info(f"【种子监控】插件{'已启用' if self._enabled else '未启用'}")

        # 处理一次性开关：清空已见记录
        if clear_seen:
            self._clear_seen()
        # 需要扫描：勾了"立即扫描"或刚从停用变为启用（含 MP 重启后自动补扫）
        should_scan = run_once or (self._enabled and not was_enabled)
        if should_scan and self._enabled:
            self.scan()
        # 复位一次性开关
        if run_once or clear_seen:
            try:
                cfg = dict(config or {})
                cfg["run_once"] = False
                cfg["clear_seen"] = False
                self.update_config(cfg)
            except Exception as e:
                logger.warning(f"【种子监控】复位一次性开关失败：{e}")

    # ------------------------------------------------------------------ 状态
    def get_state(self) -> bool:
        return self._enabled

    @staticmethod
    def get_command() -> list:
        return []

    @staticmethod
    def get_api() -> list:
        return []

    def get_service(self) -> list:
        """注册自定时扫描服务"""
        if not self._enabled:
            return []
        return [
            {
                "id": "SeedWatch_Scan",
                "name": "种子监控扫描",
                "trigger": "interval",
                "func": self.scan,
                "kwargs": {"minutes": max(1, self._scan_interval)},
            }
        ]

    def stop_service(self):
        """插件停止：无私有调度器需要清理"""
        pass

    # ------------------------------------------------------------------ 配置
    def get_form(self):
        """插件配置页面（Vuetify JSON）"""
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
                                            "title": "📡 小仓酱的种子监控助手",
                                            "text": (
                                                "读取 MP 种子缓存，发现新发布的剧集/电影（未订阅或漏订）及时提醒；"
                                                "电影出更好版本（DV+HDR 优先）也提醒。"
                                                "纯读缓存零抓取，不打扰站点。"
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
                                        "component": "VTextField",
                                        "props": {
                                            "model": "scan_interval",
                                            "label": "扫描间隔（分钟）",
                                            "type": "number",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "pub_window",
                                            "label": "发布窗口（分钟）",
                                            "type": "number",
                                        },
                                    }
                                ],
                            },
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "site_filter",
                                            "label": "站点白名单（逗号分隔域名，空=全部）",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "aggregate_max",
                                            "label": "单条通知上限（条）",
                                            "type": "number",
                                        },
                                    }
                                ],
                            },
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
                                        "props": {
                                            "model": "notify_new_episode",
                                            "label": "新剧集通知",
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
                                            "model": "notify_new_movie",
                                            "label": "新电影通知",
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
                                            "model": "notify_upgrade",
                                            "label": "电影洗版通知",
                                        },
                                    }
                                ],
                            },
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "exclude_downloaded",
                                            "label": "排除已下载（下载历史）",
                                        },
                                    }
                                ],
                            },
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "run_once",
                                            "label": "立即扫描一次（保存后执行并自动复位）",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "clear_seen",
                                            "label": "清空已见记录（保存后执行并自动复位）",
                                        },
                                    }
                                ],
                            },
                        ],
                    },
                ],
            }
        ], {
            "enabled": False,
            "scan_interval": 20,
            "pub_window": 120,
            "site_filter": "",
            "notify_new_episode": True,
            "notify_new_movie": True,
            "notify_upgrade": True,
            "aggregate_max": 10,
            "exclude_downloaded": True,
            "run_once": False,
            "clear_seen": False,
        }

    def get_page(self) -> Optional[list]:
        """详情页：暂无（v0.1 只读数据在运行报告里）"""
        return None

    # ------------------------------------------------------------------ 数据
    def _get_data(self, key: str, default: Any = None) -> Any:
        try:
            value = self.get_data(key)
            return default if value is None else value
        except Exception as e:
            logger.warning(f"【种子监控】读取数据 {key} 失败：{e}")
            return default

    def _save_data(self, key: str, value: Any) -> None:
        try:
            self.save_data(key, value)
        except Exception as e:
            logger.warning(f"【种子监控】保存数据 {key} 失败：{e}")

    def _clear_seen(self):
        """清空已见记录、通知历史与待处理队列（一次性开关触发）"""
        try:
            self.save_data(self._SEEN_KEY, [])
            self.save_data(self._NOTIFY_LOG_KEY, [])
            self.save_data(self._QUEUE_KEY, [])
            logger.info("【种子监控】已清空已见记录/通知历史/待处理队列")
            try:
                self.post_message(
                    mtype=NotificationType.Plugin,
                    title="🧹 种子监控已清空记录",
                    text="已见种子标记、通知历史、待处理队列已全部清空。\n下次扫描将把所有缓存种子视为新种子重新对比。",
                )
            except Exception:
                pass
        except Exception as e:
            logger.warning(f"【种子监控】清空记录失败：{e}")

    # ------------------------------------------------------------------ 扫描
    def scan(self):
        """主扫描入口（定时服务/一次性开关调用）"""
        if not self._enabled:
            logger.debug("【种子监控】插件未启用，跳过扫描")
            return
        with self._lock:
            if self._running:
                logger.debug("【种子监控】上一轮扫描未结束，跳过")
                return
            self._running = True
        report = self._new_run_report()
        try:
            self._do_scan(report)
            report["success"] = True
        except Exception as e:
            report["error"] = str(e)
            logger.error(f"【种子监控】扫描异常：{e}")
        finally:
            report["finished_at"] = self._now_iso()
            self._save_data(self._RUN_KEY, report)
            with self._lock:
                self._running = False

    def _do_scan(self, report: dict):
        """扫描核心"""
        # 1. 读缓存
        try:
            torrents_map = TorrentsChain().get_torrents()
        except Exception as e:
            report["error"] = f"读取种子缓存失败：{e}"
            logger.error(f"【种子监控】读取种子缓存失败：{e}")
            return
        if not torrents_map:
            report["result"] = "empty_cache"
            return

        # 站点过滤
        allow_domains = {d.strip() for d in self._site_filter.split(",") if d.strip()}
        all_contexts: List[Any] = []
        for domain, contexts in torrents_map.items():
            if allow_domains and domain not in allow_domains:
                continue
            if not contexts:
                continue
            all_contexts.extend(contexts)
        report["source_count"] = len(all_contexts)
        if not all_contexts:
            report["result"] = "no_candidates"
            return

        # 2. 已见签名
        seen: Set[str] = set(self._get_data(self._SEEN_KEY, []) or [])
        seen_added = 0
        # 3. 命中收集
        hits: List[dict] = []

        for context in all_contexts:
            torrent = getattr(context, "torrent_info", None)
            if not torrent or not torrent.title:
                continue
            signature = self._signature(torrent)
            if signature in seen:
                continue
            # 首次看到 → 加入已见
            seen.add(signature)
            seen_added += 1
            # 发布窗口判定（首次看到时）
            pub_minutes = _pub_minutes_of(torrent.pubdate)
            if pub_minutes is None:
                continue
            if pub_minutes > self._pub_window:
                continue
            report["window_passed"] += 1
            # 分类
            category = self._category_of(torrent, context)
            if category not in (MediaType.MOVIE.value, MediaType.TV.value):
                continue
            # 站点名（供文案）
            site_name = torrent.site_name or ""
            # 命中判定
            hit = self._evaluate(context, torrent, category)
            if hit:
                hit["site_name"] = site_name
                hit["pub_minutes"] = int(pub_minutes)
                hits.append(hit)
                report["hits"] += 1

        # 保存已见（有界）
        if seen_added:
            if len(seen) > self._MAX_SEEN:
                seen = set(list(seen)[-self._MAX_SEEN:])
            self._save_data(self._SEEN_KEY, list(seen))
        report["seen_added"] = seen_added

        # 4. 通知（聚合）
        if hits:
            self._notify_hits(hits, report)
        else:
            report["result"] = "no_hits"

    @staticmethod
    def _signature(torrent) -> str:
        """种子唯一签名"""
        return f"{torrent.title}|{torrent.description or ''}|{torrent.enclosure or ''}"

    @staticmethod
    def _category_of(torrent, context) -> Optional[str]:
        """种子分类：优先 torrent.category，其次 context.media_info.type（v2 都是中文字符串）"""
        cat = getattr(torrent, "category", None)
        if cat in (MediaType.MOVIE.value, MediaType.TV.value):
            return cat
        media = getattr(context, "media_info", None)
        mtype = getattr(media, "type", None)
        if mtype in (MediaType.MOVIE.value, MediaType.TV.value):
            return mtype
        return None

    # ------------------------------------------------------------------ 判定
    def _evaluate(self, context, torrent, category: str) -> Optional[dict]:
        """对单个种子做命中判定，返回命中信息或 None"""
        # 中文名
        title_cn = self._cn_title(context)
        # 媒体身份（v2：tmdb_id 等字段）
        media_ids = self._media_ids(context)
        # 媒体库查重
        exists, exists_info = self._library_exists(context, category, media_ids)
        if exists:
            # 库内已有 → 只有电影洗版才可能命中
            if category != MediaType.MOVIE.value or not self._notify_upgrade:
                return None
            # 洗版对比（保守：新种带 DV/HDR/4K 就提醒）
            if not _UPGRADE_SIGNAL_RE.search(f"{torrent.title} {torrent.description or ''}"):
                return None
            return {
                "kind": "upgrade",
                "title": title_cn,
                "raw_title": torrent.title,
                "size": torrent.size,
                "category": category,
                "quality": _quality_level(torrent.title, torrent.description),
                "reason": f"库内有旧版（{exists_info.get('desc', '')}），新种更好",
            }
        # 库内没有 → 新剧/新电影
        if category == MediaType.TV.value:
            if not self._notify_new_episode:
                return None
            kind = "episode"
            reason = "新剧集（未订阅或漏订）"
        else:
            if not self._notify_new_movie:
                return None
            kind = "movie"
            reason = "新电影（未订阅或漏订）"
        # 下载历史排除（任一媒体 ID 存在即查）
        if self._exclude_downloaded and any(media_ids.values()):
            if self._downloaded(media_ids):
                return None
        return {
            "kind": kind,
            "title": title_cn,
            "raw_title": torrent.title,
            "size": torrent.size,
            "category": category,
            "quality": _quality_level(torrent.title, torrent.description),
            "reason": reason,
        }

    def _cn_title(self, context) -> str:
        """中文名：优先 media_info.title（中文），降级原标题"""
        media = getattr(context, "media_info", None)
        if media:
            title = getattr(media, "title", None)
            if title:
                return title
            names = getattr(media, "names", None) or []
            if names:
                return names[0]
        torrent = getattr(context, "torrent_info", None)
        return getattr(torrent, "title", "") or "未知"

    @staticmethod
    def _media_ids(context) -> Dict[str, Any]:
        """提取媒体身份字段（v2 MediaInfo 分开存）"""
        media = getattr(context, "media_info", None)
        if not media:
            return {}
        return {
            "tmdb_id": getattr(media, "tmdb_id", None),
            "douban_id": getattr(media, "douban_id", None),
            "imdb_id": getattr(media, "imdb_id", None),
            "bangumi_id": getattr(media, "bangumi_id", None),
            "anilist_id": getattr(media, "anilist_id", None),
        }

    def _library_exists(self, context, category, media_ids: dict) -> Tuple[bool, dict]:
        """媒体库查重。返回 (是否存在, 存在信息dict)"""
        try:
            media = getattr(context, "media_info", None)
            if not media:
                return False, {}
            # 用 media_exists 判断库里是否有该作品（返回 ExistMediaInfo 或 None）
            # v2：_PluginBase 不继承 ChainBase，媒体库操作走 self.chain
            exist_info = self.chain.media_exists(mediainfo=media)
            if not exist_info:
                return False, {}
            # 库里有 → 构造存在信息
            seasons = (exist_info.seasons or {}) if hasattr(exist_info, "seasons") else {}
            desc = "已存在"
            if category == MediaType.TV.value and seasons:
                desc = f"已有{len(seasons)}季"
            return True, {
                "desc": desc,
                "seasons": seasons,
            }
        except Exception as e:
            logger.warning(f"【种子监控】媒体库查重失败：{e}")
            return False, {}

    def _downloaded(self, media_ids: dict) -> bool:
        """下载历史是否有记录（v2 DownloadHistoryOper.get_by_mediaid）"""
        try:
            from app.db.downloadhistory_oper import DownloadHistoryOper
            records = DownloadHistoryOper().get_by_mediaid(
                tmdbid=media_ids.get("tmdb_id"),
                doubanid=media_ids.get("douban_id"),
                bangumiid=media_ids.get("bangumi_id"),
                anilistid=media_ids.get("anilist_id"),
            )
            return bool(records)
        except Exception as e:
            logger.warning(f"【种子监控】下载历史查询失败：{e}")
            return False

    # ------------------------------------------------------------------ 通知
    def _notify_hits(self, hits: List[dict], report: dict):
        """聚合通知"""
        limit = max(1, min(50, self._aggregate_max))
        batch = hits[:limit]
        overflow = hits[limit:]
        if overflow:
            self._append_pending(overflow)
            report["queued"] = len(overflow)
        if batch:
            self._post_batch(batch)
            report["notified"] = len(batch)
            self._log_notify(batch)
        report["result"] = "notified"

    def _post_batch(self, hits: List[dict]):
        """发送聚合通知"""
        lines = []
        for hit in hits:
            size = self._fmt_size(hit.get("size"))
            site = hit.get("site_name") or ""
            pub = hit.get("pub_minutes", 0)
            lines.append(
                f"• 《{hit['title']}》 {size}\n"
                f"  {site} | 发布{pub}分钟前 | {hit.get('reason', '')}"
            )
        title = f"🎬 种子监控：{len(hits)} 条新资源"
        text = "\n".join(lines)
        try:
            self.post_message(mtype=NotificationType.Plugin, title=title, text=text)
        except Exception as e:
            logger.warning(f"【种子监控】通知发送失败：{e}")

    @staticmethod
    def _fmt_size(size) -> str:
        try:
            size = float(size or 0)
            if size <= 0:
                return "?"
            units = ["B", "KB", "MB", "GB", "TB"]
            i = 0
            while size >= 1024 and i < len(units) - 1:
                size /= 1024
                i += 1
            return f"{size:.1f}{units[i]}"
        except Exception:
            return "?"

    def _log_notify(self, hits: List[dict]):
        """记录通知历史"""
        log = self._get_data(self._NOTIFY_LOG_KEY, []) or []
        now = self._now_iso()
        for hit in hits:
            log.append({
                "time": now,
                "title": hit.get("title"),
                "raw_title": hit.get("raw_title"),
                "kind": hit.get("kind"),
                "site": hit.get("site_name", ""),
                "reason": hit.get("reason", ""),
            })
        log = log[-self._MAX_NOTIFY_LOG:]
        self._save_data(self._NOTIFY_LOG_KEY, log)

    def _append_pending(self, hits: List[dict]):
        """超出上限的进待处理队列"""
        queue = self._get_data(self._QUEUE_KEY, []) or []
        queue.extend(hits)
        queue = queue[-self._MAX_PENDING:]
        self._save_data(self._QUEUE_KEY, queue)

    # ------------------------------------------------------------------ 报告
    def _new_run_report(self) -> dict:
        return {
            "id": uuid.uuid4().hex,
            "started_at": self._now_iso(),
            "finished_at": None,
            "success": None,
            "result": None,
            "error": None,
            "source_count": 0,
            "seen_added": 0,
            "window_passed": 0,
            "hits": 0,
            "notified": 0,
            "queued": 0,
        }

    @staticmethod
    def _now_iso() -> str:
        return datetime.now().astimezone().isoformat(timespec="seconds")
