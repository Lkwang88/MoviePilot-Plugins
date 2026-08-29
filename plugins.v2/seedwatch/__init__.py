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
import copy
import re
import threading
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional, Set, Tuple

from app.chain.torrents import TorrentsChain
from app.log import logger
from app.plugins import _PluginBase
from app.schemas.types import MediaType, NotificationType

# 版本
PLUGIN_VERSION = "0.1.8"

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


# 分辨率等级：4K/UHD/2160p=3，1080p=2，720p=1，其他=0
_RES_4K_RE = re.compile(r"(?i)\b(2160p|4k|uhd)\b")
_RES_1080_RE = re.compile(r"(?i)\b1080p\b")
_RES_720_RE = re.compile(r"(?i)\b720p\b")

_QUALITY_TAGS = {0: "SDR", 1: "HDR", 2: "HDR10+", 3: "DV", 4: "DV+HDR"}


def _resolution_level(text: str) -> int:
    """从文本解析分辨率等级 4K=3 / 1080p=2 / 720p=1 / 未知=0"""
    text = text or ""
    if _RES_4K_RE.search(text):
        return 3
    if _RES_1080_RE.search(text):
        return 2
    if _RES_720_RE.search(text):
        return 1
    return 0


def _res_label(level: int) -> str:
    return {3: "4K", 2: "1080p", 1: "720p"}.get(level, "?")


def _quality_label(spec: Tuple[int, int]) -> str:
    """画质标签：(画质等级, 分辨率等级) → 'DV+HDR 4K'"""
    q, r = spec
    return f"{_QUALITY_TAGS.get(q, 'SDR')} {_res_label(r)}"


# --------------------------------------------------------------------- 洗版维度
# 白名单版本（三档，P8 > DV > HDR 在每档内排序）：
#   L3: 4K REMUX → L2: 1080p REMUX → L1: 4K WEB-DL
# 排除项（直接滚）：ISO 原盘、种子名带 DIY 的（自改原盘）
_ISO_RE = re.compile(r"(?i)\biso\b")
_DIY_RE = re.compile(r"(?i)\bdiy\b")

# 版本档位：300=4K REMUX, 200=1080p REMUX, 100=4K WEB-DL, 0=不在白名单
_TIER_REMUX = re.compile(r"(?i)remux")
_TIER_WEBDL = re.compile(r"(?i)web[ -]?dl")


def _version_tier(text: str) -> Tuple[int, str]:
    """从文本（新种标题或库内路径）识别白名单档位"""
    text = text or ""
    res = _resolution_level(text)
    if _TIER_REMUX.search(text):
        if res == 3:
            return 300, "4K REMUX"
        if res == 2:
            return 200, "1080p REMUX"
        return 0, ""
    if _TIER_WEBDL.search(text):
        if res >= 3:    # 4K/UHD
            return 100, "4K WEB-DL"
        return 0, ""    # 1080p WEB-DL 不在白名单
    return 0, ""


def _upgrade_score(text: str) -> Tuple[int, str]:
    """检查是否是洗版白名单版本（4K REMUX / 1080p REMUX / 4K WEB-DL，P8/DV/HDR）
    返回 (分数, 版本标签)；不在白名单（0）或其他垃圾（-1）
    大小写不敏感
    """
    text = text or ""
    # 排除项：ISO / DIY
    if _ISO_RE.search(text) or _DIY_RE.search(text):
        return -1, ""
    tier, tier_label = _version_tier(text)
    if tier <= 0:
        return 0, ""    # 不在白名单
    q = _quality_level(text, "")
    if q == 0:       # SDR 不在白名单
        return 0, ""
    score = tier + (4 if q == 4 else 3 if q == 3 else 1 if q in (1, 2) else 0)
    label = f"{tier_label} {_QUALITY_TAGS.get(q, '?')}"
    return score, label


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
    _debug = False

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
        self._debug = False
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
            self._debug = bool(config.get("debug"))
            run_once = bool(config.get("run_once"))
            clear_seen = bool(config.get("clear_seen"))
        self._was_enabled = self._enabled
        logger.info(f"【种子监控】插件{'已启用' if self._enabled else '未启用'}")
        if self._debug:
            logger.info(f"【种子监控-DEBUG】进入调试模式（debug=True）")

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
                                            "model": "debug",
                                            "label": "调试日志（观察扫描过程）",
                                            "hint": "开启后日志显示每轮扫描的站点数据、新增签名、过滤原因和命中数"
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
            "debug": False,
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

    def _save_data(self, key: str, value: Any) -> bool:
        """保存插件数据；成功返回 True，失败返回 False。"""
        try:
            self.save_data(key, value)
            return True
        except Exception as e:
            logger.warning(f"【种子监控】保存数据 {key} 失败：{e}")
            return False

    def _clear_seen(self):
        """清空已见记录、通知历史，并清理旧版遗留队列数据。"""
        try:
            self.save_data(self._SEEN_KEY, [])
            self.save_data(self._NOTIFY_LOG_KEY, [])
            self.save_data(self._QUEUE_KEY, [])
            logger.info("【种子监控】已清空已见记录/通知历史")
            try:
                self.post_message(
                    mtype=NotificationType.Plugin,
                    title="🧹 种子监控已清空记录",
                    text="已见种子标记和通知历史已清空。\n下次扫描将把所有缓存种子视为新种子重新对比。",
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
            report["success"] = not bool(report.get("error"))
        except Exception as e:
            report["success"] = False
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
            if self._debug:
                logger.info("【种子监控-DEBUG】扫描完成：种子缓存为空（扫描任务正常运行）")
            return

        # 站点过滤
        allow_domains = {d.strip() for d in self._site_filter.split(",") if d.strip()}
        all_contexts: List[Tuple[str, Any]] = []
        site_stats: Dict[str, Dict[str, int]] = {}
        for domain, contexts in torrents_map.items():
            if allow_domains and domain not in allow_domains:
                continue
            site_stats.setdefault(domain, {
                "count": len(contexts or []),
                "new_sign": 0, "seen": 0, "no_date": 0,
                "over_window": 0, "wrong_cat": 0, "hit": 0,
            })
            if not contexts:
                continue
            all_contexts.extend((domain, context) for context in contexts)
        report["source_count"] = len(all_contexts)
        if not all_contexts:
            report["result"] = "no_candidates"
            if self._debug:
                logger.info(
                    "【种子监控-DEBUG】扫描完成：缓存有数据，但站点白名单筛选后为 0"
                )
            return

        # 2. 已见签名：读取失败必须中止，不能把失败误当空列表导致全量重复通知。
        try:
            seen_value = self.get_data(self._SEEN_KEY)
        except Exception as e:
            report["error"] = f"读取已见记录失败；为避免重复通知，本轮停止：{e}"
            report["result"] = "seen_read_failed"
            logger.warning(f"【种子监控】读取已见记录失败，本轮停止以避免重复通知：{e}")
            return
        seen_list: List[str] = seen_value or []
        if not isinstance(seen_list, list):
            report["error"] = "已见记录格式异常；为避免重复通知，本轮停止"
            report["result"] = "seen_data_invalid"
            logger.warning("【种子监控】已见记录格式异常，本轮停止以避免重复通知")
            return
        seen_set: Set[str] = set(seen_list)
        seen_added = 0
        # 3. 命中收集
        hits: List[dict] = []
        # 跨辅助函数累计诊断数据，最终统一汇总，禁止逐候选刷屏。
        self._scan_diag = {
            "empty_media": 0,
            "empty_title": 0,
            "type_repaired": 0,
            "type_conflict": 0,
            "lookup_error": 0,
            "download_error": 0,
        }

        debug_skips: List[str] = []
        for domain, context in all_contexts:
            torrent = getattr(context, "torrent_info", None)
            if not torrent or not torrent.title:
                continue
            signature = self._signature(torrent)
            site_name = torrent.site_name or domain
            if signature in seen_set:
                site_stats[domain]["seen"] += 1
                continue
            # 首次看到 → 加入已见
            seen_set.add(signature)
            seen_list.append(signature)
            seen_added += 1
            site_stats[domain]["new_sign"] += 1
            # 发布窗口判定（首次看到时）
            pub_minutes = _pub_minutes_of(torrent.pubdate)
            if pub_minutes is None:
                site_stats[domain]["no_date"] += 1
                continue
            if pub_minutes > self._pub_window:
                site_stats[domain]["over_window"] += 1
                continue
            report["window_passed"] += 1
            # 分类
            category = self._category_of(torrent, context)
            if category not in (MediaType.MOVIE.value, MediaType.TV.value):
                site_stats[domain]["wrong_cat"] += 1
                continue
            # 命中判定
            hit = self._evaluate(context, torrent, category)
            if hit:
                hit["site_name"] = site_name
                hit["pub_minutes"] = int(pub_minutes)
                hits.append(hit)
                report["hits"] += 1
                site_stats[domain]["hit"] += 1
            elif self._debug and len(debug_skips) < 3:
                # 仅留前 3 个样本；逐轮可控，避免 DEBUG 反而刷屏。
                debug_skips.append(
                    f"《{self._cn_title(context)}》|{site_name}|发布{int(pub_minutes)}分"
                )

        # DEBUG 默认只打一条短心跳；有新签名时才补变化站点/过滤样本。
        if self._debug:
            totals = {
                key: sum(stats[key] for stats in site_stats.values())
                for key in ("count", "new_sign", "seen", "no_date", "over_window", "wrong_cat", "hit")
            }
            logger.info(
                f"【种子监控-DEBUG】扫描完成：缓存{totals['count']} | 新{totals['new_sign']}"
                f" | 已见{totals['seen']} | 无日期{totals['no_date']}"
                f" | 超窗口{totals['over_window']} | 非影视{totals['wrong_cat']}"
                f" | 命中{totals['hit']}"
            )
            changed_sites = [
                f"{domain}+{stats['new_sign']}"
                f"(超窗{stats['over_window']}/非影视{stats['wrong_cat']}/命中{stats['hit']})"
                for domain, stats in site_stats.items()
                if stats["new_sign"]
            ]
            if changed_sites:
                logger.info(f"【种子监控-DEBUG】变化站点：{'、'.join(changed_sites)}")
            if debug_skips:
                logger.info(f"【种子监控-DEBUG】过滤样本（最多3条）：{'；'.join(debug_skips)}")
            diag = getattr(self, "_scan_diag", {})
            if any(diag.values()):
                logger.info(
                    f"【种子监控-DEBUG】识别诊断：无媒体{diag['empty_media']}"
                    f" | 空标题{diag['empty_title']} | 类型修复{diag['type_repaired']}"
                    f" | 类型冲突{diag['type_conflict']} | 查库失败{diag['lookup_error']}"
                    f" | 历史失败{diag['download_error']}"
                )

        # 保存已见（有界；保留插入顺序，超限时丢最旧的）
        if seen_added:
            if len(seen_list) > self._MAX_SEEN:
                seen_list = seen_list[-self._MAX_SEEN:]
            if not self._save_data(self._SEEN_KEY, seen_list):
                report["error"] = "保存已见记录失败；为避免下轮重复通知，本轮不发送"
                report["result"] = "seen_save_failed"
                return
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
        """种子分类：优先 torrent.category，其次 context.media_info.type；兼容字符串与枚举。"""
        cat = getattr(torrent, "category", None)
        cat_value = getattr(cat, "value", cat)
        if cat_value in (MediaType.MOVIE.value, MediaType.TV.value):
            return cat_value
        media = getattr(context, "media_info", None)
        mtype = getattr(media, "type", None)
        mtype_value = getattr(mtype, "value", mtype)
        if mtype_value in (MediaType.MOVIE.value, MediaType.TV.value):
            return mtype_value
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
        # 查库条件不足或 MP 查询异常时不能推断“库里没有”；保守跳过，避免误报。
        if exists is None:
            return None
        if exists:
            # 库内已有 → 只有电影洗版才可能命中
            if category != MediaType.MOVIE.value or not self._notify_upgrade:
                return None
            # 新种必须在白名单内（4K REMUX / 1080p REMUX / 4K WEB-DL，P8/DV/HDR，ISO/DIY 不要）
            new_score, new_label = _upgrade_score(f"{torrent.title} {torrent.description or ''}")
            if new_score <= 0:
                return None
            # 库内版本打分对比（拿不到 / 打错 → 人工核对）
            old_path = exists_info.get("path") or ""
            if not old_path:
                # 拿不到库内信息 → 人工核对
                return {
                    "kind": "manual",
                    "title": title_cn,
                    "raw_title": torrent.title,
                    "size": torrent.size,
                    "category": category,
                    "reason": "库内有旧版但拿不到详情，请人工核对画质",
                    "media_ids": media_ids,
                }
            old_score, old_label = _upgrade_score(old_path)
            if old_score < 0:
                # 库里是 DIY/ISO → 当无用，不通知
                return None
            # 新种分数严格更高 → 明确升级；同级或低 → 通知不需要
            if new_score <= old_score:
                return None
            return {
                "kind": "upgrade",
                "title": title_cn,
                "raw_title": torrent.title,
                "size": torrent.size,
                "category": category,
                "quality": _quality_level(torrent.title, torrent.description),
                "reason": f"库内 {old_label} → 新种 {new_label}",
                "media_ids": media_ids,
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
            downloaded = self._downloaded(media_ids)
            if downloaded is None:
                return None
            if downloaded:
                return None
        return {
            "kind": kind,
            "title": title_cn,
            "raw_title": torrent.title,
            "size": torrent.size,
            "category": category,
            "quality": _quality_level(torrent.title, torrent.description),
            "reason": reason,
            "media_ids": media_ids,
        }

    def _cn_title(self, context) -> str:
        """中文名：优先 media_info.title（中文）→ names → 副标题提取 → 原标题"""
        media = getattr(context, "media_info", None)
        if media:
            title = getattr(media, "title", None)
            if title:
                return title
            names = getattr(media, "names", None) or []
            if names:
                return names[0]
        torrent = getattr(context, "torrent_info", None)
        # 识别失败兜底：从副标题提取中文名（如「开火开伙 開火開伙 ... 第01季 第01集 | 类型: 真人秀」）
        desc = getattr(torrent, "description", None) or ""
        cn = self._cn_from_description(desc)
        if cn:
            return cn
        return getattr(torrent, "title", "") or "未知"

    # 副标题按空格切词后，取第一个含汉字的词作为候选中文名
    _CJK_RE = re.compile(r"[\u4e00-\u9fff]")
    _CN_TAIL_RE = re.compile(r"(?:第[\d零一二三四五六七八九十百]+[季部集]?)+$")

    @classmethod
    def _cn_from_description(cls, description: str) -> Optional[str]:
        """从副标题提取中文名：按空格切词，在含汉字且不含冒号的词里取最长的，去掉季集尾巴。"""
        if not description:
            return None
        candidates = []
        for token in description.split():
            if ":" in token or "：" in token:
                continue
            if not cls._CJK_RE.search(token):
                continue
            candidates.append(token)
        # 取最长候选，逐条尝试去季集尾巴直到有效
        for token in sorted(candidates, key=len, reverse=True):
            name = cls._CN_TAIL_RE.sub("", token).strip()
            if name and cls._CJK_RE.search(name):
                return name
        return None

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

    def _diag_inc(self, key: str) -> int:
        """增加本轮诊断计数；辅助函数被独立测试调用时也安全。"""
        diag = getattr(self, "_scan_diag", None)
        if not isinstance(diag, dict):
            return 0
        diag[key] = diag.get(key, 0) + 1
        return diag[key]

    def _library_exists(self, context, category, media_ids: dict) -> Tuple[Optional[bool], dict]:
        """媒体库查重。返回 (True=存在 / False=不存在 / None=无法判断, 信息dict)

        存在信息里尽量带上库内版本的画质/分辨率（从 iteminfo 的 path 提取），
        供电影洗版对比使用。拿不到就按未知处理（保守判定：不通知洗版）。
        """
        try:
            media = getattr(context, "media_info", None)
            if not media:
                self._diag_inc("empty_media")
                return None, {}
            # 标题为空会触发 MP 自定义识别词对 None 做正则替换，无法安全查库。
            if not getattr(media, "title", None):
                self._diag_inc("empty_title")
                return None, {}
            # 缓存里偶有 media.type=None；MP media_exists 内部会访问 type.value。
            # 对副本按已确认的种子分类补齐枚举，绝不修改 MP 缓存原对象。
            query_media = copy.copy(media)
            raw_type = getattr(query_media, "type", None)
            raw_value = getattr(raw_type, "value", raw_type)
            # 媒体识别与种子分类冲突时无法安全判定，宁可漏报也不误查/误报。
            if (
                raw_value in (MediaType.MOVIE.value, MediaType.TV.value)
                and raw_value != category
            ):
                self._diag_inc("type_conflict")
                return None, {}
            # 类型缺失/旧格式字符串时，用已经确认的种子分类规范成 MP 所需枚举。
            expected_type = (
                MediaType.MOVIE if category == MediaType.MOVIE.value
                else MediaType.TV if category == MediaType.TV.value
                else None
            )
            if not expected_type:
                return None, {}
            # MP v2 的 media_exists 要求 MediaType 枚举（内部直接访问 type.value）。
            # 在副本上规范化，既兼容 None/中文字符串，也不篡改缓存原对象。
            if raw_type != expected_type:
                query_media.type = expected_type
                self._diag_inc("type_repaired")
            # 用 media_exists 判断库里是否有该作品（返回 ExistMediaInfo 或 None）
            # v2：_PluginBase 不继承 ChainBase，媒体库操作走 self.chain
            exist_info = self.chain.media_exists(mediainfo=query_media)
            if not exist_info:
                return False, {}
            # 库里有 → 取画质信息（iteminfo 的 path 含分辨率/编码等）
            info: Dict[str, Any] = {
                "desc": "已存在",
                "seasons": (exist_info.seasons or {}) if hasattr(exist_info, "seasons") else {},
                "quality": 0,
                "resolution": 0,
                "path": None,
            }
            try:
                server = getattr(exist_info, "server", None)
                itemid = getattr(exist_info, "itemid", None)
                if server and itemid:
                    detail = self.chain.iteminfo(server=server, item_id=itemid)
                    path = getattr(detail, "path", None) if detail else None
                    if path:
                        info["path"] = path
                        info["quality"] = _quality_level(path, "")
                        info["resolution"] = _resolution_level(path)
            except Exception as e:
                # 拿不到画质不影响存在判定，仅降级为未知
                logger.debug(f"【种子监控】获取库内条目画质失败：{e}")
            if category == MediaType.TV.value and info["seasons"]:
                info["desc"] = f"已有{len(info['seasons'])}季"
            return True, info
        except Exception as e:
            count = self._diag_inc("lookup_error")
            # 同轮只报第 1 次，后续由 DEBUG 汇总计数，避免同一异常刷屏。
            if count <= 1:
                logger.warning(f"【种子监控】媒体库查重失败，本轮同类错误将合并：{e}")
            return None, {}

    def _downloaded(self, media_ids: dict) -> Optional[bool]:
        """下载历史三态：True=有记录，False=确认没有，None=查询失败。"""
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
            count = self._diag_inc("download_error")
            if count <= 1:
                logger.warning(f"【种子监控】下载历史查询失败，本轮同类错误将合并：{e}")
            return None

    # ------------------------------------------------------------------ 通知
    # 单条消息字符数保护线：超过则把该组再对半拆成多条消息
    _MAX_MSG_CHARS = 1800
    _KIND_LABELS = (
        ("episode", "📺 新剧集"),
        ("movie", "🎞️ 新电影"),
        ("upgrade", "🔄 洗版候选"),
        ("manual", "🤔 人工核对"),
    )

    def _notify_hits(self, hits: List[dict], report: dict):
        """聚合通知：保留分类块；按单条总作品上限/字符保护线装箱，避免每类强拆一条。"""
        merged = self._merge_hits(hits)
        report["merged_count"] = len(merged)
        title = f"🎬 种子监控：{len(merged)} 部作品 / {len(hits)} 个种子"
        chunks = self._pack_notification_chunks(merged)
        total = len(chunks)
        sent_items: List[dict] = []
        failed_messages = 0
        for idx, items in enumerate(chunks, start=1):
            message_title = title
            if total > 1:
                message_title += f"（{idx}/{total}）"
            try:
                self.post_message(
                    mtype=NotificationType.Plugin,
                    title=message_title,
                    text=self._format_mixed_groups(items),
                )
                sent_items.extend(items)
            except Exception as e:
                failed_messages += 1
                logger.warning(f"【种子监控】通知发送失败：{e}")
        if sent_items:
            self._log_notify(sent_items)
        report["messages"] = total - failed_messages
        report["message_failures"] = failed_messages
        report["notified"] = len(sent_items)
        if not failed_messages:
            report["result"] = "notified"
        elif sent_items:
            report["result"] = "notify_partial_failed"
            report["error"] = f"{failed_messages}/{total} 条通知发送失败"
        else:
            report["result"] = "notify_failed"
            report["error"] = f"全部 {total} 条通知发送失败"

    def _pack_notification_chunks(self, merged: List[dict]) -> List[List[dict]]:
        """按类别顺序装箱；每箱不超过配置条数，也尽量不超过字符保护线。"""
        groups: Dict[str, List[dict]] = {kind: [] for kind, _ in self._KIND_LABELS}
        for item in merged:
            groups.setdefault(item["kind"], []).append(item)
        ordered = [
            item
            for kind, _ in self._KIND_LABELS
            for item in (groups.get(kind) or [])
        ]
        # 兼容未来新增类别，避免静默漏通知。
        known_kinds = {kind for kind, _ in self._KIND_LABELS}
        ordered.extend(item for item in merged if item.get("kind") not in known_kinds)

        limit = max(1, min(50, self._aggregate_max))
        chunks: List[List[dict]] = []
        current: List[dict] = []
        for item in ordered:
            candidate = current + [item]
            too_many = len(candidate) > limit
            too_long = len(self._format_mixed_groups(candidate)) > self._MAX_MSG_CHARS
            if current and (too_many or too_long):
                chunks.append(current)
                current = [item]
            else:
                current = candidate
        if current:
            chunks.append(current)
        return chunks

    def _format_mixed_groups(self, items: List[dict]) -> str:
        """把一箱作品渲染成多个分类块，同一条通知内保留清晰分区。"""
        groups: Dict[str, List[dict]] = {kind: [] for kind, _ in self._KIND_LABELS}
        for item in items:
            groups.setdefault(item["kind"], []).append(item)
        blocks = [
            self._format_group(label, groups[kind])
            for kind, label in self._KIND_LABELS
            if groups.get(kind)
        ]
        known_kinds = {kind for kind, _ in self._KIND_LABELS}
        for item in items:
            if item.get("kind") not in known_kinds:
                blocks.append(self._format_group(str(item.get("kind") or "其他"), [item]))
        return "\n\n".join(blocks)

    @staticmethod
    def _media_key(hit: dict) -> str:
        """媒体合并键：优先媒体 ID，无 ID 用标题"""
        ids = hit.get("media_ids") or {}
        for k in ("tmdb_id", "douban_id", "bangumi_id", "anilist_id", "imdb_id"):
            v = ids.get(k)
            if v:
                return f"{k}:{v}"
        return f"title:{hit.get('title') or hit.get('raw_title')}"

    def _merge_hits(self, hits: List[dict]) -> List[dict]:
        """同一作品的多个种子合并为一条（保留首次出现顺序）"""
        merged_map: Dict[str, dict] = {}
        order_keys: List[str] = []
        for hit in hits:
            key = f"{hit.get('kind')}|{self._media_key(hit)}"
            item = merged_map.get(key)
            if item is None:
                item = {
                    "kind": hit["kind"],
                    "title": hit["title"],
                    "category": hit.get("category"),
                    "reason": hit.get("reason", ""),
                    "torrent_count": 0,
                    "sites": [],
                    "sizes": [],
                    "pub_min": None,
                    "raw_titles": [],
                }
                merged_map[key] = item
                order_keys.append(key)
            item["torrent_count"] += 1
            site = hit.get("site_name") or ""
            if site and site not in item["sites"]:
                item["sites"].append(site)
            if hit.get("size"):
                try:
                    item["sizes"].append(float(hit["size"]))
                except Exception:
                    pass
            pub = hit.get("pub_minutes")
            if pub is not None and (item["pub_min"] is None or pub < item["pub_min"]):
                item["pub_min"] = pub
            item["raw_titles"].append(hit.get("raw_title", ""))
        return [merged_map[k] for k in order_keys]

    def _format_group(self, label: str, items: List[dict]) -> str:
        """渲染一个类别块"""
        lines = [f"{label}（{len(items)}）"]
        for item in items:
            sites = "/".join(item["sites"]) or "未知站"
            count = item["torrent_count"]
            torrent_desc = f"{count}个种子" if count > 1 else "1个种子"
            sizes = item["sizes"]
            if not sizes:
                size_desc = "?"
            else:
                min_size = self._fmt_size(min(sizes))
                max_size = self._fmt_size(max(sizes))
                size_desc = min_size if min_size == max_size else f"{min_size}~{max_size}"
            pub = item.get("pub_min")
            if pub is None:
                pub_desc = ""
            elif count > 1:
                pub_desc = f" | 最近发布{pub}分钟前"
            else:
                pub_desc = f" | 发布{pub}分钟前"
            lines.append(
                f"• 《{item['title']}》{torrent_desc} · {sites} | {size_desc}{pub_desc}"
            )
        return "\n".join(lines)

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

    def _log_notify(self, merged: List[dict]):
        """记录通知历史（合并后的条目）"""
        log = self._get_data(self._NOTIFY_LOG_KEY, []) or []
        now = self._now_iso()
        for item in merged:
            log.append({
                "time": now,
                "title": item.get("title"),
                "kind": item.get("kind"),
                "site": "/".join(item.get("sites") or []),
                "reason": item.get("reason", ""),
                "torrent_count": item.get("torrent_count", 1),
            })
        log = log[-self._MAX_NOTIFY_LOG:]
        self._save_data(self._NOTIFY_LOG_KEY, log)

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
            "merged_count": 0,
            "messages": 0,
            "message_failures": 0,
            "notified": 0,
        }

    @staticmethod
    def _now_iso() -> str:
        return datetime.now().astimezone().isoformat(timespec="seconds")
