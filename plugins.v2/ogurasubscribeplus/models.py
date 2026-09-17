from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime
import re
from typing import Any, Dict, List, Optional


_TIME_PATTERN = re.compile(r"^(?:[01]?\d|2[0-3]):[0-5]\d$")


def normalize_scan_times(value: Any) -> List[str]:
    """规范化用户指定的每日扫描时刻，格式 HH:mm、去重并排序。"""
    values = value if isinstance(value, (list, tuple)) else _as_list(value)
    normalized = set()
    for item in values:
        text = str(item or "").strip()
        if not _TIME_PATTERN.match(text):
            continue
        hour, minute = text.split(":")
        normalized.add(f"{int(hour):02d}:{int(minute):02d}")
    # UI 限制最多 8 次；后端同样收口，避免 API 绕过导致任务膨胀。
    return sorted(normalized)[:8]


def legacy_cron_to_scan_times(value: Any) -> List[str]:
    """迁移旧版每日 Cron；不支持的表达式安全回退默认时刻。"""
    parts = str(value or "").strip().split()
    if len(parts) != 5 or any(part != "*" for part in parts[2:]):
        return ["09:00"]
    minute, hour = parts[:2]
    if not minute.isdigit() or not 0 <= int(minute) <= 59:
        return ["09:00"]
    hours: List[int] = []
    if hour.isdigit():
        hours = [int(hour)] if 0 <= int(hour) <= 23 else []
    elif hour.startswith("*/") and hour[2:].isdigit() and int(hour[2:]) > 0:
        hours = list(range(0, 24, int(hour[2:])))
    elif all(token.isdigit() and 0 <= int(token) <= 23 for token in hour.split(",")):
        hours = [int(token) for token in hour.split(",")]
    return normalize_scan_times([f"{item}:{int(minute):02d}" for item in hours]) or ["09:00"]


def _as_list(value: Any) -> List[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    if isinstance(value, str):
        return [item.strip() for item in value.split(",") if item.strip()]
    return []


@dataclass
class PluginConfig:
    enabled: bool = False
    delay_days: int = 1
    # 旧 Cron 仅用于升级迁移；新版统一使用 scan_times 的明确每日时刻。
    cron: str = "0 9 * * *"
    scan_times: List[str] = field(default_factory=lambda: ["09:00"])
    # 系统订阅刷新运行时，本插件每 5 分钟检查一次；0 = 关闭让行。
    defer_on_system_refresh: bool = True
    system_refresh_retry_minutes: int = 5
    selected_categories: List[str] = field(default_factory=list)
    search_sites: List[str] = field(default_factory=list)
    max_scan_subscribes: int = 20
    # 保留 notify_tg 兼容旧配置；notifications_enabled 控制插件所有通知。
    notify_tg: bool = True
    notifications_enabled: bool = True
    notify_scan_complete: bool = False
    allow_tg_rule_update: bool = False
    season_pack_cleanup: str = "off"
    season_pack_full_download: bool = False
    candidate_cache_days: int = 3
    # 批量扫描时相邻订阅之间的缓冲秒数，用于避开限流站点（如观众 CF 盾）；0 = 不缓冲
    search_interval: int = 0
    # 新增：Telegram 白名单用户 ID（逗号分隔的数字列表）
    tg_user_ids: Optional[str] = None

    @classmethod
    def from_dict(cls, raw: Optional[Dict[str, Any]]) -> "PluginConfig":
        raw = raw or {}
        config = cls()
        for key in asdict(config):
            if key in raw:
                setattr(config, key, raw[key])

        config.enabled = bool(config.enabled)
        config.delay_days = max(0, int(config.delay_days or 0))
        # 有 scan_times 时以新版明确时刻为准；仅旧配置时迁移 cron，保持可升级。
        config.scan_times = normalize_scan_times(raw.get("scan_times")) if "scan_times" in raw else legacy_cron_to_scan_times(config.cron)
        config.defer_on_system_refresh = bool(config.defer_on_system_refresh)
        # 与 MP Spider 刷新冲突时固定每 5 分钟检查一次，避免暴露难以理解的调参项。
        config.system_refresh_retry_minutes = 5
        config.selected_categories = [str(item) for item in _as_list(config.selected_categories)]
        config.search_sites = [str(item) for item in _as_list(config.search_sites)]
        config.max_scan_subscribes = max(1, int(config.max_scan_subscribes or 1))
        if "notifications_enabled" not in raw:
            config.notifications_enabled = config.notify_tg
        config.notifications_enabled = bool(config.notifications_enabled)
        config.notify_scan_complete = bool(config.notify_scan_complete)
        config.allow_tg_rule_update = bool(config.allow_tg_rule_update)
        config.season_pack_full_download = bool(config.season_pack_full_download)
        config.candidate_cache_days = max(0, int(config.candidate_cache_days or 0))
        config.search_interval = max(0, int(config.search_interval or 0))
        from .season_cleanup import normalize_cleanup_mode

        config.season_pack_cleanup = normalize_cleanup_mode(config.season_pack_cleanup)
        config.cron = str(config.cron or "0 9 * * *")
        # 解析白名单用户 ID
        config.tg_user_ids = raw.get("tg_user_ids", None) or None
        return config

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class StaleEpisode:
    season: int
    episode: int
    air_date: str
    evidence: str = "未在媒体库缓存或整理历史中命中"

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class DiagnosisInput:
    subscribe_id: int
    title: str
    tmdbid: int
    season: int
    category: str
    include: str = ""
    sites: List[str] = field(default_factory=list)
    episodes: List[StaleEpisode] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        data = asdict(self)
        data["episodes"] = [episode.to_dict() for episode in self.episodes]
        return data


@dataclass
class DiagnosisItem:
    subscribe_id: int
    title: str
    tmdbid: int
    season: int
    category: str
    reason: str
    message: str
    episodes: List[Dict[str, Any]] = field(default_factory=list)
    candidates: List[Dict[str, Any]] = field(default_factory=list)
    sites: List[str] = field(default_factory=list)
    site_names: List[str] = field(default_factory=list)
    source: str = ""
    original_reason: str = ""
    subscription_sites: List[str] = field(default_factory=list)
    subscription_site_names: List[str] = field(default_factory=list)
    subscription_site_progress: List[Dict[str, Any]] = field(default_factory=list)
    search_keyword_suggestion: str = ""
    created_at: str = field(default_factory=lambda: datetime.now().isoformat(timespec="seconds"))

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class InteractionState:
    token: str
    diagnosis: Dict[str, Any]
    view: str = "main"
    stack: List[str] = field(default_factory=list)
    expires_at: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)
