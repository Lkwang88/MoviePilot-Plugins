# -*- coding: utf-8 -*-
"""
小仓酱的下载助手（OguraDownloadHelper）
MoviePilot V2 插件：手动下载去重拦截 + 提示 + TG 交互放行

功能：
1. 手动下载时，综合「下载历史 / 整理历史 / 媒体服务器同步」数据判定是否已拥有，
   命中则拦截下载并通知，防止下重。
2. 通知带 TG 交互按钮（独立 bot）：「继续下载 / 取消」，支持一次性放行。
3. 洗版对比提示：展示旧种子名、旧体积 vs 新体积，辅助用户判断是否为同一文件。
4. 订阅 / RSS 等自动下载场景不拦截（原生判定已足够准确）。
"""
from datetime import datetime, timedelta
import json
import re
import sqlite3
import threading
import time
import traceback
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

from app.core.event import Event, eventmanager
from app.log import logger
from app.plugins import _PluginBase
from app.schemas.event import ResourceDownloadEventData
from app.schemas.types import ChainEventType, EventType, MediaType, NotificationType

# telebot 可选导入（MP 依赖自带 pyTelegramBotAPI，此处防御性处理）
try:
    from telebot import TeleBot
    from telebot.types import InlineKeyboardButton, InlineKeyboardMarkup
    TELEBOT_AVAILABLE = True
except Exception:
    TeleBot = None
    InlineKeyboardButton = None
    InlineKeyboardMarkup = None
    TELEBOT_AVAILABLE = False

# 回调数据前缀
_CB_BYPASS = "ddg:bypass:"
_CB_CANCEL = "ddg:cancel:"

# 体积差异提示阈值（默认 5% 以内视为体积相近）
_DEFAULT_VOLUME_RATIO = 0.05


class OguraDownloadHelper(_PluginBase):
    """小仓酱的下载助手"""

    # 插件元数据
    plugin_name = "小仓酱的下载助手"
    plugin_desc = "手动下载去重守护：综合下载/整理/媒体库数据，拦截重复下载并提供洗版对比提示与TG交互放行。"
    plugin_icon = "Moviepilot_A.png"
    plugin_version = "1.0.4"
    plugin_author = "Lkwang88"
    author_url = "https://github.com/Lkwang88"
    plugin_config_prefix = "oguradownloadhelper_"
    plugin_order = 60
    auth_level = 1

    # 运行状态
    _enabled = False
    _tg_bot_token = ""
    _tg_chat_ids = ""
    _tg_use_mp_channel = True
    _notify = True
    _match_mode = "identity"      # identity: 媒体身份优先 / title: 标题年份 / soft: 仅提示不拦截
    _bypass_hours = 24            # 放行有效期（小时）
    _volume_ratio = _DEFAULT_VOLUME_RATIO
    _disk_prefixes = ["/ptdownload/"]  # 盘路径前缀（用于显示精简与盘名提取）

    _bot = None
    _bot_thread = None
    _bot_stop = threading.Event()
    _db_path: Optional[Path] = None
    _lock = threading.Lock()

    def init_plugin(self, config: dict = None):
        """生效配置"""
        config = config or {}
        self._enabled = bool(config.get("enabled"))
        self._tg_bot_token = (config.get("tg_bot_token") or "").strip()
        self._tg_chat_ids = (config.get("tg_chat_ids") or "").strip()
        self._tg_use_mp_channel = bool(config.get("tg_use_mp_channel", True))
        self._notify = bool(config.get("notify", True))
        self._match_mode = config.get("match_mode") or "identity"
        self._bypass_hours = int(config.get("bypass_hours") or 24)
        self._volume_ratio = float(config.get("volume_ratio") or _DEFAULT_VOLUME_RATIO)
        prefixes = (config.get("disk_prefixes") or "").strip()
        self._disk_prefixes = [p.strip() for p in prefixes.replace("，", ",").split(",") if p.strip()] \
            or ["/ptdownload/"]

        # 数据库初始化
        self._db_path = self.get_data_path() / "dedup.db"
        self._init_db()

        # 启停 TG bot
        if self._enabled:
            self._start_tg_bot()
        else:
            self._stop_tg_bot()

        # 首次启动后台全量导入本地缓存（空库 或 缓存不健康时）
        if self._enabled and self._db_path:
            try:
                count = self._count_media_records()
                if count == 0 or not self._cache_healthy():
                    if count == 0:
                        logger.info(f"【{self.plugin_name}】本地缓存为空，触发全量重建")
                    else:
                        logger.info(f"【{self.plugin_name}】缓存缺少落盘体积数据，触发全量重建")
                    threading.Thread(target=self._rebuild_cache, daemon=True).start()
            except Exception as err:
                logger.error(f"【{self.plugin_name}】检查本地缓存失败：{err}")

    def get_state(self) -> bool:
        return self._enabled

    # ==================== 数据库 ====================

    def _get_conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self._db_path), timeout=10)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self):
        """初始化本地数据库表"""
        if not self._db_path:
            return
        try:
            conn = self._get_conn()
            try:
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS media_records (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        media_source TEXT,
                        media_id TEXT,
                        title TEXT NOT NULL,
                        year TEXT,
                        mtype TEXT,
                        seasons TEXT,
                        episodes TEXT,
                        source_type TEXT,
                        torrent_name TEXT,
                        size REAL,
                        status TEXT,
                        date TEXT,
                        seasoninfo TEXT
                    )
                    """
                )
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS block_records (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        media_title TEXT,
                        media_year TEXT,
                        mtype TEXT,
                        media_source TEXT,
                        media_id TEXT,
                        seasons TEXT,
                        episodes TEXT,
                        torrent_title TEXT,
                        torrent_size REAL,
                        reason TEXT,
                        origin TEXT,
                        status TEXT DEFAULT 'blocked',
                        bypass_token TEXT,
                        created_at TEXT,
                        expires_at TEXT
                    )
                    """
                )
                conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_mr_identity ON media_records(media_source, media_id)"
                )
                conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_mr_title ON media_records(title, year)"
                )
                conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_br_status ON block_records(status)"
                )
                conn.commit()
            finally:
                conn.close()
        except Exception as err:
            logger.error(f"【{self.plugin_name}】初始化数据库失败：{err}")

    def _count_media_records(self) -> int:
        try:
            conn = self._get_conn()
            try:
                row = conn.execute("SELECT COUNT(*) AS c FROM media_records").fetchone()
                return row["c"] if row else 0
            finally:
                conn.close()
        except Exception:
            return 0

    def _cache_healthy(self) -> bool:
        """
        缓存健康检查：
        有 transfer 记录 且 全部带落盘体积 → 健康
        无 transfer 记录 或 存在无体积的 transfer（旧缓存）→ 不健康，需重建
        """
        try:
            conn = self._get_conn()
            try:
                tr_count = conn.execute(
                    "SELECT COUNT(*) AS c FROM media_records WHERE source_type='transfer'"
                ).fetchone()["c"]
                tr_no_size = conn.execute(
                    "SELECT COUNT(*) AS c FROM media_records WHERE source_type='transfer' "
                    "AND (size IS NULL OR size <= 0)"
                ).fetchone()["c"]
                return tr_count > 0 and tr_no_size == 0
            finally:
                conn.close()
        except Exception:
            return False

    def _rebuild_cache(self):
        """全量重建本地媒体缓存（从系统表导入，简单可靠）"""
        try:
            logger.info(f"【{self.plugin_name}】开始全量重建本地缓存...")
            from app.db import SessionFactory
            from app.db.models import DownloadHistory, TransferHistory
            from sqlalchemy import select

            conn = self._get_conn()
            dl_count = 0
            tr_count = 0
            err_count = 0
            try:
                conn.execute("DELETE FROM media_records")
                now = time.strftime("%Y-%m-%d %H:%M:%S")
                with SessionFactory() as db:
                    # 下载历史（未删除的下载记录视为已知，但体积不作数——可能被取消）
                    dhs = db.execute(select(DownloadHistory)).scalars().all()
                    for dh in dhs:
                        try:
                            conn.execute(
                                "INSERT INTO media_records (media_source, media_id, title, year, mtype, "
                                "seasons, episodes, source_type, torrent_name, size, status, date) "
                                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                                (
                                    dh.media_source, dh.media_id, dh.title or "", dh.year,
                                    dh.type, dh.seasons, dh.episodes, "download",
                                    dh.torrent_name, None,
                                    "downloaded", dh.date or now,
                                ),
                            )
                            dl_count += 1
                        except Exception as e:
                            err_count += 1
                            logger.error(f"【{self.plugin_name}】导入下载记录失败({dh.title}): {e}")
                    # 整理历史（成功入库的，体积 = 文件清单汇总，真正落盘）
                    ths = db.execute(
                        select(TransferHistory).where(TransferHistory.status.is_(True))
                    ).scalars().all()
                    for th in ths:
                        try:
                            size = (
                                self._sum_file_sizes(th.files)
                                or self._sum_file_sizes(th.dest_fileitem)
                                or self._sum_file_sizes(th.src_fileitem)
                            )
                            conn.execute(
                                "INSERT INTO media_records (media_source, media_id, title, year, mtype, "
                                "seasons, episodes, source_type, torrent_name, size, status, date) "
                                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                                (
                                    th.media_source, th.media_id, th.title or "", th.year,
                                    th.type, th.seasons, th.episodes, "transfer",
                                    th.src, size, "transferred", th.date or now,
                                ),
                            )
                            tr_count += 1
                        except Exception as e:
                            err_count += 1
                            logger.error(f"【{self.plugin_name}】导入整理记录失败({th.title}): {e}")
                conn.commit()
            finally:
                conn.close()
            logger.info(
                f"【{self.plugin_name}】本地缓存重建完成：下载 {dl_count} 条，整理 {tr_count} 条"
                + (f"，失败 {err_count} 条" if err_count else "")
            )
        except Exception as err:
            logger.error(f"【{self.plugin_name}】本地缓存重建失败：{err}\n{traceback.format_exc()}")

    # ==================== 判定引擎 ====================

    @staticmethod
    def _resolve_identity(media: Any) -> Tuple[Optional[str], Optional[str]]:
        """从 MediaInfo 解析媒体身份 (source, media_id)，优先复用 MP 统一解析"""
        if not media:
            return None, None
        try:
            from app.utils.media import resolve_media_identity
            return resolve_media_identity(media=media)
        except Exception:
            pass
        source = getattr(media, "source", None) or None
        media_id = getattr(media, "media_id", None) or None
        # 未显式设置时按各数据源 ID 推断
        source_ids = {
            "themoviedb": getattr(media, "tmdb_id", None),
            "douban": getattr(media, "douban_id", None),
            "bangumi": getattr(media, "bangumi_id", None),
            "anilist": getattr(media, "anilist_id", None),
        }
        if not source or not media_id:
            for src, mid in source_ids.items():
                if mid is not None:
                    source = source or src
                    media_id = media_id or str(mid)
                    break
        return source, media_id

    @staticmethod
    def _parse_episodes(eps_str: Optional[str]) -> Optional[Set[int]]:
        """解析 "E01,E03-E05" 形式的集数字符串为集合"""
        if not eps_str:
            return None
        result: Set[int] = set()
        try:
            for part in re.split(r"[,，]", str(eps_str)):
                part = part.strip()
                m = re.match(r"E?(\d+)\s*[-~]\s*E?(\d+)$", part, re.IGNORECASE)
                if m:
                    a, b = int(m.group(1)), int(m.group(2))
                    result.update(range(min(a, b), max(a, b) + 1))
                else:
                    m2 = re.match(r"E?(\d+)$", part, re.IGNORECASE)
                    if m2:
                        result.add(int(m2.group(1)))
        except Exception:
            return None
        return result or None

    @staticmethod
    def _parse_season(season_str: Optional[str]) -> Optional[int]:
        """解析 "S01" 或 1 为 1"""
        if season_str is None:
            return None
        if isinstance(season_str, int):
            return season_str if season_str > 0 else None
        m = re.match(r"S(\d+)", str(season_str), re.IGNORECASE)
        return int(m.group(1)) if m else None

    @staticmethod
    def _format_size(size_bytes: Optional[float]) -> str:
        """体积（字节）转人类可读"""
        if not size_bytes:
            return "未知"
        try:
            size = float(size_bytes)
            units = ["B", "KB", "MB", "GB", "TB", "PB"]
            idx = 0
            while size >= 1024 and idx < len(units) - 1:
                size /= 1024
                idx += 1
            if idx == 0:
                return f"{int(size)} {units[idx]}"
            return f"{size:.2f} {units[idx]}"
        except Exception:
            return "未知"

    @staticmethod
    def _sum_file_sizes(item: Any) -> Optional[float]:
        """递归汇总 FileItem/文件清单中的 size（字节）"""
        total = 0.0
        if isinstance(item, dict):
            size = item.get("size")
            if isinstance(size, (int, float)) and size > 0:
                total += float(size)
            for v in item.values():
                total += OguraDownloadHelper._sum_file_sizes(v) or 0
        elif isinstance(item, (list, tuple, set)):
            for v in item:
                total += OguraDownloadHelper._sum_file_sizes(v) or 0
        return total or None

    def _query_records(self, source: Optional[str], media_id: Optional[str],
                       title: Optional[str], year: Optional[str],
                       mtype: Optional[str], media: Any) -> List[dict]:
        """
        查询已知媒体记录（本地缓存优先，系统表兜底）
        身份查询与标题查询「合并」执行并去重——避免 media_id 缺失/不一致导致漏记录
        """
        records: List[dict] = []
        seen = set()

        def _merge(recs: List[dict]):
            for r in recs:
                key = (r.get("source_type"), r.get("torrent_name"),
                       r.get("date"), r.get("seasons"), r.get("episodes"))
                if key in seen:
                    continue
                seen.add(key)
                records.append(r)

        # ---- 1. 本地缓存 ----
        try:
            conn = self._get_conn()
            try:
                if source and media_id:
                    rows = conn.execute(
                        "SELECT * FROM media_records WHERE media_source=? AND media_id=?",
                        (source, str(media_id)),
                    ).fetchall()
                    _merge([dict(r) for r in rows])
                if title:
                    rows = conn.execute(
                        "SELECT * FROM media_records WHERE title=?",
                        (title,),
                    ).fetchall()
                    _merge([dict(r) for r in rows])
                    if not records:
                        rows = conn.execute(
                            "SELECT * FROM media_records WHERE title LIKE ?",
                            (f"%{title}%",),
                        ).fetchall()
                        _merge([dict(r) for r in rows])
            finally:
                conn.close()
        except Exception as err:
            logger.debug(f"【{self.plugin_name}】本地缓存查询失败：{err}")

        if records:
            return records

        # ---- 2. 系统表兜底（本地缓存可能滞后） ----
        try:
            from app.db import SessionFactory
            from app.db.models import DownloadHistory, MediaServerItem, TransferHistory
            from sqlalchemy import or_, select

            with SessionFactory() as db:
                # 下载历史：身份 + 标题 合并去重
                dh_rows = []
                if source and media_id:
                    dh_rows += list(db.execute(
                        select(DownloadHistory).where(
                            DownloadHistory.media_source == source,
                            DownloadHistory.media_id == str(media_id),
                        )
                    ).scalars().all())
                if title:
                    dh_rows += list(db.execute(
                        select(DownloadHistory).where(
                            DownloadHistory.title == title,
                            DownloadHistory.year == year,
                        )
                    ).scalars().all())
                seen_dh = set()
                for dh in dh_rows:
                    k = id(dh)
                    if k in seen_dh:
                        continue
                    seen_dh.add(k)
                    records.append({
                        "media_source": dh.media_source, "media_id": dh.media_id,
                        "title": dh.title, "year": dh.year, "mtype": dh.type,
                        "seasons": dh.seasons, "episodes": dh.episodes,
                        "source_type": "download", "torrent_name": dh.torrent_name,
                        "size": None, "status": "downloaded", "date": dh.date,
                    })
                # 整理历史：身份 + 标题 合并去重
                th_rows = []
                if source and media_id:
                    th_rows += list(db.execute(
                        select(TransferHistory).where(
                            TransferHistory.media_source == source,
                            TransferHistory.media_id == str(media_id),
                        )
                    ).scalars().all())
                if title:
                    th_rows += list(db.execute(
                        select(TransferHistory).where(
                            TransferHistory.title == title,
                            TransferHistory.year == year,
                        )
                    ).scalars().all())
                seen_th = set()
                for th in th_rows:
                    k = id(th)
                    if k in seen_th:
                        continue
                    seen_th.add(k)
                    size = (
                        self._sum_file_sizes(th.files)
                        or self._sum_file_sizes(th.dest_fileitem)
                        or self._sum_file_sizes(th.src_fileitem)
                    )
                    records.append({
                        "media_source": th.media_source, "media_id": th.media_id,
                        "title": th.title, "year": th.year, "mtype": th.type,
                        "seasons": th.seasons, "episodes": th.episodes,
                        "source_type": "transfer", "torrent_name": th.src,
                        "size": size, "status": "transferred" if th.status else "failed",
                        "date": th.date,
                    })
                # 媒体服务器条目（V2 用 tmdbid/imdbid/tvdbid）
                tmdb_id = getattr(media, "tmdb_id", None)
                imdb_id = getattr(media, "imdb_id", None)
                tvdb_id = getattr(media, "tvdb_id", None)
                ms_conds = []
                if tmdb_id:
                    ms_conds.append(MediaServerItem.tmdbid == int(tmdb_id))
                if imdb_id:
                    ms_conds.append(MediaServerItem.imdbid == str(imdb_id))
                if tvdb_id:
                    ms_conds.append(MediaServerItem.tvdbid == str(tvdb_id))
                if ms_conds:
                    ms_rows = db.execute(
                        select(MediaServerItem).where(or_(*ms_conds))
                    ).scalars().all()
                    for ms in ms_rows:
                        records.append({
                            "media_source": None, "media_id": None,
                            "title": ms.title, "year": ms.year, "mtype": ms.item_type,
                            "seasons": None, "episodes": None,
                            "source_type": "mediaserver", "torrent_name": ms.path,
                            "size": None, "status": "transferred", "date": ms.lst_mod_date,
                            "seasoninfo": ms.seasoninfo,
                        })
        except Exception as err:
            logger.debug(f"【{self.plugin_name}】系统表查询失败：{err}")

        return records

    # ==================== 去重判定 ====================

    def _dedup_check(self, context: Any) -> Optional[dict]:
        """
        去重判定核心：综合本地缓存与系统表，返回命中信息或 None
        :return: {reason, records, old_text, volume_hint}
        """
        media = getattr(context, "media_info", None)
        meta = getattr(context, "meta_info", None)
        torrent = getattr(context, "torrent_info", None)
        if not media:
            return None

        title = getattr(media, "title", None) or ""
        year = getattr(media, "year", None) or None
        mtype = getattr(media, "type", None)
        mtype_str = mtype.value if hasattr(mtype, "value") else (mtype or None)

        # 软模式：仅提示不拦截（由调用方决定）
        if self._match_mode == "soft":
            source, media_id = self._resolve_identity(media)
            records = self._query_records(source, media_id, title, year, mtype_str, media)
            if not records:
                return None
            return {
                "reason": "本地已存在该媒体（软模式仅提示）",
                "records": records,
                "old_text": self._build_old_text(records),
                "volume_hint": self._volume_hint(records, torrent),
            }

        # 硬拦截模式
        source, media_id = self._resolve_identity(media)
        records = self._query_records(source, media_id, title, year, mtype_str, media)
        if not records:
            return None

        # 剧集粒度判定（电视剧）
        season = self._parse_season(getattr(meta, "season", None)) if meta else None
        episodes = None
        if meta:
            eps = getattr(meta, "episode_list", None)
            if isinstance(eps, set):
                episodes = set(int(e) for e in eps if e)
            elif isinstance(eps, list):
                episodes = set(int(e) for e in eps if e)

        if mtype_str == "tv" or mtype_str == "电视剧":
            dup, reason = self._tv_dup_check(records, season, episodes)
            if not dup:
                return None
            return {
                "reason": reason,
                "records": records,
                "old_text": self._build_old_text(records),
                "volume_hint": self._volume_hint(records, torrent),
                "season": season,
                "episodes": sorted(episodes) if episodes else None,
            }

        # 电影 / 其他：命中即重复
        return {
            "reason": "本地已存在该媒体",
            "records": records,
            "old_text": self._build_old_text(records),
            "volume_hint": self._volume_hint(records, torrent),
        }

    def _tv_dup_check(self, records: List[dict], season: Optional[int],
                      episodes: Optional[Set[int]]) -> Tuple[bool, str]:
        """电视剧季/集粒度判定"""
        # 汇总已有季集
        existing: Dict[int, Optional[Set[int]]] = {}  # season -> episodes(None=整季)
        any_whole_show = False  # 存在无季信息的下载/整理记录（整剧级别）
        for rec in records:
            s = self._parse_season(rec.get("seasons"))
            if s is None:
                # 媒体库记录：解析 seasoninfo {季: [集]}
                si = rec.get("seasoninfo") or {}
                if isinstance(si, str):
                    try:
                        si = json.loads(si)
                    except Exception:
                        si = {}
                if si:
                    for ss, eplist in si.items():
                        try:
                            ss = int(ss)
                        except Exception:
                            continue
                        eps = set(int(e) for e in (eplist or [])) if eplist else None
                        if ss not in existing:
                            existing[ss] = eps or None
                        elif existing[ss] is not None and eps:
                            existing[ss] = existing[ss] | eps
                    continue
                # 下载/整理记录无季信息 → 整剧已有
                if rec.get("source_type") in ("download", "transfer"):
                    any_whole_show = True
                continue
            eps = self._parse_episodes(rec.get("episodes"))
            if s not in existing:
                existing[s] = eps
            elif existing[s] is not None and eps is not None:
                existing[s] = existing[s] | eps

        # 整剧已有（无季信息）且本次也无季信息 → 保守拦截
        if any_whole_show and season is None:
            return True, "本地已有该剧的下载/整理记录（未能确认具体季），已保守拦截"

        # 无任何季信息 → 不判定（可能是异常数据）
        if not existing:
            return False, ""

        # 新下载无季信息 → 保守拦截（防止下重，可一键放行）
        if season is None:
            return True, "本地已存在该媒体（未能识别本次请求的具体季），已保守拦截"

        if season not in existing:
            return False, ""

        have_eps = existing[season]
        # 已有整季 → 任何该季请求都重复
        if have_eps is None:
            return True, f"本地已存在 {season} 季完整资源"
        # 新请求为整季包 → 重复
        if not episodes:
            return True, f"本地已存在 {season} 季（{len(have_eps)} 集）"
        # 新请求 ⊆ 已有 → 重复
        if episodes <= have_eps:
            return True, f"本地已存在 {season} 季 E{min(have_eps)}-E{max(have_eps)}，本次请求的集数均在其中"
        # 部分重叠 → 提示（仍拦截，防止重复下已有的部分）
        overlap = episodes & have_eps
        if overlap:
            return True, (f"本地已存在 {season} 季 E{min(have_eps)}-E{max(have_eps)}，"
                          f"本次请求有 {len(overlap)} 集已存在（如 E{sorted(overlap)[:5]} 等）")
        return False, ""

    def _disk_of(self, path: Optional[str]) -> str:
        """从路径提取盘名（裁剪前缀后第一段）"""
        p = str(path or "").replace("\\", "/")
        for prefix in self._disk_prefixes:
            prefix = prefix.strip()
            if prefix and p.startswith(prefix):
                rest = p[len(prefix):].lstrip("/")
                return rest.split("/")[0] if rest else ""
        return ""

    def _short_path(self, path: Optional[str], max_parts: int = 3) -> str:
        """路径精简：裁剪盘前缀，保留前 N 段"""
        p = str(path or "").replace("\\", "/")
        for prefix in self._disk_prefixes:
            prefix = prefix.strip()
            if prefix and p.startswith(prefix):
                p = p[len(prefix):].lstrip("/")
                break
        parts = [x for x in p.split("/") if x]
        if not parts:
            return ""
        if len(parts) > max_parts:
            return "/".join(parts[:max_parts]) + "/…"
        return "/".join(parts)

    def _build_old_text(self, records: List[dict]) -> str:
        """构造已有资源描述文本（合并统计，避免逐文件刷屏）"""
        lines = []

        # 下载历史：取最新一条
        dl = [r for r in records if r.get("source_type") == "download"]
        if dl:
            latest = max(dl, key=lambda r: r.get("date") or "")
            name = latest.get("torrent_name") or latest.get("title") or ""
            date = (latest.get("date") or "")[:16]
            lines.append(f"· 下载过 {date}：{name}".rstrip())

        # 整理入库：按目录合并统计
        tr = [r for r in records if r.get("source_type") == "transfer"]
        if tr:
            dirs: Dict[str, int] = {}
            for r in tr:
                d = self._short_path(r.get("torrent_name") or r.get("dest") or "")
                if d:
                    dirs[d] = dirs.get(d, 0) + 1
            for d, cnt in list(dirs.items())[:2]:
                lines.append(f"· 已入库 {cnt} 个文件：{d}")
            if len(dirs) > 2:
                lines.append(f"· 等共 {len(dirs)} 个目录")

        # 媒体库
        ms = [r for r in records if r.get("source_type") == "mediaserver"]
        if ms:
            libs = sorted({r.get("torrent_name") or r.get("title") or "" for r in ms})
            lines.append(f"· 媒体库：{'、'.join(libs[:3])}")

        # 所在盘统计
        disks = set()
        for r in records:
            for f in (r.get("torrent_name"), r.get("dest"), r.get("src")):
                d = self._disk_of(f)
                if d:
                    disks.add(d)
        if disks:
            disk_txt = "、".join(sorted(disks))
            lines.append(f"· 所在盘：{disk_txt}" + (f"（{len(disks)} 个盘）" if len(disks) > 1 else ""))

        return "\n".join(lines) or "（无详细信息）"

    def _volume_hint(self, records: List[dict], torrent: Any) -> str:
        """
        新旧体积对比提示（仅提示，不决策）
        旧体积只取「整理入库」记录——整理成功才是真正落盘；下载记录可能被取消，不作数。
        """
        new_size = getattr(torrent, "size", None)  # KB
        # 新体积转字节（与整理记录同单位）
        new_bytes = float(new_size) * 1024 if new_size else None
        old_size = None
        for rec in records:
            if rec.get("source_type") == "transfer" and rec.get("size"):
                old_size = rec.get("size")
                break
        new_txt = self._format_size(new_bytes)
        old_txt = self._format_size(old_size)
        if old_size is None:
            return ""
        if new_bytes is None:
            return f"已落盘体积：{old_txt}（新体积未知）"
        try:
            diff = abs(new_bytes - float(old_size)) / max(float(old_size), 0.001)
            if diff <= self._volume_ratio:
                return f"体积相近（已落盘 {old_txt} / 新 {new_txt}），可能是同一版本"
            return f"体积不同（已落盘 {old_txt} / 新 {new_txt}）"
        except Exception:
            return f"已落盘 {old_txt} / 新 {new_txt}"

    # ==================== 放行机制 ====================

    def _make_bypass_token(self) -> str:
        return uuid.uuid4().hex

    def _add_block_record(self, context: Any, reason: str) -> Optional[dict]:
        """写入拦截记录，返回记录 dict（含 token）"""
        media = getattr(context, "media_info", None)
        meta = getattr(context, "meta_info", None)
        torrent = getattr(context, "torrent_info", None)
        title = getattr(media, "title", None) or ""
        year = getattr(media, "year", None) or None
        source, media_id = self._resolve_identity(media)
        season = self._parse_season(getattr(meta, "season", None)) if meta else None
        episodes = None
        if meta:
            eps = getattr(meta, "episode_list", None)
            if isinstance(eps, (set, list)):
                episodes = ",".join(str(e) for e in sorted(eps)) if eps else None
        torrent_title = getattr(torrent, "title", None) or ""
        torrent_size = getattr(torrent, "size", None)
        token = self._make_bypass_token()
        now = datetime.now()
        expires = now + timedelta(hours=self._bypass_hours)
        record = {
            "media_title": title,
            "media_year": year,
            "mtype": getattr(media, "type", None).value if hasattr(getattr(media, "type", None), "value") else None,
            "media_source": source,
            "media_id": media_id,
            "seasons": f"S{season:02d}" if season is not None else None,
            "episodes": episodes,
            "torrent_title": torrent_title,
            "torrent_size": torrent_size,
            "reason": reason,
            "origin": "Manual",
            "status": "blocked",
            "bypass_token": token,
            "created_at": now.strftime("%Y-%m-%d %H:%M:%S"),
            "expires_at": expires.strftime("%Y-%m-%d %H:%M:%S"),
        }
        try:
            conn = self._get_conn()
            try:
                conn.execute(
                    "INSERT INTO block_records (media_title, media_year, mtype, media_source, media_id, "
                    "seasons, episodes, torrent_title, torrent_size, reason, origin, status, bypass_token, "
                    "created_at, expires_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        record["media_title"], record["media_year"], record["mtype"],
                        record["media_source"], record["media_id"], record["seasons"],
                        record["episodes"], record["torrent_title"], record["torrent_size"],
                        record["reason"], record["origin"], record["status"],
                        record["bypass_token"], record["created_at"], record["expires_at"],
                    ),
                )
                conn.commit()
                record["id"] = conn.execute("SELECT last_insert_rowid() AS id").fetchone()["id"]
            finally:
                conn.close()
            return record
        except Exception as err:
            logger.error(f"【{self.plugin_name}】写入拦截记录失败：{err}")
            return None

    def _check_bypass(self, source: Optional[str], media_id: Optional[str],
                      title: str, season: Optional[int]) -> bool:
        """检查是否存在有效放行（命中则消费）"""
        try:
            conn = self._get_conn()
            try:
                now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                rows = conn.execute(
                    "SELECT * FROM block_records WHERE status='bypassed' AND expires_at > ? "
                    "AND media_title=? ORDER BY id DESC LIMIT 5",
                    (now, title),
                ).fetchall()
                for row in rows:
                    rec = dict(row)
                    # 媒体身份匹配优先
                    if source and media_id and rec.get("media_source"):
                        if rec["media_source"] == source and str(rec["media_id"]) == str(media_id):
                            self._consume_bypass(rec["id"])
                            return True
                    # 季匹配
                    rec_season = self._parse_season(rec.get("seasons"))
                    if rec_season is not None and season is not None and rec_season != season:
                        continue
                    self._consume_bypass(rec["id"])
                    return True
            finally:
                conn.close()
        except Exception as err:
            logger.debug(f"【{self.plugin_name}】检查放行状态失败：{err}")
        return False

    def _consume_bypass(self, record_id: int):
        try:
            conn = self._get_conn()
            try:
                conn.execute(
                    "UPDATE block_records SET status='consumed' WHERE id=?",
                    (record_id,),
                )
                conn.commit()
            finally:
                conn.close()
        except Exception:
            pass

    def _mark_block_status(self, record_id: int, status: str):
        try:
            conn = self._get_conn()
            try:
                conn.execute(
                    "UPDATE block_records SET status=? WHERE id=?",
                    (status, record_id),
                )
                conn.commit()
            finally:
                conn.close()
        except Exception as err:
            logger.error(f"【{self.plugin_name}】更新拦截记录状态失败：{err}")

    # ==================== 事件：下载拦截 ====================

    @eventmanager.register(ChainEventType.ResourceDownload)
    def on_resource_download(self, event: Event = None):
        """资源下载事件：仅拦截手动下载"""
        if not self._enabled or not event or not event.event_data:
            return
        try:
            data: ResourceDownloadEventData = event.event_data
            origin = (data.origin or "").lower()
            # 仅拦截手动下载
            if origin != "manual":
                return
            context = data.context
            if not context or not context.media_info:
                return

            media = context.media_info
            title = getattr(media, "title", None) or ""
            if not title:
                return
            source, media_id = self._resolve_identity(media)
            meta = context.meta_info
            season = self._parse_season(getattr(meta, "season", None)) if meta else None

            # 先检查有效放行 → 直接通过
            if self._check_bypass(source, media_id, title, season):
                logger.info(f"【{self.plugin_name}】{title} 命中放行标记，跳过拦截")
                return

            # 去重判定
            dup = self._dedup_check(context)
            if not dup:
                return

            # 拦截
            data.cancel = True
            data.source = self.plugin_name
            data.reason = dup["reason"]
            logger.info(f"【{self.plugin_name}】拦截重复下载：{title}，原因：{dup['reason']}")

            # 记录
            record = self._add_block_record(context, dup["reason"])
            token = record["bypass_token"] if record else None
            record_id = record["id"] if record else None

            # 通知
            if self._notify:
                self._notify_blocked(context, dup, token, record_id)
        except Exception as err:
            logger.error(f"【{self.plugin_name}】下载拦截事件处理异常：{err}\n{traceback.format_exc()}")

    # ==================== 事件：数据采集 ====================

    @eventmanager.register(EventType.DownloadAdded)
    def on_download_added(self, event: Event = None):
        """下载添加成功：更新本地缓存"""
        if not self._enabled or not event or not event.event_data:
            return
        try:
            data = event.event_data
            context = data.get("context") if isinstance(data, dict) else getattr(data, "context", None)
            if not context:
                return
            media = context.media_info
            if not media:
                return
            source, media_id = self._resolve_identity(media)
            title = getattr(media, "title", None) or ""
            year = getattr(media, "year", None) or None
            mtype = getattr(media, "type", None)
            mtype_str = mtype.value if hasattr(mtype, "value") else None
            meta = context.meta_info
            season = self._parse_season(getattr(meta, "season", None)) if meta else None
            episodes = None
            if meta:
                eps = getattr(meta, "episode_list", None)
                if isinstance(eps, (set, list)) and eps:
                    episodes = ",".join(f"E{int(e):02d}" for e in sorted(eps))
            torrent = context.torrent_info
            torrent_name = getattr(torrent, "title", None) or ""
            now = time.strftime("%Y-%m-%d %H:%M:%S")

            conn = self._get_conn()
            try:
                conn.execute(
                    "INSERT INTO media_records (media_source, media_id, title, year, mtype, "
                    "seasons, episodes, source_type, torrent_name, size, status, date) "
                    "VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        source, media_id, title, year, mtype_str,
                        f"S{season:02d}" if season is not None else None, episodes,
                        "download", torrent_name, None, "downloading", now,
                    ),
                )
                conn.commit()
            finally:
                conn.close()
        except Exception as err:
            logger.debug(f"【{self.plugin_name}】下载事件处理失败：{err}")

    @eventmanager.register(EventType.TransferComplete)
    def on_transfer_complete(self, event: Event = None):
        """整理完成：写入 transfer 记录（含落盘体积）"""
        self._handle_transfer_event(event, "transferred")

    @eventmanager.register(EventType.TransferFailed)
    def on_transfer_failed(self, event: Event = None):
        """整理失败：标记状态（不新增落盘记录）"""
        self._handle_transfer_event(event, "failed")

    def _handle_transfer_event(self, event: Event, status: str):
        if not self._enabled or not event or not event.event_data:
            return
        try:
            data = event.event_data
            mediainfo = data.get("mediainfo") if isinstance(data, dict) else getattr(data, "mediainfo", None)
            if not mediainfo:
                return
            title = getattr(mediainfo, "title", None) or ""
            if not title:
                return
            source, media_id = self._resolve_identity(mediainfo)
            year = getattr(mediainfo, "year", None) or None
            mtype = getattr(mediainfo, "type", None)
            mtype_str = mtype.value if hasattr(mtype, "value") else (mtype or None)
            meta = data.get("meta") if isinstance(data, dict) else getattr(data, "meta", None)
            season = self._parse_season(getattr(meta, "season", None)) if meta else None
            episode = getattr(meta, "episode", None) if meta else None
            episodes = self._format_episode_str(episode)
            # 落盘体积与路径：整理结果中提取（多级兜底）
            transferinfo = data.get("transferinfo") if isinstance(data, dict) else getattr(data, "transferinfo", None)
            size = None
            src_path = None
            if transferinfo:
                size = getattr(transferinfo, "total_size", None) or None
                if not size:
                    size = self._sum_file_sizes(getattr(transferinfo, "file_list", None))
                target_item = getattr(transferinfo, "target_item", None)
                if target_item:
                    src_path = getattr(target_item, "path", None)
            fileitem = data.get("fileitem") if isinstance(data, dict) else getattr(data, "fileitem", None)
            if not src_path and fileitem:
                src_path = getattr(fileitem, "path", None)
            if not size and fileitem:
                size = getattr(fileitem, "size", None) or None
            now = time.strftime("%Y-%m-%d %H:%M:%S")

            conn = self._get_conn()
            try:
                if status == "transferred":
                    # 已存在同源 transfer 记录则更新，否则新增
                    exists = conn.execute(
                        "SELECT id FROM media_records WHERE source_type='transfer' "
                        "AND title=? AND (? IS NULL OR seasons=?) AND (? IS NULL OR episodes=?) "
                        "AND (media_source=? OR media_source IS NULL) AND (media_id=? OR media_id IS NULL) "
                        "LIMIT 1",
                        (title, season, f"S{season:02d}" if season is not None else None,
                         episode, episodes, source, media_id),
                    ).fetchone()
                    if exists:
                        conn.execute(
                            "UPDATE media_records SET status='transferred', size=?, torrent_name=?, date=? "
                            "WHERE id=?",
                            (size, src_path, now, exists["id"]),
                        )
                    else:
                        conn.execute(
                            "INSERT INTO media_records (media_source, media_id, title, year, mtype, "
                            "seasons, episodes, source_type, torrent_name, size, status, date) "
                            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                            (
                                source, media_id, title, year, mtype_str,
                                f"S{season:02d}" if season is not None else None, episodes,
                                "transfer", src_path, size, "transferred", now,
                            ),
                        )
                else:
                    # 整理失败：标记同名 download 记录
                    conn.execute(
                        "UPDATE media_records SET status='failed' WHERE title=? AND source_type='download'",
                        (title,),
                    )
                conn.commit()
            finally:
                conn.close()
        except Exception as err:
            logger.debug(f"【{self.plugin_name}】整理事件处理失败：{err}")

    @staticmethod
    def _format_episode_str(episode) -> Optional[str]:
        """格式化集数字段（E01、E01-E12 或逗号分隔）"""
        if not episode:
            return None
        if isinstance(episode, str):
            return episode if episode.strip() else None
        if isinstance(episode, (list, set, tuple)):
            eps = sorted(int(e) for e in episode if e)
            if not eps:
                return None
            return ",".join(f"E{e:02d}" for e in eps)
        return str(episode)

    # ==================== 通知 ====================

    def _build_notify_text(self, context: Any, dup: dict, token: Optional[str]) -> Tuple[str, str]:
        """构造通知标题与正文"""
        media = context.media_info
        meta = context.meta_info
        torrent = context.torrent_info
        title = getattr(media, "title", None) or ""
        year = getattr(media, "year", None) or ""
        season = self._parse_season(getattr(meta, "season", None)) if meta else None
        season_txt = f" 第{season}季" if season else ""
        torrent_title = getattr(torrent, "title", None) or ""
        site_name = getattr(torrent, "site_name", None) or ""

        lines = [
            f"媒体：{title} {year}{season_txt}".rstrip(),
            f"判定：{dup['reason']}",
            "",
            "本地已有记录：",
            dup["old_text"],
        ]
        if dup.get("volume_hint"):
            lines.extend(["", dup["volume_hint"]])
        if torrent_title:
            lines.extend(["", f"本次种子：{torrent_title}" + (f"（{site_name}）" if site_name else "")])
        lines.extend(["", "如需洗版/再次下载，请点击「继续下载」后重新发起下载；否则点「取消」。"])

        notify_title = f"【{self.plugin_name}】拦截重复下载"
        return notify_title, "\n".join(lines)

    def _notify_blocked(self, context: Any, dup: dict, token: Optional[str], record_id: Optional[int]):
        """发送拦截通知：独立 TG bot 优先，回退 MP 通知通道"""
        title, text = self._build_notify_text(context, dup, token)

        # 优先独立 TG bot
        if self._bot is not None and token:
            try:
                self._send_tg_buttons(title, text, token, record_id)
                return
            except Exception as err:
                logger.error(f"【{self.plugin_name}】TG 通知失败，回退 MP 通道：{err}")

        # 回退 MP 通知通道
        if self._tg_use_mp_channel:
            try:
                self.post_message(
                    mtype=NotificationType.Manual,
                    title=title,
                    text=text,
                )
            except Exception as err:
                logger.error(f"【{self.plugin_name}】MP 通知失败：{err}")

    # ==================== 独立 TG Bot ====================

    def _start_tg_bot(self):
        """启动独立 TG bot（长轮询线程）"""
        if not TELEBOT_AVAILABLE or not self._tg_bot_token:
            self._stop_tg_bot()
            return
        if self._bot is not None:
            return
        try:
            bot = TeleBot(self._tg_bot_token, parse_mode="HTML")

            @bot.callback_query_handler(func=lambda call: True)
            def handle_callback(call):
                self._handle_tg_callback(call)

            def _run():
                try:
                    logger.info(f"【{self.plugin_name}】TG bot 开始轮询")
                    bot.infinity_polling(long_polling_timeout=30)
                except Exception as err:
                    logger.error(f"【{self.plugin_name}】TG bot 轮询退出：{err}")

            self._bot = bot
            self._bot_thread = threading.Thread(target=_run, daemon=True)
            self._bot_thread.start()
            logger.info(f"【{self.plugin_name}】独立 TG bot 已启动")
        except Exception as err:
            logger.error(f"【{self.plugin_name}】TG bot 启动失败：{err}")
            self._bot = None

    def _stop_tg_bot(self):
        """停止 TG bot"""
        bot = self._bot
        self._bot = None
        if bot is not None:
            try:
                bot.stop_polling()
            except Exception:
                pass
        self._bot_thread = None
        logger.info(f"【{self.plugin_name}】TG bot 已停止")

    def _chat_allowed(self, chat_id: Any) -> bool:
        """校验聊天 ID 是否在白名单"""
        if not self._tg_chat_ids:
            return False
        allowed = {str(x).strip() for x in self._tg_chat_ids.replace("，", ",").split(",") if str(x).strip()}
        return str(chat_id) in allowed

    def _send_tg_buttons(self, title: str, text: str, token: str, record_id: Optional[int]):
        """发送带按钮的 TG 通知"""
        if not self._bot or not self._tg_chat_ids:
            return
        markup = InlineKeyboardMarkup()
        markup.row(
            InlineKeyboardButton("继续下载", callback_data=f"{_CB_BYPASS}{token}"),
            InlineKeyboardButton("取消", callback_data=f"{_CB_CANCEL}{record_id or ''}"),
        )
        for chat_id in self._tg_chat_ids.replace("，", ",").split(","):
            chat_id = chat_id.strip()
            if not chat_id:
                continue
            try:
                self._bot.send_message(
                    chat_id,
                    f"<b>{title}</b>\n\n{text}",
                    reply_markup=markup,
                )
            except Exception as err:
                logger.error(f"【{self.plugin_name}】TG 发送到 {chat_id} 失败：{err}")

    def _handle_tg_callback(self, call: Any):
        """处理 TG 按钮回调"""
        try:
            chat_id = call.message.chat.id if call.message else None
            if not self._chat_allowed(chat_id):
                logger.warn(f"【{self.plugin_name}】TG 回调拒绝：chat_id {chat_id} 不在白名单")
                try:
                    self._bot.answer_callback_query(call.id, "未授权操作")
                except Exception:
                    pass
                return

            cb_data = call.data or ""
            try:
                if cb_data.startswith(_CB_BYPASS):
                    token = cb_data[len(_CB_BYPASS):]
                    if token:
                        self._apply_bypass_by_token(token)
                        self._bot.answer_callback_query(call.id, "已放行，请重新发起下载")
                        self._bot.edit_message_text(
                            call.message.text + "\n\n✅ 已放行，请重新发起下载",
                            chat_id=chat_id,
                            message_id=call.message.message_id,
                        )
                elif cb_data.startswith(_CB_CANCEL):
                    rid = cb_data[len(_CB_CANCEL):]
                    if rid:
                        self._mark_block_status(int(rid), "cancelled")
                    self._bot.answer_callback_query(call.id, "已取消下载")
                    self._bot.edit_message_text(
                        call.message.text + "\n\n❌ 已取消本次下载",
                        chat_id=chat_id,
                        message_id=call.message.message_id,
                    )
            except Exception as err:
                logger.error(f"【{self.plugin_name}】TG 回调处理失败：{err}")
                try:
                    self._bot.answer_callback_query(call.id, "处理失败，请重试")
                except Exception:
                    pass
        except Exception as err:
            logger.error(f"【{self.plugin_name}】TG 回调异常：{err}\n{traceback.format_exc()}")

    def _apply_bypass_by_token(self, token: str) -> bool:
        """按 token 放行（标记为 bypassed，下次下载时消费）"""
        try:
            conn = self._get_conn()
            try:
                row = conn.execute(
                    "SELECT * FROM block_records WHERE bypass_token=? AND status='blocked'",
                    (token,),
                ).fetchone()
                if not row:
                    return False
                now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                conn.execute(
                    "UPDATE block_records SET status='bypassed' WHERE id=?",
                    (row["id"],),
                )
                conn.commit()
                logger.info(f"【{self.plugin_name}】已放行：{row['media_title']}（token 尾号 {token[-6:]}）")
                return True
            finally:
                conn.close()
        except Exception as err:
            logger.error(f"【{self.plugin_name}】放行失败：{err}")
            return False

    # ==================== API ====================

    def get_api(self) -> List[Dict[str, Any]]:
        return [
            {
                "path": "/records",
                "endpoint": self.api_records,
                "methods": ["GET"],
                "summary": "拦截记录",
                "description": "查看最近拦截的重复下载记录",
            },
            {
                "path": "/status",
                "endpoint": self.api_status,
                "methods": ["GET"],
                "summary": "插件状态",
                "description": "查看插件运行状态与统计",
            },
            {
                "path": "/bypass",
                "endpoint": self.api_bypass,
                "methods": ["POST"],
                "summary": "放行下载",
                "description": "按 token 放行拦截的下载",
            },
            {
                "path": "/rebuild",
                "endpoint": self.api_rebuild,
                "methods": ["POST"],
                "summary": "重建缓存",
                "description": "全量重建本地媒体缓存",
            },
        ]

    def api_records(self, page: int = 1, count: int = 20) -> Dict[str, Any]:
        """查询拦截记录"""
        try:
            conn = self._get_conn()
            try:
                offset = max(0, (int(page) - 1)) * int(count)
                rows = conn.execute(
                    "SELECT * FROM block_records ORDER BY id DESC LIMIT ? OFFSET ?",
                    (int(count), offset),
                ).fetchall()
                total = conn.execute("SELECT COUNT(*) AS c FROM block_records").fetchone()["c"]
                return {
                    "total": total,
                    "records": [dict(r) for r in rows],
                }
            finally:
                conn.close()
        except Exception as err:
            return {"error": str(err)}

    def api_status(self) -> Dict[str, Any]:
        """插件状态与统计"""
        try:
            conn = self._get_conn()
            try:
                total = conn.execute("SELECT COUNT(*) AS c FROM block_records").fetchone()["c"]
                blocked = conn.execute(
                    "SELECT COUNT(*) AS c FROM block_records WHERE status='blocked'"
                ).fetchone()["c"]
                bypassed = conn.execute(
                    "SELECT COUNT(*) AS c FROM block_records WHERE status IN ('bypassed','consumed')"
                ).fetchone()["c"]
                cancelled = conn.execute(
                    "SELECT COUNT(*) AS c FROM block_records WHERE status='cancelled'"
                ).fetchone()["c"]
                media_count = conn.execute("SELECT COUNT(*) AS c FROM media_records").fetchone()["c"]
                dl_cache = conn.execute(
                    "SELECT COUNT(*) AS c FROM media_records WHERE source_type='download'"
                ).fetchone()["c"]
                tr_cache = conn.execute(
                    "SELECT COUNT(*) AS c FROM media_records WHERE source_type='transfer'"
                ).fetchone()["c"]
                ms_cache = conn.execute(
                    "SELECT COUNT(*) AS c FROM media_records WHERE source_type='mediaserver'"
                ).fetchone()["c"]
                return {
                    "enabled": self._enabled,
                    "match_mode": self._match_mode,
                    "tg_bot_configured": bool(self._tg_bot_token),
                    "tg_bot_running": self._bot is not None,
                    "telebot_available": TELEBOT_AVAILABLE,
                    "block_total": total,
                    "block_blocked": blocked,
                    "block_bypassed": bypassed,
                    "block_cancelled": cancelled,
                    "media_cache_count": media_count,
                    "media_cache_detail": {
                        "download": dl_cache,
                        "transfer": tr_cache,
                        "mediaserver": ms_cache,
                    },
                }
            finally:
                conn.close()
        except Exception as err:
            return {"error": str(err)}

    def api_bypass(self, token: str = None) -> Dict[str, Any]:
        """放行：按 token"""
        if not token:
            return {"success": False, "message": "缺少 token 参数"}
        ok = self._apply_bypass_by_token(token)
        return {"success": ok, "message": "已放行，请重新发起下载" if ok else "token 无效或已使用"}

    def api_rebuild(self) -> Dict[str, Any]:
        """重建缓存"""
        threading.Thread(target=self._rebuild_cache, daemon=True).start()
        return {"success": True, "message": "缓存重建已启动"}

    # ==================== 命令 ====================

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        return [
            {
                "cmd": "/ddg",
                "event": EventType.PluginAction,
                "desc": "查询下载去重状态",
                "category": "插件命令",
                "data": {"action": "ddg_status"},
            },
            {
                "cmd": "/ddg_bypass",
                "event": EventType.PluginAction,
                "desc": "放行最近一次拦截（示例：/ddg_bypass 媒体标题）",
                "category": "插件命令",
                "data": {"action": "ddg_bypass"},
            },
        ]

    @eventmanager.register(EventType.PluginAction)
    def on_plugin_action(self, event: Event = None):
        """远程命令处理"""
        if not self._enabled or not event or not event.event_data:
            return
        try:
            data = event.event_data
            action = data.get("action") if isinstance(data, dict) else getattr(data, "action", None)
            text = data.get("text") if isinstance(data, dict) else ""
            if action == "ddg_status":
                status = self.api_status()
                lines = [
                    f"启用：{'✅' if status.get('enabled') else '❌'}",
                    f"匹配模式：{status.get('match_mode')}",
                    f"TG bot：{'运行中' if status.get('tg_bot_running') else '未运行'}",
                    f"拦截记录：{status.get('block_total')}（待处理 {status.get('block_blocked')} / 放行 {status.get('block_bypassed')} / 取消 {status.get('block_cancelled')}）",
                    f"本地缓存：{status.get('media_cache_count')} 条",
                ]
                self.post_message(
                    mtype=NotificationType.Manual,
                    title=f"【{self.plugin_name}】状态",
                    text="\n".join(lines),
                )
            elif action == "ddg_bypass":
                keyword = (text or "").strip()
                if not keyword:
                    self.post_message(
                        mtype=NotificationType.Manual,
                        title=f"【{self.plugin_name}】放行",
                        text="用法：/ddg_bypass 媒体标题",
                    )
                    return
                conn = self._get_conn()
                try:
                    row = conn.execute(
                        "SELECT * FROM block_records WHERE status='blocked' AND media_title LIKE ? "
                        "ORDER BY id DESC LIMIT 1",
                        (f"%{keyword}%",),
                    ).fetchone()
                finally:
                    conn.close()
                if not row:
                    self.post_message(
                        mtype=NotificationType.Manual,
                        title=f"【{self.plugin_name}】放行",
                        text=f"未找到 {keyword} 的待处理拦截记录",
                    )
                    return
                self._apply_bypass_by_token(row["bypass_token"])
                self.post_message(
                    mtype=NotificationType.Manual,
                    title=f"【{self.plugin_name}】放行",
                    text=f"已放行：{row['media_title']}，请重新发起下载",
                )
        except Exception as err:
            logger.error(f"【{self.plugin_name}】命令处理失败：{err}")

    # ==================== 定时服务 ====================

    def get_service(self) -> List[Dict[str, Any]]:
        if not self.get_state():
            return []
        return [
            {
                "id": "OguraDownloadHelper.CacheRebuild",
                "name": "下载去重缓存重建",
                "trigger": "cron",
                "func": self._rebuild_cache,
                "kwargs": {"hour": "3", "minute": "0"},
            }
        ]

    # ==================== 配置表单 ====================

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        return [
            {
                "component": "VForm",
                "content": [
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
                                            "model": "enabled",
                                            "label": "启用插件",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VSelect",
                                        "props": {
                                            "model": "match_mode",
                                            "label": "匹配模式",
                                            "items": [
                                                {"title": "媒体身份优先（推荐）", "value": "identity"},
                                                {"title": "标题+年份", "value": "title"},
                                                {"title": "仅提示不拦截", "value": "soft"},
                                            ],
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
                                            "model": "notify",
                                            "label": "拦截通知",
                                            "hint": "拦截重复下载时发送通知",
                                            "persistent-hint": True,
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
                                            "model": "tg_bot_token",
                                            "label": "独立 TG Bot Token",
                                            "hint": "可选。配置后使用独立 bot 发送拦截通知与按钮（噪音隔离）",
                                            "persistent-hint": True,
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
                                            "model": "tg_chat_ids",
                                            "label": "TG 聊天 ID 白名单",
                                            "hint": "逗号分隔，仅这些聊天可操作放行按钮",
                                            "persistent-hint": True,
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
                                            "model": "tg_use_mp_channel",
                                            "label": "未配 Bot 时回退 MP 通知",
                                            "hint": "未配置独立 bot 时，使用 MP 自带通知通道发送拦截通知",
                                            "persistent-hint": True,
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
                                            "model": "bypass_hours",
                                            "label": "放行有效期（小时）",
                                            "hint": "放行后在该时间内重新下载将直接通过",
                                            "persistent-hint": True,
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
                                            "model": "volume_ratio",
                                            "label": "体积差异提示阈值",
                                            "hint": "新旧体积差异小于该比例视为相近（默认 0.05=5%）",
                                            "persistent-hint": True,
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 8},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "disk_prefixes",
                                            "label": "盘路径前缀",
                                            "hint": "逗号分隔。通知中的路径精简与盘名提取用（默认 /ptdownload/）",
                                            "persistent-hint": True,
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
                                "props": {"cols": 12},
                                "content": [
                                    {
                                        "component": "VAlert",
                                        "props": {
                                            "type": "info",
                                            "variant": "tonal",
                                            "text": "工作方式：仅拦截「手动下载」。综合下载历史/整理历史/媒体服务器数据判定是否已拥有，"
                                                    "命中则拦截并通知。订阅/RSS 自动下载不受影响。通知带 TG 按钮「继续下载/取消」，"
                                                    "点继续下载后需重新发起下载。",
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
            "match_mode": "identity",
            "notify": True,
            "tg_bot_token": "",
            "tg_chat_ids": "",
            "tg_use_mp_channel": True,
            "bypass_hours": 24,
            "volume_ratio": 0.05,
            "disk_prefixes": "/ptdownload/",
        }

    # ==================== 详情页 ====================

    def get_page(self) -> List[dict]:
        records = []
        cache_info = None
        try:
            conn = self._get_conn()
            try:
                rows = conn.execute(
                    "SELECT * FROM block_records ORDER BY id DESC LIMIT 10"
                ).fetchall()
                records = [dict(r) for r in rows]
                dl_cache = conn.execute(
                    "SELECT COUNT(*) AS c FROM media_records WHERE source_type='download'"
                ).fetchone()["c"]
                tr_cache = conn.execute(
                    "SELECT COUNT(*) AS c FROM media_records WHERE source_type='transfer'"
                ).fetchone()["c"]
                tr_size = conn.execute(
                    "SELECT COUNT(*) AS c FROM media_records WHERE source_type='transfer' "
                    "AND size IS NOT NULL AND size > 0"
                ).fetchone()["c"]
                cache_info = f"下载 {dl_cache} / 整理 {tr_cache}（带体积 {tr_size}）"
            finally:
                conn.close()
        except Exception:
            pass

        page = []
        if cache_info:
            healthy = self._cache_healthy()
            page.append(
                {
                    "component": "VAlert",
                    "props": {
                        "type": "success" if healthy else "warning",
                        "variant": "tonal",
                        "text": (
                            f"本地缓存：{cache_info}｜状态：{'正常' if healthy else '需重建（重启插件自动重建）'}"
                        ),
                    },
                }
            )

        if not records:
            page.append(
                {
                    "component": "VAlert",
                    "props": {
                        "type": "info",
                        "variant": "tonal",
                        "text": "暂无拦截记录。手动下载重复资源时会在此显示。",
                    },
                }
            )
            return page

        status_map = {
            "blocked": "待处理",
            "bypassed": "已放行（待下载）",
            "consumed": "已放行（已使用）",
            "cancelled": "已取消",
        }
        items = []
        for rec in records:
            season_txt = f" {rec['seasons']}" if rec.get("seasons") else ""
            items.append(
                {
                    "component": "VListItem",
                    "props": {
                        "title": f"{rec['media_title']} {rec['media_year'] or ''}{season_txt}".rstrip(),
                        "subtitle": (
                            f"{rec['reason']} | 状态：{status_map.get(rec['status'], rec['status'])} | "
                            f"时间：{rec['created_at']}"
                        ),
                    },
                }
            )
        return [
            {
                "component": "VList",
                "props": {"density": "compact"},
                "content": items,
            }
        ]

    # ==================== 生命周期 ====================

    def stop_service(self):
        """停止插件：清理 TG bot"""
        self._stop_tg_bot()
