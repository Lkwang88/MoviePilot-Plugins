import re
import threading
import time
import os
from typing import Any, List, Dict, Tuple, Optional

from app.core.cache import cached
from app.core.config import settings
from app.core.event import eventmanager, Event
from app.helper.mediaserver import MediaServerHelper
from app.log import logger
from app.modules.themoviedb import CategoryHelper
from app.plugins import _PluginBase
from app.schemas import WebhookEventInfo, ServiceInfo, MediaServerItem
from app.schemas.types import EventType, MediaType, MediaImageType, NotificationType
from app.utils.web import WebUtils


class wd99(_PluginBase):
    """
    媒体服务器通知插件 (大师融合终修版)
    v1.9.8: 
    1. 修复 '_aggregate_tv_episodes' 缺失导致的 AttributeError
    2. 修复剧集ID识别为字符串 "None" 的BUG
    3. 保留所有美化和聚合功能
    """

    # ==================== 常量定义 ====================
    DEFAULT_EXPIRATION_TIME = 600
    DEFAULT_AGGREGATE_TIME = 15
    DEFAULT_OVERVIEW_MAX_LENGTH = 150

    # ==================== 插件基本信息 ====================
    plugin_name = "媒体库通知(融合版)"
    plugin_desc = "基于Emby/Jellyfin/Plex的通知插件，支持防轰炸聚合与丰富元数据展示。"
    plugin_icon = "mediaplay.png"
    plugin_version = "1.9.9"
    plugin_author = "MP插件大师"
    author_url = "https://github.com/jxxghp"
    plugin_config_prefix = "mediaservermsg_pro_" 
    plugin_order = 13
    auth_level = 1

    # ==================== 插件配置 ====================
    _enabled = False
    _add_play_link = False
    _mediaservers = None
    _types = []
    _webhook_msg_keys = {}
    _aggregate_enabled = True
    _aggregate_time = DEFAULT_AGGREGATE_TIME
    _overview_max_length = DEFAULT_OVERVIEW_MAX_LENGTH
    _smart_category_enabled = True

    _pending_messages = {}
    _aggregate_timers = {}

    _webhook_actions = {
        "library.new": "已入库",
        "system.webhooktest": "测试",
        "system.notificationtest": "测试",
        "playback.start": "开始播放",
        "playback.stop": "停止播放",
        "playback.pause": "暂停播放",
        "playback.unpause": "继续播放",
        "user.authenticated": "登录成功",
        "user.authenticationfailed": "登录失败",
        "media.play": "开始播放",
        "media.stop": "停止播放",
        "media.pause": "暂停播放",
        "media.resume": "继续播放",
        "item.rate": "标记了",
        "item.markplayed": "标记已播放",
        "item.markunplayed": "标记未播放",
        "PlaybackStart": "开始播放",
        "PlaybackStop": "停止播放"
    }

    _webhook_images = {
        "emby": "https://raw.githubusercontent.com/qqcomeup/MoviePilot-Plugins/bb3ca257f74cf000640f9ebadab257bb0850baac/icons/11-11.jpg",
        "plex": "https://raw.githubusercontent.com/qqcomeup/MoviePilot-Plugins/bb3ca257f74cf000640f9ebadab257bb0850baac/icons/11-11.jpg",
        "jellyfin": "https://raw.githubusercontent.com/qqcomeup/MoviePilot-Plugins/bb3ca257f74cf000640f9ebadab257bb0850baac/icons/11-11.jpg"
    }

    _country_cn_map = {
        'CN': '中国大陆', 'US': '美国', 'JP': '日本', 'KR': '韩国',
        'HK': '中国香港', 'TW': '中国台湾', 'GB': '英国', 'FR': '法国',
        'DE': '德国', 'IT': '意大利', 'ES': '西班牙', 'IN': '印度',
        'TH': '泰国', 'RU': '俄罗斯', 'CA': '加拿大', 'AU': '澳大利亚',
        'SG': '新加坡', 'MY': '马来西亚', 'VN': '越南', 'PH': '菲律宾',
        'ID': '印度尼西亚', 'BR': '巴西', 'MX': '墨西哥', 'AR': '阿根廷',
        'NL': '荷兰', 'BE': '比利时', 'SE': '瑞典', 'DK': '丹麦',
        'NO': '挪威', 'FI': '芬兰', 'PL': '波兰', 'TR': '土耳其'
    }

    def __init__(self):
        super().__init__()
        self.category = CategoryHelper()

    def init_plugin(self, config: dict = None):
        if config:
            self._enabled = config.get("enabled")
            self._types = config.get("types") or []
            self._mediaservers = config.get("mediaservers") or []
            self._add_play_link = config.get("add_play_link", False)
            self._aggregate_enabled = config.get("aggregate_enabled", False)
            self._aggregate_time = int(config.get("aggregate_time", self.DEFAULT_AGGREGATE_TIME))
            self._smart_category_enabled = config.get("smart_category_enabled", True)

    def service_infos(self, type_filter: Optional[str] = None) -> Optional[Dict[str, ServiceInfo]]:
        services = MediaServerHelper().get_services(type_filter=type_filter, name_filters=self._mediaservers)
        if not services:
            return None
        active_services = {}
        for service_name, service_info in services.items():
            if not service_info.instance.is_inactive():
                active_services[service_name] = service_info
        return active_services

    def service_info(self, name: str) -> Optional[ServiceInfo]:
        return (self.service_infos() or {}).get(name)

    def get_state(self) -> bool:
        return self._enabled

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        return []

    def get_api(self) -> List[Dict[str, Any]]:
        return []

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        types_options = [
            {"title": "新入库", "value": "library.new"},
            {"title": "开始播放", "value": "playback.start|media.play|PlaybackStart"},
            {"title": "停止播放", "value": "playback.stop|media.stop|PlaybackStop"},
            {"title": "用户标记", "value": "item.rate|item.markplayed|item.markunplayed"},
            {"title": "登录提醒", "value": "user.authenticated|user.authenticationfailed"},
            {"title": "系统测试", "value": "system.webhooktest|system.notificationtest"},
        ]
        return [
            {
                'component': 'VForm',
                'content': [
                    {
                        'component': 'VRow',
                        'content': [
                            {'component': 'VCol', 'props': {'cols': 12, 'md': 6}, 'content': [{'component': 'VSwitch', 'props': {'model': 'enabled', 'label': '启用插件'}}]},
                            {'component': 'VCol', 'props': {'cols': 12, 'md': 6}, 'content': [{'component': 'VSwitch', 'props': {'model': 'add_play_link', 'label': '添加播放链接'}}]}
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {'component': 'VCol', 'props': {'cols': 12}, 'content': [{'component': 'VSelect', 'props': {'multiple': True, 'chips': True, 'clearable': True, 'model': 'mediaservers', 'label': '媒体服务器', 'items': [{"title": config.name, "value": config.name} for config in MediaServerHelper().get_configs().values()]}}]}
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {'component': 'VCol', 'props': {'cols': 12}, 'content': [{'component': 'VSelect', 'props': {'chips': True, 'multiple': True, 'model': 'types', 'label': '消息类型', 'items': types_options}}]}
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {'component': 'VCol', 'props': {'cols': 12, 'md': 6}, 'content': [{'component': 'VSwitch', 'props': {'model': 'aggregate_enabled', 'label': '启用TV剧集入库聚合'}}]},
                            {'component': 'VCol', 'props': {'cols': 12, 'md': 6}, 'content': [{'component': 'VSwitch', 'props': {'model': 'smart_category_enabled', 'label': '启用智能分类（关闭则使用路径解析）'}}]}
                        ]
                    },
                    {
                        'component': 'VRow',
                        'props': {'show': '{{aggregate_enabled}}'},
                        'content': [
                            {'component': 'VCol', 'props': {'cols': 12, 'md': 6}, 'content': [{'component': 'VTextField', 'props': {'model': 'aggregate_time', 'label': '聚合等待时间（秒）', 'placeholder': '15', 'type': 'number'}}]}
                        ]
                    }
                ]
            }
        ], {
            "enabled": False,
            "types": [],
            "aggregate_enabled": False,
            "aggregate_time": self.DEFAULT_AGGREGATE_TIME,
            "smart_category_enabled": True
        }

    def get_page(self) -> List[dict]:
        return []

    @eventmanager.register(EventType.WebhookMessage)
    def send(self, event: Event):
        try:
            if not self._enabled:
                return

            event_info: WebhookEventInfo = getattr(event, 'event_data', None)
            if not event_info:
                return

            event_type = str(getattr(event_info, 'event', ''))
            if not event_type:
                return
            
            logger.info(f"【融合版】收到Webhook事件: {event_type} 来自 {event_info.server_name}")

            # 检查配置
            if not self._mediaservers:
                if "test" not in event_type.lower():
                    logger.error("【融合版】拦截: 未配置媒体服务器")
                    return
            elif event_info.server_name and event_info.server_name not in self._mediaservers:
                logger.info(f"【融合版】拦截: 服务器 {event_info.server_name} 未勾选")
                return

            allowed_types = set()
            for _type in self._types:
                allowed_types.update(_type.split("|"))
            
            if "test" in event_type.lower():
                pass 
            elif event_type not in allowed_types:
                logger.info(f"【融合版】拦截: 类型 {event_type} 未勾选")
                return

            # 防重复
            item_id = getattr(event_info, 'item_id', '')
            client = getattr(event_info, 'client', '')
            user_name = getattr(event_info, 'user_name', '')
            expiring_key = f"{item_id}-{client}-{user_name}-{event_type}"
            
            self._clean_expired_cache()
            
            if "stop" in event_type.lower() and expiring_key in self._webhook_msg_keys:
                logger.info(f"【融合版】拦截: 重复停止事件")
                self._add_key_cache(expiring_key)
                return

            # 路由处理
            if "test" in event_type.lower():
                self._handle_test_event(event_info)
                return
            if "user.authentic" in event_type.lower():
                self._handle_login_event(event_info)
                return

            if self._should_aggregate_tv(event_info):
                # 获取ID并校验
                series_id = self._get_series_id(event_info)
                if series_id:
                    logger.info(f"【融合版】加入聚合队列: {series_id}")
                    self._aggregate_tv_episodes(series_id, event_info)
                    return
                else:
                    logger.warning("【融合版】无法获取SeriesID，跳过聚合，转为单条发送")

            self._process_single_media_event(event_info, expiring_key)

        except Exception as e:
            logger.error(f"【融合版】异常: {str(e)}")
            import traceback
            traceback.print_exc()

    def _should_aggregate_tv(self, event_info: WebhookEventInfo) -> bool:
        if not self._aggregate_enabled:
            return False
        if event_info.event != "library.new":
            return False
        if event_info.item_type not in ["TV", "SHOW"]:
            return False
        return True

    # ========== 修复点：添加了之前缺失的 _aggregate_tv_episodes 方法 ==========
    def _aggregate_tv_episodes(self, series_id: str, event_info: WebhookEventInfo):
        if series_id not in self._pending_messages:
            self._pending_messages[series_id] = []
        
        self._pending_messages[series_id].append(event_info)
        
        if series_id in self._aggregate_timers:
            try:
                self._aggregate_timers[series_id].cancel()
            except: pass
        
        timer = threading.Timer(self._aggregate_time, self._send_aggregated_message, [series_id])
        self._aggregate_timers[series_id] = timer
        timer.start()

    # ========== 修复点：修复 None 转换成字符串 "None" 的逻辑错误 ==========
    def _get_series_id(self, event_info: WebhookEventInfo) -> Optional[str]:
        if event_info.json_object and isinstance(event_info.json_object, dict):
            item = event_info.json_object.get("Item", {})
            val = item.get("SeriesId") or item.get("SeriesName")
            if val:
                return str(val)
        
        # 只有当 series_id 真实存在时才返回，否则返回 None
        sid = getattr(event_info, "series_id", None)
        return str(sid) if sid else None

    def _process_single_media_event(self, event_info: WebhookEventInfo, expiring_key: str):
        logger.info(f"【融合版】处理单条消息: {event_info.item_name}")
        
        tmdb_id = self._ensure_tmdb_id(event_info)
        event_info.tmdb_id = tmdb_id
        
        tmdb_info = None
        if tmdb_id:
            mtype = MediaType.MOVIE if event_info.item_type == "MOV" else MediaType.TV
            tmdb_info = self._get_tmdb_info_cached(tmdb_id, mtype, event_info.season_id)

        title_name = event_info.item_name
        if event_info.item_type in ["TV", "SHOW"] and event_info.json_object:
            title_name = event_info.json_object.get('Item', {}).get('SeriesName') or title_name
        
        year = tmdb_info.get('year') if tmdb_info else None
        if not year and event_info.json_object:
            year = event_info.json_object.get('Item', {}).get('ProductionYear')
        if year and str(year) not in title_name:
            title_name += f" ({year})"

        action_cn = self._webhook_actions.get(event_info.event, event_info.event)
        server_cn = self._get_server_name_cn(event_info)
        
        message_title = f"{title_name} {action_cn} {server_cn}"

        message_texts = []
        message_texts.append(f"⏰ {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())}")

        category = self._get_smart_category(event_info, tmdb_info)
        if category:
            message_texts.append(f"📂 分类：{category}")

        self._append_season_episode_info(message_texts, event_info, title_name)
        self._append_meta_info(message_texts, tmdb_info)
        self._append_genres_actors(message_texts, tmdb_info)

        overview = self._get_overview(event_info, tmdb_info)
        if overview:
            message_texts.append("\n━━━━━━━━━━━━━━━━━━\n") 
            message_texts.append(f"📖 剧情简介\n{overview}")

        self._append_extra_info(message_texts, event_info)

        image_url = self._get_image_url(event_info, tmdb_info)
        play_link = self._get_play_link(event_info) if self._add_play_link else None
        
        if "stop" in str(event_info.event).lower():
            self._add_key_cache(expiring_key)
        elif "start" in str(event_info.event).lower():
            self._remove_key_cache(expiring_key)

        ret = self.post_message(
            mtype=NotificationType.MediaServer,
            title=message_title,
            text="\n" + "\n".join(message_texts),
            image=image_url,
            link=play_link
        )
        if ret: logger.info("【融合版】单条消息已推送")

    def _send_aggregated_message(self, series_id: str):
        if series_id not in self._pending_messages: return
        
        msg_list = self._pending_messages.pop(series_id)
        if series_id in self._aggregate_timers:
            self._aggregate_timers.pop(series_id, None)
            
        if not msg_list: return

        if len(msg_list) == 1:
            fake_key = f"{msg_list[0].item_id}-agg-{time.time()}"
            self._process_single_media_event(msg_list[0], fake_key)
            return

        logger.info(f"【融合版】处理聚合消息: 数量 {len(msg_list)}")
        
        first_info = msg_list[0]
        count = len(msg_list)
        
        tmdb_id = self._ensure_tmdb_id(first_info)
        tmdb_info = None
        if tmdb_id:
            tmdb_info = self._get_tmdb_info_cached(tmdb_id, MediaType.TV)

        title_name = first_info.item_name
        if first_info.json_object:
            title_name = first_info.json_object.get('Item', {}).get('SeriesName') or title_name
        
        server_cn = self._get_server_name_cn(first_info)
        message_title = f"{title_name} 已入库 (含{count}个文件) {server_cn}"

        message_texts = []
        message_texts.append(f"⏰ {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())}")
        
        category = self._get_smart_category(first_info, tmdb_info)
        if category:
            message_texts.append(f"📂 分类：{category}")

        episodes_str = self._merge_continuous_episodes(msg_list)
        message_texts.append(f"📺 季集：{episodes_str}")

        self._append_meta_info(message_texts, tmdb_info)
        self._append_genres_actors(message_texts, tmdb_info)

        overview = self._get_overview(first_info, tmdb_info)
        if overview:
            message_texts.append("\n━━━━━━━━━━━━━━━━━━\n") 
            message_texts.append(f"📖 剧情简介\n{overview}")

        image_url = self._get_image_url(first_info, tmdb_info)
        play_link = self._get_play_link(first_info) if self._add_play_link else None

        ret = self.post_message(
            mtype=NotificationType.MediaServer,
            title=message_title,
            text="\n" + "\n".join(message_texts),
            image=image_url,
            link=play_link
        )
        if ret: logger.info("【融合版】聚合消息已推送")

    # ==================== 辅助方法 ====================

    def _ensure_tmdb_id(self, event_info: WebhookEventInfo) -> Optional[str]:
        if event_info.tmdb_id: return str(event_info.tmdb_id)
        if event_info.json_object:
            pids = event_info.json_object.get('Item', {}).get('ProviderIds', {})
            if pids.get('Tmdb'): return str(pids.get('Tmdb'))
        if event_info.item_path:
            if match := re.search(r'[\[{](?:tmdbid|tmdb)[=-](\d+)[\]}]', event_info.item_path, re.IGNORECASE):
                return match.group(1)
        try:
            if event_info.server_name and event_info.item_id:
                service_info = self.service_info(event_info.server_name)
                if service_info and service_info.instance:
                    info = service_info.instance.get_iteminfo(event_info.item_id)
                    if info and info.tmdbid: return str(info.tmdbid)
        except: pass
        return None

    def _handle_test_event(self, event_info: WebhookEventInfo):
        title = f"🔔 媒体服务器通知测试(融合版)"
        server_name = self._get_server_name_cn(event_info)
        texts = [
            f"来自：{server_name}",
            f"时间：{time.strftime('%Y-%m-%d %H:%M:%S')}",
            f"状态：连接正常"
        ]
        if event_info.user_name:
            texts.append(f"用户：{event_info.user_name}")
            
        self.post_message(
            mtype=NotificationType.MediaServer,
            title=title,
            text="\n".join(texts),
            image=self._webhook_images.get(event_info.channel)
        )

    def _handle_login_event(self, event_info: WebhookEventInfo):
        action = "登录成功" if "authenticated" in event_info.event and "failed" not in event_info.event else "登录失败"
        title = f"🔐 {action}提醒"
        texts = []
        texts.append(f"👤 用户：{event_info.user_name}")
        texts.append(f"⏰ 时间：{time.strftime('%Y-%m-%d %H:%M:%S')}")
        if event_info.device_name:
            texts.append(f"📱 设备：{event_info.client} {event_info.device_name}")
        if event_info.ip:
            try:
                location = WebUtils.get_location(event_info.ip)
                texts.append(f"🌐 IP：{event_info.ip} {location}")
            except:
                texts.append(f"🌐 IP：{event_info.ip}")
        server_name = self._get_server_name_cn(event_info)
        texts.append(f"🖥️ 服务器：{server_name}")

        self.post_message(
            mtype=NotificationType.MediaServer,
            title=title,
            text="\n".join(texts),
            image=self._webhook_images.get(event_info.channel)
        )

    def _get_server_name_cn(self, event_info):
        # ———— 强制自定义名称区域 ————
        # 你可以在这里直接返回你想要的名字，无视 Emby 的设置
        return "芙服" 
        # ————————————————————————
        server_name = ""
        if event_info.json_object and isinstance(event_info.json_object.get('Server'), dict):
            server_name = event_info.json_object.get('Server', {}).get('Name')
        if not server_name:
            server_name = event_info.server_name or "Emby"
        return server_name

    def _get_smart_category(self, event_info, tmdb_info):
        category = None
        if self._smart_category_enabled and tmdb_info:
            try:
                if event_info.item_type == "MOV":
                    category = self.category.get_movie_category(tmdb_info)
                else:
                    category = self.category.get_tv_category(tmdb_info)
            except: pass
        if not category:
            is_folder = event_info.json_object.get('Item', {}).get('IsFolder', False) if event_info.json_object else False
            category = self._get_category_from_path(event_info.item_path, event_info.item_type, is_folder)
        return category

    def _get_category_from_path(self, path: str, item_type: str, is_folder: bool = False) -> str:
        if not path: return ""
        try:
            path = os.path.normpath(path)
            if is_folder and item_type in ["TV", "SHOW"]:
                return os.path.basename(os.path.dirname(path))
            current_dir = os.path.dirname(path)
            dir_name = os.path.basename(current_dir)
            if re.search(r'^(Season|季|S\d)', dir_name, re.IGNORECASE):
                current_dir = os.path.dirname(current_dir)
            category_dir = os.path.dirname(current_dir)
            category = os.path.basename(category_dir)
            if not category or category == os.path.sep: return ""
            return category
        except: return ""

    def _append_season_episode_info(self, texts: List[str], event_info: WebhookEventInfo, series_name: str):
        if event_info.season_id is not None and event_info.episode_id is not None:
            s_str, e_str = str(event_info.season_id).zfill(2), str(event_info.episode_id).zfill(2)
            info = f"📺 季集：S{s_str}E{e_str}"
            ep_name = event_info.json_object.get('Item', {}).get('Name')
            if ep_name and ep_name != series_name: 
                info += f" - {ep_name}"
            texts.append(info)

    def _append_meta_info(self, texts: List[str], tmdb_info):
        if not tmdb_info: return
        if tmdb_info.get('vote_average'):
            texts.append(f"⭐️ 评分：{round(float(tmdb_info.get('vote_average')), 1)}/10")
        
        region = ""
        try:
            countries = tmdb_info.get('origin_country') or tmdb_info.get('production_countries') or []
            codes = []
            for c in countries[:2]:
                if isinstance(c, dict): code = c.get('iso_3166_1')
                else: code = str(c)
                if code: codes.append(code)
            if codes:
                cn_names = [self._country_cn_map.get(code.upper(), code) for code in codes]
                region = "、".join(cn_names)
        except: pass
        if region:
            texts.append(f"🏳️ 地区：{region}")

        status = tmdb_info.get('status')
        if status:
            status_map = {'Ended': '已完结', 'Returning Series': '连载中', 'Canceled': '已取消', 'In Production': '制作中', 'Planned': '计划中', 'Released': '已上映', 'Continuing': '连载中'}
            status_text = status_map.get(status, status)
            texts.append(f"📡 状态：{status_text}")

    def _append_genres_actors(self, texts: List[str], tmdb_info):
        if not tmdb_info: return
        genres = tmdb_info.get('genres', [])
        if genres:
            g_names = [g.get('name') if isinstance(g, dict) else str(g) for g in genres[:3]]
            if g_names: texts.append(f"🎭 类型：{'、'.join(g_names)}")
        
        credits = tmdb_info.get('credits') or {}
        cast = credits.get('cast') or tmdb_info.get('actors') or []
        if cast:
            actors = [a.get('name') if isinstance(a, dict) else str(a) for a in cast[:3]]
            if actors: texts.append(f"🎬 演员：{'、'.join(actors)}")

    def _append_extra_info(self, texts: List[str], event_info: WebhookEventInfo):
        if event_info.user_name: texts.append(f"👤 用户：{event_info.user_name}")
        if event_info.device_name: texts.append(f"📱 设备：{event_info.client} {event_info.device_name}")
        if event_info.ip: 
            loc = ""
            try: loc = WebUtils.get_location(event_info.ip)
            except: pass
            texts.append(f"🌐 IP：{event_info.ip} {loc}")
        if event_info.percentage: 
            texts.append(f"📊 进度：{round(float(event_info.percentage), 2)}%")

    def _get_overview(self, event_info, tmdb_info):
        text = ""
        if tmdb_info and tmdb_info.get('overview'):
            text = tmdb_info.get('overview')
        elif event_info.overview:
            text = event_info.overview
        
        if text and len(text) > self._overview_max_length:
            text = text[:self._overview_max_length].rstrip() + "..."
        return text

    def _get_image_url(self, event_info, tmdb_info):
        if tmdb_info:
            if event_info.item_type == "MOV":
                if tmdb_info.get('poster_path'):
                    return f"https://{settings.TMDB_IMAGE_DOMAIN}/t/p/original{tmdb_info.get('poster_path')}"
            else:
                if tmdb_info.get('backdrop_path'):
                    return f"https://{settings.TMDB_IMAGE_DOMAIN}/t/p/original{tmdb_info.get('backdrop_path')}"
                elif tmdb_info.get('poster_path'):
                    return f"https://{settings.TMDB_IMAGE_DOMAIN}/t/p/original{tmdb_info.get('poster_path')}"
        if event_info.image_url:
            return event_info.image_url
        return self._webhook_images.get(event_info.channel)

    def _get_play_link(self, event_info: WebhookEventInfo) -> Optional[str]:
        if not self._add_play_link or not event_info.server_name: return None
        service = self.service_info(event_info.server_name)
        return service.instance.get_play_url(event_info.item_id) if service else None

    def _merge_continuous_episodes(self, events: List[WebhookEventInfo]) -> str:
        season_episodes = {}
        for event in events:
            s, e = None, None
            if event.json_object:
                item = event.json_object.get("Item", {})
                s = item.get("ParentIndexNumber")
                e = item.get("IndexNumber")
            
            if s is None: s = getattr(event, "season_id", None)
            if e is None: e = getattr(event, "episode_id", None)
            
            if s is not None and e is not None:
                try:
                    s_int = int(s)
                    if s_int not in season_episodes: season_episodes[s_int] = []
                    season_episodes[s_int].append(int(e))
                except: continue
                
        merged = []
        for s in sorted(season_episodes.keys()):
            eps = sorted(list(set(season_episodes[s])))
            if not eps: continue
            
            ranges = []
            start = eps[0]
            end = eps[0]
            for i in range(1, len(eps)):
                if eps[i] == end + 1:
                    end = eps[i]
                else:
                    ranges.append(f"E{str(start).zfill(2)}-E{str(end).zfill(2)}" if start != end else f"E{str(start).zfill(2)}")
                    start = end = eps[i]
            ranges.append(f"E{str(start).zfill(2)}-E{str(end).zfill(2)}" if start != end else f"E{str(start).zfill(2)}")
            
            merged.append(f"S{str(s).zfill(2)} {' '.join(ranges)}")
            
        return ", ".join(merged)

    @cached(region="MediaServerMsg", ttl=600)
    def _get_tmdb_info_cached(self, tmdb_id, mtype, season=None):
        if mtype == MediaType.MOVIE:
            return self.chain.tmdb_info(tmdbid=tmdb_id, mtype=mtype)
        else:
            info = self.chain.tmdb_info(tmdbid=tmdb_id, mtype=mtype, season=season)
            base_info = self.chain.tmdb_info(tmdbid=tmdb_id, mtype=mtype)
            if info and base_info:
                return {**base_info, **info}
            return info or base_info

    def _add_key_cache(self, key):
        self._webhook_msg_keys[key] = time.time() + self.DEFAULT_EXPIRATION_TIME

    def _remove_key_cache(self, key):
        if key in self._webhook_msg_keys:
            del self._webhook_msg_keys[key]

    def _clean_expired_cache(self):
        current = time.time()
        expired = [k for k, v in self._webhook_msg_keys.items() if v <= current]
        for k in expired: self._webhook_msg_keys.pop(k, None)

    def stop_service(self):
        try:
            for series_id in list(self._pending_messages.keys()):
                self._send_aggregated_message(series_id)
            for timer in self._aggregate_timers.values():
                timer.cancel()
            self._aggregate_timers.clear()
            self._pending_messages.clear()
            self._webhook_msg_keys.clear()
        except Exception as e:
            logger.error(f"插件停止出错: {e}")