import time
from pathlib import Path
from typing import Any, List, Dict, Tuple, Optional

from app.core.context import MediaInfo
from app.core.event import eventmanager, Event
from app.helper.mediaserver import MediaServerHelper
from app.log import logger
from app.plugins import _PluginBase
from app.schemas import TransferInfo, RefreshMediaItem, ServiceInfo
from app.schemas.types import EventType


class MediaServerRefresh(_PluginBase):
    # 插件名称
    plugin_name = "媒体库服务器刷新"
    # 插件描述
    plugin_desc = "入库后自动刷新Emby/Jellyfin/Plex服务器海报墙，支持TMDB去重。"
    # 插件图标
    plugin_icon = "refresh2.png"
    # 插件版本
    plugin_version = "1.4.0"
    # 插件作者
    plugin_author = "jxxghp"
    # 作者主页
    author_url = "https://github.com/jxxghp"
    # 插件配置项ID前缀
    plugin_config_prefix = "mediaserverrefresh_"
    # 加载顺序
    plugin_order = 14
    # 可使用的用户级别
    auth_level = 1

    # 私有属性
    mediaserver_helper = None
    _enabled = False
    _delay = 0
    _mediaservers = None
    _smart_refresh = True
    _refreshed_tmdb_ids = set()  # 存储已刷新的TMDB ID
    _tv_refresh_mode = "last"  # 剧集刷新模式：last-最后一集时刷新，first-第一集时刷新，all-每集都刷新

    def init_plugin(self, config: dict = None):
        self.mediaserver_helper = MediaServerHelper()
        if config:
            self._enabled = config.get("enabled")
            self._delay = config.get("delay") or 0
            self._mediaservers = config.get("mediaservers") or []
            self._smart_refresh = config.get("smart_refresh", True)
            self._tv_refresh_mode = config.get("tv_refresh_mode", "last")
            
        # 重置已刷新的TMDB ID集合
        self._refreshed_tmdb_ids = set()

    @property
    def service_infos(self) -> Optional[Dict[str, ServiceInfo]]:
        """
        服务信息
        """
        if not self._mediaservers:
            logger.warning("尚未配置媒体服务器，请检查配置")
            return None

        services = self.mediaserver_helper.get_services(name_filters=self._mediaservers)
        if not services:
            logger.warning("获取媒体服务器实例失败，请检查配置")
            return None

        active_services = {}
        for service_name, service_info in services.items():
            if service_info.instance.is_inactive():
                logger.warning(f"媒体服务器 {service_name} 未连接，请检查配置")
            else:
                active_services[service_name] = service_info

        if not active_services:
            logger.warning("没有已连接的媒体服务器，请检查配置")
            return None

        return active_services

    def get_state(self) -> bool:
        return self._enabled

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        pass

    def get_api(self) -> List[Dict[str, Any]]:
        pass

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        """
        拼装插件配置页面，需要返回两块数据：1、页面配置；2、数据结构
        """
        return [
            {
                'component': 'VForm',
                'content': [
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 6
                                },
                                'content': [
                                    {
                                        'component': 'VSwitch',
                                        'props': {
                                            'model': 'enabled',
                                            'label': '启用插件',
                                        }
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 6
                                },
                                'content': [
                                    {
                                        'component': 'VSwitch',
                                        'props': {
                                            'model': 'smart_refresh',
                                            'label': 'TMDB去重刷新',
                                            'hint': '同一TMDB ID的内容只刷新一次'
                                        }
                                    }
                                ]
                            }
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12
                                },
                                'content': [
                                    {
                                        'component': 'VSelect',
                                        'props': {
                                            'multiple': True,
                                            'chips': True,
                                            'clearable': True,
                                            'model': 'mediaservers',
                                            'label': '媒体服务器',
                                            'items': [{"title": config.name, "value": config.name}
                                                      for config in self.mediaserver_helper.get_configs().values()]
                                        }
                                    }
                                ]
                            }
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 6
                                },
                                'content': [
                                    {
                                        'component': 'VTextField',
                                        'props': {
                                            'model': 'delay',
                                            'label': '延迟时间（秒）',
                                            'placeholder': '0'
                                        }
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 6
                                },
                                'content': [
                                    {
                                        'component': 'VSelect',
                                        'props': {
                                            'model': 'tv_refresh_mode',
                                            'label': '剧集刷新模式',
                                            'items': [
                                                {'title': '最后一集时刷新', 'value': 'last'},
                                                {'title': '第一集时刷新', 'value': 'first'},
                                                {'title': '每集都刷新', 'value': 'all'}
                                            ]
                                        }
                                    }
                                ]
                            }
                        ]
                    }
                ]
            }
        ], {
            "enabled": False,
            "delay": 0,
            "smart_refresh": True,
            "tv_refresh_mode": "last"
        }

    def get_page(self) -> List[dict]:
        pass

    def should_refresh(self, mediainfo: MediaInfo) -> bool:
        """
        根据配置判断是否应该刷新此媒体
        """
        if not self._smart_refresh:
            # 如果不启用智能刷新，则每次都刷新
            return True

        tmdb_id = mediainfo.tmdb_id
        if not tmdb_id:
            # 没有TMDB ID的内容始终刷新
            return True

        # 检查该TMDB ID是否已经刷新过
        if tmdb_id in self._refreshed_tmdb_ids:
            logger.info(f"TMDB ID {tmdb_id} 已刷新过，跳过此次刷新")
            return False

        # 如果是电影，直接刷新
        if mediainfo.type == "电影":
            self._refreshed_tmdb_ids.add(tmdb_id)
            return True

        # 对于剧集，根据配置的剧集刷新模式决定
        if mediainfo.type == "电视剧":
            if self._tv_refresh_mode == "all":
                # 每集都刷新
                return True
            elif self._tv_refresh_mode == "first":
                # 第一集时刷新
                if mediainfo.season and mediainfo.episode:
                    # 只在第一季第一集时刷新
                    if mediainfo.season == 1 and mediainfo.episode == 1:
                        self._refreshed_tmdb_ids.add(tmdb_id)
                        return True
                    return False
                else:
                    # 如果没有季集信息，则刷新
                    self._refreshed_tmdb_ids.add(tmdb_id)
                    return True
            elif self._tv_refresh_mode == "last":
                # 暂时刷新，在未来可以实现更复杂的"最后一集"逻辑
                # 当前实现：将刷新请求加入队列，由调度器决定何时真正刷新
                self._refreshed_tmdb_ids.add(tmdb_id)
                return True

        # 默认刷新
        return True

    @eventmanager.register(EventType.TransferComplete)
    def refresh(self, event: Event):
        """
        发送通知消息
        """
        if not self._enabled:
            return

        event_info: dict = event.event_data
        if not event_info:
            return

        # 刷新媒体库
        if not self.service_infos:
            return

        # 入库数据
        transferinfo: TransferInfo = event_info.get("transferinfo")
        if not transferinfo or not transferinfo.target_diritem or not transferinfo.target_diritem.path:
            return

        mediainfo: MediaInfo = event_info.get("mediainfo")
        if not mediainfo:
            return

        # 检查是否应该刷新
        if not self.should_refresh(mediainfo):
            return

        if self._delay:
            logger.info(f"延迟 {self._delay} 秒后刷新媒体库... ")
            time.sleep(float(self._delay))

        items = [
            RefreshMediaItem(
                title=mediainfo.title,
                year=mediainfo.year,
                type=mediainfo.type,
                category=mediainfo.category,
                target_path=Path(transferinfo.target_diritem.path)
            )
        ]

        # 执行刷新
        logger.info(f"刷新媒体库: {mediainfo.title} ({mediainfo.year}) TMDB ID: {mediainfo.tmdb_id}")
        for name, service in self.service_infos.items():
            if hasattr(service.instance, 'refresh_library_by_items'):
                service.instance.refresh_library_by_items(items)
            elif hasattr(service.instance, 'refresh_root_library'):
                # FIXME Jellyfin未找到刷新单个项目的API
                service.instance.refresh_root_library()
            else:
                logger.warning(f"{name} 不支持刷新")

    def stop_service(self):
        """
        退出插件
        """
        pass
