import threading
from pathlib import Path
from typing import Any, List, Dict, Tuple, Optional

from app.log import logger
from app.core.context import MediaInfo
from app.core.event import eventmanager, Event
from app.helper.mediaserver import MediaServerHelper
from app.plugins import _PluginBase
from app.schemas import TransferInfo, RefreshMediaItem, ServiceInfo
from app.schemas.types import EventType

# ----------------------------------------------------------------------------
# Modified by: LKWANG88
# Feature: 父目录刷新策略 + 人话日志 (Parent Directory Refresh + Detailed Logs)
# ----------------------------------------------------------------------------

class MediaServerRefresh88(_PluginBase):
    plugin_name = "媒体库刷新 (LKWANG88版)"
    plugin_desc = "入库后自动刷新Emby/Jellyfin/Plex海报墙 (LKWANG88 独立防抖版)。"
    plugin_icon = "refresh2.png"
    plugin_version = "2.0.8"
    
    plugin_author = "LKWANG88"
    author_url = "https://github.com/jxxghp"
    
    plugin_config_prefix = "mediaserverrefresh88_"
    
    plugin_order = 14
    auth_level = 1

    _enabled = False
    _delay = 0
    _target_servers = []

    _timer: Optional[threading.Timer] = None
    _pending_items: List[RefreshMediaItem] = []
    _lock = threading.Lock()

    def init_plugin(self, config: dict = None):
        if config:
            self._enabled = config.get("enabled")
            self._delay = config.get("delay") or 0
            self._target_servers = config.get("mediaservers") or []
        
        self._stop_timer()
        with self._lock:
            self._pending_items.clear()
            
        if self._enabled:
            logger.info(f"LKWANG88-Plugin: 独立防抖版已就绪，当前延迟设定: {self._delay}秒")

    @property
    def service_infos(self) -> Optional[Dict[str, ServiceInfo]]:
        if not self._target_servers:
            return None
        try:
            services = MediaServerHelper().get_services(name_filters=self._target_servers)
            if not services:
                return None
            active_services = {}
            for service_name, service_info in services.items():
                if service_info.instance and not service_info.instance.is_inactive():
                    active_services[service_name] = service_info
            return active_services
        except Exception as e:
            logger.error(f"LKWANG88-Plugin: 获取媒体服务器列表失败: {e}")
            return None

    def get_state(self) -> bool:
        return self._enabled

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        return []

    def get_api(self) -> List[Dict[str, Any]]:
        return []

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        return [
            {
                'component': 'VForm',
                'content': [
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 6},
                                'content': [
                                    {
                                        'component': 'VSwitch',
                                        'props': {
                                            'model': 'enabled',
                                            'label': '启用插件',
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
                                'props': {'cols': 12},
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
                                                      for config in MediaServerHelper().get_configs().values()]
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
                                'props': {'cols': 12},
                                'content': [
                                    {
                                        'component': 'VTextField',
                                        'props': {
                                            'model': 'delay',
                                            'label': '延迟时间（秒）',
                                            'placeholder': '30'
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
            "delay": 30,
            "mediaservers": []
        }

    def get_page(self) -> List[dict]:
        pass

    @eventmanager.register(EventType.TransferComplete)
    def refresh(self, event: Event):
        if not self._enabled:
            return

        event_info: dict = event.event_data
        if not event_info:
            return

        transferinfo: TransferInfo = event_info.get("transferinfo")
        mediainfo: MediaInfo = event_info.get("mediainfo")
        
        if not transferinfo or not transferinfo.target_diritem or not transferinfo.target_diritem.path:
            return

        # 获取原始文件路径
        file_path = Path(transferinfo.target_diritem.path)
        
        # [核心修改] 策略：不再刷新具体文件，而是刷新它的父目录（文件夹）
        # 如果是电影：/Movie/Avatar/Avatar.mkv -> 刷新 /Movie/Avatar
        # 如果是剧集：/TV/Show/Season 1/Ep01.mkv -> 刷新 /TV/Show/Season 1
        # 这通常足以触发 Emby 的目录扫描。
        target_path = file_path.parent

        # 构造对象
        item = RefreshMediaItem(
            title=mediainfo.title,
            year=mediainfo.year,
            type=mediainfo.type,
            category=mediainfo.category,
            target_path=target_path,
        )

        with self._lock:
            self._pending_items.append(item)
            self._stop_timer()
            
            try:
                delay_val = float(self._delay)
            except (TypeError, ValueError):
                delay_val = 5.0
            
            if delay_val < 1: delay_val = 1.0

            # [人话日志] 打印出具体的路径变化，方便排查
            logger.info(
                f"LKWANG88-Plugin: 监测到入库 [{mediainfo.title}]\n"
                f"    - 原始文件: {file_path}\n"
                f"    - 计划刷新: {target_path} (父目录)\n"
                f"    - 状态: 将在 {delay_val} 秒后执行 (当前队列: {len(self._pending_items)})"
            )
            
            self._timer = threading.Timer(delay_val, self._flush_queue)
            self._timer.start()

    def _flush_queue(self):
        with self._lock:
            if not self._pending_items:
                return
            items_to_refresh = list(self._pending_items)
            self._pending_items.clear()
            self._timer = None

        # [人话日志] 打印本次批量刷新的摘要
        titles = [item.title for item in items_to_refresh]
        # 取出第一个路径作为示例显示
        sample_path = items_to_refresh[0].target_path if items_to_refresh else "无"
        
        logger.info(
            f"LKWANG88-Plugin: 防抖结束，开始执行刷新\n"
            f"    - 项目数量: {len(items_to_refresh)}\n"
            f"    - 涉及内容: {titles}\n"
            f"    - 路径示例: {sample_path}"
        )
        
        services = self.service_infos
        if not services:
            logger.warning("LKWANG88-Plugin: 刷新取消，未找到活跃的媒体服务器连接。")
            return

        for name, service in services.items():
            try:
                if hasattr(service.instance, 'refresh_library_by_items'):
                    logger.info(f"LKWANG88-Plugin: 正在请求 {name} 刷新上述目录...")
                    service.instance.refresh_library_by_items(items_to_refresh)
                elif hasattr(service.instance, 'refresh_root_library'):
                    logger.info(f"{name} 不支持局部刷新，触发全库扫描...")
                    service.instance.refresh_root_library()
                else:
                    logger.warning(f"{name} 未实现刷新接口")
            except Exception as e:
                logger.error(f"刷新 {name} 失败: {e}")

    def _stop_timer(self):
        if self._timer and self._timer.is_alive():
            self._timer.cancel()
        self._timer = None

    def stop_service(self):
        self._stop_timer()
        with self._lock:
            self._pending_items.clear()