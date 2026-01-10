import threading
from pathlib import Path
from typing import Any, List, Dict, Tuple, Optional

from app.core.context import MediaInfo
from app.core.event import eventmanager, Event
from app.helper.mediaserver import MediaServerHelper
from app.log import logger
from app.plugins import _PluginBase
from app.schemas import TransferInfo, RefreshMediaItem, ServiceInfo
from app.schemas.types import EventType

# ----------------------------------------------------------------------------
# Modified by: LKWANG88
# Feature: 异步防抖刷新 (Async Debounce Refresh) - Fully Independent Class
# Class Name: MediaServerRefresh88
# ----------------------------------------------------------------------------

class MediaServerRefresh88(_PluginBase):
    # [关键修改] 插件类名已改为 MediaServerRefresh88，彻底独立

    # 插件基本信息
    plugin_name = "媒体库刷新 (LKWANG88版)"  # 修改名称，方便在UI中直接区分
    plugin_desc = "入库后自动刷新Emby/Jellyfin/Plex海报墙 (LKWANG88 独立防抖版)。"
    plugin_icon = "refresh2.png"
    plugin_version = "2.0.4"
    
    plugin_author = "LKWANG88"
    author_url = "https://github.com/jxxghp"
    
    # 独立的配置前缀，数据与原版完全隔离
    plugin_config_prefix = "mediaserverrefresh88_"
    
    plugin_order = 14
    auth_level = 1

    # 配置属性
    _enabled = False
    _delay = 0
    _target_servers = []

    # 运行时属性
    _timer: Optional[threading.Timer] = None
    _pending_items: List[RefreshMediaItem] = []
    _lock = threading.Lock()

    def init_plugin(self, config: dict = None):
        """
        初始化：加载配置
        """
        if config:
            self._enabled = config.get("enabled")
            self._delay = config.get("delay") or 0
            self._target_servers = config.get("mediaservers") or []
        
        self._stop_timer()
        with self._lock:
            self._pending_items.clear()
        
        # [新增] 启动成功日志，证明插件已活过来
        if self._enabled:
            logger.info(f"LKWANG88-Plugin: 独立版插件已就绪，当前延迟设定: {self._delay}秒")

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

        item = RefreshMediaItem(
            title=mediainfo.title,
            year=mediainfo.year,
            type=mediainfo.type,
            category=mediainfo.category,
            target_path=Path(transferinfo.target_diritem.path),
        )

        with self._lock:
            self._pending_items.append(item)
            self._stop_timer()
            
            try:
                delay_val = float(self._delay)
            except (TypeError, ValueError):
                delay_val = 5.0
            
            if delay_val < 1: 
                delay_val = 1.0

            logger.info(f"LKWANG88-Plugin: 监测到入库 [{mediainfo.title}]，将在 {delay_val} 秒后触发批量刷新 (当前队列: {len(self._pending_items)})")
            self._timer = threading.Timer(delay_val, self._flush_queue)
            self._timer.start()

    def _flush_queue(self):
        with self._lock:
            if not self._pending_items:
                return
            items_to_refresh = list(self._pending_items)
            self._pending_items.clear()
            self._timer = None

        logger.info(f"LKWANG88-Plugin: 防抖结束，开始执行媒体库刷新，共 {len(items_to_refresh)} 个项目...")
        
        services = self.service_infos
        if not services:
            logger.warning("LKWANG88-Plugin: 刷新取消，未找到活跃的媒体服务器连接。")
            return

        for name, service in services.items():
            try:
                if hasattr(service.instance, 'refresh_library_by_items'):
                    logger.info(f"正在刷新 {name} ...")
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
        """
        退出清理
        """
        self._stop_timer()
        with self._lock:
            self._pending_items.clear()