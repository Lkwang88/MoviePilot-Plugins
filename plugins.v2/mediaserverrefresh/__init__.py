import threading
from pathlib import Path
from typing import Any, List, Dict, Tuple, Optional, Set

from app.core.context import MediaInfo
from app.core.event import eventmanager, Event
from app.helper.mediaserver import MediaServerHelper
from app.log import logger
from app.plugins import _PluginBase
from app.schemas import TransferInfo, RefreshMediaItem, ServiceInfo
from app.schemas.types import EventType

# ----------------------------------------------------------------------------
# Modified by: LKWANG88
# Feature: 异步防抖刷新 (Async Debounce Refresh)
# ----------------------------------------------------------------------------

class MediaServerRefresh(_PluginBase):
    # 插件基本信息
    plugin_name = "媒体库服务器刷新"
    plugin_desc = "入库后自动刷新Emby/Jellyfin/Plex海报墙 (LKWANG88 定制防抖版)。"
    plugin_icon = "refresh2.png"
    plugin_version = "2.0.0"
    
    # 这里修改为你的名字，系统界面中会显示
    plugin_author = "LKWANG88"
    author_url = "https://github.com/jxxghp"
    
    plugin_config_prefix = "mediaserverrefresh_"
    plugin_order = 14
    auth_level = 1

    # 配置属性
    _enabled = False
    _delay = 0
    _target_servers = []  # 重命名以区分实例与配置名

    # 运行时属性
    _timer: Optional[threading.Timer] = None
    _pending_items: List[RefreshMediaItem] = []
    _lock = threading.Lock()

    def init_plugin(self, config: dict = None):
        if config:
            self._enabled = config.get("enabled")
            self._delay = config.get("delay") or 0
            self._target_servers = config.get("mediaservers") or []
        
        # 初始化清理
        self._stop_timer()
        with self._lock:
            self._pending_items.clear()

    @property
    def service_infos(self) -> Optional[Dict[str, ServiceInfo]]:
        """
        获取活跃的服务实例
        优化：增加异常捕获，防止单次网络抖动导致崩溃
        """
        if not self._target_servers:
            return None
        
        try:
            services = MediaServerHelper().get_services(name_filters=self._target_servers)
            if not services:
                return None

            active_services = {}
            for service_name, service_info in services.items():
                # 即使在这里，也不建议过于频繁检查 is_inactive，但在获取实例时无法避免
                if service_info.instance and not service_info.instance.is_inactive():
                    active_services[service_name] = service_info
            
            return active_services
        except Exception as e:
            logger.error(f"LKWANG88-Plugin: 获取媒体服务器列表失败: {e}")
            return None

    def get_state(self) -> bool:
        return self._enabled

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        """
        保持原有的表单逻辑
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
                                            'placeholder': '30' # 建议默认值设大一点
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
        """
        事件回调：极速响应，仅做入队和重置计时器操作
        """
        if not self._enabled:
            return

        event_info: dict = event.event_data
        if not event_info:
            return

        # 获取数据
        transferinfo: TransferInfo = event_info.get("transferinfo")
        mediainfo: MediaInfo = event_info.get("mediainfo")
        
        if not transferinfo or not transferinfo.target_diritem or not transferinfo.target_diritem.path:
            return

        # 构造刷新对象
        item = RefreshMediaItem(
            title=mediainfo.title,
            year=mediainfo.year,
            type=mediainfo.type,
            category=mediainfo.category,
            target_path=Path(transferinfo.target_diritem.path),
        )

        with self._lock:
            # 1. 加入队列
            self._pending_items.append(item)
            # 2. 停止旧计时器
            self._stop_timer()
            # 3. 启动新计时器 (Debounce 核心)
            delay = float(self._delay) if self._delay else 5.0 # 默认至少5秒缓冲
            logger.info(f"LKWANG88-Plugin: 监测到入库 [{mediainfo.title}]，将在 {delay} 秒后触发批量刷新 (当前队列: {len(self._pending_items)})")
            self._timer = threading.Timer(delay, self._flush_queue)
            self._timer.start()

    def _flush_queue(self):
        """
        真正执行刷新的工作线程
        """
        with self._lock:
            if not self._pending_items:
                return
            # 取出所有待处理项并清空队列
            items_to_refresh = list(self._pending_items)
            self._pending_items.clear()
            self._timer = None

        logger.info(f"LKWANG88-Plugin: 防抖结束，开始执行媒体库刷新，共 {len(items_to_refresh)} 个项目...")
        
        # 惰性获取服务实例（此时再检查网络，避免在事件风暴中频繁检查）
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
        """安全停止计时器"""
        if self._timer and self._timer.is_alive():
            self._timer.cancel()
        self._timer = None

    def stop_service(self):
        """
        插件退出清理
        """
        self._stop_timer()
        # 退出时，如果有残留数据，可以选择立即刷新一次，或者直接丢弃
        # 这里选择为了安全退出，不再执行耗时操作
        with self._lock:
            self._pending_items.clear()