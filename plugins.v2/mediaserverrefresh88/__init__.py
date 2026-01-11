import threading
import json
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
# Fix: 修复 API URL 格式，使用 [HOST]/[APIKEY] 占位符 (v2.1.2)
# Reference: CloudStrmCompanion Line 444
# ----------------------------------------------------------------------------

class MediaServerRefresh88(_PluginBase):
    plugin_name = "媒体库刷新 (LKWANG88版)"
    plugin_desc = "入库后自动刷新Emby/Jellyfin/Plex海报墙 (LKWANG88 路径推送版)。"
    plugin_icon = "refresh2.png"
    plugin_version = "2.1.2"
    
    plugin_author = "LKWANG88"
    author_url = "https://github.com/jxxghp"
    
    plugin_config_prefix = "mediaserverrefresh88_"
    plugin_order = 14
    auth_level = 1

    _enabled = False
    _delay = 0
    _target_servers = []
    _path_mapping = {}

    _timer: Optional[threading.Timer] = None
    _pending_items: List[RefreshMediaItem] = []
    _lock = threading.Lock()

    def init_plugin(self, config: dict = None):
        if config:
            self._enabled = config.get("enabled")
            self._delay = config.get("delay") or 0
            self._target_servers = config.get("mediaservers") or []
            
            # 解析路径映射
            self._path_mapping = {}
            mapping_str = config.get("path_mapping")
            if mapping_str:
                for line in str(mapping_str).split('\n'):
                    if ':' in line:
                        parts = line.split(':', 1)
                        self._path_mapping[parts[0].strip()] = parts[1].strip()

        self._stop_timer()
        with self._lock:
            self._pending_items.clear()
            
        if self._enabled:
            logger.info(f"LKWANG88-Plugin: 路径推送修正版 (v2.1.2) 已就绪，映射规则: {len(self._path_mapping)}条")

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
                                'props': {'cols': 12, 'md': 6},
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
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {'cols': 12},
                                'content': [
                                    {
                                        'component': 'VTextarea',
                                        'props': {
                                            'model': 'path_mapping',
                                            'label': '路径映射 (MP路径:Emby路径)',
                                            'rows': 3,
                                            'placeholder': '/zfsHDD/download:/data/media\n每行一条，若路径一致请留空'
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
            "mediaservers": [],
            "path_mapping": ""
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

        # 获取父目录作为刷新目标
        target_path = Path(transferinfo.target_diritem.path).parent

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

            logger.info(f"LKWANG88-Plugin: 监测到入库，将在 {delay_val} 秒后推送路径: {target_path}")
            self._timer = threading.Timer(delay_val, self._flush_queue)
            self._timer.start()

    def _convert_path(self, local_path: str) -> str:
        """
        路径映射转换
        """
        local_path_str = str(local_path)
        for mp_path, emby_path in self._path_mapping.items():
            if local_path_str.startswith(mp_path):
                remote_path = local_path_str.replace(mp_path, emby_path, 1)
                logger.info(f"LKWANG88-Plugin: 执行路径映射 {mp_path} -> {emby_path}")
                return remote_path
        return local_path_str

    def _flush_queue(self):
        with self._lock:
            if not self._pending_items:
                return
            unique_paths = list(set([item.target_path for item in self._pending_items]))
            self._pending_items.clear()
            self._timer = None

        logger.info(f"LKWANG88-Plugin: 防抖结束，准备推送 {len(unique_paths)} 个目录更新...")
        
        services = self.service_infos
        if not services:
            logger.warning("LKWANG88-Plugin: 刷新取消，未找到活跃的媒体服务器连接。")
            return

        for name, service in services.items():
            if service.type not in ['emby', 'jellyfin']:
                logger.warning(f"LKWANG88-Plugin: {name} 不是 Emby/Jellyfin，跳过。")
                continue

            try:
                emby_instance = service.instance
                
                for path in unique_paths:
                    # 1. 路径转换
                    final_path = self._convert_path(str(path))
                    
                    # 2. 构造 API Payload
                    payload = {
                        "Updates": [
                            {
                                "Path": final_path,
                                "UpdateType": "Created"
                            }
                        ]
                    }
                    
                    # 3. [关键修正] 使用 [HOST] 和 [APIKEY] 占位符
                    # MoviePilot 的 post_data 会自动替换这些值为实例的配置
                    req_url = '[HOST]emby/Library/Media/Updated?api_key=[APIKEY]'
                    
                    logger.info(f"LKWANG88-Plugin: 向 {name} 推送路径 -> {final_path}")
                    
                    response = emby_instance.post_data(
                        url=req_url,
                        data=json.dumps(payload),
                        headers={"Content-Type": "application/json"}
                    )
                    
                    if response and response.status_code in [200, 204]:
                        logger.info(f"LKWANG88-Plugin: {name} 响应成功 (Code: {response.status_code})")
                    else:
                        code = response.status_code if response else "None/ConnectError"
                        logger.error(f"LKWANG88-Plugin: {name} 响应失败: {code}，请检查网络或路径映射")

            except Exception as e:
                logger.error(f"LKWANG88-Plugin: 推送异常 {name}: {e}")

    def _stop_timer(self):
        if self._timer and self._timer.is_alive():
            self._timer.cancel()
        self._timer = None

    def stop_service(self):
        self._stop_timer()
        with self._lock:
            self._pending_items.clear()