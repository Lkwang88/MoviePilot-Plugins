import json
import os
import re
import shutil
import threading
import time
import traceback
import urllib.parse
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, List, Dict, Tuple, Optional

import pytz
import requests
from apscheduler.schedulers.background import BackgroundScheduler
from watchdog.events import FileSystemEventHandler
from watchdog.observers.polling import PollingObserver

from app.core.config import settings
from app.core.event import eventmanager, Event
from app.core.metainfo import MetaInfoPath
from app.helper.mediaserver import MediaServerHelper
from app.log import logger
from app.plugins import _PluginBase
from app.schemas import MediaInfo
from app.schemas.types import EventType, NotificationType, MediaType
from app.utils.http import RequestUtils
from app.utils.string import StringUtils

lock = threading.Lock()


class FileMonitorHandler(FileSystemEventHandler):
    """
    目录监控响应类
    """

    def __init__(self, monpath: str, sync: Any, **kwargs):
        super(FileMonitorHandler, self).__init__(**kwargs)
        self._watch_path = monpath
        self.sync = sync

    def on_created(self, event):
        self.sync.event_handler(event=event, text="创建",
                                mon_path=self._watch_path, event_path=event.src_path)

    def on_moved(self, event):
        self.sync.event_handler(event=event, text="移动",
                                mon_path=self._watch_path, event_path=event.dest_path)


class Lkwang88(_PluginBase):
    # 插件名称
    plugin_name = "CloudStrmCompanionLkwang88"
    # 插件描述
    plugin_desc = "基于CloudStrm的优化版：支持异步启动与聚合刷新。"
    # 插件图标
    plugin_icon = "cloudcompanion.png"
    # 插件版本
    plugin_version = "2.0.1"
    # 插件作者
    plugin_author = "lkwang88"
    # 作者主页
    author_url = ""
    # 插件配置项ID前缀
    plugin_config_prefix = "Lkwang88_"
    # 加载顺序
    plugin_order = 26
    # 可使用的用户级别
    auth_level = 1

    # 私有属性
    _enabled = False
    _monitor_confs = None
    _cover = False
    _monitor = False
    _onlyonce = False
    _copy_files = False
    _copy_subtitles = False
    _url = None
    _notify = False
    _refresh_emby = False
    _uriencode = False
    _strm_dir_conf = {}
    _cloud_dir_conf = {}
    _category_conf = {}
    _format_conf = {}
    _cloud_files = []
    _observer = []
    _medias = {}
    _rmt_mediaext = None
    _other_mediaext = None
    _interval: int = 10
    _mediaservers = None
    mediaserver_helper = None
    _emby_paths = {}
    _path_replacements = {}
    _cloud_files_json = "cloud_files.json"
    _headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 11_2_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.192 Safari/537.36",
        "Cookie": "",
    }

    # 优化：刷新队列和锁
    _refresh_queue = set()
    _refresh_lock = threading.Lock()
    _last_refresh_time = 0

    # 定时器
    _scheduler: Optional[BackgroundScheduler] = None
    # 退出事件
    _event = threading.Event()

    def init_plugin(self, config: dict = None):
        # 清空配置
        self._strm_dir_conf = {}
        self._cloud_dir_conf = {}
        self._format_conf = {}
        self._category_conf = {}
        self._path_replacements = {}
        self._cloud_files_json = os.path.join(self.get_data_path(), self._cloud_files_json)
        self.mediaserver_helper = MediaServerHelper()

        if config:
            self._enabled = config.get("enabled")
            self._onlyonce = config.get("onlyonce")
            self._interval = config.get("interval") or 10
            self._monitor = config.get("monitor")
            self._cover = config.get("cover")
            self._copy_files = config.get("copy_files")
            self._copy_subtitles = config.get("copy_subtitles")
            self._refresh_emby = config.get("refresh_emby")
            self._notify = config.get("notify")
            self._uriencode = config.get("uriencode")
            self._monitor_confs = config.get("monitor_confs")
            self._url = config.get("url")
            self._mediaservers = config.get("mediaservers") or []
            self._other_mediaext = config.get("other_mediaext")
            
            if config.get("path_replacements"):
                for replacement in str(config.get("path_replacements")).split("\n"):
                    if replacement and ":" in replacement:
                        source, target = replacement.split(":", 1)
                        self._path_replacements[source.strip()] = target.strip()
            
            self._rmt_mediaext = config.get(
                "rmt_mediaext") or ".mp4, .mkv, .ts, .iso,.rmvb, .avi, .mov, .mpeg,.mpg, .wmv, .3gp, .asf, .m4v, .flv, .m2ts, .strm,.tp, .f4v"
            
            if config.get("emby_path"):
                for path in str(config.get("emby_path")).split(","):
                    self._emby_paths[path.split(":")[0]] = path.split(":")[1]

        # 停止现有任务
        self.stop_service()

        if self._enabled or self._onlyonce:
            # 定时服务
            self._scheduler = BackgroundScheduler(timezone=settings.TZ)

            if self._notify:
                # 追加入库消息统一发送服务
                self._scheduler.add_job(self.send_msg, trigger='interval', seconds=15)

            # 优化：注册聚合刷新服务，每30秒检查一次待刷新队列
            if self._refresh_emby:
                self._scheduler.add_job(self.__process_refresh_queue, trigger='interval', seconds=30)

            # 读取目录配置
            monitor_confs = self._monitor_confs.split("\n")
            if not monitor_confs:
                return
            for monitor_conf in monitor_confs:
                if not monitor_conf or str(monitor_conf).startswith("#"):
                    continue

                monitor = None
                if monitor_conf.count("$") == 1:
                    monitor = str(monitor_conf.split("$")[1])
                    monitor_conf = monitor_conf.split("$")[0]
                category = None
                if monitor_conf.count("@") == 1:
                    category = str(monitor_conf.split("@")[1])
                    monitor_conf = monitor_conf.split("@")[0]
                if str(monitor_conf).count("#") == 3:
                    local_dir = str(monitor_conf).split("#")[0]
                    strm_dir = str(monitor_conf).split("#")[1]
                    cloud_dir = str(monitor_conf).split("#")[2]
                    format_str = str(monitor_conf).split("#")[3]
                else:
                    logger.error(f"{monitor_conf} 格式错误")
                    continue
                
                self._strm_dir_conf[local_dir] = strm_dir
                self._cloud_dir_conf[local_dir] = cloud_dir
                self._format_conf[local_dir] = format_str
                self._category_conf[local_dir] = category

                # 优化：不在此处直接启动 observer
                try:
                    if strm_dir and Path(strm_dir).is_relative_to(Path(local_dir)):
                        logger.warn(f"{strm_dir} 是 {local_dir} 的子目录，无法监控")
                        continue
                except Exception as e:
                    pass

            # 优化：异步启动监控服务
            if self._monitor:
                threading.Thread(target=self.__async_start_monitor, daemon=True).start()

            # 运行一次定时服务
            if self._onlyonce:
                logger.info("lkwang88 全量执行服务启动，立即运行一次")
                self._scheduler.add_job(func=self.scan, trigger='date',
                                        run_date=datetime.now(tz=pytz.timezone(settings.TZ)) + timedelta(seconds=3),
                                        name="lkwang88 全量执行服务")
                self._onlyonce = False
                self.__update_config()

            # 启动任务
            if self._scheduler.get_jobs():
                self._scheduler.print_jobs()
                self._scheduler.start()

    def __async_start_monitor(self):
        """
        优化：异步启动监控，带重试机制
        """
        time.sleep(10)
        
        for local_dir in self._strm_dir_conf.keys():
            # 简单的挂载检测
            retries = 12
            while not os.path.exists(local_dir) and retries > 0:
                logger.info(f"[lkwang88] 监控目录 {local_dir} 尚未就绪，等待挂载... (剩余重试: {retries})")
                time.sleep(10)
                retries -= 1
            
            if not os.path.exists(local_dir):
                logger.error(f"[lkwang88] 监控目录 {local_dir} 无法访问，跳过该目录监控")
                continue

            try:
                observer = PollingObserver(timeout=10)
                self._observer.append(observer)
                observer.schedule(FileMonitorHandler(local_dir, self), path=local_dir, recursive=True)
                observer.daemon = True
                observer.start()
                logger.info(f"[lkwang88] {local_dir} 实时监控服务已启动")
            except Exception as e:
                err_msg = str(e)
                if "inotify" in err_msg and "reached" in err_msg:
                    logger.warn(f"[lkwang88] inotify资源不足，请优化宿主机配置: {err_msg}")
                else:
                    logger.error(f"[lkwang88] {local_dir} 启动监控失败：{err_msg}")

    def scan(self):
        logger.info("lkwang88 开始全量执行")
        for mon_path in self._strm_dir_conf.keys():
            if not os.path.exists(mon_path):
                continue
            for root, dirs, files in os.walk(mon_path):
                if "extrafanart" in dirs:
                    dirs.remove("extrafanart")
                for file in files:
                    source_file = os.path.join(root, file)
                    if (source_file.find("/@Recycle") != -1
                            or source_file.find("/#recycle") != -1
                            or source_file.find("/.") != -1
                            or source_file.find("/@eaDir") != -1):
                        continue
                    self.__handle_file(event_path=source_file, mon_path=mon_path)
        logger.info("lkwang88 全量执行完成")

    @eventmanager.register(EventType.PluginAction)
    def strm_one(self, event: Event = None):
        if event:
            event_data = event.event_data
            if not event_data or event_data.get("action") != "cloudstrm_file":
                return
            file_path = event_data.get("file_path")
            if not file_path:
                return
            mon_path = None
            for mon in self._strm_dir_conf.keys():
                if str(file_path).startswith(mon):
                    mon_path = mon
                    break
            if not mon_path:
                return
            self.__handle_file(event_path=file_path, mon_path=mon_path)

    def event_handler(self, event, mon_path: str, text: str, event_path: str):
        if not event.is_directory:
            if '.fuse_hidden' in event_path:
                return
            logger.debug("监控到文件%s：%s" % (text, event_path))
            self.__handle_file(event_path=event_path, mon_path=mon_path)

    def __handle_file(self, event_path: str, mon_path: str):
        try:
            if not Path(event_path).exists():
                return
            with lock:
                cloud_dir = self._cloud_dir_conf.get(mon_path)
                strm_dir = self._strm_dir_conf.get(mon_path)
                format_str = self._format_conf.get(mon_path)
                target_file = str(event_path).replace(mon_path, strm_dir)
                cloud_file = str(event_path).replace(mon_path, cloud_dir)

                if Path(event_path).suffix.lower() in [ext.strip() for ext in
                                                       self._rmt_mediaext.split(",")]:
                    strm_content = self.__format_content(format_str=format_str,
                                                         local_file=event_path,
                                                         cloud_file=str(cloud_file),
                                                         uriencode=self._uriencode)
                    
                    created = self.__create_strm_file(strm_file=target_file,
                                            strm_content=strm_content)

                    if created:
                        pattern = Path(event_path).stem.replace('[', '?').replace(']', '?')
                        files = list(Path(event_path).parent.glob(f"{pattern}.*"))
                        for file in files:
                            target_sub = str(file).replace(mon_path, strm_dir)
                            self.__handle_other_files(event_path=str(file), target_file=target_sub)

                        thumb_file = Path(event_path).parent / (Path(event_path).stem + "-thumb.jpg")
                        if thumb_file.exists():
                            target_thumb = str(thumb_file).replace(mon_path, strm_dir)
                            self.__handle_other_files(event_path=str(thumb_file), target_file=target_thumb)
                else:
                    self.__handle_other_files(event_path=event_path, target_file=target_file)
        except Exception as e:
            logger.error("目录监控发生错误：%s - %s" % (str(e), traceback.format_exc()))

    def __handle_other_files(self, event_path: str, target_file: str):
        if self._copy_files and self._other_mediaext and Path(event_path).suffix.lower() in [ext.strip() for
                                                                                             ext in
                                                                                             self._other_mediaext.split(
                                                                                                 ",")]:
            os.makedirs(os.path.dirname(target_file), exist_ok=True)
            shutil.copy2(str(event_path), target_file)
            logger.info(f"复制非媒体文件 {str(event_path)} 到 {target_file}")

        if self._copy_subtitles and Path(event_path).suffix.lower() in ['.srt', '.ass', '.ssa', '.sub']:
            os.makedirs(os.path.dirname(target_file), exist_ok=True)
            shutil.copy2(str(event_path), target_file)
            logger.info(f"复制字幕文件 {str(event_path)} 到 {target_file}")

    @staticmethod
    def __format_content(format_str: str, local_file: str, cloud_file: str, uriencode: bool):
        if "{local_file}" in format_str:
            return format_str.replace("{local_file}", local_file)
        elif "{cloud_file}" in format_str:
            if uriencode:
                cloud_file = urllib.parse.quote(cloud_file, safe='')
            else:
                cloud_file = cloud_file.replace("\\", "/")
            return format_str.replace("{cloud_file}", cloud_file)
        else:
            return None

    def __create_strm_file(self, strm_file: str, strm_content: str):
        try:
            if not Path(strm_file).parent.exists():
                os.makedirs(Path(strm_file).parent)

            strm_file_path = os.path.join(Path(strm_file).parent, f"{os.path.splitext(Path(strm_file).name)[0]}.strm")

            if Path(strm_file_path).exists() and not self._cover:
                return False
            
            for source, target in self._path_replacements.items():
                if source in strm_content:
                    strm_content = strm_content.replace(source, target)

            with open(strm_file_path, 'w', encoding='utf-8') as f:
                f.write(strm_content)

            logger.info(f"创建strm文件成功 {strm_file_path}")
            
            if self._url and Path(strm_content).suffix in settings.RMT_MEDIAEXT:
                RequestUtils(content_type="application/json").post(
                    url=self._url,
                    json={"path": str(strm_content), "type": "add"},
                )

            if self._notify and Path(strm_content).suffix in settings.RMT_MEDIAEXT:
                self.__collect_notify_info(strm_file_path)

            if self._refresh_emby and self._mediaservers:
                self.__add_to_refresh_queue(strm_file_path)
            
            return True
        except Exception as e:
            logger.error(f"创建strm文件失败 {strm_file} -> {str(e)}")
        return False

    def __collect_notify_info(self, strm_file):
        file_meta = MetaInfoPath(Path(strm_file))
        pattern = r'tmdbid=(\d+)'
        match = re.search(pattern, str(strm_file))
        if match:
            tmdbid = match.group(1)
            file_meta.tmdbid = tmdbid

        key = f"{file_meta.cn_name} ({file_meta.year}){f' {file_meta.season}' if file_meta.season else ''}"
        media_list = self._medias.get(key) or {}
        if media_list:
            episodes = media_list.get("episodes") or []
            if file_meta.begin_episode:
                if int(file_meta.begin_episode) not in episodes:
                    episodes.append(int(file_meta.begin_episode))
            media_list["episodes"] = episodes
            media_list["time"] = datetime.now()
        else:
            media_list = {
                "episodes": [int(file_meta.begin_episode)] if file_meta.begin_episode else [],
                "file_meta": file_meta,
                "type": "tv" if file_meta.season else "movie",
                "time": datetime.now()
            }
        self._medias[key] = media_list

    def __add_to_refresh_queue(self, strm_file_path: str):
        parent_dir = str(Path(strm_file_path).parent)
        with self._refresh_lock:
            self._refresh_queue.add(parent_dir)

    def __process_refresh_queue(self):
        if not self._refresh_queue:
            return

        with self._refresh_lock:
            paths = list(self._refresh_queue)
            self._refresh_queue.clear()
        
        logger.info(f"[lkwang88] 触发聚合刷新，本批次包含 {len(paths)} 个目录")
        for path in paths:
            self.__refresh_emby_file(path)

    def __refresh_emby_file(self, strm_file: str):
        emby_servers = self.mediaserver_helper.get_services(name_filters=self._mediaservers, type_filter="emby")
        if not emby_servers:
            return

        strm_file = self.__get_path(paths=self._emby_paths, file_path=strm_file)
        for emby_name, emby_server in emby_servers.items():
            emby = emby_server.instance
            try:
                res = emby.post_data(
                    url='emby/Library/Media/Updated',
                    data={
                        "Updates": [
                            {
                                "Path": strm_file,
                                "UpdateType": "Modified"
                            }
                        ]
                    }
                )
                if res and res.status_code in [200, 204]:
                    logger.info(f"[lkwang88] 媒体服务器 {emby_name} 刷新路径成功: {strm_file}")
                else:
                    logger.error(f"[lkwang88] 媒体服务器 {emby_name} 刷新失败 {res.status_code}")
            except Exception as err:
                logger.error(f"通知媒体服务器刷新失败：{str(err)}")

    def __get_path(self, paths, file_path: str):
        if paths and paths.keys():
            for library_path in paths.keys():
                if str(file_path).startswith(str(library_path)):
                    return str(file_path).replace(str(library_path), str(paths.get(str(library_path))))
        return file_path

    @eventmanager.register(EventType.PluginAction)
    def remote_sync_one(self, event: Event = None):
        if event:
            event_data = event.event_data
            if not event_data or event_data.get("action") != "strm_one":
                return
            args = event_data.get("arg_str")
            if not args:
                logger.error(f"缺少参数：{event_data}")
                return
            
            # 使用正则表达式匹配
            category = None
            args_arr = args.split(maxsplit=1)
            limit = None
            if len(args_arr) == 2:
                category = args_arr[0]
                args = args_arr[1]
                if str(args).isdigit():
                    limit = int(args)

            if category:
                # 判断是不是目录
                if Path(category).is_dir() and Path(category).exists() and limit is not None:
                    # 遍历所有监控目录
                    mon_path = None
                    for mon in self._category_conf.keys():
                        if str(category).startswith(mon):
                            mon_path = mon
                            break

                    # 指定路径
                    if not mon_path:
                        logger.error(f"未找到 {category} 对应的监控目录")
                        return

                    self.__handle_limit(path=category, mon_path=mon_path, limit=limit, event=event)
                    return
                else:
                    for mon_path in self._category_conf.keys():
                        mon_category = self._category_conf.get(mon_path)
                        if mon_category and str(category) in mon_category:
                            parent_path = os.path.join(mon_path, category)
                            if limit:
                                self.__handle_limit(path=parent_path, mon_path=mon_path, limit=limit, event=event)
                            else:
                                target_path = os.path.join(str(parent_path), args)
                                target_paths = self.__find_related_paths(os.path.join(str(parent_path), args))
                                if not target_paths:
                                    logger.error(f"未查找到 {category} {args} 对应的具体目录")
                                    return
                                for target_path in target_paths:
                                    for sroot, sdirs, sfiles in os.walk(target_path):
                                        for file_name in sdirs + sfiles:
                                            src_file = os.path.join(sroot, file_name)
                                            if Path(src_file).is_file():
                                                self.__handle_file(event_path=str(src_file), mon_path=mon_path)
                                    time.sleep(2)
                            return
            else:
                # 遍历所有监控目录
                mon_path = None
                for mon in self._category_conf.keys():
                    if str(args).startswith(mon):
                        mon_path = mon
                        break

                # 指定路径
                if mon_path:
                    if not Path(args).exists():
                        return
                    # 处理单文件
                    if Path(args).is_file():
                        self.__handle_file(event_path=str(args), mon_path=mon_path)
                        return
                    else:
                        # 处理指定目录
                        for sroot, sdirs, sfiles in os.walk(args):
                            for file_name in sdirs + sfiles:
                                src_file = os.path.join(sroot, file_name)
                                if Path(str(src_file)).is_file():
                                    self.__handle_file(event_path=str(src_file), mon_path=mon_path)
                        return
                else:
                    for mon_path in self._category_conf.keys():
                        mon_category = self._category_conf.get(mon_path)
                        if mon_category and str(args) in mon_category:
                            parent_path = os.path.join(mon_path, args)
                            for sroot, sdirs, sfiles in os.walk(parent_path):
                                for file_name in sdirs + sfiles:
                                    src_file = os.path.join(sroot, file_name)
                                    if Path(str(src_file)).is_file():
                                        self.__handle_file(event_path=str(src_file), mon_path=mon_path)
                            return

    @staticmethod
    def __find_related_paths(base_path):
        related_paths = []
        base_dir = os.path.dirname(base_path)
        base_name = os.path.basename(base_path)

        for entry in os.listdir(base_dir):
            if entry.startswith(base_name):
                full_path = os.path.join(base_dir, entry)
                if os.path.isdir(full_path):
                    related_paths.append(full_path)

        # 按照修改时间倒序排列
        related_paths.sort(key=lambda path: os.path.getmtime(path), reverse=True)
        return related_paths

    def __handle_limit(self, path, limit, mon_path, event):
        sub_paths = []
        for entry in os.listdir(path):
            full_path = os.path.join(path, entry)
            if os.path.isdir(full_path):
                sub_paths.append(full_path)

        if not sub_paths:
            logger.error(f"未找到 {path} 目录下的文件夹")
            return

        sub_paths.sort(key=lambda path: os.path.getmtime(path), reverse=True)
        logger.info(f"开始定向处理文件夹 ...{path}, 最新 {limit} 个文件夹")
        for sub_path in sub_paths[:limit]:
            logger.info(f"开始定向处理文件夹 ...{sub_path}")
            for sroot, sdirs, sfiles in os.walk(sub_path):
                for file_name in sdirs + sfiles:
                    src_file = os.path.join(sroot, file_name)
                    if Path(src_file).is_file():
                        self.__handle_file(event_path=str(src_file), mon_path=mon_path)
            time.sleep(2)

    def send_msg(self):
        if not self._medias or not self._medias.keys():
            return

        for medis_title_year_season in list(self._medias.keys()):
            media_list = self._medias.get(medis_title_year_season)
            if not media_list:
                continue

            last_update_time = media_list.get("time")
            file_meta = media_list.get("file_meta")
            mtype = media_list.get("type")
            episodes = media_list.get("episodes")
            if not last_update_time:
                continue

            if (datetime.now() - last_update_time).total_seconds() > int(self._interval) \
                    or str(mtype) == "movie":
                if self._notify:
                    file_count = len(episodes) if episodes else 1
                    media_type = None
                    if str(mtype) == "tv":
                        season_episode = f"{medis_title_year_season} {StringUtils.format_ep(episodes)}"
                        media_type = MediaType.TV
                    else:
                        season_episode = f"{medis_title_year_season}"
                        media_type = MediaType.MOVIE

                    mediainfo: MediaInfo = self.chain.recognize_media(meta=file_meta,
                                                                  mtype=media_type,
                                                                  tmdbid=file_meta.tmdbid)

                    self.send_transfer_message(msg_title=season_episode,
                                               file_count=file_count,
                                               image=(
                                                   mediainfo.backdrop_path if mediainfo.backdrop_path else mediainfo.poster_path) if mediainfo else None)
                del self._medias[medis_title_year_season]
                continue

    def send_transfer_message(self, msg_title, file_count, image):
        self.post_message(
            mtype=NotificationType.Plugin,
            title=f"{msg_title} Strm已生成", text=f"共{file_count}个文件",
            image=image,
            link=settings.MP_DOMAIN('#/history'))

    def __update_config(self):
        self.update_config({
            "enabled": self._enabled,
            "onlyonce": self._onlyonce,
            "cover": self._cover,
            "notify": self._notify,
            "monitor": self._monitor,
            "interval": self._interval,
            "copy_files": self._copy_files,
            "copy_subtitles": self._copy_subtitles,
            "refresh_emby": self._refresh_emby,
            "url": self._url,
            "monitor_confs": self._monitor_confs,
            "rmt_mediaext": self._rmt_mediaext,
            "other_mediaext": self._other_mediaext,
            "mediaservers": self._mediaservers,
            "path_replacements": "\n".join([f"{source}:{target}" for source, target in
                                            self._path_replacements.items()]) if self._path_replacements else "",
        })

    def get_state(self) -> bool:
        return self._enabled

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        return [
            {
                "cmd": "/lkwang88",
                "event": EventType.PluginAction,
                "desc": "lkwang88全量执行",
                "category": "",
                "data": {
                    "action": "CloudStrmCompanion"
                }
            },
            {
                "cmd": "/strm",
                "event": EventType.PluginAction,
                "desc": "定向云盘Strm同步",
                "category": "",
                "data": {
                    "action": "strm_one"
                }
            },
        ]

    def get_service(self) -> List[Dict[str, Any]]:
        return []

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        # 你的 mediaserver_helper 需要确保被实例化，通常 init_plugin 会处理，
        # 为了安全起见，这里重新获取实例或复用成员变量
        if not self.mediaserver_helper:
            self.mediaserver_helper = MediaServerHelper()
            
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
                                },
                                'content': [
                                    {
                                        'component': 'VAlert',
                                        'props': {
                                            'type': 'warning',
                                            'variant': 'tonal',
                                            'text': '云盘实时监控任何问题不予处理，请自行消化。'
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
                                    'md': 4
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
                                    'md': 4
                                },
                                'content': [
                                    {
                                        'component': 'VSwitch',
                                        'props': {
                                            'model': 'monitor',
                                            'label': '实时监控',
                                        }
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 4
                                },
                                'content': [
                                    {
                                        'component': 'VSwitch',
                                        'props': {
                                            'model': 'copy_files',
                                            'label': '复制非媒体文件',
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
                                    'md': 4
                                },
                                'content': [
                                    {
                                        'component': 'VSwitch',
                                        'props': {
                                            'model': 'notify',
                                            'label': '发送通知',
                                        }
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 4
                                },
                                'content': [
                                    {
                                        'component': 'VSwitch',
                                        'props': {
                                            'model': 'cover',
                                            'label': '覆盖已存在文件',
                                        }
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 4
                                },
                                'content': [
                                    {
                                        'component': 'VSwitch',
                                        'props': {
                                            'model': 'uriencode',
                                            'label': 'url编码',
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
                                    'md': 4
                                },
                                'content': [
                                    {
                                        'component': 'VSwitch',
                                        'props': {
                                            'model': 'refresh_emby',
                                            'label': '刷新媒体库（Emby）',
                                        }
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 4
                                },
                                'content': [
                                    {
                                        'component': 'VSwitch',
                                        'props': {
                                            'model': 'copy_subtitles',
                                            'label': '复制字幕文件',
                                        }
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 4
                                },
                                'content': [
                                    {
                                        'component': 'VSwitch',
                                        'props': {
                                            'model': 'onlyonce',
                                            'label': '立即运行一次',
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
                                    'md': 4
                                },
                                'content': [
                                    {
                                        'component': 'VTextField',
                                        'props': {
                                            'model': 'interval',
                                            'label': '消息延迟',
                                            'placeholder': '10'
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
                                        'component': 'VTextarea',
                                        'props': {
                                            'model': 'monitor_confs',
                                            'label': '目录配置',
                                            'rows': 5,
                                            'placeholder': 'MoviePilot中云盘挂载本地的路径#MoviePilot中strm生成路径#alist/cd2上115路径#strm格式化'
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
                                        'component': 'VTextarea',
                                        'props': {
                                            'model': 'rmt_mediaext',
                                            'label': '视频格式',
                                            'rows': 2,
                                            'placeholder': ".mp4, .mkv, .ts, .iso,.rmvb, .avi, .mov, .mpeg,.mpg, .wmv, .3gp, .asf, .m4v, .flv, .m2ts, .strm,.tp, .f4v"
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
                                        'component': 'VTextarea',
                                        'props': {
                                            'model': 'other_mediaext',
                                            'label': '非媒体文件格式',
                                            'rows': 2,
                                            'placeholder': ".nfo, .jpg"
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
                                    'md': 4
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
                                                      for config in self.mediaserver_helper.get_configs().values() if
                                                      config.type == "emby"]
                                        }
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 8
                                },
                                'content': [
                                    {
                                        'component': 'VTextarea',
                                        'props': {
                                            'model': 'emby_path',
                                            'rows': '1',
                                            'label': '媒体库路径映射',
                                            'placeholder': 'MoviePilot本地文件路径:Emby文件路径（多组路径英文逗号拼接）'
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
                                        'component': 'VTextarea',
                                        'props': {
                                            'model': 'path_replacements',
                                            'label': '路径替换规则',
                                            'rows': 3,
                                            'placeholder': '源路径:目标路径（每行一条规则）'
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
                                },
                                'content': [
                                    {
                                        'component': 'VAlert',
                                        'props': {
                                            'type': 'info',
                                            'variant': 'tonal',
                                            'text': 'MoviePilot中云盘挂载本地的路径：/mnt/media/series/国产剧/雪迷宫 (2024)；MoviePilot中strm生成路径：/mnt/library/series/国产剧/雪迷宫 (2024)；云盘路径：/cloud/media/series/国产剧/雪迷宫 (2024)；则目录配置为：/mnt/media#/mnt/library#/cloud/media#{local_file}'
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
                                },
                                'content': [
                                    {
                                        'component': 'VAlert',
                                        'props': {
                                            'type': 'info',
                                            'variant': 'tonal',
                                            'text': 'strm格式化方式，自行把()替换为alist/cd2上路径：1.本地源文件路径：{local_file}。2.alist路径：http://192.168.31.103:5244/d/115{cloud_file}。3.cd2路径：http://192.168.31.103:19798/static/http/192.168.31.103:19798/False/115{cloud_file}。4.其他api路径：http://192.168.31.103:2001/{cloud_file}'
                                        }
                                    }
                                ]
                            }
                        ]
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {
                                    "cols": 12,
                                },
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "url",
                                            "label": "任务推送url",
                                            "placeholder": "post请求json方式推送path和type(add)字段",
                                        },
                                    }
                                ],
                            },
                        ],
                    },
                ]
            }
        ], {
            "enabled": False,
            "notify": False,
            "monitor": False,
            "cover": False,
            "onlyonce": False,
            "copy_files": False,
            "uriencode": False,
            "copy_subtitles": False,
            "refresh_emby": False,
            "mediaservers": [],
            "monitor_confs": "",
            "emby_path": "",
            "interval": 10,
            "url": "",
            "other_mediaext": ".nfo, .jpg, .png, .json",
            "rmt_mediaext": ".mp4, .mkv, .ts, .iso,.rmvb, .avi, .mov, .mpeg,.mpg, .wmv, .3gp, .asf, .m4v, .flv, .m2ts, .strm,.tp, .f4v",
            "path_replacements": ""
        }

    def get_page(self) -> List[dict]:
        pass

    def stop_service(self):
        if self._observer:
            for observer in self._observer:
                try:
                    if observer.is_alive():
                        observer.stop()
                        observer.join()
                except Exception as e:
                    print(str(e))
        self._observer = []
        if self._scheduler:
            self._scheduler.remove_all_jobs()
            if self._scheduler.running:
                self._event.set()
                self._scheduler.shutdown()
                self._event.clear()
            self._scheduler = None