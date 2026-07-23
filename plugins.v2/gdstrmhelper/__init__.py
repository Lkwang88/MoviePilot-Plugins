import json
import os
import shutil
import sqlite3
import threading
import time
import traceback
from datetime import datetime, timedelta
from pathlib import Path
from queue import Queue, Empty
from typing import Any, List, Dict, Tuple, Optional

import pytz
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer
from watchdog.observers.polling import PollingObserver

from app.core.config import settings
from app.core.event import eventmanager, Event
from app.core.metainfo import MetaInfoPath
from app.helper.mediaserver import MediaServerHelper
from app.log import logger
from app.plugins import _PluginBase
from app.schemas import MediaInfo
from app.schemas.types import EventType, NotificationType, MediaType
from app.utils.string import StringUtils


class FileMonitorHandler(FileSystemEventHandler):
    """
    目录监控响应类：把文件事件交给插件处理
    """

    def __init__(self, mon_path: str, sync: Any, **kwargs):
        super(FileMonitorHandler, self).__init__(**kwargs)
        self._watch_path = mon_path
        self.sync = sync

    def on_created(self, event):
        self.sync.event_handler(event=event, mon_path=self._watch_path,
                                text="创建", event_path=event.src_path)

    def on_moved(self, event):
        self.sync.event_handler(event=event, mon_path=self._watch_path,
                                text="移动", event_path=event.dest_path)


class GDStrmHelper(_PluginBase):
    # 插件名称
    plugin_name = "GD网盘Strm助手"
    # 插件描述
    plugin_desc = "为rclone挂载的谷歌网盘生成STRM：异步全量+增量扫描、SQLite状态记忆、实时监控、聚合刷新Emby。"
    # 插件图标
    plugin_icon = "Google_cloud_A.png"
    # 插件版本
    plugin_version = "1.2.0"
    # 插件作者
    plugin_author = "lkwang88"
    # 作者主页
    author_url = "https://github.com/Lkwang88"
    # 插件配置项ID前缀
    plugin_config_prefix = "gdstrmhelper_"
    # 加载顺序
    plugin_order = 26
    # 可使用的用户级别
    auth_level = 1

    # ==================== 配置属性 ====================
    _enabled = False
    _onlyonce = False            # 立即全量扫描一次
    _onlyonce_incr = False       # 立即增量扫描一次
    _onlyonce_clean = False      # 立即清理孤儿strm一次
    _monitor = False             # 实时监控开关
    _notify = False              # 发送通知
    _cover = False               # 覆盖已存在的strm
    _copy_files = False          # 复制非媒体文件(nfo/jpg等)
    _copy_subtitles = False      # 复制字幕文件
    _refresh_emby = False        # 刷新Emby媒体库
    _del_sync = False            # 删除同步(云端删了同步删strm)

    _startup_delay: int = 60     # 启动延迟(秒)
    _workers: int = 4            # 工作线程数
    _incr_cron: str = "*/30 * * * *"  # 增量扫描cron(默认30分钟)
    _monitor_mode = "polling"    # 实时监控模式：polling(轮询,rclone推荐) / inotify
    _poll_interval: int = 10     # 轮询监控间隔(秒)
    _notify_delay: int = 10      # 通知聚合延迟(秒)
    _refresh_quiet: int = 30     # Emby刷新安静期(秒)
    _del_check_times: int = 3    # 删除前挂载存活检查次数
    _del_max: int = 10           # 单轮最大删除数(熔断阈值)

    _monitor_confs = None        # 目录配置(textarea)
    _mediaservers = None
    _emby_paths = {}
    _rmt_mediaext = None
    _other_mediaext = None
    _exclude_keywords = None

    # ==================== 运行时属性 ====================
    # 网盘配置解析结果 mon_path -> {strm_dir, emby_play, emby_strm}
    _dir_conf = {}
    mediaserver_helper = None

    # 任务队列 + 工作线程
    _queue: Optional[Queue] = None
    _work_threads: List[threading.Thread] = []
    _inflight = set()            # 在途文件去重
    _inflight_lock = threading.Lock()

    # 通知聚合
    _medias = {}
    _medias_lock = threading.Lock()

    # Emby刷新聚合
    _refresh_queue = set()
    _refresh_lock = threading.Lock()
    _last_refresh_time = 0

    # 删除同步锁(避免和扫描/其它删除并发)
    _del_lock = threading.Lock()

    # 监控 & 调度
    _observers = []
    _scheduler: Optional[BackgroundScheduler] = None
    _event = threading.Event()
    _startup_thread: Optional[threading.Thread] = None

    # SQLite
    _db_path = None
    _db_lock = threading.Lock()

    # 统计
    _stat = {}
    # 每个盘的真实样例媒体路径(用于路径预览) mon_path -> src_path
    _sample_media = {}

    def init_plugin(self, config: dict = None):
        # 重置运行时状态
        self._dir_conf = {}
        self._emby_paths = {}
        self._inflight = set()
        self._medias = {}
        self._refresh_queue = set()
        self._sample_media = {}
        self._observers = []
        self._work_threads = []
        self._last_refresh_time = 0
        self._stat = {
            "last_full_scan": None,
            "last_incr_scan": None,
            "processed": 0,
            "created": 0,
            "deleted": 0,
            "errors": 0,
        }
        self._db_path = os.path.join(self.get_data_path(), "gdstrm.db")
        self.mediaserver_helper = MediaServerHelper()

        if config:
            self._enabled = config.get("enabled")
            self._onlyonce = config.get("onlyonce")
            self._onlyonce_incr = config.get("onlyonce_incr")
            self._onlyonce_clean = config.get("onlyonce_clean")
            self._monitor = config.get("monitor")
            self._notify = config.get("notify")
            self._cover = config.get("cover")
            self._copy_files = config.get("copy_files")
            self._copy_subtitles = config.get("copy_subtitles")
            self._refresh_emby = config.get("refresh_emby")
            self._del_sync = config.get("del_sync")

            self._startup_delay = int(config.get("startup_delay") or 60)
            self._workers = int(config.get("workers") or 4)
            self._incr_cron = config.get("incr_cron") or "*/30 * * * *"
            self._monitor_mode = config.get("monitor_mode") or "polling"
            self._poll_interval = int(config.get("poll_interval") or 10)
            self._notify_delay = int(config.get("notify_delay") or 10)
            self._refresh_quiet = int(config.get("refresh_quiet") or 30)
            self._del_check_times = int(config.get("del_check_times") or 3)
            self._del_max = int(config.get("del_max") or 10)

            self._monitor_confs = config.get("monitor_confs")
            self._mediaservers = config.get("mediaservers") or []
            self._rmt_mediaext = config.get("rmt_mediaext") \
                or ".mp4, .mkv, .ts, .iso, .rmvb, .avi, .mov, .mpeg, .mpg, .wmv, .3gp, .asf, .m4v, .flv, .m2ts, .tp, .f4v"
            self._other_mediaext = config.get("other_mediaext") \
                or ".nfo, .jpg, .png, .json, .ass, .srt, .sup"
            self._exclude_keywords = config.get("exclude_keywords") or ""

            # 媒体库路径映射(strm在MP和Emby容器里的路径不一致时使用)
            if config.get("emby_path"):
                for p in str(config.get("emby_path")).split(","):
                    if ":" in p:
                        k, v = p.split(":", 1)
                        self._emby_paths[k.strip()] = v.strip()

        # 停止现有任务
        self.stop_service()

        # 解析目录配置
        self.__parse_confs()

        # 初始化数据库
        self.__init_db()

        if not (self._enabled or self._onlyonce or self._onlyonce_incr or self._onlyonce_clean):
            return

        # 定时服务
        self._scheduler = BackgroundScheduler(timezone=settings.TZ)

        # 启动工作线程池 + 队列
        self.__start_workers()

        # 通知聚合发送
        if self._notify:
            self._scheduler.add_job(self.__send_msg, trigger="interval", seconds=5,
                                    id="gdstrm_send_msg")

        # Emby聚合刷新
        if self._refresh_emby:
            self._scheduler.add_job(self.__flush_refresh, trigger="interval", seconds=5,
                                    id="gdstrm_flush_refresh")

        # 增量扫描cron(仅在启用+有配置时)
        if self._enabled and self._dir_conf:
            try:
                self._scheduler.add_job(self.scan_incr, trigger=CronTrigger.from_crontab(self._incr_cron),
                                        id="gdstrm_incr_scan", name="GD网盘增量扫描")
                logger.info(f"增量扫描已按 [{self._incr_cron}] 注册")
            except Exception as e:
                logger.error(f"增量扫描cron [{self._incr_cron}] 无效：{e}")

        # 后台异步启动：首次全量(+监控)，不阻塞MP启动
        # 只要启用插件就跑首次全量；实时监控开关只决定是否起watchdog
        if self._enabled and self._dir_conf:
            self._startup_thread = threading.Thread(target=self.__delayed_startup, daemon=True)
            self._startup_thread.start()

        # 立即执行(一次性开关)
        if self._onlyonce:
            logger.info("立即运行一次【全量扫描】")
            self._scheduler.add_job(func=self.scan_full, trigger="date",
                                    run_date=datetime.now(tz=pytz.timezone(settings.TZ)) + timedelta(seconds=3),
                                    name="GD网盘全量扫描")
            self._onlyonce = False
        if self._onlyonce_incr:
            logger.info("立即运行一次【增量扫描】")
            self._scheduler.add_job(func=self.scan_incr, trigger="date",
                                    run_date=datetime.now(tz=pytz.timezone(settings.TZ)) + timedelta(seconds=3),
                                    name="GD网盘增量扫描")
            self._onlyonce_incr = False
        if self._onlyonce_clean:
            logger.info("立即运行一次【清理孤儿STRM】")
            self._scheduler.add_job(func=self.clean_orphans, trigger="date",
                                    run_date=datetime.now(tz=pytz.timezone(settings.TZ)) + timedelta(seconds=3),
                                    name="GD网盘清理孤儿STRM")
            self._onlyonce_clean = False

        self.__update_config()

        if self._scheduler.get_jobs():
            self._scheduler.print_jobs()
            self._scheduler.start()

    # ==================== 配置解析 ====================
    def __parse_confs(self):
        """
        解析目录配置
        格式: 监控目录#STRM生成目录#Emby播放路径#[Emby STRM目录]
        第4段可省略(留空即与第2段一致，不做strm路径映射)
        """
        if not self._monitor_confs:
            return
        for line in str(self._monitor_confs).split("\n"):
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            parts = line.split("#")
            if len(parts) < 3:
                logger.error(f"配置格式错误(至少需要3段)：{line}")
                continue
            mon_path = parts[0].strip().rstrip("/")
            strm_dir = parts[1].strip().rstrip("/")
            emby_play = parts[2].strip().rstrip("/")
            emby_strm = parts[3].strip().rstrip("/") if len(parts) >= 4 and parts[3].strip() else strm_dir

            if not mon_path or not strm_dir or not emby_play:
                logger.error(f"配置存在空字段：{line}")
                continue

            # strm目录不能是监控目录的子目录(否则会自己监控自己)
            try:
                if Path(strm_dir).is_relative_to(Path(mon_path)):
                    logger.warn(f"{strm_dir} 是 {mon_path} 的子目录，跳过")
                    self.systemmessage.put(f"{strm_dir} 是 {mon_path} 的子目录，无法监控")
                    continue
            except Exception:
                pass

            self._dir_conf[mon_path] = {
                "strm_dir": strm_dir,
                "emby_play": emby_play,
                "emby_strm": emby_strm,
            }
        logger.info(f"共解析到 {len(self._dir_conf)} 个网盘配置")

    # ==================== SQLite ====================
    def __init_db(self):
        try:
            with self._db_lock:
                conn = sqlite3.connect(self._db_path)
                conn.execute("PRAGMA journal_mode=WAL")
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS files (
                        path TEXT PRIMARY KEY,
                        size INTEGER,
                        mtime REAL,
                        strm_path TEXT,
                        mon_path TEXT,
                        updated_at REAL
                    )
                """)
                conn.execute("CREATE INDEX IF NOT EXISTS idx_mon ON files(mon_path)")
                conn.commit()
                conn.close()
        except Exception as e:
            logger.error(f"初始化数据库失败：{e}")

    def __db_conn(self):
        conn = sqlite3.connect(self._db_path, timeout=30)
        conn.execute("PRAGMA journal_mode=WAL")
        return conn

    def __db_get(self, path: str):
        """返回 (size, mtime, strm_path) 或 None"""
        try:
            with self._db_lock:
                conn = self.__db_conn()
                cur = conn.execute("SELECT size, mtime, strm_path FROM files WHERE path=?", (path,))
                row = cur.fetchone()
                conn.close()
                return row
        except Exception as e:
            logger.debug(f"查询数据库失败 {path}: {e}")
            return None

    def __db_upsert(self, path: str, size: int, mtime: float, strm_path: str, mon_path: str):
        try:
            with self._db_lock:
                conn = self.__db_conn()
                conn.execute(
                    "INSERT OR REPLACE INTO files(path, size, mtime, strm_path, mon_path, updated_at) "
                    "VALUES(?,?,?,?,?,?)",
                    (path, size, mtime, strm_path, mon_path, time.time()))
                conn.commit()
                conn.close()
        except Exception as e:
            logger.debug(f"写入数据库失败 {path}: {e}")

    def __db_delete(self, path: str):
        try:
            with self._db_lock:
                conn = self.__db_conn()
                conn.execute("DELETE FROM files WHERE path=?", (path,))
                conn.commit()
                conn.close()
        except Exception as e:
            logger.debug(f"删除数据库记录失败 {path}: {e}")

    def __db_all_by_mon(self, mon_path: str) -> Dict[str, str]:
        """返回该监控目录下所有 {path: strm_path}"""
        result = {}
        try:
            with self._db_lock:
                conn = self.__db_conn()
                cur = conn.execute("SELECT path, strm_path FROM files WHERE mon_path=?", (mon_path,))
                for path, strm_path in cur.fetchall():
                    result[path] = strm_path
                conn.close()
        except Exception as e:
            logger.debug(f"查询数据库失败 {mon_path}: {e}")
        return result

    def __db_count(self) -> int:
        try:
            with self._db_lock:
                conn = self.__db_conn()
                cur = conn.execute("SELECT COUNT(1) FROM files")
                n = cur.fetchone()[0]
                conn.close()
                return n
        except Exception:
            return 0

    # ==================== 工作线程池 ====================
    def __start_workers(self):
        self._queue = Queue()
        self._event.clear()
        self._work_threads = []
        for i in range(max(1, self._workers)):
            t = threading.Thread(target=self.__worker_loop, name=f"gdstrm-worker-{i}", daemon=True)
            t.start()
            self._work_threads.append(t)
        logger.info(f"启动 {len(self._work_threads)} 个工作线程")

    def __worker_loop(self):
        while not self._event.is_set():
            try:
                task = self._queue.get(timeout=1)
            except Empty:
                continue
            if task is None:
                self._queue.task_done()
                break
            event_path, mon_path = task
            try:
                self.__handle_file(event_path=event_path, mon_path=mon_path)
            except Exception as e:
                self._stat["errors"] = self._stat.get("errors", 0) + 1
                logger.error(f"处理文件出错 {event_path}: {e} - {traceback.format_exc()}")
            finally:
                with self._inflight_lock:
                    self._inflight.discard(event_path)
                self._queue.task_done()

    def __enqueue(self, event_path: str, mon_path: str):
        """入队，带在途去重"""
        with self._inflight_lock:
            if event_path in self._inflight:
                return
            self._inflight.add(event_path)
        if self._queue is not None:
            self._queue.put((event_path, mon_path))

    # ==================== 后台延迟启动 ====================
    def __delayed_startup(self):
        """延迟后启动监控 + 首次全量扫描(后台线程，不阻塞MP)"""
        try:
            if self._startup_delay > 0:
                logger.info(f"等待 {self._startup_delay}s 后启动监控与全量扫描...")
                # 分段sleep以便及时响应停止
                waited = 0
                while waited < self._startup_delay:
                    if self._event.is_set():
                        return
                    time.sleep(1)
                    waited += 1
            # 先起监控(秒级，不等扫描)。仅在开启实时监控时启动
            if self._monitor:
                self.__start_monitor()
            # 再跑首次全量扫描
            logger.info("开始首次全量扫描(后台)")
            self.scan_full()
        except Exception as e:
            logger.error(f"后台启动失败：{e} - {traceback.format_exc()}")

    # ==================== 实时监控 ====================
    def __start_monitor(self):
        """
        为每个监控目录启动watchdog。
        - polling模式(默认，rclone GD推荐)：PollingObserver，对云端新增敏感(不依赖inotify事件)
        - inotify模式：Observer，省CPU、可休眠，仅适合本地磁盘/只有本机写入的场景
        """
        mode = self._monitor_mode or "polling"
        interval = max(1, int(self._poll_interval or 10))
        for mon_path in self._dir_conf.keys():
            if self._event.is_set():
                return
            if not os.path.exists(mon_path):
                logger.warn(f"监控目录不存在，跳过监控：{mon_path}")
                continue
            observer = None
            try:
                if mode == "inotify":
                    observer = Observer(timeout=10)
                    observer.schedule(FileMonitorHandler(mon_path, self), path=mon_path, recursive=True)
                    observer.daemon = True
                    observer.start()
                    logger.info(f"{mon_path} 实时监控(inotify)已启动")
                else:
                    # 轮询模式：对rclone挂载的云端新增敏感
                    observer = PollingObserver(timeout=interval)
                    observer.schedule(FileMonitorHandler(mon_path, self), path=mon_path, recursive=True)
                    observer.daemon = True
                    observer.start()
                    logger.info(f"{mon_path} 实时监控(轮询，间隔{interval}s)已启动")
            except Exception as e:
                err = str(e)
                if "inotify" in err and "reached" in err:
                    logger.warn(
                        "inotify监控数量已达上限，请在宿主机执行：\n"
                        "echo fs.inotify.max_user_watches=524288 | sudo tee -a /etc/sysctl.conf\n"
                        "echo fs.inotify.max_user_instances=524288 | sudo tee -a /etc/sysctl.conf\n"
                        "sudo sysctl -p")
                # inotify失败时兜底尝试轮询
                if mode == "inotify":
                    try:
                        observer = PollingObserver(timeout=interval)
                        observer.schedule(FileMonitorHandler(mon_path, self), path=mon_path, recursive=True)
                        observer.daemon = True
                        observer.start()
                        logger.info(f"{mon_path} inotify失败，已回退轮询(间隔{interval}s)")
                    except Exception as e2:
                        logger.error(f"{mon_path} 监控启动失败：{e2}")
                        self.systemmessage.put(f"{mon_path} 监控启动失败：{e2}")
                        continue
                else:
                    logger.error(f"{mon_path} 监控启动失败：{err}")
                    self.systemmessage.put(f"{mon_path} 监控启动失败：{err}")
                    continue
            self._observers.append(observer)

    # ==================== 监控事件 ====================
    def event_handler(self, event, mon_path: str, text: str, event_path: str):
        if event.is_directory:
            return
        if self.__is_skip(event_path):
            return
        logger.debug(f"监控到文件{text}：{event_path}")
        self.__enqueue(event_path, mon_path)

    def __is_skip(self, path: str) -> bool:
        """临时文件/回收站/隐藏文件跳过"""
        low = path.lower()
        skip_marks = [".fuse_hidden", ".partial", ".rclone", "~", ".tmp",
                      "/@recycle", "/#recycle", "/@eadir", "/.", "/lost+found"]
        for m in skip_marks:
            if m in low:
                return True
        if self._exclude_keywords:
            for kw in str(self._exclude_keywords).split("\n"):
                kw = kw.strip()
                if kw and kw in path:
                    return True
        return False

    # ==================== 全量 / 增量扫描 ====================
    def __check_mount(self, mon_path: str) -> bool:
        """挂载存活检查：存在且非空"""
        try:
            if not os.path.exists(mon_path):
                return False
            with os.scandir(mon_path) as it:
                for _ in it:
                    return True
            return False
        except Exception:
            return False

    def __iter_files(self, mon_path: str):
        """用scandir递归遍历，返回DirEntry"""
        stack = [mon_path]
        while stack:
            if self._event.is_set():
                return
            cur = stack.pop()
            try:
                with os.scandir(cur) as it:
                    for entry in it:
                        try:
                            if entry.is_dir(follow_symlinks=False):
                                if entry.name in ("extrafanart", "@eaDir", "lost+found"):
                                    continue
                                stack.append(entry.path)
                            elif entry.is_file(follow_symlinks=False):
                                yield entry
                        except Exception:
                            continue
            except Exception as e:
                logger.debug(f"遍历目录失败 {cur}: {e}")

    def scan_full(self):
        """全量扫描：借助SQLite做增量跳过"""
        self.__do_scan(full=True)

    def scan_incr(self):
        """增量扫描"""
        self.__do_scan(full=False)

    def __do_scan(self, full: bool):
        scan_type = "全量" if full else "增量"
        logger.info(f"开始{scan_type}扫描")
        start = time.time()
        total = 0
        for mon_path, conf in self._dir_conf.items():
            if self._event.is_set():
                logger.info("收到停止信号，中断扫描")
                return
            if not self.__check_mount(mon_path):
                logger.warn(f"挂载未就绪或为空，跳过扫描：{mon_path}")
                continue
            for entry in self.__iter_files(mon_path):
                if self._event.is_set():
                    logger.info("收到停止信号，中断扫描")
                    return
                path = entry.path
                if self.__is_skip(path):
                    continue
                # 增量判断：与DB比对 size+mtime
                try:
                    st = entry.stat()
                    size, mtime = st.st_size, st.st_mtime
                except Exception:
                    continue
                suffix = Path(path).suffix.lower()
                is_media = suffix in self.__ext_list(self._rmt_mediaext)
                # 计算本地产物路径(媒体=strm，非媒体=复制目标)
                target = path.replace(mon_path, conf["strm_dir"], 1)
                product = (os.path.splitext(target)[0] + ".strm") if is_media else target
                row = self.__db_get(path)
                if row:
                    db_size, db_mtime, db_strm = row
                    # 大小/时间未变：只要本地产物已就位就跳过(核心：本地有的不重复处理)
                    if db_size == size and abs((db_mtime or 0) - mtime) < 1:
                        if is_media:
                            # 媒体：对应strm仍存在即跳过
                            if db_strm and os.path.exists(db_strm):
                                continue
                        else:
                            # 非媒体：当前不需要复制、或复制目标已存在，即跳过
                            if not self.__wants_copy(suffix) or (db_strm and os.path.exists(db_strm)):
                                continue
                else:
                    # DB无记录(如升级/删库)：若本地产物已存在，补写DB并跳过，避免重复处理
                    if is_media:
                        if os.path.exists(product):
                            self.__db_upsert(path, size, mtime, product, mon_path)
                            continue
                    else:
                        # 非媒体：不需要复制、或复制目标已存在，补记并跳过
                        if not self.__wants_copy(suffix):
                            self.__db_upsert(path, size, mtime, product, mon_path)
                            continue
                        if os.path.exists(product):
                            self.__db_upsert(path, size, mtime, product, mon_path)
                            continue
                self.__enqueue(path, mon_path)
                total += 1
        # 等待队列消费完
        if self._queue:
            self._queue.join()
        elapsed = round(time.time() - start, 1)
        if full:
            self._stat["last_full_scan"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        else:
            self._stat["last_incr_scan"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logger.info(f"{scan_type}扫描完成，入队 {total} 个变更文件，耗时 {elapsed}s")

        # 增量/全量扫描后可选删除同步
        if self._del_sync:
            self.clean_orphans()

    def __ext_list(self, ext_str: str) -> List[str]:
        return [e.strip().lower() for e in str(ext_str or "").split(",") if e.strip()]

    # ==================== 文件处理 ====================
    def __handle_file(self, event_path: str, mon_path: str):
        """处理单个文件(无全局锁)"""
        # 慢I/O放在锁外
        if not os.path.exists(event_path):
            return
        conf = self._dir_conf.get(mon_path)
        if not conf:
            return
        strm_dir = conf["strm_dir"]
        emby_play = conf["emby_play"]

        target_file = event_path.replace(mon_path, strm_dir, 1)
        suffix = Path(event_path).suffix.lower()

        self._stat["processed"] = self._stat.get("processed", 0) + 1

        if suffix in self.__ext_list(self._rmt_mediaext):
            # 媒体文件 -> 生成strm
            # 记录真实样例(每个盘一条，供路径预览)
            if mon_path not in self._sample_media:
                self._sample_media[mon_path] = event_path
            # strm内容 = 把监控路径替换为Emby播放路径(即Emby容器看到的网盘挂载路径)
            strm_content = event_path.replace(mon_path, emby_play, 1).replace("\\", "/")
            strm_path = self.__create_strm(target_file, strm_content)
            # 无论新生成还是已存在，都记录状态(纳入SQLite记忆，下次扫描直接跳过)
            product = strm_path or (os.path.splitext(target_file)[0] + ".strm")
            self.__record_state(event_path, product, mon_path)
            # 同名附属文件(nfo/jpg等)
            self.__handle_siblings(event_path, mon_path, strm_dir)
        else:
            # 非媒体文件 -> 视开关复制，同样纳入SQLite记忆
            self.__handle_other_file(event_path, target_file)
            self.__record_state(event_path, target_file, mon_path)

    def __record_state(self, src_path: str, strm_path: str, mon_path: str):
        """把已处理文件的状态写入SQLite，供增量扫描跳过"""
        try:
            st = os.stat(src_path)
            self.__db_upsert(src_path, st.st_size, st.st_mtime, strm_path, mon_path)
        except Exception:
            pass

    def __wants_copy(self, suffix: str) -> bool:
        """该后缀是否会被复制到本地(据当前开关)"""
        if self._copy_files and suffix in self.__ext_list(self._other_mediaext):
            return True
        if self._copy_subtitles and suffix in [".srt", ".ass", ".ssa", ".sub", ".sup"]:
            return True
        return False

    def __handle_siblings(self, event_path: str, mon_path: str, strm_dir: str):
        try:
            parent = Path(event_path).parent
            stem = Path(event_path).stem.replace("[", "?").replace("]", "?")
            for f in parent.glob(f"{stem}.*"):
                if str(f) == event_path:
                    continue
                target = str(f).replace(mon_path, strm_dir, 1)
                self.__handle_other_file(str(f), target)
        except Exception as e:
            logger.debug(f"处理同名附属文件失败 {event_path}: {e}")

    def __handle_other_file(self, event_path: str, target_file: str):
        suffix = Path(event_path).suffix.lower()
        # 复制非媒体文件
        if self._copy_files and suffix in self.__ext_list(self._other_mediaext):
            self.__copy_file(event_path, target_file)
        # 复制字幕
        elif self._copy_subtitles and suffix in [".srt", ".ass", ".ssa", ".sub", ".sup"]:
            self.__copy_file(event_path, target_file)

    def __copy_file(self, src: str, dst: str):
        try:
            if os.path.exists(dst) and not self._cover:
                return
            os.makedirs(os.path.dirname(dst), exist_ok=True)
            shutil.copy2(src, dst)
            logger.debug(f"复制文件 {src} -> {dst}")
        except Exception as e:
            logger.error(f"复制文件失败 {src}: {e}")

    def __create_strm(self, strm_file: str, strm_content: str) -> Optional[str]:
        """原子写入strm，返回strm路径；无变化则跳过"""
        try:
            strm_file = os.path.splitext(strm_file)[0] + ".strm"
            parent = os.path.dirname(strm_file)

            # 幂等：已存在且内容一致则跳过(不重写、不刷新)
            if os.path.exists(strm_file):
                try:
                    with open(strm_file, "r", encoding="utf-8") as f:
                        if f.read() == strm_content:
                            return strm_file
                except Exception:
                    pass
                # 内容不同但未开启覆盖，也跳过
                if not self._cover:
                    return strm_file

            os.makedirs(parent, exist_ok=True)
            # 原子写入：先写临时文件再rename
            tmp = strm_file + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                f.write(strm_content)
            os.replace(tmp, strm_file)

            self._stat["created"] = self._stat.get("created", 0) + 1
            logger.info(f"生成STRM {strm_file}")

            # 聚合通知
            if self._notify:
                self.__collect_media(strm_file)
            # 聚合刷新Emby
            if self._refresh_emby and self._mediaservers:
                with self._refresh_lock:
                    self._refresh_queue.add(strm_file)
                    self._last_refresh_time = time.time()
            return strm_file
        except Exception as e:
            logger.error(f"生成STRM失败 {strm_file}: {e}")
            return None

    # ==================== 删除同步 ====================
    def clean_orphans(self):
        """清理孤儿STRM：DB有记录但源文件已不在。带挂载多次存活检查+熔断"""
        if not self._del_lock.acquire(blocking=False):
            logger.info("删除同步任务已在运行，跳过本次")
            return
        try:
            logger.info("开始清理孤儿STRM(删除同步)")
            # 强制多次挂载存活检查
            times = max(1, self._del_check_times)
            for mon_path in self._dir_conf.keys():
                for i in range(times):
                    if not self.__check_mount(mon_path):
                        logger.warn(f"挂载存活检查失败({mon_path} 第{i + 1}/{times}次)，放弃本轮删除同步")
                        return
                    if i < times - 1:
                        time.sleep(2)
            logger.info(f"挂载存活检查通过(每盘{times}次)")

            # 收集所有待删除的孤儿
            orphans = []  # [(src_path, strm_path, mon_path)]
            for mon_path in self._dir_conf.keys():
                db_files = self.__db_all_by_mon(mon_path)
                for src_path, strm_path in db_files.items():
                    if not os.path.exists(src_path):
                        orphans.append((src_path, strm_path, mon_path))

            if not orphans:
                logger.info("没有需要清理的孤儿STRM")
                return

            # 熔断：超过阈值则整轮放弃，0删除
            if len(orphans) >= self._del_max:
                msg = (f"检测到 {len(orphans)} 个待删除文件，达到熔断阈值({self._del_max})，"
                       f"疑似挂载异常或误判，已放弃本轮删除同步(0删除)")
                logger.warn(msg)
                if self._notify:
                    self.post_message(mtype=NotificationType.Plugin,
                                      title="GD网盘Strm助手 · 删除同步已熔断",
                                      text=msg)
                return

            # 执行删除
            deleted = 0
            for src_path, strm_path, mon_path in orphans:
                try:
                    if strm_path and os.path.exists(strm_path):
                        os.remove(strm_path)
                        logger.info(f"删除孤儿STRM {strm_path}")
                        # 清理空目录
                        self.__remove_empty_dir(os.path.dirname(strm_path))
                    self.__db_delete(src_path)
                    deleted += 1
                except Exception as e:
                    logger.error(f"删除孤儿STRM失败 {strm_path}: {e}")
            self._stat["deleted"] = self._stat.get("deleted", 0) + deleted
            logger.info(f"删除同步完成，共清理 {deleted} 个孤儿STRM")
            if self._notify and deleted > 0:
                self.post_message(mtype=NotificationType.Plugin,
                                  title="GD网盘Strm助手 · 删除同步完成",
                                  text=f"共清理 {deleted} 个失效STRM")
        finally:
            self._del_lock.release()

    def __remove_empty_dir(self, dir_path: str):
        try:
            if os.path.isdir(dir_path) and not os.listdir(dir_path):
                os.rmdir(dir_path)
        except Exception:
            pass

    # ==================== 通知聚合 ====================
    def __collect_media(self, strm_file: str):
        try:
            file_meta = MetaInfoPath(Path(strm_file))
            key = f"{file_meta.cn_name} ({file_meta.year}){f' {file_meta.season}' if file_meta.season else ''}"
            with self._medias_lock:
                media_list = self._medias.get(key) or {}
                if media_list:
                    episodes = media_list.get("episodes") or []
                    if file_meta.begin_episode and int(file_meta.begin_episode) not in episodes:
                        episodes.append(int(file_meta.begin_episode))
                    media_list.update({"episodes": episodes, "file_meta": file_meta,
                                       "type": "tv" if file_meta.season else "movie",
                                       "time": datetime.now()})
                else:
                    media_list = {
                        "episodes": [int(file_meta.begin_episode)] if file_meta.begin_episode else [],
                        "file_meta": file_meta,
                        "type": "tv" if file_meta.season else "movie",
                        "time": datetime.now(),
                    }
                self._medias[key] = media_list
        except Exception as e:
            logger.debug(f"收集通知信息失败 {strm_file}: {e}")

    def __send_msg(self):
        """定时检查聚合的媒体，安静期后发送汇总通知"""
        if not self._medias:
            return
        with self._medias_lock:
            keys = list(self._medias.keys())
        for key in keys:
            with self._medias_lock:
                media_list = self._medias.get(key)
            if not media_list:
                continue
            last_time = media_list.get("time")
            mtype = media_list.get("type")
            episodes = media_list.get("episodes")
            file_meta = media_list.get("file_meta")
            if not last_time:
                continue
            # 剧集需静默超过notify_delay，电影直接发
            if (datetime.now() - last_time).total_seconds() > int(self._notify_delay) or str(mtype) == "movie":
                try:
                    if str(mtype) == "tv":
                        season_episode = f"{key} {StringUtils.format_ep(episodes)}"
                        media_type = MediaType.TV
                        file_count = len(episodes) if episodes else 1
                    else:
                        season_episode = key
                        media_type = MediaType.MOVIE
                        file_count = 1
                    image = None
                    try:
                        mediainfo: MediaInfo = self.chain.recognize_media(
                            meta=file_meta, mtype=media_type, tmdbid=file_meta.tmdbid)
                        if mediainfo:
                            image = mediainfo.backdrop_path or mediainfo.poster_path
                    except Exception:
                        pass
                    self.post_message(
                        mtype=NotificationType.Plugin,
                        title=f"{season_episode} STRM已生成",
                        text=f"共{file_count}个文件",
                        image=image,
                        link=settings.MP_DOMAIN('#/history'))
                except Exception as e:
                    logger.error(f"发送通知失败 {key}: {e}")
                finally:
                    with self._medias_lock:
                        self._medias.pop(key, None)

    # ==================== Emby聚合刷新 ====================
    def __flush_refresh(self):
        """安静期后合并刷新Emby"""
        if not self._refresh_queue:
            return
        with self._refresh_lock:
            # 安静期未到则等待
            if time.time() - self._last_refresh_time < int(self._refresh_quiet):
                return
            files = list(self._refresh_queue)
            self._refresh_queue.clear()
        if not files:
            return
        self.__refresh_emby(files)

    def __refresh_emby(self, strm_files: List[str]):
        emby_servers = self.mediaserver_helper.get_services(
            name_filters=self._mediaservers, type_filter="emby")
        if not emby_servers:
            logger.error("未配置Emby媒体服务器，跳过刷新")
            return
        # 组装Updates(应用strm路径映射)
        updates = []
        for f in strm_files:
            mapped = self.__map_emby_strm(f)
            updates.append({"Path": mapped, "UpdateType": "Created"})
        # 分批发送，避免全量时单次payload过大打挂Emby
        batch_size = 100
        for emby_name, emby_server in emby_servers.items():
            emby = emby_server.instance
            ok, fail = 0, 0
            for i in range(0, len(updates), batch_size):
                batch = updates[i:i + batch_size]
                try:
                    res = emby.post_data(
                        url='[HOST]emby/Library/Media/Updated?api_key=[APIKEY]&reqformat=json',
                        data=json.dumps({"Updates": batch}),
                        headers={"Content-Type": "application/json"})
                    if res and res.status_code in [200, 204]:
                        ok += len(batch)
                    else:
                        fail += len(batch)
                        code = res.status_code if res else "无响应"
                        logger.error(f"通知 {emby_name} 刷新失败(批{i // batch_size + 1})，错误码：{code}")
                except Exception as e:
                    fail += len(batch)
                    logger.error(f"通知 {emby_name} 刷新出错(批{i // batch_size + 1})：{e}")
            logger.info(f"已通知 {emby_name} 刷新STRM：成功{ok} 失败{fail}")

    def __map_emby_strm(self, strm_file: str) -> str:
        """把MP侧strm路径映射为Emby侧strm路径"""
        # 优先用每盘配置的 emby_strm 映射
        for mon_path, conf in self._dir_conf.items():
            strm_dir = conf["strm_dir"]
            emby_strm = conf["emby_strm"]
            if strm_dir != emby_strm and strm_file.startswith(strm_dir):
                return strm_file.replace(strm_dir, emby_strm, 1)
        # 兼容全局 emby_path 映射
        for src, dst in self._emby_paths.items():
            if strm_file.startswith(src):
                return strm_file.replace(src, dst, 1)
        return strm_file

    # ==================== 手动命令 ====================
    @eventmanager.register(EventType.PluginAction)
    def strm_one(self, event: Event = None):
        """定向处理单文件/目录"""
        if not event:
            return
        event_data = event.event_data
        if not event_data or event_data.get("action") != "gdstrm_file":
            return
        file_path = event_data.get("file_path")
        if not file_path:
            logger.error(f"缺少参数：{event_data}")
            return
        mon_path = None
        for mon in self._dir_conf.keys():
            if str(file_path).startswith(mon):
                mon_path = mon
                break
        if not mon_path:
            logger.error(f"未找到 {file_path} 对应的监控目录")
            return
        self.__enqueue(file_path, mon_path)

    # ==================== 配置回写 ====================
    def __update_config(self):
        self.update_config({
            "enabled": self._enabled,
            "onlyonce": self._onlyonce,
            "onlyonce_incr": self._onlyonce_incr,
            "onlyonce_clean": self._onlyonce_clean,
            "monitor": self._monitor,
            "notify": self._notify,
            "cover": self._cover,
            "copy_files": self._copy_files,
            "copy_subtitles": self._copy_subtitles,
            "refresh_emby": self._refresh_emby,
            "del_sync": self._del_sync,
            "startup_delay": self._startup_delay,
            "workers": self._workers,
            "incr_cron": self._incr_cron,
            "monitor_mode": self._monitor_mode,
            "poll_interval": self._poll_interval,
            "notify_delay": self._notify_delay,
            "refresh_quiet": self._refresh_quiet,
            "del_check_times": self._del_check_times,
            "del_max": self._del_max,
            "monitor_confs": self._monitor_confs,
            "mediaservers": self._mediaservers,
            "rmt_mediaext": self._rmt_mediaext,
            "other_mediaext": self._other_mediaext,
            "exclude_keywords": self._exclude_keywords,
            "emby_path": ",".join([f"{k}:{v}" for k, v in self._emby_paths.items()]) if self._emby_paths else "",
        })

    def get_state(self) -> bool:
        return self._enabled

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        return [{
            "cmd": "/gdstrm",
            "event": EventType.PluginAction,
            "desc": "GD网盘定向生成STRM",
            "category": "",
            "data": {"action": "gdstrm_file"}
        }]

    def get_api(self) -> List[Dict[str, Any]]:
        return []

    def get_service(self) -> List[Dict[str, Any]]:
        return []

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        return [
            {
                "component": "VForm",
                "content": [
                    # 说明
                    {
                        "component": "VRow",
                        "content": [{
                            "component": "VCol",
                            "props": {"cols": 12},
                            "content": [{
                                "component": "VAlert",
                                "props": {
                                    "type": "info", "variant": "tonal",
                                    "text": "为rclone挂载的谷歌网盘生成STRM。启动后先延迟异步全量扫描(不阻塞MP)，之后实时监控+定时增量扫描。SQLite记忆已处理文件，二次扫描只查新增/变更。"
                                }
                            }]
                        }]
                    },
                    # 主开关行
                    {
                        "component": "VRow",
                        "content": [
                            {"component": "VCol", "props": {"cols": 12, "md": 3},
                             "content": [{"component": "VSwitch", "props": {"model": "enabled", "label": "启用插件"}}]},
                            {"component": "VCol", "props": {"cols": 12, "md": 3},
                             "content": [{"component": "VSwitch", "props": {"model": "monitor", "label": "实时监控"}}]},
                            {"component": "VCol", "props": {"cols": 12, "md": 3},
                             "content": [{"component": "VSwitch", "props": {"model": "notify", "label": "发送通知"}}]},
                            {"component": "VCol", "props": {"cols": 12, "md": 3},
                             "content": [{"component": "VSwitch", "props": {"model": "refresh_emby", "label": "刷新Emby"}}]},
                        ]
                    },
                    # 第二开关行
                    {
                        "component": "VRow",
                        "content": [
                            {"component": "VCol", "props": {"cols": 12, "md": 3},
                             "content": [{"component": "VSwitch", "props": {"model": "cover", "label": "覆盖已存在STRM"}}]},
                            {"component": "VCol", "props": {"cols": 12, "md": 3},
                             "content": [{"component": "VSwitch", "props": {"model": "copy_files", "label": "复制非媒体文件"}}]},
                            {"component": "VCol", "props": {"cols": 12, "md": 3},
                             "content": [{"component": "VSwitch", "props": {"model": "copy_subtitles", "label": "复制字幕"}}]},
                            {"component": "VCol", "props": {"cols": 12, "md": 3},
                             "content": [{"component": "VSwitch", "props": {"model": "del_sync", "label": "删除同步"}}]},
                        ]
                    },
                    # 一次性操作行
                    {
                        "component": "VRow",
                        "content": [
                            {"component": "VCol", "props": {"cols": 12, "md": 4},
                             "content": [{"component": "VSwitch", "props": {"model": "onlyonce", "label": "立即全量扫描一次"}}]},
                            {"component": "VCol", "props": {"cols": 12, "md": 4},
                             "content": [{"component": "VSwitch", "props": {"model": "onlyonce_incr", "label": "立即增量扫描一次"}}]},
                            {"component": "VCol", "props": {"cols": 12, "md": 4},
                             "content": [{"component": "VSwitch", "props": {"model": "onlyonce_clean", "label": "立即清理孤儿STRM"}}]},
                        ]
                    },
                    # 数值参数行1
                    {
                        "component": "VRow",
                        "content": [
                            {"component": "VCol", "props": {"cols": 12, "md": 3},
                             "content": [{"component": "VTextField", "props": {"model": "startup_delay", "label": "启动延迟(秒)", "placeholder": "60", "type": "number"}}]},
                            {"component": "VCol", "props": {"cols": 12, "md": 3},
                             "content": [{"component": "VTextField", "props": {"model": "workers", "label": "工作线程数", "placeholder": "4", "type": "number"}}]},
                            {"component": "VCol", "props": {"cols": 12, "md": 3},
                             "content": [{"component": "VTextField", "props": {"model": "notify_delay", "label": "通知聚合延迟(秒)", "placeholder": "10", "type": "number"}}]},
                            {"component": "VCol", "props": {"cols": 12, "md": 3},
                             "content": [{"component": "VTextField", "props": {"model": "refresh_quiet", "label": "Emby刷新安静期(秒)", "placeholder": "30", "type": "number"}}]},
                        ]
                    },
                    # 监控模式行
                    {
                        "component": "VRow",
                        "content": [
                            {"component": "VCol", "props": {"cols": 12, "md": 6},
                             "content": [{"component": "VSelect", "props": {
                                 "model": "monitor_mode", "label": "实时监控模式",
                                 "items": [
                                     {"title": "轮询(推荐rclone网盘，对云端新增敏感)", "value": "polling"},
                                     {"title": "inotify(省CPU，仅适合本地磁盘/本机写入)", "value": "inotify"},
                                 ]}}]},
                            {"component": "VCol", "props": {"cols": 12, "md": 6},
                             "content": [{"component": "VTextField", "props": {
                                 "model": "poll_interval", "label": "轮询间隔(秒，仅轮询模式)", "placeholder": "10", "type": "number"}}]},
                        ]
                    },
                    # 数值参数行2
                    {
                        "component": "VRow",
                        "content": [
                            {"component": "VCol", "props": {"cols": 12, "md": 4},
                             "content": [{"component": "VTextField", "props": {"model": "incr_cron", "label": "增量扫描周期(cron)", "placeholder": "*/30 * * * *"}}]},
                            {"component": "VCol", "props": {"cols": 12, "md": 4},
                             "content": [{"component": "VTextField", "props": {"model": "del_check_times", "label": "删除前挂载检查次数", "placeholder": "3", "type": "number"}}]},
                            {"component": "VCol", "props": {"cols": 12, "md": 4},
                             "content": [{"component": "VTextField", "props": {"model": "del_max", "label": "单轮最大删除数(熔断)", "placeholder": "10", "type": "number"}}]},
                        ]
                    },
                    # 目录配置
                    {
                        "component": "VRow",
                        "content": [{
                            "component": "VCol",
                            "props": {"cols": 12},
                            "content": [{
                                "component": "VTextarea",
                                "props": {
                                    "model": "monitor_confs",
                                    "label": "目录配置",
                                    "rows": 6,
                                    "placeholder":
                                        "每行一个网盘，格式：\n"
                                        "监控目录#STRM生成目录#Emby播放路径#[Emby STRM目录(可选)]\n"
                                        "例：/opt/fufu/gd1#/media/strm/gd1#/mnt/gd1\n"
                                        "例：/opt/fufu/gd2#/media/strm/gd2#/mnt/gd2#/strm/gd2"
                                }
                            }]
                        }]
                    },
                    # 媒体服务器
                    {
                        "component": "VRow",
                        "content": [{
                            "component": "VCol",
                            "props": {"cols": 12, "md": 6},
                            "content": [{
                                "component": "VSelect",
                                "props": {
                                    "chips": True, "multiple": True, "clearable": True,
                                    "model": "mediaservers",
                                    "label": "媒体服务器(Emby)",
                                    "items": [{"title": s.name, "value": s.name}
                                              for s in self.mediaserver_helper.get_configs().values()]
                                    if self.mediaserver_helper else []
                                }
                            }]
                        }, {
                            "component": "VCol",
                            "props": {"cols": 12, "md": 6},
                            "content": [{
                                "component": "VTextField",
                                "props": {
                                    "model": "emby_path",
                                    "label": "全局STRM路径映射(可选)",
                                    "placeholder": "MP侧路径:Emby侧路径，多组用英文逗号"
                                }
                            }]
                        }]
                    },
                    # 扩展名
                    {
                        "component": "VRow",
                        "content": [{
                            "component": "VCol", "props": {"cols": 12, "md": 6},
                            "content": [{
                                "component": "VTextarea",
                                "props": {"model": "rmt_mediaext", "label": "视频格式", "rows": 2,
                                          "placeholder": ".mp4, .mkv, .ts ..."}
                            }]
                        }, {
                            "component": "VCol", "props": {"cols": 12, "md": 6},
                            "content": [{
                                "component": "VTextarea",
                                "props": {"model": "other_mediaext", "label": "非媒体文件格式", "rows": 2,
                                          "placeholder": ".nfo, .jpg, .png ..."}
                            }]
                        }]
                    },
                    # 排除关键词
                    {
                        "component": "VRow",
                        "content": [{
                            "component": "VCol", "props": {"cols": 12},
                            "content": [{
                                "component": "VTextarea",
                                "props": {"model": "exclude_keywords", "label": "排除关键词(每行一个，路径含则跳过)", "rows": 2,
                                          "placeholder": "sample\n预告"}
                            }]
                        }]
                    },
                ]
            }
        ], {
            "enabled": False,
            "onlyonce": False,
            "onlyonce_incr": False,
            "onlyonce_clean": False,
            "monitor": False,
            "notify": False,
            "cover": False,
            "copy_files": False,
            "copy_subtitles": False,
            "refresh_emby": False,
            "del_sync": False,
            "startup_delay": 60,
            "workers": 4,
            "monitor_mode": "polling",
            "poll_interval": 10,
            "incr_cron": "*/30 * * * *",
            "notify_delay": 10,
            "refresh_quiet": 30,
            "del_check_times": 3,
            "del_max": 10,
            "monitor_confs": "",
            "mediaservers": [],
            "rmt_mediaext": ".mp4, .mkv, .ts, .iso, .rmvb, .avi, .mov, .mpeg, .mpg, .wmv, .3gp, .asf, .m4v, .flv, .m2ts, .tp, .f4v",
            "other_mediaext": ".nfo, .jpg, .png, .json, .ass, .srt, .sup",
            "exclude_keywords": "",
            "emby_path": "",
        }

    def __preview_paths(self) -> List[Dict[str, str]]:
        """
        按当前目录配置，给每个盘生成一条STRM映射预览。
        优先用扫描时真实抓到的样例(_sample_media)，没有则用占位示例。
        """
        previews = []
        for mon_path, conf in self._dir_conf.items():
            strm_dir = conf["strm_dir"]
            emby_play = conf["emby_play"]
            emby_strm = conf["emby_strm"]
            # 取真实样例；没有则用占位
            sample = (self._sample_media or {}).get(mon_path)
            if sample:
                src = sample
            else:
                src = f"{mon_path}/国产剧集/剧名 (2026) {{tmdb=123456}}/Season 01/剧名 - S01E01.mp4"
            # strm文件路径
            strm_file = os.path.splitext(src.replace(mon_path, strm_dir, 1))[0] + ".strm"
            # strm内容(Emby播放路径)
            strm_content = src.replace(mon_path, emby_play, 1).replace("\\", "/")
            # Emby侧strm路径(用于刷新)
            emby_strm_path = strm_file.replace(strm_dir, emby_strm, 1) if strm_dir != emby_strm else strm_file
            previews.append({
                "mon": mon_path,
                "src": src,
                "strm_file": strm_file,
                "strm_content": strm_content,
                "emby_strm": emby_strm_path,
                "real": "真实样例" if sample else "占位示例",
            })
        return previews

    def get_page(self) -> List[dict]:
        """统计页"""
        stat = self._stat or {}
        db_count = self.__db_count()
        queue_len = self._queue.qsize() if self._queue else 0
        rows = [
            ("插件状态", "运行中" if self._enabled else "已停止"),
            ("配置网盘数", str(len(self._dir_conf))),
            ("已记忆文件数(DB)", str(db_count)),
            ("当前队列长度", str(queue_len)),
            ("上次全量扫描", stat.get("last_full_scan") or "-"),
            ("上次增量扫描", stat.get("last_incr_scan") or "-"),
            ("累计处理文件", str(stat.get("processed", 0))),
            ("累计生成STRM", str(stat.get("created", 0))),
            ("累计删除STRM", str(stat.get("deleted", 0))),
            ("累计错误数", str(stat.get("errors", 0))),
        ]

        # ===== STRM路径预览卡片 =====
        preview_cards = []
        for pv in self.__preview_paths():
            preview_cards.append({
                "component": "VCard",
                "props": {"variant": "outlined", "class": "mb-3"},
                "content": [
                    {
                        "component": "VCardTitle",
                        "props": {"class": "text-subtitle-1"},
                        "text": f"📁 {pv['mon']}  （{pv['real']}）"
                    },
                    {
                        "component": "VCardText",
                        "content": [
                            {
                                "component": "VTable",
                                "props": {"density": "compact"},
                                "content": [{
                                    "component": "tbody",
                                    "content": [
                                        {
                                            "component": "tr",
                                            "content": [
                                                {"component": "td",
                                                 "props": {"class": "text-medium-emphasis",
                                                           "style": "width:130px;white-space:nowrap"},
                                                 "text": label},
                                                {"component": "td",
                                                 "props": {"style": "word-break:break-all;font-family:monospace;font-size:12px"},
                                                 "text": val},
                                            ]
                                        }
                                        for label, val in [
                                            ("源文件", pv["src"]),
                                            ("生成STRM", pv["strm_file"]),
                                            ("STRM内容", pv["strm_content"]),
                                            ("Emby侧STRM", pv["emby_strm"]),
                                        ]
                                    ]
                                }]
                            }
                        ]
                    }
                ]
            })

        stat_table = {
            "component": "VTable",
            "props": {"hover": True},
            "content": [
                {
                    "component": "thead",
                    "content": [{
                        "component": "tr",
                        "content": [
                            {"component": "th", "text": "项目"},
                            {"component": "th", "text": "值"},
                        ]
                    }]
                },
                {
                    "component": "tbody",
                    "content": [
                        {
                            "component": "tr",
                            "content": [
                                {"component": "td", "text": k},
                                {"component": "td", "text": v},
                            ]
                        } for k, v in rows
                    ]
                }
            ]
        }

        content = []
        if preview_cards:
            content.append({
                "component": "div",
                "props": {"class": "text-h6 mb-2"},
                "text": "STRM 路径预览"
            })
            content.extend(preview_cards)
        content.append({
            "component": "div",
            "props": {"class": "text-h6 mb-2 mt-2"},
            "text": "运行统计"
        })
        content.append(stat_table)

        return [{
            "component": "div",
            "content": content
        }]

    def stop_service(self):
        """停止插件所有服务"""
        # 通知所有线程退出
        self._event.set()

        # 停止watchdog
        if self._observers:
            for observer in self._observers:
                try:
                    observer.stop()
                    observer.join(timeout=5)
                except Exception as e:
                    logger.debug(f"停止监控出错：{e}")
        self._observers = []

        # 唤醒并停止工作线程
        if self._queue is not None:
            try:
                for _ in range(len(self._work_threads)):
                    self._queue.put(None)
            except Exception:
                pass
        for t in self._work_threads:
            try:
                t.join(timeout=3)
            except Exception:
                pass
        self._work_threads = []
        self._queue = None

        # 停止调度器
        if self._scheduler:
            try:
                self._scheduler.remove_all_jobs()
                if self._scheduler.running:
                    self._scheduler.shutdown()
            except Exception as e:
                logger.debug(f"停止调度器出错：{e}")
            self._scheduler = None
