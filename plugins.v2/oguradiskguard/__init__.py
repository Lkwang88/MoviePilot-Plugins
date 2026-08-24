import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from apscheduler.triggers.interval import IntervalTrigger

from app.helper.downloader import DownloaderHelper
from app.log import logger
from app.plugins import _PluginBase
from app.schemas.types import NotificationType
from app.utils.system import SystemUtils


class OguraDiskGuard(_PluginBase):
    """
    小仓酱磁盘卫士：实时统计正在下载的种子体积与磁盘剩余空间，
    定时播报（数据无变化静默）；剩余空间触及阈值时自动暂停下载任务防止爆盘。
    """

    plugin_name = "小仓酱磁盘卫士"
    plugin_desc = "统计正在下载的种子体积与磁盘剩余空间，定时播报；触及阈值自动暂停下载，防止爆盘。"
    plugin_icon = "diskusage.jpg"
    plugin_version = "1.0.1"
    plugin_author = "Lkwang88"
    author_url = "https://github.com/Lkwang88"
    plugin_config_prefix = "oguradiskguard_"
    plugin_order = 50
    auth_level = 1

    # 运行时状态
    _enabled = False
    _monitor_dirs: List[str] = []
    _downloader_names: List[str] = []
    _threshold_mode = "percent"
    _threshold_value = 10.0
    _auto_stop = False
    _notify_interval = 30          # 分钟
    _collect_interval = 60         # 秒

    # 内存状态（重启即复位）
    _services: Dict[str, Any] = {}       # name -> ServiceInfo
    _last_snapshot: Optional[tuple] = None
    _last_notify_ts: float = 0.0
    _braked = False                      # 制动状态机
    _err_notified = False                # 异常告警节流标志
    _action_log: List[str] = []          # 最近动作记录（详情页展示）

    def init_plugin(self, config: dict = None):
        config = config or {}
        self._enabled = bool(config.get("enabled"))
        dirs_raw = str(config.get("monitor_dirs") or "/ptdownload")
        self._monitor_dirs = [d.strip() for d in dirs_raw.split(",") if d.strip()]
        names_raw = str(config.get("downloader_names") or "")
        self._downloader_names = [n.strip() for n in names_raw.split(",") if n.strip()]
        self._threshold_mode = config.get("threshold_mode") or "percent"
        try:
            self._threshold_value = float(config.get("threshold_value") or 10)
        except (TypeError, ValueError):
            self._threshold_value = 10.0
        self._auto_stop = bool(config.get("auto_stop"))
        try:
            self._notify_interval = max(1, int(config.get("notify_interval") or 30))
        except (TypeError, ValueError):
            self._notify_interval = 30
        try:
            self._collect_interval = max(30, int(config.get("collect_interval") or 60))
        except (TypeError, ValueError):
            self._collect_interval = 60

    def get_state(self) -> bool:
        return self._enabled

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        return []

    def get_api(self) -> List[Dict[str, Any]]:
        return []

    def stop_service(self):
        self._braked = False
        self._err_notified = False

    def get_service(self) -> List[Dict[str, Any]]:
        """
        注册定时服务：60s 心跳触发，内部按检查间隔节流；
        next_run_time 使服务注册后立即执行一次。
        """
        if not self.get_state():
            return []
        return [
            {
                "id": "OguraDiskGuard",
                "name": "小仓酱磁盘卫士检查",
                "trigger": IntervalTrigger(seconds=60),
                "func": self.check,
                "kwargs": {"next_run_time": datetime.now()},
            },
        ]

    # ============================ 单位换算 ============================

    @staticmethod
    def _to_bytes(value: Any) -> int:
        """
        统一转 bytes 整数。
        qb WebUI API 与 transmission RPC 的 size/downloaded 字段规范均为 bytes；
        SystemUtils.free_space/total_space 为 bytes（psutil）。
        如真机发现数量级异常，仅需修改本函数。
        """
        try:
            return int(value or 0)
        except (TypeError, ValueError):
            return 0

    @staticmethod
    def _fmt_gb(num_bytes: int) -> str:
        """bytes -> GB 字符串（1024 进制，2 位小数）"""
        return f"{num_bytes / 1024 ** 3:.2f} GB"

    @staticmethod
    def _fmt_pct(part: float, whole: float) -> str:
        if whole <= 0:
            return "0.0%"
        return f"{part / whole * 100:.1f}%"

    # ============================ 数据采集 ============================

    def _collect_services(self) -> Dict[str, Any]:
        """获取目标下载器服务映射"""
        try:
            services = DownloaderHelper().get_services()
        except Exception as err:
            logger.error(f"小仓酱磁盘卫士 获取下载器服务失败：{err}")
            return {}
        if not self._downloader_names:
            return services
        return {k: v for k, v in services.items() if k in self._downloader_names}

    def _collect_torrents(self) -> Optional[List[Dict[str, Any]]]:
        """
        收集全部下载器中「正在下载」的种子（不含做种）。
        返回统一结构列表；任一下载器查询异常返回 None（本次跳过）。
        """
        stats: List[Dict[str, Any]] = []
        for name, service in self._services.items():
            instance = getattr(service, "instance", None)
            stype = getattr(service, "type", None)
            if not instance:
                continue
            try:
                torrents = instance.get_downloading_torrents()
            except Exception as err:
                logger.error(f"小仓酱磁盘卫士 获取 [{name}] 下载中种子失败：{err}")
                return None
            if torrents is None:
                return None
            for t in torrents:
                if stype == "qbittorrent":
                    tid = getattr(t, "hash", None)
                    size = self._to_bytes(getattr(t, "size", 0))
                    done = self._to_bytes(getattr(t, "downloaded", 0))
                else:
                    tid = getattr(t, "id", None)
                    size = self._to_bytes(getattr(t, "size_when_done", 0))
                    left = self._to_bytes(getattr(t, "left_until_done", 0))
                    done = max(size - left, 0)
                stats.append({
                    "id": tid,
                    "name": getattr(t, "name", "") or "",
                    "size": size,
                    "done": min(done, size),
                    "downloader": name,
                })
        return stats

    def _collect_disk(self) -> List[Dict[str, Any]]:
        """逐监控目录采集磁盘空间"""
        disks = []
        for d in self._monitor_dirs:
            path = Path(d)
            total = self._to_bytes(SystemUtils.total_space(path))
            free = self._to_bytes(SystemUtils.free_space(path))
            disks.append({"dir": d, "total": total, "free": free,
                          "exists": path.exists()})
        return disks

    # ============================ 引擎判定 ============================

    @staticmethod
    def _make_snapshot(stats: List[Dict], disks: List[Dict]) -> tuple:
        """快照四元组：bytes 整数精确比较，无浮点误差"""
        return (
            len(stats),
            sum(s["size"] for s in stats),
            sum(s["done"] for s in stats),
            sum(d["free"] for d in disks),
        )

    def _is_triggered(self, disk: Dict) -> bool:
        """单目录阈值判定"""
        if not disk["exists"] or disk["total"] <= 0:
            return False
        if self._threshold_mode == "absolute":
            limit = self._to_bytes(self._threshold_value * 1024 ** 3)
            return disk["free"] <= limit
        return disk["free"] / disk["total"] * 100 <= self._threshold_value

    def _is_recovered(self, disk: Dict) -> bool:
        """滞回复位判定：回升超过阈值×1.1 才算解除"""
        if not disk["exists"] or disk["total"] <= 0:
            return True
        if self._threshold_mode == "absolute":
            limit = self._to_bytes(self._threshold_value * 1.1 * 1024 ** 3)
            return disk["free"] > limit
        return disk["free"] / disk["total"] * 100 > self._threshold_value * 1.1

    # ============================ 主流程 ============================

    def check(self):
        """定时入口（60s 触发一次）：内部按检查间隔节流 → 采集 → 阈值判定 → 告警/制动 → 播报"""
        if not self._enabled or not self._monitor_dirs:
            return
        now = time.time()
        if now - getattr(self, "_last_check_ts", 0.0) < self._collect_interval - 1:
            return
        self._last_check_ts = now
        try:
            self._services = self._collect_services()
            if not self._services:
                self._log_action("未找到可用下载器服务，跳过本次检查")
                return

            stats = self._collect_torrents()
            if stats is None:
                self._notify_error_once("下载器查询失败，本次检查跳过")
                return
            disks = self._collect_disk()

            # --- 阈值判定与制动 ---
            triggered = [d for d in disks if self._is_triggered(d)]
            if triggered and not self._braked:
                self._braked = True
                stopped = self._do_brake(stats) if self._auto_stop else []
                self._notify_brake(triggered, stopped, stats)
                self._last_notify_ts = time.time()   # 制动后顺延常规播报，防轰炸
            elif self._braked and not triggered:
                if all(self._is_recovered(d) for d in disks):
                    self._braked = False
                    self._notify_recover(triggered=None)

            # --- 常规播报（节流）---
            snapshot = self._make_snapshot(stats, disks)
            self._page_disks = disks
            changed = snapshot != self._last_snapshot
            due = now - self._last_notify_ts >= self._notify_interval * 60
            if changed and due:
                self._notify_report(snapshot, stats, disks)
            self._last_snapshot = snapshot
            self._err_notified = False
            logger.info(
                "小仓酱磁盘卫士 检查完成：%d 个下载任务，目标 %s / 已完成 %s / 待写入 %s；%s",
                len(stats),
                self._fmt_gb(snapshot[1]), self._fmt_gb(snapshot[2]),
                self._fmt_gb(max(snapshot[1] - snapshot[2], 0)),
                "；".join("%s 剩余 %s" % (d["dir"], self._fmt_gb(d["free"]))
                          for d in disks if d["exists"]) or "监控目录不可用",
            )
        except Exception as err:
            logger.error(f"小仓酱磁盘卫士 检查异常：{str(err)}", exc_info=True)

    def _do_brake(self, stats: List[Dict]) -> List[Dict]:
        """全停正在下载任务（不动做种），返回停成功的列表"""
        by_dl: Dict[str, List[Dict]] = {}
        for s in stats:
            by_dl.setdefault(s["downloader"], []).append(s)
        stopped: List[Dict] = []
        for dl_name, items in by_dl.items():
            service = self._services.get(dl_name)
            if not service:
                continue
            ids = [s["id"] for s in items if s["id"] is not None]
            if not ids:
                continue
            try:
                if service.instance.stop_torrents(ids):
                    stopped.extend(items)
                    self._log_action(f"[{dl_name}] 已暂停 {len(items)} 个下载任务")
                else:
                    self._log_action(f"[{dl_name}] 暂停操作返回失败")
            except Exception as err:
                logger.error(f"小仓酱磁盘卫士 [{dl_name}] 暂停任务失败：{err}")
                self._log_action(f"[{dl_name}] 暂停任务出错：{err}")
        return stopped

    def _log_action(self, text: str, level: str = "info"):
        stamp = datetime.now().strftime("%H:%M:%S")
        self._action_log.insert(0, f"[{stamp}] {text}")
        del self._action_log[20:]
        getattr(logger, level, logger.info)(f"小仓酱磁盘卫士 {text}")

    # ============================ 通知输出 ============================

    def _notify_report(self, snapshot: tuple, stats: List[Dict], disks: List[Dict]):
        """常规播报：统计 + 磁盘状态"""
        total_size, total_done = snapshot[1], snapshot[2]
        lines = ["📥 正在下载：%d 个任务" % len(stats)]
        if stats:
            lines += [
                "   目标体积：%s" % self._fmt_gb(total_size),
                "   已完成：%s（%s）" % (self._fmt_gb(total_done),
                                        self._fmt_pct(total_done, total_size)),
                "   待写入：%s" % self._fmt_gb(max(total_size - total_done, 0)),
            ]
        else:
            lines.append("   当前没有正在下载的任务～")
        for d in disks:
            if not d["exists"]:
                lines.append("\n💾 %s\n   ⚠️ 目录不存在，请检查配置" % d["dir"])
                continue
            lines += [
                "\n💾 %s" % d["dir"],
                "   剩余 %s / 共 %s（%s）" % (
                    self._fmt_gb(d["free"]), self._fmt_gb(d["total"]),
                    self._fmt_pct(d["free"], d["total"])),
            ]
        text = "\n".join(lines)
        self.post_message(mtype=NotificationType.Plugin,
                          title="🛡️ 小仓酱磁盘卫士 · 播报", text=text)
        self._last_notify_ts = time.time()
        self._log_action("已发送常规播报")

    def _notify_brake(self, triggered: List[Dict], stopped: List[Dict],
                      stats: List[Dict]):
        """制动告警：触线目录 + 被停任务清单"""
        if self._auto_stop and stopped:
            head = "已自动暂停下载"
        elif self._auto_stop:
            head = "自动暂停未成功"
        else:
            head = "请及时处理"
        lines = []
        for d in triggered:
            pct = self._fmt_pct(d["free"], d["total"]) if d["exists"] else "N/A"
            free = self._fmt_gb(d["free"]) if d["exists"] else "目录不可用"
            lines.append("💾 %s 剩余 %s（%s），触及阈值 %s" % (
                d["dir"], free, pct,
                ("%g%%" % self._threshold_value) if self._threshold_mode == "percent"
                else "%g GB" % self._threshold_value))
        if self._auto_stop:
            if stopped:
                lines.append("\n⏸️ 暂停了 %d 个下载任务：" % len(stopped))
                for i, s in enumerate(sorted(stopped, key=lambda x: -x["size"])[:10], 1):
                    lines.append("   %d. [%s] %s（%s）" % (
                        i, s["downloader"], s["name"][:48], self._fmt_gb(s["size"])))
                if len(stopped) > 10:
                    lines.append("   ...等共 %d 个" % len(stopped))
            elif stats:
                lines.append("\n⚠️ 暂停操作未成功，请尽快手动处理！")
            lines.append("\n💡 清理空间后请在下载器中手动恢复任务")
        self.post_message(mtype=NotificationType.Plugin,
                          title="🚨 磁盘空间告警 · %s" % head,
                          text="\n".join(lines))
        self._log_action("触发告警：" + "；".join(d["dir"] for d in triggered))

    def _notify_recover(self, triggered: Optional[List[Dict]]):
        """空间回升提示（轻量）"""
        try:
            self.post_message(mtype=NotificationType.Plugin,
                              title="✅ 磁盘空间已回升",
                              text="剩余空间回到安全范围，小仓酱解除告警状态。"
                                   "如需继续下载，请在下载器中恢复任务。")
            self._log_action("空间回升，解除告警状态")
        except Exception as err:
            logger.error(f"小仓酱磁盘卫士 发送恢复通知失败：{err}")

    def _notify_error_once(self, message: str):
        """异常告警节流：连续异常只报一次"""
        self._log_action(message)
        if self._err_notified:
            return
        self._err_notified = True
        try:
            self.post_message(mtype=NotificationType.Plugin,
                              title="⚠️ 小仓酱磁盘卫士 异常",
                              text=message + "，恢复正常后将继续播报。")
        except Exception as err:
            logger.error(f"小仓酱磁盘卫士 发送异常通知失败：{err}")

    # ============================ 配置页 ============================

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
                                "props": {"cols": 12, "md": 6},
                                "content": [{
                                    "component": "VSwitch",
                                    "props": {"model": "enabled",
                                              "label": "启用插件"},
                                }],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [{
                                    "component": "VSwitch",
                                    "props": {"model": "auto_stop",
                                              "label": "触阈值自动暂停下载任务"},
                                }],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [{
                                    "component": "VTextField",
                                    "props": {"model": "monitor_dirs",
                                              "label": "监控目录",
                                              "placeholder": "/ptdownload",
                                              "hint": "多个目录用英文逗号分隔，逐目录独立判定",
                                              "persistent-hint": True},
                                }],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [{
                                    "component": "VTextField",
                                    "props": {"model": "downloader_names",
                                              "label": "下载器名称",
                                              "placeholder": "留空监控全部下载器",
                                              "hint": "多个名称用英文逗号分隔",
                                              "persistent-hint": True},
                                }],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [{
                                    "component": "VSelect",
                                    "props": {"model": "threshold_mode",
                                              "label": "阈值模式",
                                              "items": [
                                                  {"title": "剩余百分比", "value": "percent"},
                                                  {"title": "剩余 GB", "value": "absolute"},
                                              ]},
                                }],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [{
                                    "component": "VTextField",
                                    "props": {"model": "threshold_value",
                                              "label": "阈值",
                                              "type": "number",
                                              "hint": "随模式表示 % 或 GB",
                                              "persistent-hint": True},
                                }],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 2},
                                "content": [{
                                    "component": "VTextField",
                                    "props": {"model": "notify_interval",
                                              "label": "播报间隔(分)",
                                              "type": "number"},
                                }],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 2},
                                "content": [{
                                    "component": "VTextField",
                                    "props": {"model": "collect_interval",
                                              "label": "检查间隔(秒)",
                                              "type": "number",
                                              "hint": "最小30；60秒开销极小，仅看播报可调大",
                                              "persistent-hint": True},
                                }],
                            },
                        ],
                    }
                ],
            }
        ], {
            "enabled": False,
            "auto_stop": False,
            "monitor_dirs": "/ptdownload",
            "downloader_names": "",
            "threshold_mode": "percent",
            "threshold_value": 10,
            "notify_interval": 30,
            "collect_interval": 60,
        }

    # ============================ 详情页 ============================

    def get_page(self) -> List[dict]:
        """
        详情页。根元素统一 div 包裹、文本放组件层 text——
        对齐真机验证过的 vuetify 渲染器写法。
        """
        content: List[dict] = []
        if not self._enabled:
            content.append({
                "component": "VAlert",
                "props": {"type": "info", "variant": "tonal"},
                "text": "插件未启用，请在设置中开启。",
            })
            return [{"component": "div", "content": content}]
        if not self._last_snapshot:
            content.append({
                "component": "VAlert",
                "props": {"type": "info", "variant": "tonal"},
                "text": "尚未完成首次检查：服务注册后会立即运行一次，稍候刷新本页查看。",
            })
            return [{"component": "div", "content": content}]

        n, total_size, total_done, _free = self._last_snapshot
        status = ("🚨 已制动（等待空间回升）" if self._braked else "✅ 监控中")
        cards = [
            self._stat_card("状态", status),
            self._stat_card("正在下载", "%d 个任务" % n),
            self._stat_card("待写入", self._fmt_gb(max(total_size - total_done, 0))),
            self._stat_card("已完成", "%s（%s）" % (
                self._fmt_gb(total_done),
                self._fmt_pct(total_done, total_size))),
        ]
        for d in getattr(self, "_page_disks", []) or []:
            if not d["exists"]:
                cards.append(self._stat_card(d["dir"], "⚠️ 目录不存在"))
            else:
                cards.append(self._stat_card(
                    d["dir"], "剩 %s / 共 %s" % (self._fmt_gb(d["free"]),
                                                 self._fmt_gb(d["total"]))))
                cards.append(self._stat_card(
                    "剩余比例", self._fmt_pct(d["free"], d["total"])))
        content.append({"component": "VRow", "content": cards})

        log_lines = "\n".join(self._action_log[:8]) or "暂无"
        content.append({
            "component": "VCard",
            "props": {"variant": "tonal", "class": "mt-3"},
            "content": [{
                "component": "VCardText",
                "props": {"class": "text-caption",
                          "style": "white-space:pre-wrap;"},
                "text": "最近动作：\n" + log_lines,
            }],
        })
        return [{"component": "div", "content": content}]

    def _stat_card(self, label: str, value: str) -> dict:
        return {
            "component": "VCol",
            "props": {"cols": 6, "md": 3},
            "content": [{
                "component": "VCard",
                "props": {"variant": "tonal", "class": "pa-2"},
                "content": [{
                    "component": "VCardText",
                    "props": {"class": "text-center",
                              "style": "white-space:pre-wrap;"},
                    "text": "%s\n%s" % (value, label),
                }],
            }],
        }

