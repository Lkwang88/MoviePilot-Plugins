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
    plugin_version = "1.1.1"
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
    _brake_mode = "pause"          # none/pause/limit/alt
    _limit_speed = 256             # limit 模式限速值 KB/s
    _recover_factor = 1.1          # 恢复线 = 阈值 × 系数
    _notify_interval = 30          # 分钟
    _collect_interval = 60         # 秒

    # 内存状态（brake_scene 持久化，其余重启复位）
    _services: Dict[str, Any] = {}       # name -> ServiceInfo
    _last_snapshot: Optional[tuple] = None
    _last_notify_ts: float = 0.0
    _braked = False                      # 制动状态机
    _brake_scene: Optional[Dict] = None  # 制动现场存档（模式/原限速/被停清单）
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
        # 制动模式：none仅告警/pause暂停/limit限速/alt备用速度
        mode = str(config.get("brake_mode") or "").strip()
        if mode not in ("none", "pause", "limit", "alt"):
            # 旧版配置迁移：auto_stop=true→pause，false→none；全新安装默认 pause
            mode = "pause" if config.get("auto_stop") else (
                "none" if "auto_stop" in config else "pause")
        self._brake_mode = mode
        try:
            self._limit_speed = max(1, int(config.get("limit_speed") or 256))
        except (TypeError, ValueError):
            self._limit_speed = 256
        # 恢复系数：恢复线=阈值×系数，调大可避免空间缓慢回升时反复触发
        try:
            self._recover_factor = min(5.0, max(1.05,
                                         float(config.get("recover_factor") or 1.1)))
        except (TypeError, ValueError):
            self._recover_factor = 1.1
        try:
            self._notify_interval = max(1, int(config.get("notify_interval") or 30))
        except (TypeError, ValueError):
            self._notify_interval = 30
        try:
            self._collect_interval = max(30, int(config.get("collect_interval") or 60))
        except (TypeError, ValueError):
            self._collect_interval = 60
        # 恢复持久化的制动现场（MP 重启后继续未完成的制动周期）
        try:
            scene = self.get_data("brake_scene")
            if scene and isinstance(scene, dict):
                self._brake_scene = scene
                self._braked = True
        except Exception as err:
            logger.error(f"小仓酱磁盘卫士 读取制动现场失败：{err}")

    def get_state(self) -> bool:
        return self._enabled

    @staticmethod
    def _mode_label(mode: str) -> str:
        return {"none": "仅告警", "pause": "暂停下载",
                "limit": "限速", "alt": "备用速度"}.get(mode, mode)

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        return []

    def get_api(self) -> List[Dict[str, Any]]:
        return []

    # ============================ 仪表板 ============================

    def get_dashboard(self, key: str = None, **kwargs):
        """
        首页仪表板卡片：磁盘卫士实时状态直达。
        返回 (列配置, 全局配置, 页面JSON)；未启用返回 None 不占位。
        """
        if not self.get_state():
            return None

        col_config = {"cols": 12, "md": 4}
        global_config = {
            "title": "小仓酱磁盘卫士",
            "refresh": 30,
            "border": True,
        }

        if not self._last_snapshot:
            page = [{
                "component": "div",
                "content": [{
                    "component": "VAlert",
                    "props": {"type": "info", "variant": "tonal",
                              "density": "compact"},
                    "text": "等待首次检查…",
                }],
            }]
            return col_config, global_config, page

        n, total_size, total_done, _free = self._last_snapshot
        disks = getattr(self._page_disks, "copy", lambda: [])() or []
        color = "error" if self._braked else "success"
        pending = max(total_size - total_done, 0)

        content = []
        # 状态行
        if self._braked:
            applied = (self._brake_scene or {}).get("applied") or self._brake_mode
            head = "🚨 已制动·%s · 空间不足" % self._mode_label(applied)
        else:
            head = "✅ 监控中"
        content.append({
            "component": "div",
            "props": {"class": "text-subtitle-1 font-weight-bold text-%s" % color},
            "text": head,
        })
        # 主数字：磁盘剩余（多目录合计），无数据时用待写入
        free_sum = sum(d["free"] for d in disks if d["exists"])
        if free_sum > 0:
            big_num, big_label = self._fmt_gb(free_sum), "磁盘剩余空间"
        else:
            big_num, big_label = self._fmt_gb(pending), "待写入体积"
        content.append({
            "component": "div",
            "props": {"class": "text-h5 font-weight-bold mt-1 text-%s" % color},
            "text": big_num,
        })
        content.append({
            "component": "div",
            "props": {"class": "text-caption text-medium-emphasis"},
            "text": big_label,
        })
        # 明细表：标签左 / 数值右
        rows = [
            ("📥 正在下载", "%d 个任务" % n),
            ("📦 目标体积", self._fmt_gb(total_size)),
            ("✅ 已完成", "%s（%s）" % (self._fmt_gb(total_done),
                                        self._fmt_pct(total_done, total_size))),
            ("⏳ 待写入", self._fmt_gb(pending)),
        ]
        for i, d in enumerate(disks):
            if d["exists"] and d["total"] > 0:
                multi = sum(1 for x in disks if x["exists"]) > 1
                tag = "💾 磁盘剩余" + ("①②③④⑤⑥⑦⑧⑨"[i] if multi else "")
                rows.append((tag, "剩 %s（%s）" % (
                    self._fmt_gb(d["free"]),
                    self._fmt_pct(d["free"], d["total"]))))
            else:
                rows.append(("💾 磁盘剩余", "⚠️ 目录不可用"))
        content.append({
            "component": "VTable",
            "props": {"density": "compact", "class": "mt-1"},
            "content": [{
                "component": "tbody",
                "content": [
                    {
                        "component": "tr",
                        "content": [
                            {"component": "td",
                             "props": {"class": "text-body-2 pl-0 text-medium-emphasis"},
                             "text": label},
                            {"component": "td",
                             "props": {"class": "text-body-2 text-right pr-0 font-weight-medium"},
                             "text": value},
                        ],
                    }
                    for label, value in rows
                ],
            }],
        })
        if self._braked:
            applied = (self._brake_scene or {}).get("applied") or self._brake_mode
            foot = {"pause": "已自动暂停下载，空间回升后将自动恢复任务",
                    "limit": "已自动限速，空间回升后将自动恢复原速",
                    "alt": "已切换备用速度，空间回升后将自动切回"}.get(
                        applied, "请及时清理空间或手动暂停下载任务")
            content.append({
                "component": "div",
                "props": {"class": "text-caption text-error mt-1"},
                "text": foot,
            })
        page = [{"component": "div", "content": content}]
        return col_config, global_config, page

    def stop_service(self):
        # 制动现场已持久化，停用/重配不丢状态；仅清异常节流标志
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
        """恢复判定：回升超过 阈值×恢复系数 才解除（系数可调，防缓慢回升反复触发）"""
        if not disk["exists"] or disk["total"] <= 0:
            return True
        if self._threshold_mode == "absolute":
            limit = self._to_bytes(self._threshold_value
                                   * self._recover_factor * 1024 ** 3)
            return disk["free"] > limit
        return disk["free"] / disk["total"] * 100 > (self._threshold_value
                                                     * self._recover_factor)

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

            # --- 阈值判定与制动/恢复 ---
            triggered = [d for d in disks if self._is_triggered(d)]
            if triggered and not self._braked:
                scene = self._do_brake(stats)
                self._brake_scene = scene
                self._braked = True
                try:
                    self.save_data("brake_scene", scene)
                except Exception as err:
                    logger.error(f"小仓酱磁盘卫士 保存制动现场失败：{err}")
                self._notify_brake(triggered, scene, stats)
                self._last_notify_ts = time.time()   # 制动后顺延常规播报，防轰炸
            elif self._braked and not triggered:
                if all(self._is_recovered(d) for d in disks):
                    self._do_recover()
                    self._notify_recover()
                    self._braked = False
                    self._brake_scene = None
                    try:
                        self.del_data("brake_scene")
                    except Exception as err:
                        logger.error(f"小仓酱磁盘卫士 清理制动现场失败：{err}")

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

    def _do_brake(self, stats: List[Dict]) -> Dict:
        """
        按制动模式执行动作，返回现场存档 dict（持久化供恢复用）。
        none=仅告警；pause=停下载任务；limit=全局限速；alt=qb备用速度(TR回退pause)。
        """
        scene: Dict = {"mode": self._brake_mode, "applied": self._brake_mode,
                       "stopped": {}, "orig_speed": None,
                       "orig_alt": None, "switched": False, "ts": time.time()}
        if self._brake_mode == "none":
            return scene

        if self._brake_mode == "alt":
            # 备用速度仅 qBittorrent 支持，TR 实例回退为暂停模式
            qb_services = {k: v for k, v in self._services.items()
                           if v.type == "qbittorrent"}
            if qb_services:
                switched_all = True
                for dl_name, service in qb_services.items():
                    qbc = getattr(service.instance, "qbc", None)
                    try:
                        cur = getattr(qbc.transfer, "speed_limits_mode", "normal")
                        if cur != "alternative":
                            qbc.toggle_speed_limits_mode()
                            scene["orig_alt"] = False
                            scene["switched"] = True
                            self._log_action(f"[{dl_name}] 已切换至备用速度")
                        else:
                            scene["orig_alt"] = True
                            self._log_action(f"[{dl_name}] 已处于备用速度，无需切换")
                    except Exception as err:
                        switched_all = False
                        logger.error(f"小仓酱磁盘卫士 [{dl_name}] 切换备用速度失败：{err}")
                        self._log_action(f"[{dl_name}] 切换备用速度出错", "error")
                if switched_all:
                    return scene
            scene["applied"] = "pause"   # 回退

        if scene["applied"] == "limit":
            orig_all, ok_all = [], True
            for dl_name, service in self._services.items():
                try:
                    orig = service.instance.get_speed_limit() or (0, 0)
                    orig_all.append((dl_name, orig))
                    # 仅压低下载速度，上传保持原值（None 会被转成 0=不限）
                    service.instance.set_speed_limit(self._limit_speed, orig[1])
                    self._log_action(
                        f"[{dl_name}] 下载限速 {self._limit_speed} KB/s（原 {orig[0]}）")
                except Exception as err:
                    ok_all = False
                    logger.error(f"小仓酱磁盘卫士 [{dl_name}] 限速失败：{err}")
                    self._log_action(f"[{dl_name}] 限速出错", "error")
            if ok_all:
                scene["orig_speed"] = orig_all
                return scene
            scene["applied"] = "pause"   # 限速失败回退为暂停

        # pause（含回退）：停全部下载任务，记录清单供恢复
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
                    scene["stopped"][dl_name] = ids
                    stopped.extend(items)
                    self._log_action(f"[{dl_name}] 已暂停 {len(items)} 个下载任务")
                else:
                    self._log_action(f"[{dl_name}] 暂停操作返回失败")
            except Exception as err:
                logger.error(f"小仓酱磁盘卫士 [{dl_name}] 暂停任务失败：{err}")
                self._log_action(f"[{dl_name}] 暂停任务出错", "error")
        return scene

    def _do_recover(self):
        """按现场存档恢复：只动自己制动时的操作，不碰用户手动变更"""
        scene = self._brake_scene or {}
        applied = scene.get("applied") or scene.get("mode") or "none"
        if applied == "pause" and scene.get("stopped"):
            for dl_name, ids in scene["stopped"].items():
                service = self._services.get(dl_name)
                if not service:
                    continue
                try:
                    # 重复 start 对运行中的任务无副作用；已删除的 id 会被忽略
                    service.instance.start_torrents(ids)
                    self._log_action(f"[{dl_name}] 已恢复 {len(ids)} 个下载任务")
                except Exception as err:
                    logger.error(f"小仓酱磁盘卫士 [{dl_name}] 恢复任务失败：{err}")
                    self._log_action(f"[{dl_name}] 恢复任务出错", "error")
        elif applied == "limit" and scene.get("orig_speed"):
            for dl_name, orig in scene["orig_speed"]:
                service = self._services.get(dl_name)
                if not service:
                    continue
                try:
                    service.instance.set_speed_limit(orig[0], orig[1])
                    self._log_action(f"[{dl_name}] 已恢复原限速 {orig[0]} KB/s")
                except Exception as err:
                    logger.error(f"小仓酱磁盘卫士 [{dl_name}] 恢复限速失败：{err}")
        elif applied == "alt" and scene.get("switched"):
            for dl_name, service in self._services.items():
                if service.type != "qbittorrent":
                    continue
                try:
                    service.instance.qbc.toggle_speed_limits_mode()
                    self._log_action(f"[{dl_name}] 已切回正常速度")
                except Exception as err:
                    logger.error(f"小仓酱磁盘卫士 [{dl_name}] 切回正常速度失败：{err}")

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

    def _notify_brake(self, triggered: List[Dict], scene: Dict,
                      stats: List[Dict]):
        """制动告警：触线目录 + 按模式说明已执行的动作"""
        mode = scene.get("applied") or self._brake_mode
        if mode == "pause":
            head = "已自动暂停下载"
        elif mode == "limit":
            head = "已自动限速"
        elif mode == "alt":
            head = "已切换备用速度"
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
        if mode == "pause":
            stopped = [s for s in stats
                       if s["id"] in (scene.get("stopped", {})
                                       .get(s["downloader"], []))]
            if stopped:
                lines.append("\n⏸️ 暂停了 %d 个下载任务：" % len(stopped))
                for i, s in enumerate(sorted(stopped, key=lambda x: -x["size"])[:10], 1):
                    lines.append("   %d. [%s] %s（%s）" % (
                        i, s["downloader"], s["name"][:48], self._fmt_gb(s["size"])))
                if len(stopped) > 10:
                    lines.append("   ...等共 %d 个" % len(stopped))
            elif stats:
                lines.append("\n⚠️ 暂停操作未成功，请尽快手动处理！")
            lines.append("\n💡 空间回升后将自动恢复这些任务")
        elif mode == "limit":
            lines.append("\n🐢 全局下载速度已限制为 %d KB/s" % self._limit_speed)
            lines.append("💡 空间回升后将自动恢复原限速")
        elif mode == "alt":
            lines.append("\n🐢 已切换至 qBittorrent 备用速度")
            lines.append("💡 空间回升后将自动切回正常速度")
        else:
            lines.append("\n💡 请及时清理空间或手动暂停下载任务")
        self.post_message(mtype=NotificationType.Plugin,
                          title="🚨 磁盘空间告警 · %s" % head,
                          text="\n".join(lines))
        self._log_action("触发告警：" + "；".join(d["dir"] for d in triggered))

    def _notify_recover(self):
        """空间回升通知：按制动模式说明已恢复的动作"""
        scene = self._brake_scene or {}
        applied = scene.get("applied") or scene.get("mode") or "none"
        lines = ["剩余空间已回到安全范围，小仓酱解除告警状态。"]
        if applied == "pause" and scene.get("stopped"):
            total = sum(len(v) for v in scene["stopped"].values())
            lines.append(f"▶️ 已自动恢复 {total} 个下载任务")
        elif applied == "limit" and scene.get("orig_speed"):
            lines.append("▶️ 已恢复原下载限速设置")
        elif applied == "alt" and scene.get("switched"):
            lines.append("▶️ 已切回正常下载速度")
        else:
            lines.append("如需继续下载，请在下载器中恢复任务。")
        try:
            self.post_message(mtype=NotificationType.Plugin,
                              title="✅ 磁盘空间已回升",
                              text="\n".join(lines))
            self._log_action("空间回升，已解除告警并恢复")
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
                                    "component": "VSelect",
                                    "props": {
                                        "model": "brake_mode",
                                        "label": "触阈值动作",
                                        "items": [
                                            {"title": "暂停全部下载任务",
                                             "value": "pause"},
                                            {"title": "全局限速",
                                             "value": "limit"},
                                            {"title": "备用速度(仅qB)",
                                             "value": "alt"},
                                            {"title": "仅告警不动作",
                                             "value": "none"},
                                        ],
                                        "hint": "空间回升超过恢复线(阈值×恢复系数)时自动恢复",
                                        "persistent-hint": True},
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
                                "props": {"cols": 12, "md": 4},
                                "content": [{
                                    "component": "VTextField",
                                    "props": {"model": "limit_speed",
                                              "label": "限速值(KB/s)",
                                              "type": "number",
                                              "hint": "限速模式生效，空间回升自动恢复原速",
                                              "persistent-hint": True},
                                }],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [{
                                    "component": "VTextField",
                                    "props": {"model": "recover_factor",
                                              "label": "恢复系数",
                                              "type": "number",
                                              "hint": "恢复线=阈值×系数，如阈值50GB系数2.0即回升到100GB才恢复；调大防反复触发",
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
            "monitor_dirs": "/ptdownload",
            "downloader_names": "",
            "threshold_mode": "percent",
            "threshold_value": 10,
            "brake_mode": "pause",
            "limit_speed": 256,
            "recover_factor": 1.1,
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
        status = ("🚨 已制动·%s（等待空间回升）" % self._mode_label(
                      (self._brake_scene or {}).get("applied") or self._brake_mode)
                  if self._braked else "✅ 监控中")
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

