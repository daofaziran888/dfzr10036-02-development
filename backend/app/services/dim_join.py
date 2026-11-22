"""维表关联服务：关联补全后推动水位推进和Schema演进检测。

关联资格来自「同键事件校验清单」：只有判定为可参与关联的事件才能生成关联数据行，
暂缓事件确认放行后再补齐数据行并登记需要重算的下游输出。
"""
from __future__ import annotations

from datetime import datetime
from typing import Any

from app.services.base import BaseModuleService
from app.store import store

ELIGIBLE = "可参与关联"
PENDING_ELIGIBLE = "暂缓-确认后参与"


def _now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


class DimJoinService(BaseModuleService):
    MODULE = "dim_join"
    ENTITY = "关联配置"
    OPERATION_TERM = "关联"
    LIST_FIELDS = ["配置编号", "事实表", "维度表", "关联键", "关联类型", "缺失策略", "缓存TTL", "命中率"]
    REQUIRED_FIELDS = ["配置编号", "事实表", "维度表"]
    STATUS_ORDER = ["草稿", "关联中", "缓存失效", "已停止"]
    ACTION_RULES = {"启动关联": "关联中", "重建缓存": "缓存失效", "停止关联": "已停止"}
    NEGATIVE_ACTIONS = []

    # ---------- 同键事件校验清单消费 ----------

    def key_check_summary(self, entry_id: int) -> dict[str, Any] | None:
        """读取与关联配置同数据流的校验清单，汇总各判定能参与关联的事件。"""
        entry = self.get_entry(entry_id)
        if entry is None:
            return None
        windows = self._windows_for(str(entry.get("事实表", "")))
        rows = self._entries_for(windows)
        return {
            "配置编号": entry.get("配置编号", ""),
            "事实表": entry.get("事实表", ""),
            "窗口": [
                {
                    "窗口编号": window.get("窗口编号", ""),
                    "状态": window.get("状态", ""),
                    "阈值版本": window.get("阈值版本", ""),
                    "解析规则版本": window.get("解析规则版本", ""),
                    "校验批次号": window.get("校验批次号", ""),
                    "最近校验": window.get("最近校验", ""),
                }
                for window in windows
            ],
            "可参与关联事件": [self._event_brief(row) for row in rows if row.get("关联资格") == ELIGIBLE],
            "暂缓事件": [self._event_brief(row) for row in rows if row.get("关联资格") == PENDING_ELIGIBLE],
            "已判重复事件": [
                self._event_brief(row) for row in rows
                if row.get("关联资格") == "不可参与关联"
            ],
        }

    def build_join_rows(self, entry_id: int) -> dict[str, Any] | None:
        """按校验清单为可确认事件分配关联资格并生成数据行；暂缓事件只留占位不写行。"""
        entry = self.get_entry(entry_id)
        if entry is None:
            return None
        windows = self._windows_for(str(entry.get("事实表", "")))
        rows = self._entries_for(windows)
        join_rows = store.rows("_dim_join_rows")
        existing = {
            (str(row.get("配置编号", "")), str(row.get("事件编号", "")))
            for row in join_rows
        }

        created: list[dict[str, Any]] = []
        skipped_pending: list[str] = []
        skipped_duplicate: list[str] = []
        for check_row in rows:
            event_id = str(check_row.get("事件编号", ""))
            identity = (str(entry.get("配置编号", "")), event_id)
            if check_row.get("关联资格") == ELIGIBLE:
                if identity in existing:
                    continue
                join_row = self._new_join_row(entry, check_row, "首次关联")
                join_rows.append(join_row)
                created.append(join_row)
                existing.add(identity)
            elif check_row.get("关联资格") == PENDING_ELIGIBLE:
                skipped_pending.append(event_id)
            else:
                skipped_duplicate.append(event_id)

        if created or skipped_pending:
            entry["命中率"] = self._recompute_hit_rate(entry, join_rows)
            entry["pending"] = bool(skipped_pending)
            entry["abnormal"] = bool(skipped_pending)

        return {
            "配置编号": entry.get("配置编号", ""),
            "事实表": entry.get("事实表", ""),
            "created": created,
            "skipped_pending": skipped_pending,
            "skipped_duplicate": skipped_duplicate,
            "total_join_rows": sum(
                1 for row in join_rows if str(row.get("配置编号", "")) == str(entry.get("配置编号", ""))
            ),
        }

    def admit_reviewed_entry(self, check_row: dict[str, Any]) -> dict[str, Any] | None:
        """校验清单里暂缓事件被复核放行后，补齐维表关联数据行并登记重算任务。"""
        fact_table = str(check_row.get("数据流", ""))
        entry = next(
            (row for row in store.rows("dim_join") if str(row.get("事实表", "")) == fact_table),
            None,
        )
        if entry is None:
            return None

        join_rows = store.rows("_dim_join_rows")
        already = next(
            (row for row in join_rows
             if str(row.get("配置编号", "")) == str(entry.get("配置编号", ""))
             and str(row.get("事件编号", "")) == str(check_row.get("事件编号", ""))),
            None,
        )
        if already is None:
            join_row = self._new_join_row(entry, check_row, "复核补齐")
            join_rows.append(join_row)
        else:
            join_row = already
            join_row["状态"] = "已关联"
            join_row["补齐时间"] = _now()

        entry["命中率"] = self._recompute_hit_rate(entry, join_rows)
        tasks = self._ensure_recompute_tasks(entry, check_row)
        pending_rows = self._entries_for(
            self._windows_for(str(entry.get("事实表", "")))
        )
        remaining_pending = [
            row for row in pending_rows if row.get("关联资格") == PENDING_ELIGIBLE
        ]
        entry["pending"] = bool(remaining_pending)
        entry["abnormal"] = bool(remaining_pending)
        return {
            "config": entry,
            "join_row": join_row,
            "recompute_tasks": tasks,
        }

    def list_join_rows(self, entry_id: int) -> dict[str, Any] | None:
        entry = self.get_entry(entry_id)
        if entry is None:
            return None
        rows = [
            row for row in store.rows("_dim_join_rows")
            if str(row.get("配置编号", "")) == str(entry.get("配置编号", ""))
        ]
        tasks = [
            row for row in store.rows("_recompute_tasks")
            if str(row.get("配置编号", "")) == str(entry.get("配置编号", ""))
        ]
        return {"rows": rows, "recompute_tasks": tasks}

    def complete_recompute(self, task_id: int) -> tuple[dict[str, Any] | None, str]:
        task = next(
            (row for row in store.rows("_recompute_tasks") if int(row.get("id", 0)) == int(task_id)),
            None,
        )
        if task is None:
            return None, f"重算任务 {task_id} 不存在"
        if task.get("状态") == "已重算":
            return task, "重算任务已经完成，无需重复执行"
        task["状态"] = "已重算"
        task["重算时间"] = _now()
        task["pending"] = False
        return task, f"重算任务 {task.get('任务编号')}（{task.get('输出对象')}）已完成"

    # ---------- 原有下游联动 ----------

    def trigger_watermark_and_schema(self, entry_id: int) -> dict[str, Any]:
        """维表关联启动后，触发水位推进和Schema演进检测。

        数据流链路: dim_join -> watermark_mgr + schema_evo
        """
        entry = self.get_entry(entry_id)
        if entry is None or entry.get("status") != "关联中":
            return None

        tag = str(entry.get("事实表", ""))
        results: dict[str, Any] = {"watermark": None, "schema": None}

        # 水位推进
        wm_rows = store.rows("watermark_mgr")
        has_wm = [r for r in wm_rows if str(r.get("数据流", "")) == tag]
        if has_wm:
            has_wm[0]["status"] = "已追上"
            has_wm[0]["pending"] = True
            has_wm[0]["abnormal"] = False
            has_wm[0]["当前位点"] = "binlog.001234:891200"
            has_wm[0]["提交位点"] = "binlog.001234:891200"
            has_wm[0]["延迟行数"] = 0
            results["watermark"] = has_wm[0]
        else:
            new_wm_id = max((int(r.get("id", 0)) for r in wm_rows), default=0) + 1
            wm_config = {
                "id": new_wm_id,
                "水位编号": f"WATE-{new_wm_id:04d}",
                "数据流": tag,
                "当前位点": "binlog.001234:891200",
                "提交位点": "binlog.001234:891200",
                "延迟行数": 0,
                "水位类型": "事件时间水位",
                "最后推进": "2026-11-18 10:32:15",
                "追赶进度": "100%",
                "status": "已追上",
                "pending": True,
                "abnormal": False,
            }
            wm_rows.append(wm_config)
            results["watermark"] = wm_config

        # Schema演进检测
        se_rows = store.rows("schema_evo")
        new_se_id = max((int(r.get("id", 0)) for r in se_rows), default=0) + 1
        schema_rec = {
            "id": new_se_id,
            "记录编号": f"SCHE-{new_se_id:04d}",
            "数据流": tag,
            "变更列": "customer_tier(enum→varchar)",
            "列变更前": "ENUM('bronze','silver','gold')",
            "列变更后": "VARCHAR(32)",
            "兼容模式": "向前兼容",
            "下游影响": "仅影响实时打标任务",
            "演进时间": "2026-11-18 10:32:20",
            "status": "兼容通过",
            "pending": True,
            "abnormal": False,
        }
        se_rows.append(schema_rec)
        results["schema"] = schema_rec

        return results

    def handle_cache_invalidation(self, entry_id: int) -> dict[str, Any]:
        """缓存失效时，发出告警并级联影响下游延迟指标。

        数据流链路: dim_join(缓存失效) -> alert_rule + lag_monitor
        """
        entry = self.get_entry(entry_id)
        if entry is None or entry.get("status") != "缓存失效":
            return None

        tag = str(entry.get("事实表", ""))
        results = {"alert": None, "lag": None}

        # 创建告警
        alert_rows = store.rows("alert_rule")
        new_alert_id = max((int(r.get("id", 0)) for r in alert_rows), default=0) + 1
        alert = {
            "id": new_alert_id,
            "策略编号": f"ALER-{new_alert_id:04d}",
            "监控指标": "维表缓存命中率",
            "告警条件": "命中率 < 60% 持续5min",
            "告警级别": "P2-警告",
            "通知渠道": "飞书群+电话",
            "聚合窗口": "5min",
            "静默时段": "0-6点",
            "触发次数": 3,
            "status": "已触发",
            "pending": True,
            "abnormal": True,
        }
        alert_rows.append(alert)
        results["alert"] = alert

        # 标记延迟
        for lm in store.rows("lag_monitor"):
            if str(lm.get("数据流", "")) == tag and lm.get("status") in ("正常", "采集中"):
                lm["status"] = "延迟偏高"
                lm["pending"] = True
                lm["abnormal"] = True
                lm["延迟秒数"] = 45
                results["lag"] = lm
                break

        return results

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        if action == "执行关联":
            entry = self.get_entry(entry_id)
            if entry is None:
                return None, f"{self.ENTITY} {entry_id} 不存在或已归档"
            if entry.get("status") != "关联中":
                return entry, f"关联配置当前为「{entry.get('status')}」，需先启动关联并处于关联中才能执行"
            result = self.build_join_rows(entry_id)
            message = (
                f"按同键事件校验清单生成 {len(result['created'])} 条关联数据行，"
                f"暂缓 {len(result['skipped_pending'])} 条事件等待复核后才参与关联"
            )
            return entry, message
        return super().run_action(entry_id, action)

    # ---------- 内部辅助 ----------

    def _windows_for(self, fact_table: str) -> list[dict[str, Any]]:
        return [
            window for window in store.rows("_key_check_windows")
            if str(window.get("数据流", "")) == fact_table
        ]

    def _entries_for(self, windows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        window_ids = {str(window.get("窗口编号", "")) for window in windows}
        return [
            row for row in store.rows("key_check")
            if str(row.get("窗口编号", "")) in window_ids
        ]

    def _new_join_row(
        self, entry: dict[str, Any], check_row: dict[str, Any], source: str
    ) -> dict[str, Any]:
        join_rows = store.rows("_dim_join_rows")
        payload = check_row.get("负载") or {}
        key_column = str(entry.get("关联键", "")) or "关联键"
        dim_value = payload.get(key_column) or payload.get("customer_id") or "DIM-" + str(check_row.get("去重键值", ""))
        return {
            "id": max((int(row.get("id", 0)) for row in join_rows), default=0) + 1,
            "配置编号": entry.get("配置编号", ""),
            "事实表": entry.get("事实表", ""),
            "事件编号": check_row.get("事件编号", ""),
            "去重键值": check_row.get("去重键值", ""),
            "来源": check_row.get("来源", ""),
            "位点": check_row.get("位点", ""),
            "关联键": key_column,
            "关联键值": str(dim_value),
            "维度表": entry.get("维度表", ""),
            "来源类型": source,
            "判定依据": check_row.get("判定依据", ""),
            "阈值版本": check_row.get("阈值版本", ""),
            "解析规则版本": check_row.get("解析规则版本", ""),
            "补齐时间": _now(),
            "状态": "已关联",
        }

    def _recompute_hit_rate(self, entry: dict[str, Any], join_rows: list[dict[str, Any]]) -> str:
        rows = [
            row for row in join_rows
            if str(row.get("配置编号", "")) == str(entry.get("配置编号", ""))
        ]
        total = len(self._entries_for(self._windows_for(str(entry.get("事实表", "")))))
        if total == 0:
            return "0%"
        return f"{round(len(rows) * 100 / total, 1)}%"

    def _ensure_recompute_tasks(
        self, entry: dict[str, Any], check_row: dict[str, Any]
    ) -> list[dict[str, Any]]:
        tasks = store.rows("_recompute_tasks")
        outputs = self._expected_outputs(entry, check_row)
        created: list[dict[str, Any]] = []
        for output in outputs:
            existing = next(
                (
                    row for row in tasks
                    if str(row.get("配置编号", "")) == str(entry.get("配置编号", ""))
                    and str(row.get("输出对象", "")) == output
                    and str(row.get("事件编号", "")) == str(check_row.get("事件编号", ""))
                ),
                None,
            )
            if existing is not None:
                created.append(existing)
                continue
            task = {
                "id": max((int(row.get("id", 0)) for row in tasks), default=0) + 1,
                "任务编号": f"RCAL-{max((int(row.get('id', 0)) for row in tasks), default=0) + 1:04d}",
                "配置编号": entry.get("配置编号", ""),
                "事实表": entry.get("事实表", ""),
                "事件编号": check_row.get("事件编号", ""),
                "去重键值": check_row.get("去重键值", ""),
                "输出对象": output,
                "受影响范围": self._scope(entry, check_row, output),
                "重算原因": f"暂缓事件 {check_row.get('事件编号')} 复核放行后需补齐该输出",
                "状态": "待重算",
                "pending": True,
                "abnormal": False,
                "创建时间": _now(),
            }
            tasks.append(task)
            created.append(task)
        return created

    def _expected_outputs(self, entry: dict[str, Any], check_row: dict[str, Any]) -> list[str]:
        data_flow = str(entry.get("事实表", ""))
        outputs = ["维表关联结果行"]
        sink = next(
            (row for row in store.rows("sink_writer") if str(row.get("数据流", "")) == data_flow), None
        )
        if sink is not None:
            outputs.append("结果落盘批次")
        metric = next(
            (row for row in store.rows("metric_aggr") if str(row.get("数据流", "")) == data_flow), None
        )
        if metric is not None:
            outputs.append("流聚合窗口")
        return outputs

    def _scope(self, entry: dict[str, Any], check_row: dict[str, Any], output: str) -> str:
        data_flow = str(entry.get("事实表", ""))
        if output == "维表关联结果行":
            return f"{data_flow} / {check_row.get('去重键值')}"
        if output == "结果落盘批次":
            sink = next(
                (row for row in store.rows("sink_writer") if str(row.get("数据流", "")) == data_flow), None
            )
            return str((sink or {}).get("目标载体", data_flow))
        metric = next(
            (row for row in store.rows("metric_aggr") if str(row.get("数据流", "")) == data_flow), None
        )
        return str((metric or {}).get("窗口编号", data_flow))

    @staticmethod
    def _event_brief(row: dict[str, Any]) -> dict[str, Any]:
        return {
            "清单编号": row.get("清单编号", ""),
            "事件编号": row.get("事件编号", ""),
            "窗口编号": row.get("窗口编号", ""),
            "去重键值": row.get("去重键值", ""),
            "来源": row.get("来源", ""),
            "位点": row.get("位点", ""),
            "修改字段": row.get("修改字段", ""),
            "判定": row.get("判定", ""),
            "判定依据": row.get("判定依据", ""),
            "令牌": row.get("关联资格", ""),
            "阈值版本": row.get("阈值版本", ""),
            "解析规则版本": row.get("解析规则版本", ""),
        }