"""事件转换服务：转换完成后触发维表关联和Dedup去重。"""
from __future__ import annotations

from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class EventTransService(BaseModuleService):
    MODULE = "event_trans"
    ENTITY = "转换管道"
    OPERATION_TERM = "转换"
    LIST_FIELDS = ["管道编号", "源端", "事件类型", "转换函数", "输出格式", "失败策略", "转换顺序", "最近吞吐"]
    REQUIRED_FIELDS = ["管道编号", "源端", "事件类型"]
    STATUS_ORDER = ["草稿", "转换中", "转换过载", "已停止"]
    ACTION_RULES = {"启动转换": "转换中", "扩容处理": "转换过载", "停止管道": "已停止"}
    NEGATIVE_ACTIONS = []

    def trigger_dim_join_and_dedup(self, entry_id: int) -> dict[str, Any]:
        """事件转换管道启动后，级联创建维表关联和去重窗口。

        数据流链路: event_trans -> dim_join + dedup_stream
        """
        entry = self.get_entry(entry_id)
        if entry is None or entry.get("status") != "转换中":
            return None

        tag = str(entry.get("源端", ""))
        results: dict[str, Any] = {"dim_join": None, "dedup": None}

        # 创建/恢复 dim_join 维表关联
        dim_rows = store.rows("dim_join")
        has_dim = [r for r in dim_rows if str(r.get("事实表", "")) == tag]
        if has_dim:
            has_dim[0]["status"] = "关联中"
            has_dim[0]["pending"] = True
            has_dim[0]["abnormal"] = False
            has_dim[0]["命中率"] = "87.3%"
            results["dim_join"] = has_dim[0]
        else:
            new_dim_id = max((int(r.get("id", 0)) for r in dim_rows), default=0) + 1
            dim_config = {
                "id": new_dim_id,
                "配置编号": f"DIM_-{new_dim_id:04d}",
                "事实表": tag,
                "维度表": "dim_customer_profile",
                "关联键": "customer_id",
                "关联类型": "LEFT JOIN",
                "缺失策略": "填充默认值",
                "缓存TTL": "600s",
                "命中率": "87.3%",
                "status": "关联中",
                "pending": True,
                "abnormal": False,
            }
            dim_rows.append(dim_config)
            results["dim_join"] = dim_config

        # 创建/恢复 dedup_stream 去重窗口
        dedup_rows = store.rows("dedup_stream")
        has_dedup = [r for r in dedup_rows if str(r.get("数据流", "")) == tag]
        if has_dedup:
            has_dedup[0]["status"] = "去重中"
            has_dedup[0]["pending"] = True
            has_dedup[0]["abnormal"] = False
            results["dedup"] = has_dedup[0]
        else:
            new_dedup_id = max((int(r.get("id", 0)) for r in dedup_rows), default=0) + 1
            dedup_config = {
                "id": new_dedup_id,
                "窗口编号": f"DEDU-{new_dedup_id:04d}",
                "数据流": tag,
                "去重键": "event_id+timestamp",
                "窗口宽度": "10min",
                "去重算法": "BloomFilter",
                "误杀率": "0.01%",
                "内存占用": "128MB",
                "最近命中": "1420次/h",
                "status": "去重中",
                "pending": True,
                "abnormal": False,
            }
            dedup_rows.append(dedup_config)
            results["dedup"] = dedup_config

        # 转换管道启动后开启同键事件校验窗口；同一数据流存在登记窗口时不再重复开窗。
        check_rows = store.rows("_key_check_windows")
        has_check = [r for r in check_rows if str(r.get("数据流", "")) == tag]
        if has_check:
            results["key_check"] = has_check[0]
        else:
            new_check_id = max((int(r.get("id", 0)) for r in check_rows), default=0) + 1
            rule_row = next(
                (r for r in store.rows("binlog_parse") if str(r.get("源端", "")) == tag),
                None,
            )
            check_window = {
                "id": new_check_id,
                "窗口编号": f"KCHK-{new_check_id:04d}",
                "管道编号": str(entry.get("管道编号", "")),
                "数据流": tag,
                "去重键": "order_id",
                "窗口起点": "2026-11-18 09:00:00",
                "窗口终点": "2026-11-18 09:10:00",
                "解析规则版本": str((rule_row or {}).get("规则版本", "v1")),
                "阈值版本": "TH-2026-11-18-01",
                "同键阈值": {
                    "同位点重送生效次数": 1,
                    "同键位点跨度上限": 1000,
                    "同键更新放行上限": 3,
                    "负载差异处理": "暂缓复核",
                },
                "状态": "待校验",
                "校验批次号": "",
                "最近校验": "",
            }
            check_rows.append(check_window)
            results["key_check"] = check_window

        return results