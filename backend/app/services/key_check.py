"""同键事件校验清单：对一个窗口内的变更事件流逐条判定，并把判定结果留给维表关联消费。

核心约束：
- 同源同位点、负载完全一致的重复日志只生效一次；
- 同源同位点但负载不同的日志不能直接当重复件丢弃，必须先暂缓复核；
- 同键不同位点的更新按窗口阈值的位点跨度、更新条数和修改字段重叠度判定；
- 每条判定绑定本次校验使用的阈值版本与解析规则版本，重跑时保留历史审计。
"""
from __future__ import annotations

import copy
import json
from collections import defaultdict
from datetime import datetime
from typing import Any

from app.services.base import BaseModuleService
from app.store import store

DECISION_ADMIT = "放行"
DECISION_REVIEW = "暂缓复核"
DECISION_DUPLICATE = "重复丢弃"
STATUS_CONFIRMED_ADMIT = "已确认放行"
STATUS_CONFIRMED_DUPLICATE = "已确认重复"

ELIGIBLE = "可参与关联"
NOT_ELIGIBLE = "不可参与关联"
PENDING_ELIGIBLE = "暂缓-确认后参与"


def _now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _canonical(payload: Any) -> str:
    return json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str)


def _summary(payload: Any, limit: int = 140) -> str:
    if not payload:
        return ""
    if isinstance(payload, dict):
        text = ",".join(f"{key}={value}" for key, value in payload.items())
    else:
        text = str(payload)
    return text if len(text) <= limit else text[: limit - 1] + "…"


def _modified_fields(event: dict[str, Any]) -> set[str]:
    raw = event.get("修改字段", "")
    if isinstance(raw, (list, tuple, set)):
        fields = {str(item).strip() for item in raw}
    else:
        fields = {item.strip() for item in str(raw).replace("，", ",").split(",")}
    return {field for field in fields if field}


def _diff_detail(left: Any, right: Any) -> tuple[list[str], str]:
    """给出两份负载里发生变化的字段，以及一段可读的差异说明。"""
    if not isinstance(left, dict) or not isinstance(right, dict):
        return [], "负载不是键值结构，无法逐字段比对"
    changed: list[str] = []
    parts: list[str] = []
    for key in sorted(set(left) | set(right)):
        before = left.get(key, "<缺失>")
        after = right.get(key, "<缺失>")
        if before != after:
            changed.append(str(key))
            parts.append(f"{key}: {before}→{after}")
    return changed, ("；".join(parts) if parts else "负载完全一致")


class KeyCheckService(BaseModuleService):
    MODULE = "key_check"
    ENTITY = "校验条目"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["清单编号", "窗口编号", "去重键值", "事件编号", "来源", "位点", "判定", "判定依据", "阈值版本", "解析规则版本"]
    REQUIRED_FIELDS = ["清单编号", "窗口编号", "事件编号"]
    STATUS_ORDER = ["待校验", DECISION_REVIEW, DECISION_ADMIT, DECISION_DUPLICATE,
                    STATUS_CONFIRMED_ADMIT, STATUS_CONFIRMED_DUPLICATE]
    ACTION_RULES = {"确认放行": STATUS_CONFIRMED_ADMIT, "确认重复": STATUS_CONFIRMED_DUPLICATE,
                    "重新校验": "待校验"}
    NEGATIVE_ACTIONS = []

    # ---------- 窗口与事件 ----------

    def list_windows(self, *, status: str | None = None) -> list[dict[str, Any]]:
        windows = [copy.deepcopy(row) for row in store.rows("_key_check_windows")]
        if status:
            windows = [row for row in windows if row.get("状态") == status]
        return windows

    def get_window(self, window_id: str | int) -> dict[str, Any] | None:
        return self._find_window(window_id)

    def add_events(self, window_id: str | int, events: list[dict[str, Any]]) -> tuple[dict[str, Any] | None, str]:
        window = self._find_window(window_id)
        if window is None:
            return None, f"校验窗口 {window_id} 不存在"
        if not events:
            return window, "未提供事件"
        rows = store.rows("_key_check_events")
        next_id = max((int(row.get("id", 0)) for row in rows), default=0) + 1
        existing_numbers = {str(row.get("事件编号", "")) for row in rows}
        added: list[dict[str, Any]] = []
        for offset, raw in enumerate(events):
            event = dict(raw)
            event["id"] = next_id + offset
            event.setdefault("事件编号", f"{window['窗口编号']}-N{event['id']:03d}")
            if str(event["事件编号"]) in existing_numbers:
                continue
            event.setdefault("窗口编号", window["窗口编号"])
            event.setdefault("数据流", window["数据流"])
            event.setdefault("管道编号", window["管道编号"])
            event.setdefault("去重键", window["去重键"])
            event.setdefault("来源", "未知来源")
            event.setdefault("位点", "")
            event.setdefault("位点序号", self._position_number(event.get("位点")))
            event.setdefault("负载", {})
            event.setdefault("修改字段", "")
            event.setdefault("解析规则版本", window["解析规则版本"])
            event.setdefault("事件时间", "")
            rows.append(event)
            added.append(copy.deepcopy(event))
        if not added:
            return window, "事件编号已存在，未新增事件"
        window["状态"] = "待校验"
        return window, f"已向窗口追加 {len(added)} 条变更事件"

    def update_threshold(
        self, window_id: str | int, threshold: dict[str, Any], version: str
    ) -> tuple[dict[str, Any] | None, str]:
        window = self._find_window(window_id)
        if window is None:
            return None, f"校验窗口 {window_id} 不存在"
        merged = dict(window.get("同键阈值", {}))
        merged.update({key: value for key, value in threshold.items() if value is not None})
        if merged.get("同位点重送生效次数") != 1:
            return None, "「同位点重送生效次数」固定为 1：相同来源、相同位点的重复日志只能生效一次"
        required = ("同位点重送生效次数", "同键位点跨度上限", "同键更新放行上限", "负载差异处理")
        missing = [key for key in required if merged.get(key) in (None, "")]
        if missing:
            return None, f"阈值缺少字段：{'、'.join(missing)}"
        window["同键阈值"] = merged
        window["阈值版本"] = version or f"TH-{_now()}"
        window["状态"] = "待校验"
        window["校验批次号"] = ""
        return window, (
            f"阈值已更新为 {window['阈值版本']}；原有判定仍绑定旧阈值，"
            "需重新校验后才能复核暂缓事件"
        )

    # ---------- 校验主流程 ----------

    def run_window_check(self, window_id: str | int) -> tuple[dict[str, Any] | None, str]:
        window = self._find_window(window_id)
        if window is None:
            return None, f"校验窗口 {window_id} 不存在"

        events = [
            copy.deepcopy(event)
            for event in store.rows("_key_check_events")
            if str(event.get("窗口编号", "")) == str(window["窗口编号"])
        ]
        events.sort(key=lambda event: (str(event.get("事件时间", "")), int(event.get("id", 0))))

        threshold = copy.deepcopy(window.get("同键阈值", {}))
        rule_version = str(window.get("解析规则版本", ""))
        threshold_version = str(window.get("阈值版本", ""))
        current_rule = self._current_rule_version(window)
        batch = self._next_batch(window)

        decisions = self._judge_events(events, threshold, rule_version, threshold_version)

        # 同阈值/同规则版本重跑时，沿用已经完成的复核结论，避免丢掉复核历史。
        carried = self._carry_over_reviewed(window, rule_version, threshold_version)
        for decision in decisions:
            identity = str(decision["事件"].get("事件编号", ""))
            previous = carried.get(identity)
            if previous is not None:
                decision["status"] = previous["status"]
                decision["判定"] = previous["判定"]
                decision["关联资格"] = previous["关联资格"]
                base_reason = str(previous.get("判定依据", ""))
                base_reason = base_reason.split("；沿用同版本上一次复核结论")[0]
                decision["判定依据"] = base_reason + "；沿用同版本上一次复核结论"
                decision["pending"] = False
                decision["abnormal"] = False

        self._replace_entries(window, decisions, batch)
        audit = self._record_audit(
            window, decisions, batch, threshold, threshold_version, rule_version, current_rule
        )
        window["状态"] = "已校验"
        window["校验批次号"] = batch
        window["最近校验"] = audit["执行时间"]
        window["当前解析规则版本"] = current_rule

        admitted = [row for row in decisions if row["status"] in (DECISION_ADMIT, STATUS_CONFIRMED_ADMIT)]
        reviewing = [row for row in decisions if row["status"] == DECISION_REVIEW]
        duplicates = [row for row in decisions if row["status"] in (DECISION_DUPLICATE, STATUS_CONFIRMED_DUPLICATE)]
        message = (
            f"窗口 {window['窗口编号']} 校验完成（批次 {batch}）："
            f"事件 {len(decisions)} 条，放行 {len(admitted)}、暂缓 {len(reviewing)}、重复丢弃 {len(duplicates)}；"
            f"判定绑定阈值 {threshold_version} / 解析规则 {rule_version}"
        )
        if current_rule and current_rule != rule_version:
            message += f"；注意当前解析规则已为 {current_rule}，本次仍按窗口登记的 {rule_version} 判定"
        return window, message

    def _judge_events(
        self,
        events: list[dict[str, Any]],
        threshold: dict[str, Any],
        rule_version: str,
        threshold_version: str,
    ) -> list[dict[str, Any]]:
        decisions: dict[int, dict[str, Any]] = {}
        same_position: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
        for event in events:
            key = (str(event.get("来源", "")), str(event.get("位点", "")), str(event.get("去重键值", "")))
            same_position[key].append(event)

        # 第一遍：同源同位点内部只处理重送。首次到达的那条留到第二遍，
        # 由跨位点规则决定它和同键其他位点更新如何共存。
        for (source, position, key_value), group in same_position.items():
            group.sort(key=lambda event: (str(event.get("事件时间", "")), int(event.get("id", 0))))
            first = group[0]
            base_payload = first.get("负载") or {}
            base_canonical = _canonical(base_payload)
            if str(first.get("解析规则版本", "")) != rule_version:
                for event in group:
                    decisions[int(event["id"])] = self._decision(
                        event, DECISION_REVIEW,
                        f"事件的解析规则版本 {event.get('解析规则版本')} 与窗口登记版本 {rule_version} 不一致，"
                        "不同版本解析出的负载不可直接比同",
                        threshold_version, rule_version,
                    )
                continue
            for event in group[1:]:
                if str(event.get("解析规则版本", "")) != rule_version:
                    decisions[int(event["id"])] = self._decision(
                        event, DECISION_REVIEW,
                        f"事件的解析规则版本 {event.get('解析规则版本')} 与窗口登记版本 {rule_version} 不一致，"
                        "不同版本解析出的负载不可直接比同",
                        threshold_version, rule_version,
                    )
                    continue
                canonical = _canonical(event.get("负载") or {})
                if canonical == base_canonical:
                    decisions[int(event["id"])] = self._decision(
                        event, DECISION_DUPLICATE,
                        f"与同位点首次日志 {first.get('事件编号')} 来源、位点、负载完全一致，"
                        f"按阈值「同位点重送生效次数={threshold.get('同位点重送生效次数')}」"
                        "只生效一次，后续重送不重复生效",
                        threshold_version, rule_version,
                    )
                    continue
                changed, detail = _diff_detail(base_payload, event.get("负载") or {})
                decisions[int(event["id"])] = self._decision(
                    event, DECISION_REVIEW,
                    f"与同位点日志 {first.get('事件编号')} 来源、位点相同但负载不一致（{detail}），"
                    "在确认两次提交身份之前不能当作重复件丢弃，先暂缓复核",
                    threshold_version, rule_version,
                    changed_fields=changed,
                )

        # 第二遍：同键不同位点。每个位点取一条代表事件，按跨度、条数和修改字段重叠度放宽。
        span_limit = int(threshold.get("同键位点跨度上限") or 0)
        update_limit = int(threshold.get("同键更新放行上限") or 0)
        for key_value in sorted({str(event.get("去重键值", "")) for event in events}):
            key_events = [event for event in events if str(event.get("去重键值", "")) == key_value]
            positions = {
                (str(event.get("来源", "")), str(event.get("位点", ""))) for event in key_events
            }
            representatives: list[dict[str, Any]] = []
            for identity in positions:
                head = sorted(
                    same_position[(identity[0], identity[1], key_value)],
                    key=lambda item: (str(item.get("事件时间", "")), int(item.get("id", 0))),
                )[0]
                if int(head["id"]) not in decisions:
                    representatives.append(head)
            representatives.sort(
                key=lambda event: (self._position_number(event.get("位点")), str(event.get("事件时间", "")))
            )

            admitted = [
                item for item in (decisions.get(int(event["id"])) for event in key_events)
                if item is not None and item["status"] == DECISION_ADMIT
            ]
            admitted_count = len(admitted)
            admitted_fields: set[str] = set()
            for item in admitted:
                admitted_fields |= _modified_fields(item["事件"])
            admitted_positions = {
                (str(item["事件"].get("来源", "")), str(item["事件"].get("位点", ""))) for item in admitted
            }
            earliest_admitted = min(
                (self._position_number(item["事件"].get("位点")) for item in admitted),
                default=None,
            )
            multi_position = len(positions) > 1

            for event in representatives:
                identity = (str(event.get("来源", "")), str(event.get("位点", "")))
                if identity in admitted_positions:
                    continue
                if str(event.get("解析规则版本", "")) != rule_version:
                    decisions[int(event["id"])] = self._decision(
                        event, DECISION_REVIEW,
                        f"解析规则版本 {event.get('解析规则版本')} 与窗口登记版本 {rule_version} 不一致，"
                        "需按原规则重新解析后再判定",
                        threshold_version, rule_version,
                    )
                    continue
                position = self._position_number(event.get("位点"))
                fields = _modified_fields(event)
                if earliest_admitted is None:
                    reason = (
                        f"同键不同位点更新：该键在当前窗口内的首条可确认更新，位点 {position}，"
                        f"修改字段 {'、'.join(sorted(fields)) or '未标注'}，作为后续位点比对的基准"
                        if multi_position
                        else (
                            f"同源同位点首次到达，按阈值「同位点重送生效次数="
                            f"{threshold.get('同位点重送生效次数')}」保留生效 1 次，"
                            "该条负载可作为同位点重送的比对基准"
                        )
                    )
                    decisions[int(event["id"])] = self._decision(
                        event, DECISION_ADMIT, reason, threshold_version, rule_version,
                    )
                    admitted_count = 1
                    admitted_fields = fields
                    admitted_positions.add(identity)
                    earliest_admitted = position
                    continue
                if span_limit and position - earliest_admitted > span_limit:
                    decisions[int(event["id"])] = self._decision(
                        event, DECISION_REVIEW,
                        f"同键位点跨度 {position - earliest_admitted} 超过阈值 "
                        f"「同键位点跨度上限={span_limit}」，需确认是否同一批更新",
                        threshold_version, rule_version,
                    )
                    continue
                if fields and admitted_fields & fields:
                    overlap = "、".join(sorted(admitted_fields & fields))
                    decisions[int(event["id"])] = self._decision(
                        event, DECISION_REVIEW,
                        f"修改字段 {overlap} 与同键已放行更新重叠，需确认覆盖顺序后再参与关联",
                        threshold_version, rule_version,
                    )
                    continue
                if update_limit and admitted_count + 1 > update_limit:
                    decisions[int(event["id"])] = self._decision(
                        event, DECISION_REVIEW,
                        f"同键已放行 {admitted_count} 条，再放行将超过阈值 "
                        f"「同键更新放行上限={update_limit}」",
                        threshold_version, rule_version,
                    )
                    continue

                lags = position - earliest_admitted
                decisions[int(event["id"])] = self._decision(
                    event, DECISION_ADMIT,
                    f"同键不同位点更新：位点跨度 {lags} ≤ {span_limit}，"
                    f"修改字段 {'、'.join(sorted(fields)) or '未标注'} 未与已放行更新重叠，"
                    f"放行后同键更新第 {admitted_count + 1} 条 ≤ 上限 {update_limit or '不限'}",
                    threshold_version, rule_version,
                )
                admitted_count += 1
                admitted_fields |= fields

        # 兜底：未命中任何分支的事件不得静默丢弃，一律转入暂缓复核。
        for event in events:
            if int(event["id"]) not in decisions:
                decisions[int(event["id"])] = self._decision(
                    event, DECISION_REVIEW,
                    "该事件未命中任何判定分支，暂缓复核后再分配关联资格",
                    threshold_version, rule_version,
                )

        ordered = [decisions[int(event["id"])] for event in events if int(event["id"]) in decisions]
        return ordered

    def _decision(
        self,
        event: dict[str, Any],
        status: str,
        reason: str,
        threshold_version: str,
        rule_version: str,
        *,
        changed_fields: list[str] | None = None,
    ) -> dict[str, Any]:
        eligible = {
            DECISION_ADMIT: ELIGIBLE,
            DECISION_REVIEW: PENDING_ELIGIBLE,
            DECISION_DUPLICATE: NOT_ELIGIBLE,
        }[status]
        return {
            "事件": event,
            "status": status,
            "判定": status,
            "判定依据": reason,
            "关联资格": eligible,
            "阈值版本": threshold_version,
            "解析规则版本": rule_version,
            "修改字段": sorted(_modified_fields(event)) if changed_fields is None else changed_fields,
            "负载摘要": _summary(event.get("负载")),
            "pending": status in (DECISION_REVIEW, "待校验"),
            "abnormal": status == DECISION_REVIEW,
        }

    def _carry_over_reviewed(
        self, window: dict[str, Any], rule_version: str, threshold_version: str
    ) -> dict[str, dict[str, Any]]:
        carried: dict[str, dict[str, Any]] = {}
        for row in store.rows(self.MODULE):
            if str(row.get("窗口编号", "")) != str(window["窗口编号"]):
                continue
            if row.get("status") not in (STATUS_CONFIRMED_ADMIT, STATUS_CONFIRMED_DUPLICATE):
                continue
            if str(row.get("阈值版本", "")) != threshold_version:
                continue
            if str(row.get("解析规则版本", "")) != rule_version:
                continue
            carried[str(row.get("事件编号", ""))] = row
        return carried

    def _replace_entries(
        self, window: dict[str, Any], decisions: list[dict[str, Any]], batch: str
    ) -> None:
        all_rows = store.rows(self.MODULE)
        all_rows[:] = [
            row for row in all_rows if str(row.get("窗口编号", "")) != str(window["窗口编号"])
        ]
        for index, decision in enumerate(decisions, start=1):
            event = decision["事件"]
            row = {
                "id": max((int(item.get("id", 0)) for item in all_rows), default=0) + 1,
                "status": decision["status"],
                "pending": decision["pending"],
                "abnormal": decision["abnormal"],
                "清单编号": f"{window['窗口编号']}-E{index:02d}",
                "窗口编号": window["窗口编号"],
                "数据流": event.get("数据流", window.get("数据流", "")),
                "管道编号": event.get("管道编号", window.get("管道编号", "")),
                "去重键": event.get("去重键", window.get("去重键", "")),
                "去重键值": event.get("去重键值", ""),
                "事件编号": event.get("事件编号", ""),
                "来源": event.get("来源", ""),
                "位点": event.get("位点", ""),
                "位点序号": self._position_number(event.get("位点")),
                "修改字段": "、".join(decision["修改字段"]),
                "事件时间": event.get("事件时间", ""),
                "判定": decision["判定"],
                "判定依据": decision["判定依据"],
                "关联资格": decision["关联资格"],
                "阈值版本": decision["阈值版本"],
                "解析规则版本": decision["解析规则版本"],
                "阈值快照": copy.deepcopy(window.get("同键阈值", {})),
                "负载摘要": decision["负载摘要"],
                "负载": copy.deepcopy(event.get("负载") or {}),
                "批次号": batch,
                "校验时间": window.get("最近校验", "") or _now(),
            }
            all_rows.append(row)
        # 回写最近一次校验时间，供条目引用
        if all_rows:
            for row in all_rows:
                if row.get("批次号") == batch:
                    row["校验时间"] = _now()

    def _record_audit(
        self,
        window: dict[str, Any],
        decisions: list[dict[str, Any]],
        batch: str,
        threshold: dict[str, Any],
        threshold_version: str,
        rule_version: str,
        current_rule: str,
    ) -> dict[str, Any]:
        audits = store.rows("_key_check_audits")
        audit = {
            "id": max((int(row.get("id", 0)) for row in audits), default=0) + 1,
            "批次号": batch,
            "窗口编号": window["窗口编号"],
            "数据流": window.get("数据流", ""),
            "管道编号": window.get("管道编号", ""),
            "去重键": window.get("去重键", ""),
            "解析规则版本": rule_version,
            "当前解析规则版本": current_rule,
            "阈值版本": threshold_version,
            "阈值快照": copy.deepcopy(threshold),
            "事件总数": len(decisions),
            "放行数": sum(1 for item in decisions if item["status"] in (DECISION_ADMIT, STATUS_CONFIRMED_ADMIT)),
            "暂缓数": sum(1 for item in decisions if item["status"] == DECISION_REVIEW),
            "重复丢弃数": sum(1 for item in decisions if item["status"] in (DECISION_DUPLICATE, STATUS_CONFIRMED_DUPLICATE)),
            "首次关联可用更新": [
                {
                    "事件编号": item["事件"].get("事件编号", ""),
                    "来源": item["事件"].get("来源", ""),
                    "位点": item["事件"].get("位点", ""),
                    "修改字段": item["修改字段"],
                    "负载摘要": item["负载摘要"],
                }
                for item in decisions
                if item["status"] in (DECISION_ADMIT, STATUS_CONFIRMED_ADMIT)
            ],
            "暂缓事件": [
                {
                    "事件编号": item["事件"].get("事件编号", ""),
                    "来源": item["事件"].get("来源", ""),
                    "位点": item["事件"].get("位点", ""),
                    "判定依据": item["判定依据"],
                }
                for item in decisions
                if item["status"] == DECISION_REVIEW
            ],
            "暂缓确认后需重算输出": self._expected_recompute(window, decisions),
            "判定明细": [
                {
                    "事件编号": item["事件"].get("事件编号", ""),
                    "来源": item["事件"].get("来源", ""),
                    "位点": item["事件"].get("位点", ""),
                    "修改字段": item["修改字段"],
                    "负载摘要": item["负载摘要"],
                    "判定": item["判定"],
                    "判定依据": item["判定依据"],
                    "关联资格": item["关联资格"],
                    "阈值版本": item["阈值版本"],
                    "解析规则版本": item["解析规则版本"],
                }
                for item in decisions
            ],
            "执行时间": _now(),
            "status": "已记录",
            "pending": False,
            "abnormal": False,
        }
        audits.append(audit)
        return audit

    def _expected_recompute(
        self, window: dict[str, Any], decisions: list[dict[str, Any]]
    ) -> list[dict[str, str]]:
        pending = [item for item in decisions if item["status"] == DECISION_REVIEW]
        if not pending:
            return []
        key_values = sorted({str(item["事件"].get("去重键值", "")) for item in pending})
        data_flow = str(window.get("数据流", ""))
        sink = next(
            (row for row in store.rows("sink_writer") if str(row.get("数据流", "")) == data_flow),
            None,
        )
        metric = next(
            (row for row in store.rows("metric_aggr") if str(row.get("数据流", "")) == data_flow),
            None,
        )
        outputs = [
            {
                "输出对象": "维表关联结果行",
                "受影响范围": f"{data_flow} / {'、'.join(key_values)}",
                "重算原因": "暂缓事件确认后需补齐这些键在维表中的关联数据行",
            },
            {
                "输出对象": "结果落盘批次",
                "受影响范围": str((sink or {}).get("目标载体", data_flow)),
                "重算原因": "关联行补齐后需重写受影响的目标表批次",
            },
            {
                "输出对象": "流聚合窗口",
                "受影响范围": str((metric or {}).get("窗口编号", data_flow)),
                "重算原因": "补齐事件可能改变金额与计数聚合，需要重算对应窗口",
            },
        ]
        return outputs

    # ---------- 复核动作 ----------

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, f"{self.ENTITY} {entry_id} 不存在或已归档"

        if action == "重新校验":
            window = self._find_window(entry.get("窗口编号", ""))
            if window is None:
                return entry, f"条目的窗口 {entry.get('窗口编号')} 已不存在"
            checked, message = self.run_window_check(window["窗口编号"])
            return checked, message

        if action in ("确认放行", "确认重复"):
            return self._review_entry(entry, action)

        return super().run_action(entry_id, action)

    def _review_entry(self, entry: dict[str, Any], action: str) -> tuple[dict[str, Any] | None, str]:
        if entry.get("status") != DECISION_REVIEW:
            return entry, (
                f"条目当前为「{entry.get('status')}」，只有暂缓复核条目可以确认；"
                "如需重判请先重新校验窗口"
            )
        window = self._find_window(entry.get("窗口编号", ""))
        if window is None:
            return entry, f"条目的窗口 {entry.get('窗口编号')} 已不存在"
        mismatches: list[str] = []
        if str(entry.get("阈值版本", "")) != str(window.get("阈值版本", "")):
            mismatches.append(
                f"判定阈值版本 {entry.get('阈值版本')} 与窗口当前 {window.get('阈值版本')} 不一致"
            )
        if str(entry.get("解析规则版本", "")) != str(window.get("解析规则版本", "")):
            mismatches.append(
                f"判定解析规则版本 {entry.get('解析规则版本')} 与窗口当前 {window.get('解析规则版本')} 不一致"
            )
        if mismatches:
            return entry, "判定绑定的版本已过期：" + "；".join(mismatches) + "，请重新校验后再复核"

        if action == "确认重复":
            entry["status"] = STATUS_CONFIRMED_DUPLICATE
            entry["判定"] = STATUS_CONFIRMED_DUPLICATE
            entry["关联资格"] = NOT_ELIGIBLE
            entry["判定依据"] = str(entry.get("判定依据", "")) + f"；复核确认于 {_now()}：确属重复提交，不参与关联"
            entry["pending"] = False
            entry["abnormal"] = False
            return entry, (
                f"暂缓事件 {entry.get('事件编号')} 已确认按重复件处理，不分配关联资格，"
                "不触发生成下游重算"
            )

        entry["status"] = STATUS_CONFIRMED_ADMIT
        entry["判定"] = STATUS_CONFIRMED_ADMIT
        entry["关联资格"] = ELIGIBLE
        entry["判定依据"] = str(entry.get("判定依据", "")) + (
            f"；复核确认于 {_now()}：负载差异来自同一逻辑更新的多次提交，确认放行"
        )
        entry["pending"] = False
        entry["abnormal"] = False

        from app.services import service_for

        dim_service = service_for("dim_join")
        result = dim_service.admit_reviewed_entry(entry)
        if result is None:
            return entry, "暂缓事件已确认放行，但维表关联未找到可写入的配置"
        tasks = result.get("recompute_tasks") or []
        message = (
            f"暂缓事件 {entry.get('事件编号')} 已确认放行，维表关联补齐 1 条数据行，"
            f"生成 {len(tasks)} 项重算任务："
            + "、".join(str(task.get("输出对象", "")) for task in tasks)
        )
        return entry, message

    # ---------- 辅助 ----------

    def _find_window(self, window_id: str | int) -> dict[str, Any] | None:
        for window in store.rows("_key_check_windows"):
            if str(window.get("窗口编号", "")) == str(window_id):
                return window
            try:
                if int(window.get("id", 0)) == int(window_id):
                    return window
            except (TypeError, ValueError):
                continue
        return None

    def _current_rule_version(self, window: dict[str, Any]) -> str:
        pipeline = next(
            (row for row in store.rows("event_trans")
             if str(row.get("管道编号", "")) == str(window.get("管道编号", ""))),
            None,
        )
        source = str((pipeline or {}).get("源端", ""))
        rule = next(
            (row for row in store.rows("binlog_parse") if str(row.get("源端", "")) == source),
            None,
        )
        return str((rule or {}).get("规则版本", window.get("解析规则版本", "")))

    def _next_batch(self, window: dict[str, Any]) -> str:
        audits = [
            row for row in store.rows("_key_check_audits")
            if str(row.get("窗口编号", "")) == str(window["窗口编号"])
        ]
        return f"{window['窗口编号']}-B{len(audits) + 1:02d}"

    @staticmethod
    def _position_number(position: Any) -> int:
        if isinstance(position, int):
            return position
        text = str(position or "")
        tail = text.rsplit(":", 1)[-1].rsplit("/", 1)[-1]
        try:
            return int(tail)
        except (TypeError, ValueError):
            return 0
