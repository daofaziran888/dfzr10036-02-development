"""统一资源接口：查询、导出、登记和动作按模块 key 分派到领域服务。"""
from __future__ import annotations

import csv
import io
from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import StreamingResponse

from app.schemas import (
    ActionPayload,
    ActionResult,
    CheckEventPayload,
    CheckRunPayload,
    EntryPayload,
    PageResult,
)
from app.services import service_for

router = APIRouter(prefix="/api", tags=["业务资源"])


def require_service(module: str):
    service = service_for(module)
    if service is None:
        raise HTTPException(status_code=404, detail=f"未知业务模块：{module}")
    return service


# ---- 同键事件校验清单 ----
# 这些固定路径必须注册在 /{module}/{entry_id} 之前，避免被动态段吞掉。

@router.get("/key_check/windows")
def list_key_check_windows(status: str | None = Query(default=None)) -> dict[str, Any]:
    from app.services.key_check import KeyCheckService
    windows = KeyCheckService().list_windows(status=status)
    return {"items": windows, "total": len(windows)}


@router.get("/key_check/windows/{window_id}")
def get_key_check_window(window_id: str) -> dict[str, Any]:
    from app.services.key_check import KeyCheckService
    service = KeyCheckService()
    window = service.get_window(window_id)
    if window is None:
        raise HTTPException(status_code=404, detail=f"校验窗口 {window_id} 不存在")
    events = [
        event for event in __import__("app.store", fromlist=["store"]).store.rows("_key_check_events")
        if str(event.get("窗口编号", "")) == str(window["窗口编号"])
    ]
    return {"window": window, "events": events}


@router.post("/key_check/windows/{window_id}/events")
def add_key_check_events(window_id: str, payload: CheckEventPayload) -> dict[str, Any]:
    from app.services.key_check import KeyCheckService
    window, message = KeyCheckService().add_events(window_id, payload.events)
    return {"ok": window is not None, "message": message, "window": window}


@router.post("/key_check/windows/{window_id}/run")
def run_key_check(window_id: str, payload: CheckRunPayload | None = None) -> dict[str, Any]:
    from app.services.key_check import KeyCheckService
    service = KeyCheckService()
    threshold = (payload.threshold if payload else {}) or {}
    version = payload.threshold_version if payload else None
    if threshold or version:
        window, message = service.update_threshold(window_id, threshold, version or "")
        if window is None:
            return {"ok": False, "message": message}
    window, message = service.run_window_check(window_id)
    return {"ok": window is not None, "message": message, "window": window}


@router.get("/key_check/audits")
def list_key_check_audits(window_id: str | None = Query(default=None)) -> dict[str, Any]:
    from app.store import store
    audits = list(store.rows("_key_check_audits"))
    if window_id:
        audits = [row for row in audits if str(row.get("窗口编号", "")) == window_id]
    return {"items": audits, "total": len(audits)}


@router.get("/key_check/audits/{batch_id}")
def get_key_check_audit(batch_id: str) -> dict[str, Any]:
    from app.store import store
    audit = next(
        (row for row in store.rows("_key_check_audits") if str(row.get("批次号", "")) == batch_id),
        None,
    )
    if audit is None:
        raise HTTPException(status_code=404, detail=f"校验批次 {batch_id} 不存在")
    return audit


# ---- 维表关联消费校验清单 ----

@router.get("/dim_join/{entry_id}/key-check")
def dim_join_key_check(entry_id: int) -> dict[str, Any]:
    from app.services import service_for
    service = service_for("dim_join")
    summary = service.key_check_summary(entry_id)
    if summary is None:
        raise HTTPException(status_code=404, detail=f"关联配置 {entry_id} 不存在或已归档")
    return summary


@router.post("/dim_join/{entry_id}/build-rows")
def dim_join_build_rows(entry_id: int) -> dict[str, Any]:
    from app.services import service_for
    service = service_for("dim_join")
    result = service.build_join_rows(entry_id)
    if result is None:
        raise HTTPException(status_code=404, detail=f"关联配置 {entry_id} 不存在或已归档")
    return {"ok": True, "message": "已按校验清单分配关联资格", "result": result}


@router.get("/dim_join/{entry_id}/join-rows")
def dim_join_rows(entry_id: int) -> dict[str, Any]:
    from app.services import service_for
    service = service_for("dim_join")
    result = service.list_join_rows(entry_id)
    if result is None:
        raise HTTPException(status_code=404, detail=f"关联配置 {entry_id} 不存在或已归档")
    return result


@router.post("/dim_join/recompute/{task_id}/complete")
def dim_join_complete_recompute(task_id: int) -> dict[str, Any]:
    from app.services import service_for
    service = service_for("dim_join")
    task, message = service.complete_recompute(task_id)
    return {"ok": task is not None, "message": message, "task": task}


@router.get("/{module}", response_model=PageResult[dict])
def list_entries(
    module: str,
    request: Request,
    keyword: str | None = Query(default=None),
    status: str | None = Query(default=None),
    page: int = Query(default=1, ge=1),
    size: int = Query(default=20, ge=1, le=200),
) -> PageResult[dict]:
    service = require_service(module)
    filters = {
        key: value.strip()
        for key, value in request.query_params.items()
        if key in service.LIST_FIELDS and value.strip()
    }
    items, total = service.list_entries(
        filters=filters, keyword=keyword, status=status, page=page, size=size)
    return PageResult(items=items, total=total, page=page, size=size)


@router.get("/{module}/export")
def export_entries(
    module: str,
    request: Request,
    keyword: str | None = Query(default=None),
    status: str | None = Query(default=None),
) -> StreamingResponse:
    service = require_service(module)
    filters = {
        key: value.strip()
        for key, value in request.query_params.items()
        if key in service.LIST_FIELDS and value.strip()
    }
    items, _ = service.list_entries(
        filters=filters, keyword=keyword, status=status, page=1, size=1_000_000)
    output = io.StringIO()
    output.write("\ufeff")
    writer = csv.writer(output, lineterminator="\n")
    writer.writerow(["id", *service.LIST_FIELDS, "status"])
    for item in items:
        values = [item.get("id"), *(item.get(field, "") for field in service.LIST_FIELDS),
                  item.get("status", "")]
        writer.writerow([
            "'" + str(value) if str(value or "").startswith(("=", "+", "-", "@")) else value
            for value in values
        ])
    return StreamingResponse(
        iter([output.getvalue()]), media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{module}.csv"'},
    )


@router.get("/{module}/{entry_id}", response_model=dict)
def get_entry(module: str, entry_id: int) -> dict[str, Any]:
    service = require_service(module)
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"{service.ENTITY} {entry_id} 不存在或已归档")
    return entry


@router.post("/{module}", response_model=ActionResult)
def create_entry(module: str, payload: EntryPayload) -> ActionResult:
    service = require_service(module)
    entry, missing = service.create_entry(payload.values)
    if missing:
        return ActionResult(ok=False, message=f"缺少必填字段：{'、'.join(missing)}")
    return ActionResult(ok=True, message=f"{service.ENTITY}已登记", entry=entry)


@router.post("/{module}/{entry_id}/actions", response_model=ActionResult)
def run_action(module: str, entry_id: int, payload: ActionPayload) -> ActionResult:
    service = require_service(module)
    entry, message = service.run_action(entry_id, payload.action.strip())
    return ActionResult(ok=entry is not None, message=message, entry=entry)


# ---- 跨模块编排流端点 ----

@router.post("/flows/cdc-to-lake")
def trigger_flow_cdc_to_lake(cdc_entry_id: int) -> dict[str, Any]:
    """主线链路: CDC源端 -> Binlog解析 -> 事件转换 -> 维表关联 -> 水位 -> 落盘 -> 湖仓提交 -> 小文件合并"""
    from app.services.orchestrator import flow_cdc_to_lake
    return flow_cdc_to_lake(cdc_entry_id)


@router.post("/flows/lag-recovery")
def trigger_flow_lag_recovery(cdc_entry_id: int = 1) -> dict[str, Any]:
    """主线链路: 乱序处理 -> 延迟监控 -> 断点恢复 -> 回填"""
    from app.services.orchestrator import flow_lag_recovery
    return flow_lag_recovery(cdc_entry_id)


@router.post("/flows/ddl-propagation")
def trigger_flow_ddl_propagation(ddl_entry_id: int) -> dict[str, Any]:
    """主线链路: DDL适配 -> Binlog调整 -> Schema演进"""
    from app.services.orchestrator import flow_ddl_propagation
    return flow_ddl_propagation(ddl_entry_id)


@router.post("/flows/commit-failure-recovery")
def trigger_flow_commit_failure_recovery(commit_entry_id: int) -> dict[str, Any]:
    """主线链路: Iceberg提交冲突 -> 数据比对 -> 告警 -> 回填修复"""
    from app.services.orchestrator import flow_commit_failure_recovery
    return flow_commit_failure_recovery(commit_entry_id)


@router.post("/flows/cache-invalidation")
def trigger_flow_cache_invalidation(dim_entry_id: int) -> dict[str, Any]:
    """主线链路: 维表缓存失效 -> 告警 -> 延迟指标"""
    from app.services.orchestrator import flow_cache_invalidation
    return flow_cache_invalidation(dim_entry_id)


@router.post("/flows/out-of-order-to-checkpoint")
def trigger_flow_out_of_order_to_checkpoint(ooo_entry_id: int) -> dict[str, Any]:
    """主线链路: 乱序超窗口 -> checkpoint保底 + 告警"""
    from app.services.orchestrator import flow_out_of_order_to_checkpoint
    return flow_out_of_order_to_checkpoint(ooo_entry_id)


@router.post("/flows/rate-propagation")
def trigger_flow_rate_propagation() -> dict[str, Any]:
    """主线链路: 解析速率下降 -> 事件过载 -> 延迟上升"""
    from app.services.orchestrator import flow_rate_propagation
    return flow_rate_propagation()


@router.post("/flows/sink-block-to-compaction")
def trigger_flow_sink_block_to_compaction(sink_entry_id: int) -> dict[str, Any]:
    """主线链路: 落盘阻塞 -> 小文件合并清理"""
    from app.services.orchestrator import flow_sink_block_to_compaction
    return flow_sink_block_to_compaction(sink_entry_id)


@router.get("/flows/list")
def list_flows() -> dict[str, Any]:
    """列出所有可编排流链路"""
    from app.services.orchestrator import FLOWS
    return {"flows": list(FLOWS.keys()), "count": len(FLOWS)}