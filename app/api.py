"""FastAPI 接入层：HTTP 批次上报、设备连接状态、规则更新与告警查询。

所有结论性响应都携带：
- accepted/rejected 明细，rejected 中包含可区分的 reason 与人类可读 detail；
- 告警携带 rule_version，说明结论依据哪一版规则得出。
"""

from __future__ import annotations

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from .gateway import TelemetryGateway
from .models import Rejection

app = FastAPI(title="edge-telemetry-gateway", version="0.1.0")
gateway = TelemetryGateway()


class BatchIn(BaseModel):
    # 单条记录保持宽松类型（dict），具体校验与拒绝原因由领域层给出
    records: list[dict] = Field(..., description="遥测记录数组，单条字段缺失不会导致整批失败")


class RuleIn(BaseModel):
    metric: str
    op: str = Field(..., description="比较算子: >, <, >=, <=")
    threshold: float
    effective_from_ms: int | None = None


def _rejection_out(r: Rejection) -> dict:
    return {
        "reason": r.reason.value,
        "detail": r.detail,
        "raw": r.raw,
        "device_id": r.record.device_id if r.record is not None else None,
        "timestamp_ms": r.record.timestamp_ms if r.record is not None else None,
    }


def _result_out(result) -> dict:
    return {
        "accepted_count": len(result.accepted),
        "rejected_count": len(result.rejected),
        "alert_count": len(result.alerts),
        "accepted": [
            {"device_id": r.device_id, "metric": r.metric, "value": r.value,
             "timestamp_ms": r.timestamp_ms, "seq": r.seq}
            for r in result.accepted
        ],
        "rejected": [_rejection_out(r) for r in result.rejected],
        "alerts": [
            {"device_id": a.record.device_id, "metric": a.metric,
             "value": a.value, "op": a.op, "threshold": a.threshold,
             "timestamp_ms": a.record.timestamp_ms, "rule_version": a.rule_version}
            for a in result.alerts
        ],
    }


@app.get("/")
def read_root() -> dict:
    return {"service": "edge-telemetry-gateway", "status": "ok"}


@app.post("/devices/{device_id}/register")
def register(device_id: str) -> dict:
    gateway.register_device(device_id)
    return {"device_id": device_id, "registered": True, "online": True}


@app.post("/devices/{device_id}/telemetry")
def ingest(device_id: str, batch: BatchIn) -> dict:
    """上报一批遥测数据。单条坏记录只影响自身，结果逐条可区分。"""
    result = gateway.ingest_batch(device_id, batch.records)
    # 批次结构错误（空批次）才视为 4xx；逐条拒绝仍返回 200 并在体内说明
    if result.rejected and not result.accepted and all(
        r.reason.value == "empty_batch" for r in result.rejected
    ):
        raise HTTPException(status_code=400, detail=_result_out(result))
    return _result_out(result)


@app.post("/devices/{device_id}/offline")
def go_offline(device_id: str) -> dict:
    if not gateway.store.is_registered(device_id):
        raise HTTPException(status_code=404, detail=f"设备 {device_id} 未注册")
    gateway.mark_offline(device_id)
    return {"device_id": device_id, "online": False}


@app.post("/devices/{device_id}/reconnect")
def reconnect(device_id: str) -> dict:
    if not gateway.store.is_registered(device_id):
        raise HTTPException(status_code=404, detail=f"设备 {device_id} 未注册")
    result = gateway.mark_online_and_replay(device_id)
    return _result_out(result) | {"device_id": device_id, "online": True}


@app.get("/devices/{device_id}/backlog")
def backlog_status(device_id: str) -> dict:
    if not gateway.store.is_registered(device_id):
        raise HTTPException(status_code=404, detail=f"设备 {device_id} 未注册")
    return {"device_id": device_id, "online": gateway.backlogs.is_online(device_id),
            "backlog_size": gateway.backlogs.backlog_size(device_id)}


@app.put("/rules")
def put_rule(rule: RuleIn) -> dict:
    try:
        saved = gateway.update_rule(
            rule.metric, rule.op, rule.threshold,
            effective_from_ms=rule.effective_from_ms)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"metric": saved.metric, "op": saved.op, "threshold": saved.threshold,
            "rule_version": saved.version,
            "effective_from_ms": saved.effective_from_ms}


@app.get("/rules/{metric}")
def get_rule_history(metric: str) -> dict:
    history = gateway.rules.history(metric)
    return {"metric": metric, "versions": [
        {"rule_version": r.version, "op": r.op, "threshold": r.threshold,
         "effective_from_ms": r.effective_from_ms}
        for r in history
    ]}


@app.get("/alerts")
def get_alerts(device_id: str | None = None) -> dict:
    alerts = (gateway.store.alerts_for(device_id) if device_id
              else gateway.store.all_alerts())
    return {"alerts": [
        {"device_id": a.record.device_id, "metric": a.metric, "value": a.value,
         "op": a.op, "threshold": a.threshold,
         "timestamp_ms": a.record.timestamp_ms, "rule_version": a.rule_version}
        for a in alerts
    ]}
