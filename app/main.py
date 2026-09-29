"""FastAPI 应用与路由。

路由总览：
POST /devices/register          注册设备
POST /devices/connect           标记设备在线/离线
POST /telemetry/batch           在线批量上报（batch_id 幂等）
POST /telemetry/buffer          离线期间缓存上报（区分 duplicate/stale/overflow）
POST /devices/{id}/replay       设备恢复后按 ts 升序补传回放
POST /rules                     运行中更新阈值规则（产生新版本）
GET  /alerts                    查看已触发告警（含规则版本）
"""
from __future__ import annotations

from fastapi import Depends, FastAPI, HTTPException

from . import api_schemas as schemas
from .buffer import OfflineBuffer
from .ingest import TelemetryIngestService
from .models import BatchResult, RecordVerdict
from .rules import RuleEngine


def create_app(
    *,
    buffer_capacity: int = 100,
    buffer_ttl_seconds: int = 3600,
    clock=None,
) -> FastAPI:
    app = FastAPI(title="edge-telemetry-gateway")
    engine = RuleEngine()
    clock_fn = clock if clock is not None else None
    svc = TelemetryIngestService(engine, **({"clock": clock_fn} if clock_fn else {}))
    buffer = OfflineBuffer(capacity=buffer_capacity, ttl_seconds=buffer_ttl_seconds)
    state = {"svc": svc, "rules": engine, "buffer": buffer, "offline": set()}
    app.state.gateway = state

    def get_state():
        return app.state.gateway

    def _verdict_out(v: RecordVerdict) -> schemas.VerdictOut:
        rec = v.record
        return schemas.VerdictOut(
            outcome=v.outcome.value,
            reject_reason=v.reject_reason.value if v.reject_reason else None,
            detail=v.detail,
            device_id=rec.device_id if rec else None,
            metric=rec.metric if rec else None,
            value=rec.value if rec else None,
            ts=rec.ts if rec else None,
            rule_id=v.rule_id,
            rule_version=v.rule_version,
            alert_id=v.alert.alert_id if v.alert else None,
        )

    def _batch_out(result: BatchResult, *, replay: bool = False) -> schemas.BatchOut:
        return schemas.BatchOut(
            batch_id=result.batch_id,
            device_id=result.device_id,
            accepted=result.accepted,
            duplicate=result.duplicate,
            rejected=result.rejected,
            replay=replay,
            verdicts=[_verdict_out(v) for v in result.verdicts],
        )

    @app.get("/")
    def read_root() -> dict:
        return {"service": "edge-telemetry-gateway", "status": "ok"}

    @app.post("/devices/register", status_code=201)
    def register_device(
        payload: schemas.DeviceIn, st=Depends(get_state)
    ) -> dict:
        st["svc"].register_device(payload.device_id)
        return {"device_id": payload.device_id, "registered": True}

    @app.post("/devices/connect")
    def connect_device(payload: schemas.ConnectIn, st=Depends(get_state)) -> dict:
        if not st["svc"].is_registered(payload.device_id):
            raise HTTPException(status_code=404, detail="device not registered")
        offline = st["offline"]
        if payload.connected:
            offline.discard(payload.device_id)
        else:
            offline.add(payload.device_id)
        return {"device_id": payload.device_id, "offline": not payload.connected}

    @app.post("/telemetry/batch", response_model=schemas.BatchOut)
    async def ingest_batch(
        payload: schemas.BatchIn, st=Depends(get_state)
    ) -> schemas.BatchOut:
        raw = [r.model_dump() for r in payload.records]
        result = await st["svc"].ingest_batch(
            payload.batch_id, payload.device_id, raw
        )
        return _batch_out(result)

    @app.post("/telemetry/buffer", response_model=schemas.BatchOut)
    async def buffer_batch(
        payload: schemas.BatchIn, st=Depends(get_state)
    ) -> schemas.BatchOut:
        svc: TelemetryIngestService = st["svc"]
        buf: OfflineBuffer = st["buffer"]
        now = svc._clock()
        result = BatchResult(batch_id=payload.batch_id, device_id=payload.device_id)
        for raw_model in payload.records:
            raw = raw_model.model_dump()
            record, bad = svc._validate(raw, payload.device_id, now)
            if bad is not None:
                result.verdicts.append(bad)
                result.rejected += 1
                continue
            verdict = await buf.enqueue(payload.device_id, record, now)
            result.verdicts.append(verdict)
            if verdict.outcome.value == "accepted":
                result.accepted += 1
            else:
                result.rejected += 1
        return _batch_out(result)

    @app.post("/devices/{device_id}/replay", response_model=schemas.ReplayOut)
    async def replay_device(device_id: str, st=Depends(get_state)) -> schemas.ReplayOut:
        svc: TelemetryIngestService = st["svc"]
        if not svc.is_registered(device_id):
            raise HTTPException(status_code=404, detail="device not registered")
        buf: OfflineBuffer = st["buffer"]
        buffered = buf.size(device_id)
        now = svc._clock()
        verdicts = await buf.replay(device_id, svc.process_record, now)
        st["offline"].discard(device_id)
        return schemas.ReplayOut(
            device_id=device_id,
            buffered=buffered,
            verdicts=[_verdict_out(v) for v in verdicts],
        )

    @app.post("/rules", response_model=schemas.RuleOut, status_code=201)
    def add_rule(payload: schemas.RuleIn, st=Depends(get_state)) -> schemas.RuleOut:
        svc: TelemetryIngestService = st["svc"]
        effective_from = (
            payload.effective_from if payload.effective_from is not None else svc._clock()
        )
        try:
            rule = st["rules"].add_rule(
                rule_id=payload.rule_id,
                metric=payload.metric,
                operator=payload.operator,
                threshold=payload.threshold,
                effective_from=effective_from,
                device_id=payload.device_id,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return schemas.RuleOut(
            rule_id=rule.rule_id,
            metric=rule.metric,
            operator=rule.operator,
            threshold=rule.threshold,
            version=rule.version,
            effective_from=rule.effective_from,
            device_id=rule.device_id,
        )

    @app.get("/alerts")
    def list_alerts(st=Depends(get_state)) -> dict:
        alerts = st["svc"].alerts()
        return {
            "count": len(alerts),
            "alerts": [
                {
                    "alert_id": a.alert_id,
                    "device_id": a.device_id,
                    "metric": a.metric,
                    "value": a.value,
                    "ts": a.ts,
                    "rule_id": a.rule_id,
                    "rule_version": a.rule_version,
                    "operator": a.operator,
                    "threshold": a.threshold,
                }
                for a in alerts
            ],
        }

    return app


app = create_app()
