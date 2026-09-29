"""FastAPI 请求/响应 Pydantic 模型。

字段校验故意放宽：value/ts 等不在入口直接 422，而是进入服务流水线后给出
带区分原因的拒绝结论（missing_field / invalid_timestamp / device_mismatch）。
因此 RecordIn 的字段全部可选，由 TelemetryIngestService._validate 统一判定。
"""
from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field


class RecordIn(BaseModel):
    device_id: str | None = None
    metric: str | None = None
    value: Any = None
    ts: Any = None


class BatchIn(BaseModel):
    batch_id: str = Field(min_length=1)
    device_id: str = Field(min_length=1)
    records: list[RecordIn]


class VerdictOut(BaseModel):
    outcome: str
    reject_reason: str | None = None
    detail: str = ""
    device_id: str | None = None
    metric: str | None = None
    value: float | None = None
    ts: int | None = None
    rule_id: str = ""
    rule_version: int | None = None
    alert_id: str | None = None


class BatchOut(BaseModel):
    batch_id: str
    device_id: str
    accepted: int
    duplicate: int
    rejected: int
    replay: bool = False
    verdicts: list[VerdictOut]


class RuleIn(BaseModel):
    rule_id: str = Field(min_length=1)
    metric: str = Field(min_length=1)
    operator: Literal["gt", "ge", "lt", "le"]
    threshold: float
    effective_from: int | None = None  # 缺省=当前时间（运行中变更即时生效）
    device_id: str = "*"


class RuleOut(BaseModel):
    rule_id: str
    metric: str
    operator: str
    threshold: float
    version: int
    effective_from: int
    device_id: str


class DeviceIn(BaseModel):
    device_id: str = Field(min_length=1)


class ConnectIn(BaseModel):
    device_id: str = Field(min_length=1)
    connected: bool


class ReplayOut(BaseModel):
    device_id: str
    buffered: int
    verdicts: list[VerdictOut]
