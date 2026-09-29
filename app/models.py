"""领域模型：遥测记录、判定结果、告警事件。

时间戳一律使用“自 Unix 纪元起的整数秒”，由调用方保证时钟来源；
核心逻辑通过注入 now 函数取当前时间，便于测试中确定性复现。
"""
from __future__ import annotations

import enum
from dataclasses import dataclass, field


class RejectReason(str, enum.Enum):
    """记录被拒绝的可区分原因。"""

    MISSING_FIELD = "missing_field"          # 必填字段缺失
    INVALID_TIMESTAMP = "invalid_timestamp"  # 时间戳非法/非整数秒/为未来时间
    DEVICE_MISMATCH = "device_mismatch"      # 记录内设备标识与批次设备不一致
    UNREGISTERED_DEVICE = "unregistered_device"
    DUPLICATE = "duplicate"                  # 幂等命中：重复提交
    STALE = "stale"                          # 超过离线补传 TTL，已过期
    OVERFLOW = "overflow"                    # 设备积压超过容量上限


class RecordOutcome(str, enum.Enum):
    ACCEPTED = "accepted"   # 首次接收并完成判定
    DUPLICATE = "duplicate" # 幂等重复，沿用首次结论
    REJECTED = "rejected"   # 拒绝，附带 RejectReason


@dataclass(frozen=True)
class TelemetryRecord:
    """单条遥测读数。"""

    device_id: str
    metric: str
    value: float
    ts: int  # 读数产生时间（epoch 秒）


@dataclass(frozen=True)
class Alert:
    """一次阈值告警结论，固化判定时所用规则版本。"""

    alert_id: str
    device_id: str
    metric: str
    value: float
    ts: int
    rule_id: str
    rule_version: int
    operator: str
    threshold: float


@dataclass(frozen=True)
class RecordVerdict:
    """单条记录的处理结论。"""

    outcome: RecordOutcome
    record: TelemetryRecord | None = None
    reject_reason: RejectReason | None = None
    detail: str = ""
    alert: Alert | None = None
    rule_id: str = ""
    rule_version: int | None = None


@dataclass
class BatchResult:
    """一个上报批次的处理结果。"""

    batch_id: str
    device_id: str
    accepted: int = 0
    duplicate: int = 0
    rejected: int = 0
    verdicts: list[RecordVerdict] = field(default_factory=list)
