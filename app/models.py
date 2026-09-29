"""核心领域模型：遥测记录、规则版本、告警、拒绝原因等类型定义。"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum


class RejectReason(str, Enum):
    """可区分的拒绝原因。"""

    EMPTY_BATCH = "empty_batch"                 # 批次为空
    MISSING_DEVICE_ID = "missing_device_id"     # 记录缺少设备标识
    DEVICE_ID_MISMATCH = "device_id_mismatch"   # 记录设备标识与批次路径不一致
    UNKNOWN_DEVICE = "unknown_device"           # 设备未注册
    MISSING_METRIC = "missing_metric"           # 缺少指标名/指标值
    VALUE_NOT_NUMBER = "value_not_number"       # 指标值不是数值
    TIMESTAMP_MISSING = "timestamp_missing"     # 缺少时间戳
    TIMESTAMP_INVALID = "timestamp_invalid"     # 时间戳非法（不可解析）
    TIMESTAMP_IN_FUTURE = "timestamp_in_future"  # 时间戳超过允许的未来偏差
    TIMESTAMP_EXPIRED = "timestamp_expired"     # 时间戳早于允许的保留窗口（过期）
    DUPLICATE_RECORD = "duplicate_record"       # 幂等命中：同一 (device, seq/ts+nonce) 已处理
    BACKLOG_OVER_LIMIT = "backlog_over_limit"   # 离线积压超过上限


@dataclass(frozen=True)
class TelemetryRecord:
    """一条遥测记录的归一化形态。"""

    device_id: str
    metric: str
    value: float
    timestamp_ms: int
    seq: int | None = None
    nonce: str | None = None
    raw: dict = field(default_factory=dict)

    def dedup_key(self) -> tuple:
        """幂等键：优先使用设备序列号，否则用 (指标, 时间戳, nonce)。"""
        if self.seq is not None:
            return ("seq", self.device_id, self.seq)
        return ("evt", self.device_id, self.metric, self.timestamp_ms, self.nonce)


@dataclass(frozen=True)
class Rejection:
    """一条被拒绝记录及其可解释原因。"""

    reason: RejectReason
    detail: str
    raw: dict = field(default_factory=dict)
    record: TelemetryRecord | None = None


@dataclass(frozen=True)
class AlertRule:
    """某一版阈值告警规则（版本不可变）。"""

    version: int
    metric: str
    op: str            # ">" | "<" | ">=" | "<="
    threshold: float
    effective_from_ms: int   # 该版本生效起点（含），按数据事件时间选版

    def evaluate(self, value: float) -> bool:
        if self.op == ">":
            return value > self.threshold
        if self.op == "<":
            return value < self.threshold
        if self.op == ">=":
            return value >= self.threshold
        if self.op == "<=":
            return value <= self.threshold
        raise ValueError(f"unsupported comparison operator: {self.op!r}")


@dataclass(frozen=True)
class Alert:
    """一条告警结论，携带产生它的规则版本，保证结论可追溯。"""

    record: TelemetryRecord
    rule_version: int
    metric: str
    value: float
    op: str
    threshold: float


@dataclass
class BatchResult:
    """一个批次的处理结果。"""

    accepted: list[TelemetryRecord] = field(default_factory=list)
    rejected: list[Rejection] = field(default_factory=list)
    alerts: list[Alert] = field(default_factory=list)
    duplicate_of: dict[int, str] = field(default_factory=dict)


@dataclass
class BacklogSubmitResult:
    """离线积压写入结果。"""

    accepted: list[TelemetryRecord] = field(default_factory=list)
    rejected: list[Rejection] = field(default_factory=list)
    backlog_size: int = 0


@dataclass
class ReplayResult:
    """补传回放结果。"""

    replayed: list[TelemetryRecord] = field(default_factory=list)
    rejected: list[Rejection] = field(default_factory=list)
    alerts: list[Alert] = field(default_factory=list)
