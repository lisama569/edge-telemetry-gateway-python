"""入站记录校验：把原始 dict 归一化为 TelemetryRecord，或给出明确拒绝原因。

校验顺序是固定的（先标识、再指标/取值、最后时间戳），保证同一条坏记录
每次都得到同一个拒绝原因，结论可复现、可解释。
"""

from __future__ import annotations

from datetime import datetime, timezone

from .models import Rejection, RejectReason, TelemetryRecord

# 允许的最大未来时间偏差（毫秒），防止时钟漂移/非法未来时间戳
MAX_FUTURE_SKEW_MS = 60_000
# 默认数据保留窗口（毫秒）：早于该窗口的记录视为过期
DEFAULT_RETENTION_MS = 7 * 24 * 3600 * 1000


def _reject(reason: RejectReason, detail: str, raw: dict) -> Rejection:
    return Rejection(reason=reason, detail=detail, raw=raw)


def _coerce_timestamp(value: object) -> tuple[int | None, bool]:
    """把时间戳归一化为 epoch 毫秒。

    返回 (timestamp_ms, ok)：
    - 整数/浮点按 epoch 毫秒处理；
    - 纯数字字符串同样按 epoch 毫秒处理；
    - 其他字符串按 ISO-8601 解析；
    - 解析失败返回 (None, False)；缺省（None）返回 (None, True)。
    """
    if value is None:
        return None, True
    if isinstance(value, bool):
        return None, False
    if isinstance(value, int):
        return value, True
    if isinstance(value, float):
        return int(value), True
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None, False
        if text.lstrip("-").isdigit():
            return int(text), True
        try:
            dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            return None, False
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp() * 1000), True
    return None, False


def parse_and_validate(
    raw: dict,
    *,
    now_ms: int,
    retention_ms: int = DEFAULT_RETENTION_MS,
    max_future_skew_ms: int = MAX_FUTURE_SKEW_MS,
) -> tuple[TelemetryRecord | None, Rejection | None]:
    """校验并解析单条原始记录。成功返回 (record, None)，失败返回 (None, rejection)。

    注意：此函数不做设备归属与幂等检查，那些由 gateway/store 层负责。
    """
    if not isinstance(raw, dict):
        return None, _reject(RejectReason.MISSING_METRIC,
                             f"记录必须是对象，实际为 {type(raw).__name__}", {})

    # 1) 设备标识
    device_id = raw.get("device_id")
    if device_id is None or (isinstance(device_id, str) and not device_id.strip()):
        return None, _reject(RejectReason.MISSING_DEVICE_ID, "记录缺少 device_id", raw)
    device_id = str(device_id).strip()

    # 2) 指标名
    metric = raw.get("metric")
    if metric is None or (isinstance(metric, str) and not metric.strip()):
        return None, _reject(RejectReason.MISSING_METRIC, "记录缺少 metric", raw)
    metric = str(metric).strip()

    # 3) 指标值：即便取值非法也尽量构造 record，使该条记录具备稳定的
    # 幂等/定位键（seq 或 ts+nonce），重试时可以给出幂等响应
    if "value" not in raw or raw.get("value") is None:
        return None, _reject(RejectReason.MISSING_METRIC, "记录缺少 value", raw)
    try:
        value = float(raw["value"])
    except (TypeError, ValueError):
        value = None
    if value is None or value != value or value in (float("inf"), float("-inf")):
        ts_preview, _ = _coerce_timestamp(raw.get("timestamp_ms", raw.get("ts")))
        seq_raw = raw.get("seq")
        try:
            seq_preview = int(seq_raw) if seq_raw is not None else None
        except (TypeError, ValueError):
            seq_preview = None
        nonce_preview = (str(raw["nonce"]) if raw.get("nonce") is not None else None)
        partial = TelemetryRecord(
            device_id=device_id, metric=metric, value=float("nan"),
            timestamp_ms=ts_preview if isinstance(ts_preview, int) else -1,
            seq=seq_preview, nonce=nonce_preview, raw=raw)
        return None, Rejection(
            reason=RejectReason.VALUE_NOT_NUMBER,
            detail=f"value 必须是有限数值: {raw['value']!r}",
            raw=raw, record=partial)

    # 4) 时间戳
    ts_raw = raw.get("timestamp_ms", raw.get("ts"))
    if ts_raw is None:
        return None, _reject(RejectReason.TIMESTAMP_MISSING, "记录缺少 timestamp_ms/ts", raw)
    ts_ms, ok = _coerce_timestamp(ts_raw)
    if not ok:
        return None, _reject(
            RejectReason.TIMESTAMP_INVALID, f"无法解析的时间戳: {ts_raw!r}", raw)

    if ts_ms > now_ms + max_future_skew_ms:
        return None, _reject(
            RejectReason.TIMESTAMP_IN_FUTURE,
            f"时间戳 {ts_ms} 超过当前 {now_ms} 允许偏差 {max_future_skew_ms}ms", raw)
    if ts_ms < now_ms - retention_ms:
        return None, _reject(
            RejectReason.TIMESTAMP_EXPIRED,
            f"时间戳 {ts_ms} 早于保留窗口起点 {now_ms - retention_ms}", raw)

    seq = raw.get("seq")
    if seq is not None:
        try:
            seq = int(seq)
        except (TypeError, ValueError):
            seq = None
    nonce = raw.get("nonce")
    if nonce is not None:
        nonce = str(nonce)

    record = TelemetryRecord(
        device_id=device_id, metric=metric, value=value, timestamp_ms=ts_ms,
        seq=seq, nonce=nonce, raw=raw)
    return record, None
