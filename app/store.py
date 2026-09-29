"""设备注册表、幂等去重表与不可变告警/结论存档。

线程安全：所有状态变更都在同一把锁内完成，accept() 把“登记去重键 +
计数 + 告警存档”做成一个原子步骤，避免同一设备并发写入时重复计数。
已产生的告警只追加、从不修改，历史结论保持不可变。
"""

from __future__ import annotations

import threading

from .models import Alert, RejectReason, TelemetryRecord


class DeviceStore:
    def __init__(self) -> None:
        self._devices: set[str] = set()
        # device_id -> 已处理记录的幂等键集合
        self._seen: dict[str, set[tuple]] = {}
        # device_id -> 已拒绝键 -> 首次拒绝原因（用于坏记录重试的幂等响应）
        self._rejected: dict[str, dict[tuple, RejectReason]] = {}
        self._seen_count: dict[str, int] = {}
        # device_id -> 按接受顺序追加的告警
        self._alerts: dict[str, list[Alert]] = {}
        self._lock = threading.RLock()

    def register_device(self, device_id: str) -> None:
        with self._lock:
            self._devices.add(device_id)
            self._seen.setdefault(device_id, set())
            self._rejected.setdefault(device_id, {})
            self._seen_count.setdefault(device_id, 0)
            self._alerts.setdefault(device_id, [])

    def is_registered(self, device_id: str) -> bool:
        with self._lock:
            return device_id in self._devices

    def seen_count(self, device_id: str) -> int:
        """该设备已接受（去重后）的记录数。"""
        with self._lock:
            return self._seen_count.get(device_id, 0)

    def check_duplicate(
        self,
        record: TelemetryRecord,
        extra_keys: set[tuple] | None = None,
    ) -> RejectReason | None:
        """返回 DUPLICATE_RECORD 或 None；不修改状态。

        extra_keys 可传入“尚未正式接受、但已在离线积压队列中”的键集合，
        使在线路径与积压路径在同一把设备锁下对“重复”的判断保持一致。
        """
        with self._lock:
            key = record.dedup_key()
            if key in self._seen.get(record.device_id, set()):
                return RejectReason.DUPLICATE_RECORD
            if extra_keys is not None and key in extra_keys:
                return RejectReason.DUPLICATE_RECORD
            return None

    def check_rejected(self, record: TelemetryRecord) -> RejectReason | None:
        """若该键此前已被拒绝，返回首次拒绝原因；否则 None。"""
        with self._lock:
            return self._rejected.get(record.device_id, {}).get(record.dedup_key())

    def mark_rejected(self, record: TelemetryRecord, reason: RejectReason) -> None:
        """登记一条因结构性原因被拒、但具备稳定幂等键的记录。"""
        with self._lock:
            self._rejected.setdefault(record.device_id, {}).setdefault(
                record.dedup_key(), reason)

    def mark_seen(self, record: TelemetryRecord) -> None:
        """把记录登记为已处理（仅在校验通过后调用）。"""
        with self._lock:
            self._seen.setdefault(record.device_id, set()).add(record.dedup_key())
            self._seen_count[record.device_id] = (
                self._seen_count.get(record.device_id, 0) + 1)

    def append_alert(self, alert: Alert) -> None:
        with self._lock:
            self._alerts.setdefault(alert.record.device_id, []).append(alert)

    def alerts_for(self, device_id: str) -> list[Alert]:
        with self._lock:
            return list(self._alerts.get(device_id, []))

    def all_alerts(self) -> list[Alert]:
        with self._lock:
            out: list[Alert] = []
            for device_id in sorted(self._alerts):
                out.extend(self._alerts[device_id])
            return out

    def accept(self, record: TelemetryRecord, alert: Alert | None = None) -> bool:
        """原子地登记去重键/计数并在命中时存档告警。

        若记录幂等键已存在则不做任何改动并返回 False；接受成功返回 True
        （无论是否同时产生告警，避免把“无告警”与“重复”混淆）。
        """
        with self._lock:
            seen = self._seen.setdefault(record.device_id, set())
            key = record.dedup_key()
            if key in seen:
                return False
            seen.add(key)
            self._seen_count[record.device_id] = (
                self._seen_count.get(record.device_id, 0) + 1)
            if alert is not None:
                self._alerts.setdefault(record.device_id, []).append(alert)
            return True
