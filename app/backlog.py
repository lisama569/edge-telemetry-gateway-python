"""设备离线期间的积压缓存、超限/过期/重复拒绝与上线后顺序回放。

并发模型：BacklogManager 为每个设备维护一把专用锁，同一设备的所有写入、
状态切换与回放都在该锁内串行完成，因此并发写入不会丢失或串序；不同设备
之间互不阻塞。

拒绝原因在积压阶段即可区分（检查顺序固定为 重复 → 超限 → 过期）：
- DUPLICATE_RECORD：与已在积压队列中、或已被正式接受的记录幂等键相同；
- BACKLOG_OVER_LIMIT：加入该记录会使积压长度超过单设备上限（硬性容量保护）；
- TIMESTAMP_EXPIRED：事件时间早于保留窗口起点。

回放时按 (事件时间, 序列号) 升序处理；此时再次做过期与幂等检查，规则判定
依然按每条记录自身的事件时间选版，因此补传结论与实时到达完全一致。
"""

from __future__ import annotations

import threading
from contextlib import contextmanager
from typing import Iterator

from .models import (
    BacklogSubmitResult,
    Rejection,
    ReplayResult,
    RejectReason,
    TelemetryRecord,
)
from .rules import RuleRegistry
from .store import DeviceStore
from .validation import DEFAULT_RETENTION_MS

DEFAULT_BACKLOG_LIMIT = 1000


class DeviceBacklog:
    """单设备积压队列。所有方法均假定调用方已持有该设备的专用锁。"""

    def __init__(self, device_id: str, limit: int = DEFAULT_BACKLOG_LIMIT) -> None:
        self.device_id = device_id
        self.limit = limit
        self._queue: list[TelemetryRecord] = []
        self._queued_keys: set[tuple] = set()

    def size(self) -> int:
        return len(self._queue)

    def queued_keys(self) -> set[tuple]:
        return set(self._queued_keys)

    def submit(
        self,
        records: list[TelemetryRecord],
        *,
        now_ms: int,
        store: DeviceStore,
        retention_ms: int = DEFAULT_RETENTION_MS,
    ) -> BacklogSubmitResult:
        """把已校验记录按提交顺序追加进积压队列；逐条拒绝重复/过期/超限记录。"""
        result = BacklogSubmitResult()
        # 两遍扫描，保证拒绝原因只取决于记录自身与队列现状，不取决于同批兄弟记录
        # 的先后组合：
        # 第一遍：重复、过期都是记录自身属性，先拒绝且绝不入队；
        survivors: list[TelemetryRecord] = []
        for record in records:
            key = record.dedup_key()
            if key in self._queued_keys or store.check_duplicate(record) is not None:
                result.rejected.append(Rejection(
                    reason=RejectReason.DUPLICATE_RECORD,
                    detail=f"记录 {key} 已在积压队列或已被接受，重复提交被忽略",
                    raw=record.raw, record=record))
                continue
            if record.timestamp_ms < now_ms - retention_ms:
                result.rejected.append(Rejection(
                    reason=RejectReason.TIMESTAMP_EXPIRED,
                    detail=(f"事件时间 {record.timestamp_ms} 早于积压保留窗口起点 "
                            f"{now_ms - retention_ms}"),
                    raw=record.raw, record=record))
                continue
            survivors.append(record)

        # 第二遍：容量是硬性保护，只对通过第一遍的记录按顺序生效
        for record in survivors:
            if len(self._queue) >= self.limit:
                result.rejected.append(Rejection(
                    reason=RejectReason.BACKLOG_OVER_LIMIT,
                    detail=(f"设备 {self.device_id} 积压已达上限 {self.limit}，"
                            f"记录被拒绝以防内存无限增长"),
                    raw=record.raw, record=record))
                continue
            self._queue.append(record)
            self._queued_keys.add(record.dedup_key())
            result.accepted.append(record)
        result.backlog_size = len(self._queue)
        return result

    def replay(
        self,
        store: DeviceStore,
        rules: RuleRegistry,
        *,
        now_ms: int,
        retention_ms: int = DEFAULT_RETENTION_MS,
    ) -> ReplayResult:
        """按事件时间升序回放全部积压，逐条幂等判定并产出告警。"""
        result = ReplayResult()
        # 稳定排序：同事件时间按 seq（缺失视为 0），再按提交顺序兜底
        ordered = sorted(
            enumerate(self._queue),
            key=lambda item: (item[1].timestamp_ms, item[1].seq or 0, item[0]),
        )
        for _, record in ordered:
            if record.timestamp_ms < now_ms - retention_ms:
                result.rejected.append(Rejection(
                    reason=RejectReason.TIMESTAMP_EXPIRED,
                    detail=f"记录在回放时已超出保留窗口: {record.timestamp_ms}",
                    raw=record.raw, record=record))
                continue
            alert = rules.evaluate(record)
            # accept 是原子的：返回 False 表示与已接受记录撞键（重复），整体不生效
            if not store.accept(record, alert):
                result.rejected.append(Rejection(
                    reason=RejectReason.DUPLICATE_RECORD,
                    detail="回放时发现记录已被其他路径接受，跳过以保证幂等",
                    raw=record.raw, record=record))
                continue
            result.replayed.append(record)
            if alert is not None:
                result.alerts.append(alert)
        self._queue.clear()
        self._queued_keys.clear()
        return result


class BacklogManager:
    """管理全部设备的在线状态与积压队列，提供按设备加锁的并发安全访问。"""

    def __init__(
        self,
        store: DeviceStore,
        rules: RuleRegistry,
        limit_per_device: int = DEFAULT_BACKLOG_LIMIT,
        retention_ms: int = DEFAULT_RETENTION_MS,
    ) -> None:
        self._store = store
        self._rules = rules
        self._limit = limit_per_device
        self._retention_ms = retention_ms
        self._backlogs: dict[str, DeviceBacklog] = {}
        self._online: dict[str, bool] = {}
        self._device_locks: dict[str, threading.Lock] = {}
        self._registry_lock = threading.Lock()

    def _lock_for(self, device_id: str) -> threading.Lock:
        with self._registry_lock:
            lock = self._device_locks.get(device_id)
            if lock is None:
                lock = threading.Lock()
                self._device_locks[device_id] = lock
            return lock

    @contextmanager
    def device_lock(self, device_id: str) -> Iterator[None]:
        """暴露给编排层：把同一设备的批次处理整体串行化。"""
        lock = self._lock_for(device_id)
        lock.acquire()
        try:
            yield
        finally:
            lock.release()

    def _backlog(self, device_id: str) -> DeviceBacklog:
        backlog = self._backlogs.get(device_id)
        if backlog is None:
            backlog = DeviceBacklog(device_id, self._limit)
            self._backlogs[device_id] = backlog
        return backlog

    def device_online(self, device_id: str) -> None:
        with self._lock_for(device_id):
            self._online[device_id] = True

    def device_offline(self, device_id: str) -> None:
        with self._lock_for(device_id):
            self._online[device_id] = False

    def is_online(self, device_id: str) -> bool:
        return self._online.get(device_id, True)

    def backlog_size(self, device_id: str) -> int:
        with self._lock_for(device_id):
            return self._backlog_size_locked(device_id)

    def _backlog_size_locked(self, device_id: str) -> int:
        backlog = self._backlogs.get(device_id)
        return backlog.size() if backlog is not None else 0

    def _queued_keys_locked(self, device_id: str) -> set[tuple]:
        """调用方必须已持有 device_id 的设备锁。"""
        backlog = self._backlogs.get(device_id)
        return backlog.queued_keys() if backlog is not None else set()

    def _submit_locked(
        self, device_id: str, records: list[TelemetryRecord], *, now_ms: int
    ) -> BacklogSubmitResult:
        """调用方必须已持有 device_id 的设备锁。"""
        return self._backlog(device_id).submit(
            records, now_ms=now_ms, store=self._store,
            retention_ms=self._retention_ms)

    def submit_offline(
        self, device_id: str, records: list[TelemetryRecord], *, now_ms: int
    ) -> BacklogSubmitResult:
        with self._lock_for(device_id):
            return self._submit_locked(device_id, records, now_ms=now_ms)

    def _replay_locked(self, device_id: str, *, now_ms: int) -> ReplayResult:
        """调用方必须已持有 device_id 的设备锁。"""
        self._online[device_id] = True
        if self._backlog_size_locked(device_id) == 0:
            return ReplayResult()
        return self._backlogs[device_id].replay(
            self._store, self._rules, now_ms=now_ms,
            retention_ms=self._retention_ms)

    def reconnect_and_replay(self, device_id: str, *, now_ms: int) -> ReplayResult:
        with self._lock_for(device_id):
            return self._replay_locked(device_id, now_ms=now_ms)
