"""设备断连缓存与恢复后补传回放。

语义：
* 每台设备一把锁，多个协程并发写入同一设备时串行化，不丢失、不串序；
* 去重键 (device_id, metric, ts)：同一读数重复写入判为 duplicate；
* now - ts > ttl 的读数判为 stale（过期补传），不收；
* 积压条数达到 capacity 时再写入判为 overflow（超限），明确拒绝；
  三类拒绝原因彼此可区分，判定顺序 duplicate -> stale -> overflow；
* replay 时按 ts 升序逐条处理；回放时刻已过期的记录仍给出 stale 结论，
  不静默丢弃。
"""
from __future__ import annotations

import asyncio
from collections import OrderedDict
from typing import Awaitable, Callable

from .models import RecordOutcome, RecordVerdict, RejectReason, TelemetryRecord

ProcessFn = Callable[[TelemetryRecord], Awaitable[RecordVerdict]]


def _dedup_key(record: TelemetryRecord) -> tuple[str, str, int]:
    return (record.device_id, record.metric, record.ts)


class OfflineBuffer:
    def __init__(self, capacity: int = 100, ttl_seconds: int = 3600) -> None:
        if capacity <= 0:
            raise ValueError("capacity must be positive")
        if ttl_seconds <= 0:
            raise ValueError("ttl_seconds must be positive")
        self.capacity = capacity
        self.ttl_seconds = ttl_seconds
        # device_id -> {dedup_key: TelemetryRecord}
        self._queues: dict[str, OrderedDict[tuple[str, str, int], TelemetryRecord]] = {}
        self._locks: dict[str, asyncio.Lock] = {}

    def _lock_for(self, device_id: str) -> asyncio.Lock:
        lock = self._locks.get(device_id)
        if lock is None:
            lock = asyncio.Lock()
            self._locks[device_id] = lock
        return lock

    def _is_stale(self, ts: int, now: int) -> bool:
        return now - ts > self.ttl_seconds

    async def enqueue(
        self, device_id: str, record: TelemetryRecord, now: int
    ) -> RecordVerdict:
        if record.device_id != device_id:
            return RecordVerdict(
                outcome=RecordOutcome.REJECTED,
                record=record,
                reject_reason=RejectReason.DEVICE_MISMATCH,
                detail=f"record.device_id={record.device_id!r} != batch device {device_id!r}",
            )
        async with self._lock_for(device_id):
            queue = self._queues.setdefault(device_id, OrderedDict())
            key = _dedup_key(record)
            if key in queue:
                return RecordVerdict(
                    outcome=RecordOutcome.REJECTED,
                    record=record,
                    reject_reason=RejectReason.DUPLICATE,
                    detail=f"record {key} already buffered for device {device_id!r}",
                )
            if self._is_stale(record.ts, now):
                return RecordVerdict(
                    outcome=RecordOutcome.REJECTED,
                    record=record,
                    reject_reason=RejectReason.STALE,
                    detail=(
                        f"record ts={record.ts} is older than ttl={self.ttl_seconds}s "
                        f"(now={now})"
                    ),
                )
            if len(queue) >= self.capacity:
                return RecordVerdict(
                    outcome=RecordOutcome.REJECTED,
                    record=record,
                    reject_reason=RejectReason.OVERFLOW,
                    detail=(
                        f"buffer for {device_id!r} is full "
                        f"({len(queue)}/{self.capacity})"
                    ),
                )
            queue[key] = record
            return RecordVerdict(
                outcome=RecordOutcome.ACCEPTED,
                record=record,
                detail=f"buffered for {device_id!r} (size={len(queue)})",
            )

    async def replay(
        self, device_id: str, process_fn: ProcessFn, now: int
    ) -> list[RecordVerdict]:
        """设备恢复后调用：按 ts 升序回放缓存，返回每条记录的判定。"""
        async with self._lock_for(device_id):
            queue = self._queues.pop(device_id, OrderedDict())
            records = sorted(queue.values(), key=lambda r: (r.ts, r.metric))
        verdicts: list[RecordVerdict] = []
        for record in records:
            if self._is_stale(record.ts, now):
                verdicts.append(
                    RecordVerdict(
                        outcome=RecordOutcome.REJECTED,
                        record=record,
                        reject_reason=RejectReason.STALE,
                        detail=f"record ts={record.ts} expired before replay (now={now})",
                    )
                )
                continue
            verdicts.append(await process_fn(record))
        return verdicts

    def size(self, device_id: str) -> int:
        return len(self._queues.get(device_id, {}))
