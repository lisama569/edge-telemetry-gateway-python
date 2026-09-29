"""遥测接入：字段校验、幂等去重、规则判定流水线。

幂等分两层，重试时都不会重复计数/重复告警：
1. 批次层：batch_id。同一 batch_id 的重试直接返回首次的 BatchResult，
   不重新执行任何处理；
2. 记录层：去重键 (device_id, metric, ts)。已处理过的读数再次到达
   （包括跨批次重报、补传回放）一律判 duplicate 并原样返回首次结论，
   告警 id 也保持不变。

校验失败不静默丢弃，按原因区分：
* missing_field     —— 缺字段、空 metric、value 非数值；
* invalid_timestamp —— ts 缺失/非整数秒/为未来时间；
* device_mismatch   —— 记录 device_id 与批次设备对不上；
* unregistered_device —— 设备未注册（批次级，整批拒绝）。

判定语义：以读数产生时间 ts 选取当时生效的规则版本（见 app.rules），
因此乱序/滞后到达与规则变更前后都有确定、可复现、带版本号的结论。
"""
from __future__ import annotations

import asyncio
import time
from typing import Callable

from .models import (
    Alert,
    BatchResult,
    RecordOutcome,
    RecordVerdict,
    RejectReason,
    TelemetryRecord,
)
from .rules import RuleEngine, ThresholdRule

_REQUIRED_FIELDS = ("device_id", "metric", "value", "ts")


def default_clock() -> int:
    return int(time.time())


class TelemetryIngestService:
    def __init__(
        self,
        rules: RuleEngine,
        *,
        clock: Callable[[], int] = default_clock,
        future_skew_seconds: int = 0,
    ) -> None:
        self.rules = rules
        self._clock = clock
        self._future_skew = future_skew_seconds
        self._devices: set[str] = set()
        # batch_id -> BatchResult（首次结论）
        self._batch_results: dict[str, BatchResult] = {}
        # (device_id, metric, ts) -> RecordVerdict（首次结论）
        self._seen: dict[tuple[str, str, int], RecordVerdict] = {}
        self._batch_locks: dict[str, asyncio.Lock] = {}
        self._device_locks: dict[str, asyncio.Lock] = {}

    def register_device(self, device_id: str) -> None:
        self._devices.add(device_id)

    def is_registered(self, device_id: str) -> bool:
        return device_id in self._devices

    # ---- 工具 -------------------------------------------------------------
    def _batch_lock(self, batch_id: str) -> asyncio.Lock:
        lock = self._batch_locks.get(batch_id)
        if lock is None:
            lock = asyncio.Lock()
            self._batch_locks[batch_id] = lock
        return lock

    def _device_lock(self, device_id: str) -> asyncio.Lock:
        lock = self._device_locks.get(device_id)
        if lock is None:
            lock = asyncio.Lock()
            self._device_locks[device_id] = lock
        return lock

    def _alert_id(self, record: TelemetryRecord, rule: ThresholdRule) -> str:
        # 由读数身份 + 规则版本确定：同一读数重复处理得到同一告警 id，
        # 换一版规则则得到不同 id，便于追溯。
        return f"alert:{record.device_id}:{record.metric}:{record.ts}:{rule.rule_id}:v{rule.version}"

    @staticmethod
    def _validate(raw: object, batch_device: str, now: int) -> tuple[TelemetryRecord | None, RecordVerdict | None]:
        """把原始 dict 校验成 TelemetryRecord；失败返回可区分的拒绝结论。"""
        if not isinstance(raw, dict):
            return None, RecordVerdict(
                outcome=RecordOutcome.REJECTED,
                reject_reason=RejectReason.MISSING_FIELD,
                detail=f"record must be an object, got {type(raw).__name__}",
            )

        missing = [f for f in _REQUIRED_FIELDS if f not in raw or raw[f] is None]
        if missing:
            return None, RecordVerdict(
                outcome=RecordOutcome.REJECTED,
                reject_reason=RejectReason.MISSING_FIELD,
                detail=f"missing or null fields: {missing}",
            )

        device_id = raw["device_id"]
        metric = raw["metric"]
        value = raw["value"]
        ts = raw["ts"]

        if not isinstance(device_id, str) or not device_id:
            return None, RecordVerdict(
                outcome=RecordOutcome.REJECTED,
                reject_reason=RejectReason.MISSING_FIELD,
                detail="device_id must be a non-empty string",
            )
        if not isinstance(metric, str) or not metric:
            return None, RecordVerdict(
                outcome=RecordOutcome.REJECTED,
                reject_reason=RejectReason.MISSING_FIELD,
                detail="metric must be a non-empty string",
            )
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            return None, RecordVerdict(
                outcome=RecordOutcome.REJECTED,
                reject_reason=RejectReason.MISSING_FIELD,
                detail=f"value must be a number, got {type(value).__name__}",
            )
        # bool 是 int 的子类，ts 必须为“整数秒”且不能为布尔
        if isinstance(ts, bool) or not isinstance(ts, int):
            return None, RecordVerdict(
                outcome=RecordOutcome.REJECTED,
                reject_reason=RejectReason.INVALID_TIMESTAMP,
                detail=f"ts must be integer epoch seconds, got {type(ts).__name__}",
            )
        if ts <= 0:
            return None, RecordVerdict(
                outcome=RecordOutcome.REJECTED,
                reject_reason=RejectReason.INVALID_TIMESTAMP,
                detail=f"ts must be positive, got {ts}",
            )
        if ts > now:
            return None, RecordVerdict(
                outcome=RecordOutcome.REJECTED,
                reject_reason=RejectReason.INVALID_TIMESTAMP,
                detail=f"ts={ts} is in the future (now={now})",
            )
        if device_id != batch_device:
            return None, RecordVerdict(
                outcome=RecordOutcome.REJECTED,
                reject_reason=RejectReason.DEVICE_MISMATCH,
                detail=f"record device_id={device_id!r} != batch device {batch_device!r}",
            )

        return TelemetryRecord(
            device_id=device_id, metric=metric, value=float(value), ts=ts
        ), None

    # ---- 单条处理（在线与补传共用） ---------------------------------------
    async def process_record(self, record: TelemetryRecord) -> RecordVerdict:
        key = (record.device_id, record.metric, record.ts)
        async with self._device_lock(record.device_id):
            first = self._seen.get(key)
            if first is not None:
                return RecordVerdict(
                    outcome=RecordOutcome.DUPLICATE,
                    record=record,
                    reject_reason=RejectReason.DUPLICATE,
                    detail="record already processed; returning first verdict",
                    alert=first.alert,
                    rule_id=first.rule_id,
                    rule_version=first.rule_version,
                )

            rule, alert = self.rules.evaluate(
                record.device_id, record, alert_id_fn=self._alert_id
            )
            verdict = RecordVerdict(
                outcome=RecordOutcome.ACCEPTED,
                record=record,
                alert=alert,
                rule_id=rule.rule_id if rule else "",
                rule_version=rule.version if rule else None,
                detail=(
                    f"evaluated by {rule.rule_id} v{rule.version} "
                    f"({rule.operator} {rule.threshold}); "
                    + ("alert fired" if alert else "no alert")
                    if rule
                    else "no effective rule; recorded without alert"
                ),
            )
            self._seen[key] = verdict
            return verdict

    # ---- 批次接入 ---------------------------------------------------------
    async def ingest_batch(
        self,
        batch_id: str,
        device_id: str,
        raw_records: list[dict],
    ) -> BatchResult:
        if not isinstance(batch_id, str) or not batch_id:
            raise ValueError("batch_id must be a non-empty string")
        lock = self._batch_lock(batch_id)
        async with lock:
            cached = self._batch_results.get(batch_id)
            if cached is not None:
                return cached  # 整批重试：幂等返回首次结论，不重复处理

            result = BatchResult(batch_id=batch_id, device_id=device_id)

            if device_id not in self._devices:
                # 未注册设备：批次内每条都明确给出拒绝原因，不静默丢弃
                for raw in raw_records:
                    result.verdicts.append(
                        RecordVerdict(
                            outcome=RecordOutcome.REJECTED,
                            reject_reason=RejectReason.UNREGISTERED_DEVICE,
                            detail=f"device {device_id!r} is not registered",
                        )
                    )
                    result.rejected += 1
                self._batch_results[batch_id] = result
                return result

            now = self._clock()
            for raw in raw_records:
                record, bad = self._validate(raw, device_id, now)
                if bad is not None:
                    result.verdicts.append(bad)
                    result.rejected += 1
                    continue
                verdict = await self.process_record(record)
                result.verdicts.append(verdict)
                if verdict.outcome is RecordOutcome.ACCEPTED:
                    result.accepted += 1
                elif verdict.outcome is RecordOutcome.DUPLICATE:
                    result.duplicate += 1
                else:
                    result.rejected += 1

            self._batch_results[batch_id] = result
            return result

    # ---- 查询（便于测试/接口核对） ----------------------------------------
    def alerts(self) -> list[Alert]:
        return [v.alert for v in self._seen.values() if v.alert is not None]
