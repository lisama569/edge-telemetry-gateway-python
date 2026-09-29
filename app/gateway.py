"""网关编排层：批次接入、校验、幂等、在线直处理/离线积压、规则判定。

一个批次的处理固定为以下顺序，且整个批次在该设备的专用锁内串行完成：
1. 批次非空检查；
2. 逐条校验（缺字段/坏值/坏时间戳 -> 带原因拒绝）；
3. 记录 device_id 与批次路径 device_id 一致性检查；
4. 设备注册检查；
5. 幂等检查（重复提交不再计数、不再触发告警）；
6. 设备在线：立即按事件时间选版判定并原子接受；
   设备离线：转入单设备积压队列（可能产生过期/超限拒绝）。
"""

from __future__ import annotations

import time

from .backlog import BacklogManager, DEFAULT_BACKLOG_LIMIT
from .models import AlertRule, BatchResult, Rejection, RejectReason
from .rules import RuleRegistry
from .store import DeviceStore
from .validation import DEFAULT_RETENTION_MS, parse_and_validate


def now_ms() -> int:
    return int(time.time() * 1000)


class TelemetryGateway:
    def __init__(
        self,
        *,
        backlog_limit: int = DEFAULT_BACKLOG_LIMIT,
        retention_ms: int = DEFAULT_RETENTION_MS,
        backlog_retention_ms: int | None = None,
    ) -> None:
        self.retention_ms = retention_ms
        self.store = DeviceStore()
        self.rules = RuleRegistry()
        self.backlogs = BacklogManager(
            self.store, self.rules,
            limit_per_device=backlog_limit,
            retention_ms=(retention_ms if backlog_retention_ms is None
                          else backlog_retention_ms))

    def register_device(self, device_id: str) -> None:
        self.store.register_device(device_id)
        self.backlogs.device_online(device_id)

    # ----- 规则管理 ---------------------------------------------------

    def update_rule(
        self,
        metric: str,
        op: str,
        threshold: float,
        *,
        effective_from_ms: int | None = None,
        now_ms_: int | None = None,
    ) -> AlertRule:
        """发布一版新规则（追加、不可变），返回该不可变规则版本。

        effective_from_ms 缺省为当前时间，即“从此刻新事件时间的数据按新版判定”；
        而旧事件时间的数据（含迟到/补传）仍按旧版判定。
        """
        if not metric or not metric.strip():
            raise ValueError("metric 不能为空")
        ts = now_ms_ if now_ms_ is not None else now_ms()
        is_first_version = self.rules.next_version_number(metric) == 1
        version = self.rules.next_version_number(metric)
        # 首版规则默认对全部（含历史）事件时间有效；未显式指定时
        # 后续版本默认从发布时刻生效，避免新版本追溯改写历史结论。
        if effective_from_ms is None:
            effective_from_ms = 0 if is_first_version else ts
        return self.rules.add_version(AlertRule(
            version=version,
            metric=metric.strip(),
            op=op,
            threshold=float(threshold),
            effective_from_ms=effective_from_ms,
        ))

    # ----- 批次接入 ---------------------------------------------------

    def ingest_batch(
        self,
        batch_device_id: str,
        raw_records: list[dict],
        *,
        now_ms_: int | None = None,
    ) -> BatchResult:
        """处理一个遥测批次：逐条校验、去重，在线立即判定，离线入积压。"""
        result = BatchResult()
        ts = now_ms_ if now_ms_ is not None else now_ms()

        if not isinstance(raw_records, list):
            result.rejected.append(Rejection(
                reason=RejectReason.EMPTY_BATCH,
                detail=f"records 必须是数组，实际为 {type(raw_records).__name__}",
                raw={}))
            return result
        if len(raw_records) == 0:
            result.rejected.append(Rejection(
                reason=RejectReason.EMPTY_BATCH, detail="批次不允许为空", raw={}))
            return result

        # 整个批次按设备串行化：同一设备并发批次不会丢失、串序或重复计数
        with self.backlogs.device_lock(batch_device_id):
            online = self.backlogs.is_online(batch_device_id)
            offline_records = []

            def reject(reason: RejectReason, detail: str,
                       rec=None, raw: dict | None = None) -> None:
                result.rejected.append(Rejection(
                    reason=reason, detail=detail,
                    raw=raw if raw is not None else (rec.raw if rec is not None else {}),
                    record=rec))
                # 具备稳定幂等键的结构性拒绝需要记忆，使重试结论幂等
                if rec is not None:
                    self.store.mark_rejected(rec, reason)

            for raw in raw_records:
                record, rejection = parse_and_validate(
                    raw if isinstance(raw, dict) else {},
                    now_ms=ts, retention_ms=self.retention_ms)
                if rejection is not None:
                    # 具备稳定键的结构性坏记录：重试时改报幂等命中，
                    # 并保留首次拒绝原因，结论稳定可解释
                    if rejection.record is not None:
                        prior = self.store.check_rejected(rejection.record)
                        if prior is not None:
                            result.rejected.append(Rejection(
                                reason=RejectReason.DUPLICATE_RECORD,
                                detail=(f"记录 {rejection.record.dedup_key()} 首次已被拒绝"
                                        f"（{prior.value}），重试不再改变结论"),
                                raw=rejection.raw, record=rejection.record))
                            continue
                        self.store.mark_rejected(rejection.record, rejection.reason)
                    result.rejected.append(rejection)
                    continue
                assert record is not None

                # 既往结构性拒绝优先：重试同一键不再给出新的拒绝原因
                prior_reason = self.store.check_rejected(record)
                if prior_reason is not None:
                    result.rejected.append(Rejection(
                        reason=RejectReason.DUPLICATE_RECORD,
                        detail=(f"记录 {record.dedup_key()} 首次已被拒绝"
                                f"（{prior_reason.value}），重试不再改变结论"),
                        raw=record.raw, record=record))
                    continue

                if record.device_id != batch_device_id:
                    reject(RejectReason.DEVICE_ID_MISMATCH,
                           (f"记录 device_id={record.device_id!r} 与批次路径 "
                            f"device_id={batch_device_id!r} 不一致"),
                           rec=record)
                    continue
                if not self.store.is_registered(record.device_id):
                    reject(RejectReason.UNKNOWN_DEVICE,
                           f"设备 {record.device_id} 未注册，请先注册", rec=record)
                    continue
                # 离线时把已在积压队列中的键也视为重复；在线时只查正式接受表
                extra_keys = (self.backlogs._queued_keys_locked(batch_device_id)
                              if not online else None)
                if self.store.check_duplicate(record, extra_keys) is not None:
                    result.rejected.append(Rejection(
                        reason=RejectReason.DUPLICATE_RECORD,
                        detail=f"记录 {record.dedup_key()} 此前已处理/已排队，幂等忽略",
                        raw=record.raw, record=record))
                    continue

                if online:
                    alert = self.rules.evaluate(record)
                    self.store.accept(record, alert)  # 已查过重复，必为 True
                    result.accepted.append(record)
                    if alert is not None:
                        result.alerts.append(alert)
                else:
                    offline_records.append(record)

            if offline_records:
                # 已持有该设备锁，直接走不加锁的内部方法避免重入死锁
                backlog_result = self.backlogs._submit_locked(
                    batch_device_id, offline_records, now_ms=ts)
                result.accepted.extend(backlog_result.accepted)
                result.rejected.extend(backlog_result.rejected)

        return result

    def mark_offline(self, device_id: str) -> None:
        self.backlogs.device_offline(device_id)

    def mark_online_and_replay(
        self, device_id: str, *, now_ms_: int | None = None
    ) -> BatchResult:
        """设备恢复：按时间顺序回放积压，返回与正常批次一致结构的结果。"""
        ts = now_ms_ if now_ms_ is not None else now_ms()
        result = BatchResult()
        with self.backlogs.device_lock(device_id):
            replay = self.backlogs._replay_locked(device_id, now_ms=ts)
        result.accepted.extend(replay.replayed)
        result.rejected.extend(replay.rejected)
        result.alerts.extend(replay.alerts)
        return result
