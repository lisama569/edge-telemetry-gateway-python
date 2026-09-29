"""阈值规则版本管理与按事件时间选版判定。

关键语义：
- 每个规则版本（AlertRule）一旦发布即不可变；改阈值 = 追加新版本。
- 选版依据是数据自身的事件时间（timestamp_ms），而不是处理时刻：
  取 effective_from_ms <= ts 中版本号最大（即最新生效）的一版。
  因此迟到/补传的数据与当时实时到达的数据得到完全一致的判定，
  事后修改规则也不会改写历史结论——每条告警都固化了 rule_version。
"""

from __future__ import annotations

import threading

from .models import Alert, AlertRule, TelemetryRecord

_VALID_OPS = {">", "<", ">=", "<="}


class RuleRegistry:
    """保存某指标全部不可变历史版本，支持按事件时间选取生效版本。"""

    def __init__(self) -> None:
        # metric -> 按版本号升序排列的不可变版本列表
        self._versions: dict[str, list[AlertRule]] = {}
        self._lock = threading.RLock()

    def add_version(self, rule: AlertRule) -> AlertRule:
        """追加一个规则版本；同版本号重复追加将抛出 ValueError。"""
        if rule.op not in _VALID_OPS:
            raise ValueError(f"不支持的比较算子: {rule.op!r}，允许 {sorted(_VALID_OPS)}")
        with self._lock:
            versions = self._versions.setdefault(rule.metric, [])
            if any(v.version == rule.version for v in versions):
                raise ValueError(
                    f"指标 {rule.metric} 的规则版本 {rule.version} 已存在，版本不可变")
            versions.append(rule)
            versions.sort(key=lambda r: r.version)
        return rule

    def next_version_number(self, metric: str) -> int:
        with self._lock:
            versions = self._versions.get(metric, [])
            return (versions[-1].version + 1) if versions else 1

    def version_at(self, metric: str, timestamp_ms: int) -> AlertRule | None:
        """返回事件时刻对该指标生效的规则版本（effective_from <= ts 中版本号最大的）。"""
        with self._lock:
            candidates = [
                v for v in self._versions.get(metric, [])
                if v.effective_from_ms <= timestamp_ms
            ]
        return candidates[-1] if candidates else None

    def current_version(self, metric: str) -> AlertRule | None:
        with self._lock:
            versions = self._versions.get(metric, [])
            return versions[-1] if versions else None

    def history(self, metric: str) -> list[AlertRule]:
        with self._lock:
            return list(self._versions.get(metric, []))

    def evaluate(self, record: TelemetryRecord) -> Alert | None:
        """按记录事件时间选版判定；命中则返回携带规则版本的告警，否则 None。"""
        rule = self.version_at(record.metric, record.timestamp_ms)
        if rule is None or not rule.evaluate(record.value):
            return None
        return Alert(
            record=record,
            rule_version=rule.version,
            metric=rule.metric,
            value=record.value,
            op=rule.op,
            threshold=rule.threshold,
        )
