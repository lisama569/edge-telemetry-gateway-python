"""版本化阈值规则引擎。

规则只追加、不改写：每次变更产生一个新版本并带生效时间（effective_from，
含）。判定时以“读数产生时间 ts”选取当时生效的规则版本，因此：

* 规则变更前已产生（哪怕滞后到达）的数据，永远按旧版规则判定；
* 规则变更后产生的数据按新版规则判定；
* 任何时候重新处理同一条读数，选出的规则版本与结论都一致、可复现；
* 告警上固化 rule_id / rule_version，可直接看出依据的是哪一版规则。
"""
from __future__ import annotations

from dataclasses import dataclass

from .models import Alert, TelemetryRecord

_VALID_OPERATORS = {"gt", "ge", "lt", "le"}


@dataclass(frozen=True)
class ThresholdRule:
    rule_id: str
    metric: str
    operator: str  # gt / ge / lt / le
    threshold: float
    version: int
    effective_from: int  # epoch 秒（含）
    device_id: str = "*"  # "*" 表示全局规则


def _compare(value: float, operator: str, threshold: float) -> bool:
    if operator == "gt":
        return value > threshold
    if operator == "ge":
        return value >= threshold
    if operator == "lt":
        return value < threshold
    if operator == "le":
        return value <= threshold
    raise ValueError(f"unknown operator: {operator}")


class RuleEngine:
    def __init__(self) -> None:
        # key: (device_id_or_*, metric) -> rule_id -> list[ThresholdRule]
        self._rules: dict[tuple[str, str], dict[str, list[ThresholdRule]]] = {}

    # ---- 规则变更（运行中可调用） ----------------------------------------
    def add_rule(
        self,
        rule_id: str,
        metric: str,
        operator: str,
        threshold: float,
        effective_from: int,
        device_id: str = "*",
    ) -> ThresholdRule:
        """新增/更新一条规则，返回新生成的不可变版本。

        同一 rule_id 的新版本号 = 旧最大版本 + 1。允许新版本的 effective_from
        早于旧版本（用于录入历史规则）；判定时始终按 ts 选择，互不覆盖。
        """
        if operator not in _VALID_OPERATORS:
            raise ValueError(f"operator must be one of {sorted(_VALID_OPERATORS)}")
        key = (device_id, metric)
        versions = self._rules.setdefault(key, {}).setdefault(rule_id, [])
        next_version = max((r.version for r in versions), default=0) + 1
        rule = ThresholdRule(
            rule_id=rule_id,
            metric=metric,
            operator=operator,
            threshold=float(threshold),
            version=next_version,
            effective_from=int(effective_from),
            device_id=device_id,
        )
        versions.append(rule)
        return rule

    # ---- 查询与判定 -------------------------------------------------------
    def effective_rule(self, device_id: str, metric: str, ts: int) -> ThresholdRule | None:
        """返回读数 ts 时刻对该设备/指标生效的规则版本。

        选择顺序：设备专用规则优先，其次全局规则；同桶内多条规则都生效时，
        取 effective_from 最大（最近开始生效）者。
        """
        for scope in (device_id, "*"):
            bucket = self._rules.get((scope, metric))
            if not bucket:
                continue
            chosen: ThresholdRule | None = None
            for versions in bucket.values():
                for rule in versions:
                    if ts < rule.effective_from:
                        continue
                    if chosen is None or (
                        rule.effective_from, rule.version
                    ) > (chosen.effective_from, chosen.version):
                        chosen = rule
            if chosen is not None:
                return chosen
        return None

    def evaluate(
        self, device_id: str, record: TelemetryRecord, *, alert_id_fn=None
    ) -> tuple[ThresholdRule | None, Alert | None]:
        """按记录的产生时间选规则版本并判定，返回 (规则, 告警或 None)。"""
        rule = self.effective_rule(device_id, record.metric, record.ts)
        alert: Alert | None = None
        if rule is not None and _compare(record.value, rule.operator, rule.threshold):
            if alert_id_fn is None:
                raise ValueError("alert_id_fn is required when an alert fires")
            alert = Alert(
                alert_id=alert_id_fn(record, rule),
                device_id=record.device_id,
                metric=record.metric,
                value=record.value,
                ts=record.ts,
                rule_id=rule.rule_id,
                rule_version=rule.version,
                operator=rule.operator,
                threshold=rule.threshold,
            )
        return rule, alert
