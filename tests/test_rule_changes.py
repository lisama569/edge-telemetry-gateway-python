"""规则运行中更新：旧结论不可改写，迟到/补传数据按其事件时间选版。"""

from __future__ import annotations

from tests.conftest import HOUR, T0, log_case


def test_old_alerts_keep_old_rule_version(gw):
    gw.register_device("d1")
    # v1: temp > 10 自 T0-10h 生效
    gw.update_rule("temp", ">", 10, effective_from_ms=T0 - 10 * HOUR, now_ms_=T0)

    old = [{"device_id": "d1", "metric": "temp", "value": 8,
            "timestamp_ms": T0 - 5 * HOUR, "seq": 1}]
    r1 = gw.ingest_batch("d1", old, now_ms_=T0 - 4 * HOUR)
    log_case("v1 时代：8 不大于 10，无告警", old, r1)
    assert r1.alerts == []

    # 运行中规则收紧：v2 temp > 5 自 T0 生效
    v2 = gw.update_rule("temp", ">", 5, effective_from_ms=T0, now_ms_=T0).version
    assert v2 == 2

    # 规则变更后，一条事件时间更早的迟到数据到达：仍按 v1 判定，不能翻案
    late = [{"device_id": "d1", "metric": "temp", "value": 8,
             "timestamp_ms": T0 - 3 * HOUR, "seq": 2}]
    r2 = gw.ingest_batch("d1", late, now_ms_=T0 + 1 * HOUR)
    log_case("v2 发布后迟到的旧数据（ts 在 v1 区间）", late, r2)
    assert r2.alerts == []
    assert len(r2.accepted) == 1

    # 新事件时间数据按 v2 判定并明确标注版本
    fresh = [{"device_id": "d1", "metric": "temp", "value": 8,
              "timestamp_ms": T0 + 2 * HOUR, "seq": 3}]
    r3 = gw.ingest_batch("d1", fresh, now_ms_=T0 + 2 * HOUR)
    log_case("v2 时代：8 > 5，告警并标注 v2", fresh, r3)
    assert len(r3.alerts) == 1
    assert r3.alerts[0].rule_version == 2
    assert r3.alerts[0].threshold == 5

    # 历史告警存档中依然只能看到 v2 这一条；v1 时期结论未被回填/改写
    alerts = gw.store.alerts_for("d1")
    assert [(a.record.seq, a.rule_version) for a in alerts] == [(3, 2)]


def test_rule_version_pinned_on_each_alert(gw):
    gw.register_device("d1")
    gw.update_rule("temp", ">", 100, effective_from_ms=T0 - 10 * HOUR, now_ms_=T0)
    # 阈值逐版放宽，数据事件时间跨越两个版本边界
    gw.update_rule("temp", ">", 1, effective_from_ms=T0, now_ms_=T0)

    batch = [
        {"device_id": "d1", "metric": "temp", "value": 50,
         "timestamp_ms": T0 - 1 * HOUR, "seq": 1},   # 落在 v1：不告警
        {"device_id": "d1", "metric": "temp", "value": 50,
         "timestamp_ms": T0 + 1 * HOUR, "seq": 2},   # 落在 v2：告警 v2
    ]
    result = gw.ingest_batch("d1", batch, now_ms_=T0 + 2 * HOUR)
    log_case("同一批次跨规则版本边界", batch, result)
    assert [a.record.seq for a in result.alerts] == [2]
    assert result.alerts[0].rule_version == 2


def test_republished_same_version_is_rejected(gw):
    import pytest
    from app.models import AlertRule
    gw.update_rule("temp", ">", 10, effective_from_ms=0, now_ms_=T0)
    with pytest.raises(ValueError):
        gw.rules.add_version(AlertRule(1, "temp", ">", 999, 0))
