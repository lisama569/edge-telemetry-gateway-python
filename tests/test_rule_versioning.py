"""规则变更：按读数产生时间选版本，旧结论不被新规则改写。"""
from __future__ import annotations

from tests.conftest import log_case


async def test_rule_change_pinned_by_measurement_ts(svc, engine, clock):
    # v1: temp > 30 自 ts=0 生效；v2: temp > 40 自 ts=950 生效
    engine.add_rule("temp_high", "temp", "gt", 30.0, effective_from=0)
    clock["now"] = 950
    v2 = engine.add_rule("temp_high", "temp", "gt", 40.0, effective_from=950)
    assert v2.version == 2
    clock["now"] = 1000  # 数据上报时刻在新规则生效之后

    # 旧时期产生的数据即使现在才到达，也必须按 v1 判定
    old_rec = [{"device_id": "d1", "metric": "temp", "value": 35.0, "ts": 100}]
    r_old = await svc.ingest_batch("b-old", "d1", old_rec)
    old_verdict = r_old.verdicts[0]

    # 新时期数据按 v2：35 不再告警
    new_rec = [{"device_id": "d1", "metric": "temp", "value": 35.0, "ts": 960}]
    r_new = await svc.ingest_batch("b-new", "d1", new_rec)
    new_verdict = r_new.verdicts[0]

    log_case("rule-change", {"v1": ">30@0", "v2": ">40@950"}, {
        "old_ts=100,v=35": {"version": old_verdict.rule_version, "alert": bool(old_verdict.alert)},
        "new_ts=960,v=35": {"version": new_verdict.rule_version, "alert": bool(new_verdict.alert)},
    })

    assert old_verdict.rule_version == 1
    assert old_verdict.alert is not None
    assert old_verdict.alert.rule_version == 1
    assert new_verdict.rule_version == 2
    assert new_verdict.alert is None


async def test_old_verdict_not_rewritten_by_later_rule(svc, engine, clock):
    engine.add_rule("temp_high", "temp", "gt", 30.0, effective_from=0)
    r1 = await svc.ingest_batch("b1", "d1", [
        {"device_id": "d1", "metric": "temp", "value": 35.0, "ts": 100},
    ])
    alert_before = r1.verdicts[0].alert
    assert alert_before.rule_version == 1

    # 之后规则变严
    clock["now"] = 500
    engine.add_rule("temp_high", "temp", "gt", 40.0, effective_from=500)

    # 同读数再次上报：重复，沿用旧告警 id 与 v1 结论
    r2 = await svc.ingest_batch("b2", "d1", [
        {"device_id": "d1", "metric": "temp", "value": 35.0, "ts": 100},
    ])
    dup = r2.verdicts[0]
    log_case("no-rewrite", {"ts": 100, "value": 35.0}, {
        "first_alert_id": alert_before.alert_id,
        "duplicate_alert_id": dup.alert.alert_id,
        "duplicate_version": dup.rule_version,
    })
    assert dup.alert.alert_id == alert_before.alert_id
    assert dup.rule_version == 1
    assert len(svc.alerts()) == 1


async def test_evaluation_is_deterministic(svc, engine):
    """规则历史给定后，任意到达顺序下重新判定结果一致。"""
    engine.add_rule("r", "temp", "gt", 30.0, effective_from=0)
    engine.add_rule("r", "temp", "gt", 40.0, effective_from=500)
    cases = [(100, 35.0, 1, True), (600, 35.0, 2, False), (601, 41.0, 2, True)]
    for ts, value, want_version, want_alert in cases:
        out = await svc.ingest_batch(f"b-{ts}-{value}", "d1", [
            {"device_id": "d1", "metric": "temp", "value": value, "ts": ts},
        ])
        v = out.verdicts[0]
        log_case("deterministic", {"ts": ts, "value": value},
                 {"version": v.rule_version, "alert": bool(v.alert)})
        assert v.rule_version == want_version
        assert (v.alert is not None) == want_alert
