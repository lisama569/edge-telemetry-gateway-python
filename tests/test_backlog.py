"""离线缓存、恢复补传：顺序回放、超限/过期/重复三类拒绝可区分。"""

from __future__ import annotations

from app.models import RejectReason
from tests.conftest import HOUR, T0, log_case


def _r(seq, ts, value=1.0, metric="temp", device="d1"):
    return {"device_id": device, "metric": metric, "value": value,
            "timestamp_ms": ts, "seq": seq}


def test_offline_batches_buffered_and_replayed_in_time_order(gw):
    gw.register_device("d1")
    gw.update_rule("temp", ">", 10, effective_from_ms=0, now_ms_=T0)
    gw.mark_offline("d1")

    # 设备离线期间分多批上报，顺序是乱的
    b1 = [_r(3, T0 + 3000, 11)]
    b2 = [_r(1, T0 + 1000, 5), _r(2, T0 + 2000, 12)]
    r_b1 = gw.ingest_batch("d1", b1, now_ms_=T0 + 3000)
    r_b2 = gw.ingest_batch("d1", b2, now_ms_=T0 + 3000)
    log_case("离线批次1", b1, r_b1)
    log_case("离线批次2", b2, r_b2)
    assert len(r_b1.accepted) == 1 and len(r_b2.accepted) == 2
    # 离线期间不产生告警（尚未判定）
    assert r_b1.alerts == [] and r_b2.alerts == []
    assert gw.store.seen_count("d1") == 0

    # 恢复连接：按事件时间升序回放，seq 顺序应为 1,2,3
    replay = gw.mark_online_and_replay("d1", now_ms_=T0 + 4000)
    log_case("恢复后补传回放（乱序 -> 按时间排序）", b1 + b2, replay)
    assert [r.seq for r in replay.accepted] == [1, 2, 3]
    assert [a.record.seq for a in replay.alerts] == [2, 3]
    assert [a.rule_version for a in replay.alerts] == [1, 1]
    assert gw.store.seen_count("d1") == 3
    assert gw.backlogs.backlog_size("d1") == 0


def test_backlog_over_limit_expired_duplicate_are_distinct(gw):
    gw.register_device("d1")
    gw.mark_offline("d1")
    now = T0 + 10_000

    # 先写满（夹具上限为 3）
    full = [_r(1, now - 300), _r(2, now - 200), _r(3, now - 100)]
    r1 = gw.ingest_batch("d1", full, now_ms_=now)
    log_case("写满积压(上限3)", full, r1)
    assert len(r1.accepted) == 3

    # 过期记录单独先来：不占用积压容量，原因必须是“过期”而不是“超限”
    expired = [_r(5, now - 31 * 24 * HOUR)]
    r_exp = gw.ingest_batch("d1", expired, now_ms_=now)
    log_case("积压窗口外的过期记录", expired, r_exp)
    assert {rej.record.seq: rej.reason for rej in r_exp.rejected} == {
        5: RejectReason.TIMESTAMP_EXPIRED}
    assert gw.backlogs.backlog_size("d1") == 3

    # 队列仍满：新记录按超限拒绝；重复提交按重复拒绝，三者可明确区分
    more = [
        _r(4, now - 50),                                   # 超限
        _r(1, now - 300),                                   # 重复（值不同也一样）
    ]
    r2 = gw.ingest_batch("d1", more, now_ms_=now)
    log_case("超限/重复混合提交（队列满）", more, r2)
    reasons = {rej.record.seq: rej.reason for rej in r2.rejected}
    assert reasons[4] == RejectReason.BACKLOG_OVER_LIMIT
    assert reasons[1] == RejectReason.DUPLICATE_RECORD
    assert gw.backlogs.backlog_size("d1") == 3


def test_replay_is_idempotent_against_concurrent_direct_accept(gw):
    gw.register_device("d1")
    gw.update_rule("temp", ">", 10, effective_from_ms=0, now_ms_=T0)
    gw.mark_offline("d1")
    buffered = [_r(1, T0 + 1000, 11), _r(2, T0 + 2000, 5)]
    gw.ingest_batch("d1", buffered, now_ms_=T0 + 2000)

    # seq=1 在回放前已通过别的路径被接受（模拟并发直处理/重复补传）
    from app.validation import parse_and_validate
    rec, _ = parse_and_validate(buffered[0], now_ms=T0 + 3000,
                                retention_ms=gw.retention_ms)
    gw.store.accept(rec, gw.rules.evaluate(rec))

    replay = gw.mark_online_and_replay("d1", now_ms_=T0 + 3000)
    log_case("回放时 seq1 已被接受", buffered, replay)
    assert [r.seq for r in replay.accepted] == [2]
    assert [rej.reason for rej in replay.rejected] == [RejectReason.DUPLICATE_RECORD]
    assert gw.store.seen_count("d1") == 2
    assert len(gw.store.alerts_for("d1")) == 1  # seq1 的告警只存一次


def test_replay_after_rule_change_uses_event_time_versions(gw):
    """补传期间规则已更新：回放仍按每条数据事件时间选版，结论可复现。"""
    gw.register_device("d1")
    # v1 阈值 10；设备断连期间积压了一条 value=7
    gw.update_rule("temp", ">", 10, effective_from_ms=0, now_ms_=T0)
    gw.mark_offline("d1")
    old_data = [_r(1, T0 + 1000, 7)]
    gw.ingest_batch("d1", old_data, now_ms_=T0 + 1000)

    # 断连期间规则收紧到 v2（阈值 5），但旧数据事件时间早于 v2 生效
    gw.update_rule("temp", ">", 5, effective_from_ms=T0 + 5000, now_ms_=T0 + 5000)

    replay = gw.mark_online_and_replay("d1", now_ms_=T0 + 6000)
    log_case("规则变更后的补传：旧事件按 v1", old_data, replay)
    assert replay.alerts == []  # 7 在 v1 下不告警，结论不被 v2 改写
    assert [r.seq for r in replay.accepted] == [1]
