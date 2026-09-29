"""重复上报 / 幂等场景：重试不得重复计数、不得重复告警。"""

from __future__ import annotations

from app.models import RejectReason
from tests.conftest import T0, log_case


def _batch(base_ts=T0 - 1000):
    return [
        {"device_id": "d1", "metric": "temp", "value": 42,
         "timestamp_ms": base_ts, "seq": 1},
        {"device_id": "d1", "metric": "temp", "value": 8,
         "timestamp_ms": base_ts + 10, "seq": 2},
    ]


def test_duplicate_batch_retry_is_idempotent(gw):
    gw.register_device("d1")
    gw.update_rule("temp", ">", 10, effective_from_ms=0, now_ms_=T0)
    batch = _batch()

    first = gw.ingest_batch("d1", batch, now_ms_=T0)
    log_case("首次上报", batch, first)
    assert [r.seq for r in first.accepted] == [1, 2]
    assert len(first.alerts) == 1
    assert first.alerts[0].rule_version == 1

    # 同样的数据因重试被重复提交：不计数、不告警，拒绝原因明确为重复
    retry = gw.ingest_batch("d1", batch, now_ms_=T0)
    log_case("同批重试（应全部幂等）", batch, retry)
    assert retry.accepted == []
    assert retry.alerts == []
    assert {r.reason for r in retry.rejected} == {RejectReason.DUPLICATE_RECORD}
    assert gw.store.seen_count("d1") == 2
    assert len(gw.store.alerts_for("d1")) == 1


def test_retry_with_records_in_different_order(gw):
    gw.register_device("d1")
    gw.update_rule("temp", ">", 10, effective_from_ms=0, now_ms_=T0)
    batch = _batch()

    gw.ingest_batch("d1", batch, now_ms_=T0)
    # 重试批次打乱顺序，结论必须一致
    shuffled = list(reversed(batch))
    retry = gw.ingest_batch("d1", shuffled, now_ms_=T0)
    log_case("乱序重试", shuffled, retry)
    assert retry.accepted == []
    assert {r.reason for r in retry.rejected} == {RejectReason.DUPLICATE_RECORD}
    assert gw.store.seen_count("d1") == 2


def test_dedup_falls_back_to_ts_nonce_without_seq(gw):
    gw.register_device("d1")
    gw.update_rule("temp", ">", 100, effective_from_ms=0, now_ms_=T0)
    rec = {"device_id": "d1", "metric": "temp", "value": 1,
           "timestamp_ms": T0 - 5, "nonce": "abc"}
    first = gw.ingest_batch("d1", [rec], now_ms_=T0)
    retry = gw.ingest_batch("d1", [rec], now_ms_=T0)
    log_case("无 seq 用 ts+nonce 去重-重试", [rec], retry)
    assert len(first.accepted) == 1
    assert {r.reason for r in retry.rejected} == {RejectReason.DUPLICATE_RECORD}
    assert gw.store.seen_count("d1") == 1


def test_distinct_records_same_timestamp_not_duplicates(gw):
    gw.register_device("d1")
    # 同时间戳但 seq 不同：是两条独立记录
    batch = [
        {"device_id": "d1", "metric": "temp", "value": 1,
         "timestamp_ms": T0 - 5, "seq": 10},
        {"device_id": "d1", "metric": "temp", "value": 1,
         "timestamp_ms": T0 - 5, "seq": 11},
    ]
    result = gw.ingest_batch("d1", batch, now_ms_=T0)
    log_case("同 ts 不同 seq", batch, result)
    assert len(result.accepted) == 2
    assert result.rejected == []


def test_retry_of_structurally_rejected_record_is_stable(gw):
    """坏记录的重试不再给出新的拒绝原因：首次原因保留在 detail 中。"""
    gw.register_device("d1")
    bad = [
        {"device_id": "d1", "metric": "temp", "value": "oops",
         "timestamp_ms": T0 - 1000, "seq": 91},                      # 坏值
        {"device_id": "dX", "metric": "temp", "value": 1,
         "timestamp_ms": T0 - 1000, "seq": 92},                      # 标识不符
    ]
    first = gw.ingest_batch("d1", bad, now_ms_=T0)
    log_case("结构性坏记录首次", bad, first)
    assert [r.reason for r in first.rejected] == [
        RejectReason.VALUE_NOT_NUMBER, RejectReason.DEVICE_ID_MISMATCH]

    retry = gw.ingest_batch("d1", bad, now_ms_=T0)
    log_case("同样的坏记录重试（结论稳定且可追溯首次原因）", bad, retry)
    assert {r.reason for r in retry.rejected} == {RejectReason.DUPLICATE_RECORD}
    assert all("首次已被拒绝" in r.detail for r in retry.rejected)
    # 从未接受任何记录、没有任何告警
    assert gw.store.seen_count("d1") == 0
