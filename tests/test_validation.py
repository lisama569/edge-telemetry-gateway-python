"""入站校验场景：坏记录不能静默丢弃，且每条都有可区分的拒绝原因。"""

from __future__ import annotations

from app.models import RejectReason
from tests.conftest import T0, log_case


def test_each_bad_record_gets_distinct_reason(gw):
    gw.register_device("d1")
    batch = [
        {"device_id": "d1", "metric": "temp", "value": 1,
         "timestamp_ms": T0 - 1000, "seq": 1},                        # 正常
        {"metric": "temp", "value": 1, "timestamp_ms": T0},           # 缺设备标识
        {"device_id": "d1", "value": 1, "timestamp_ms": T0},          # 缺指标名
        {"device_id": "d1", "metric": "temp", "timestamp_ms": T0},    # 缺值
        {"device_id": "d1", "metric": "temp", "value": "hot",
         "timestamp_ms": T0},                                         # 值非数值
        {"device_id": "d1", "metric": "temp", "value": 1},            # 缺时间戳
        {"device_id": "d1", "metric": "temp", "value": 1,
         "timestamp_ms": "yesterday-ish"},                            # 时间戳非法
        {"device_id": "d1", "metric": "temp", "value": 1,
         "timestamp_ms": T0 + 999_999},                               # 未来时间戳
        {"device_id": "d1", "metric": "temp", "value": 1,
         "timestamp_ms": T0 - 61 * 24 * 3600 * 1000},                 # 过期
        {"device_id": "dX", "metric": "temp", "value": 1,
         "timestamp_ms": T0 - 1000, "seq": 2},                        # 标识不匹配
    ]
    result = gw.ingest_batch("d1", batch, now_ms_=T0)
    log_case("一条正常 + 九类坏记录混合批次", batch, result)

    assert [r.seq for r in result.accepted] == [1]
    reasons = [r.reason for r in result.rejected]
    expected = [
        RejectReason.MISSING_DEVICE_ID,
        RejectReason.MISSING_METRIC,
        RejectReason.MISSING_METRIC,
        RejectReason.VALUE_NOT_NUMBER,
        RejectReason.TIMESTAMP_MISSING,
        RejectReason.TIMESTAMP_INVALID,
        RejectReason.TIMESTAMP_IN_FUTURE,
        RejectReason.TIMESTAMP_EXPIRED,
        RejectReason.DEVICE_ID_MISMATCH,
    ]
    assert reasons == expected
    # detail 非空，调用方可以据此向设备返回可操作的错误说明
    assert all(r.detail for r in result.rejected)


def test_unknown_device_is_rejected(gw):
    batch = [{"device_id": "ghost", "metric": "temp", "value": 1,
              "timestamp_ms": T0 - 1, "seq": 1}]
    result = gw.ingest_batch("ghost", batch, now_ms_=T0)
    log_case("未注册设备", batch, result)
    assert result.accepted == []
    assert result.rejected[0].reason == RejectReason.UNKNOWN_DEVICE


def test_empty_batch_rejected(gw):
    result = gw.ingest_batch("d1", [], now_ms_=T0)
    log_case("空批次", [], result)
    assert result.rejected[0].reason == RejectReason.EMPTY_BATCH


def test_one_bad_record_does_not_drop_others(gw):
    gw.register_device("d1")
    batch = [
        {"device_id": "d1", "metric": "temp", "value": 5,
         "timestamp_ms": T0 - 30, "seq": 1},
        {"device_id": "d1", "metric": "temp", "value": "bad",
         "timestamp_ms": T0 - 20, "seq": 2},
        {"device_id": "d1", "metric": "temp", "value": 6,
         "timestamp_ms": T0 - 10, "seq": 3},
    ]
    result = gw.ingest_batch("d1", batch, now_ms_=T0)
    log_case("坏记录不影响兄弟记录", batch, result)
    assert [r.seq for r in result.accepted] == [1, 3]
    assert [r.reason for r in result.rejected] == [RejectReason.VALUE_NOT_NUMBER]
