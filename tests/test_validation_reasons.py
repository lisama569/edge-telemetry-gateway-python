"""坏数据不能静默丢弃，拒绝原因必须彼此可区分。"""
from __future__ import annotations

from app.models import RejectReason
from tests.conftest import log_case


async def test_distinguishable_reject_reasons(svc):
    records = [
        {"metric": "temp", "value": 1.0, "ts": 100},                    # 缺 device_id
        {"device_id": "d1", "metric": "", "value": 1.0, "ts": 100},     # 空 metric
        {"device_id": "d1", "metric": "temp", "value": "x", "ts": 100}, # value 非数值
        {"device_id": "d1", "metric": "temp", "value": 1.0, "ts": 1.5}, # ts 非整数
        {"device_id": "d1", "metric": "temp", "value": 1.0, "ts": 5000},# 未来 ts
        {"device_id": "d2", "metric": "temp", "value": 1.0, "ts": 100}, # 设备不匹配
    ]
    result = await svc.ingest_batch("b-bad", "d1", records)
    reasons = [v.reject_reason for v in result.verdicts]

    log_case("reject-reasons", records, [
        {"reason": v.reject_reason.value, "detail": v.detail} for v in result.verdicts
    ])

    assert reasons == [
        RejectReason.MISSING_FIELD,
        RejectReason.MISSING_FIELD,
        RejectReason.MISSING_FIELD,
        RejectReason.INVALID_TIMESTAMP,
        RejectReason.INVALID_TIMESTAMP,
        RejectReason.DEVICE_MISMATCH,
    ]
    assert result.accepted == 0 and result.rejected == 6
    # detail 必须人类可读、能定位具体字段
    assert "missing or null" in result.verdicts[0].detail
    assert "integer epoch seconds" in result.verdicts[3].detail
    assert "future" in result.verdicts[4].detail
    assert "d2" in result.verdicts[5].detail


async def test_unregistered_device_batch_rejected(svc):
    result = await svc.ingest_batch("b-x", "ghost", [
        {"device_id": "ghost", "metric": "temp", "value": 1.0, "ts": 100},
    ])
    log_case("unregistered-device", {"device_id": "ghost"}, {
        "reason": result.verdicts[0].reject_reason.value,
    })
    assert result.verdicts[0].reject_reason is RejectReason.UNREGISTERED_DEVICE
