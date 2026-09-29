"""重复上报：批次重试与跨批次重报都必须幂等。"""
from __future__ import annotations

import pytest

from app.models import RejectReason
from tests.conftest import log_case



async def test_batch_retry_is_idempotent(svc, engine):
    engine.add_rule("r_temp", "temp", "gt", 30.0, effective_from=0)
    payload = [
        {"device_id": "d1", "metric": "temp", "value": 31.0, "ts": 100},
        {"device_id": "d1", "metric": "temp", "value": 20.0, "ts": 200},
    ]
    first = await svc.ingest_batch("b1", "d1", payload)
    retry = await svc.ingest_batch("b1", "d1", payload)

    log_case("batch-retry", {"batch_id": "b1", "records": payload}, {
        "first": [(v.outcome.value, v.alert.alert_id if v.alert else None) for v in first.verdicts],
        "retry": [(v.outcome.value, v.alert.alert_id if v.alert else None) for v in retry.verdicts],
    })

    assert retry is first, "同 batch_id 重试必须返回首次结论对象"
    assert (first.accepted, first.duplicate, first.rejected) == (2, 0, 0)
    assert len(svc.alerts()) == 1, "重试不得重复触发告警"



async def test_same_record_across_batches_is_duplicate(svc, engine):
    engine.add_rule("r_temp", "temp", "gt", 30.0, effective_from=0)
    rec = [{"device_id": "d1", "metric": "temp", "value": 31.0, "ts": 100}]

    r1 = await svc.ingest_batch("b1", "d1", rec)
    r2 = await svc.ingest_batch("b2", "d1", rec)  # 不同批次、同一读数

    log_case("cross-batch-duplicate", rec, {
        "r1": r1.verdicts[0].outcome.value,
        "r2": r2.verdicts[0].outcome.value,
        "r2_alert_id": r2.verdicts[0].alert.alert_id if r2.verdicts[0].alert else None,
        "r1_alert_id": r1.verdicts[0].alert.alert_id,
    })

    assert r1.accepted == 1
    assert r2.duplicate == 1 and r2.accepted == 0
    assert r2.verdicts[0].reject_reason is RejectReason.DUPLICATE
    assert r2.verdicts[0].alert.alert_id == r1.verdicts[0].alert.alert_id
    assert len(svc.alerts()) == 1
