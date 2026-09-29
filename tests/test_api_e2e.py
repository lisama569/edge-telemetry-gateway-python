"""HTTP 端到端：注册 -> 上报/重试 -> 离线缓存 -> 规则变更 -> 回放/滞后到达。"""
from __future__ import annotations

from fastapi.testclient import TestClient

from app.main import create_app
from tests.conftest import log_case


def _client():
    clock = {"now": 500}
    app = create_app(buffer_capacity=3, buffer_ttl_seconds=600,
                     clock=lambda: clock["now"])
    return TestClient(app), clock


def test_end_to_end_gateway_flow():
    client, clock = _client()
    client.post("/devices/register", json={"device_id": "d1"})
    r = client.post("/rules", json={
        "rule_id": "temp_high", "metric": "temp", "operator": "gt",
        "threshold": 30.0, "effective_from": 0,
    })
    assert r.json()["version"] == 1

    batch = {"batch_id": "b1", "device_id": "d1", "records": [
        {"device_id": "d1", "metric": "temp", "value": 31.0, "ts": 100},
    ]}
    resp1 = client.post("/telemetry/batch", json=batch).json()
    resp2 = client.post("/telemetry/batch", json=batch).json()
    log_case("http-idempotent", batch, {"first": resp1["accepted"], "retry": resp2["accepted"]})
    assert resp1 == resp2

    # 离线缓存
    client.post("/devices/connect", json={"device_id": "d1", "connected": False})
    buf = client.post("/telemetry/buffer", json={
        "batch_id": "b-off", "device_id": "d1", "records": [
            {"device_id": "d1", "metric": "temp", "value": 33.0, "ts": 490},
            {"device_id": "d1", "metric": "temp", "value": 28.0, "ts": 450},
        ],
    }).json()
    assert buf["accepted"] == 2

    # 断连期间规则升级
    clock["now"] = 950
    client.post("/rules", json={
        "rule_id": "temp_high", "metric": "temp", "operator": "gt",
        "threshold": 40.0,
    })
    replay = client.post("/devices/d1/replay").json()
    versions = {(v["ts"], v["rule_version"], v["alert_id"] is not None)
                for v in replay["verdicts"]}
    log_case("http-replay-under-v2", {"replay": replay["buffered"]},
             sorted(versions))
    # 两条旧读数都按 v1：33 告警、28 不告警；按 ts 升序
    tss = [v["ts"] for v in replay["verdicts"]]
    assert tss == [450, 490]
    assert versions == {(450, 1, False), (490, 1, True)}

    # 在线路径上跨批次重复不产生新告警
    alerts_before = client.get("/alerts").json()["count"]
    client.post("/telemetry/batch", json={
        "batch_id": "b-dup", "device_id": "d1", "records": [
            {"device_id": "d1", "metric": "temp", "value": 31.0, "ts": 100},
        ],
    })
    alerts_after = client.get("/alerts").json()["count"]
    assert alerts_before == alerts_after == 2


def test_http_buffer_overflow_and_stale():
    client, clock = _client()
    client.post("/devices/register", json={"device_id": "d1"})
    resp = client.post("/telemetry/buffer", json={
        "batch_id": "b", "device_id": "d1", "records": [
            {"device_id": "d1", "metric": "temp", "value": 1.0, "ts": 496},
            {"device_id": "d1", "metric": "temp", "value": 2.0, "ts": 497},
            {"device_id": "d1", "metric": "temp", "value": 2.0, "ts": 497},  # duplicate
            {"device_id": "d1", "metric": "temp", "value": 3.0, "ts": 498},
            {"device_id": "d1", "metric": "temp", "value": 4.0, "ts": 499},  # overflow
        ],
    }).json()
    reasons = [v["reject_reason"] for v in resp["verdicts"]]
    log_case("http-buffer-reasons", {}, reasons)
    assert reasons == [None, None, "duplicate", None, "overflow"]
    assert resp["accepted"] == 3 and resp["rejected"] == 2
