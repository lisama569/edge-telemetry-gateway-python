"""HTTP 端到端：注册、上报、离线缓存、重连补传、规则版本可追溯。"""

from __future__ import annotations

import time

import pytest
from fastapi.testclient import TestClient

from app.api import app


@pytest.fixture
def client():
    # 每个用例使用全新网关，避免单例状态互相污染
    import app.api as api
    api.gateway = type(api.gateway)(backlog_limit=10)
    return TestClient(app)


def _now():
    return int(time.time() * 1000)


def test_full_lifecycle_over_http(client):
    now = _now()
    assert client.post("/devices/dev-a/register").status_code == 200
    v1 = client.put("/rules", json={
        "metric": "temp", "op": ">", "threshold": 10,
        "effective_from_ms": 0}).json()["rule_version"]
    assert v1 == 1

    batch = {"records": [
        {"device_id": "dev-a", "metric": "temp", "value": 12,
         "timestamp_ms": now - 1000, "seq": 1},
        {"device_id": "dev-a", "metric": "temp", "value": 3,
         "timestamp_ms": now - 900, "seq": 2},
    ]}
    resp = client.post("/devices/dev-a/telemetry", json=batch).json()
    assert resp["accepted_count"] == 2
    assert resp["alerts"][0]["rule_version"] == 1

    # 重试幂等
    retry = client.post("/devices/dev-a/telemetry", json=batch).json()
    assert retry["accepted_count"] == 0
    assert {r["reason"] for r in retry["rejected"]} == {"duplicate_record"}

    # 离线后数据进积压，重连后按序回放
    client.post("/devices/dev-a/offline")
    offline_batch = {"records": [
        {"device_id": "dev-a", "metric": "temp", "value": 30,
         "timestamp_ms": now + 5000, "seq": 4},
        {"device_id": "dev-a", "metric": "temp", "value": 8,
         "timestamp_ms": now + 4000, "seq": 3},
    ]}
    buffered = client.post("/devices/dev-a/telemetry", json=offline_batch).json()
    assert buffered["alert_count"] == 0
    assert client.get("/devices/dev-a/backlog").json()["backlog_size"] == 2

    rec = client.post("/devices/dev-a/reconnect").json()
    accepted_ts = [a["timestamp_ms"] for a in rec["accepted"]]
    assert accepted_ts == sorted(accepted_ts)  # 补传按事件时间顺序回放
    assert len(rec["alerts"]) == 1
    assert rec["alerts"][0]["rule_version"] == 1
    assert rec["alerts"][0]["value"] == 30


def test_rejection_reasons_visible_in_response(client):
    client.post("/devices/dev-a/register")
    resp = client.post("/devices/dev-a/telemetry", json={"records": [
        {"metric": "temp", "value": 1, "timestamp_ms": _now()},
    ]}).json()
    assert resp["rejected"][0]["reason"] == "missing_device_id"
    assert resp["rejected"][0]["detail"]


def test_empty_batch_is_400(client):
    client.post("/devices/dev-a/register")
    resp = client.post("/devices/dev-a/telemetry", json={"records": []})
    assert resp.status_code == 400


def test_rule_history_endpoint(client):
    client.put("/rules", json={"metric": "temp", "op": ">", "threshold": 10})
    client.put("/rules", json={"metric": "temp", "op": ">", "threshold": 5})
    history = client.get("/rules/temp").json()["versions"]
    assert [v["rule_version"] for v in history] == [1, 2]
