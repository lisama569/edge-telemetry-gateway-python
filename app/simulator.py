"""本地设备模拟上报驱动。

用确定性时钟 + FastAPI TestClient 跑一条完整故事线，逐步打印“输入数据”与
“判定依据”：
1. 设备在线正常上报，含一条超阈值告警（规则 v1）；
2. 同一批次因网络重试重复提交 -> 幂等，计数/告警不重复；
3. 设备断连，数据进入离线缓存（含重复/过期/超限样例）；
4. 恢复后补传，按 ts 升序回放；期间规则已升级到 v2；
5. 一条在 v1 时期产生、但滞后到 v2 之后才到达的数据 -> 仍按 v1 判定。
"""
from __future__ import annotations

import json

from fastapi.testclient import TestClient

from .main import create_app

DEVICE = "sensor-A"
METRIC = "temp"
RULE_ID = "temp_high"


def _show(title: str, payload: dict | None, resp: dict) -> None:
    print(f"\n=== {title} ===")
    if payload is not None:
        print("输入: " + json.dumps(payload, ensure_ascii=False, sort_keys=True))
    print("结论: " + json.dumps(resp, ensure_ascii=False, sort_keys=True))


def main() -> None:
    t = {"now": 500}
    app = create_app(
        buffer_capacity=3, buffer_ttl_seconds=600, clock=lambda: t["now"]
    )
    client = TestClient(app)

    client.post("/devices/register", json={"device_id": DEVICE})

    # 规则 v1：temp > 30，自 0 起生效
    r = client.post(
        "/rules",
        json={
            "rule_id": RULE_ID, "metric": METRIC, "operator": "gt",
            "threshold": 30.0, "effective_from": 0,
        },
    ).json()
    _show("定义规则 v1", None, r)

    # 1) 正常批次：31.5 告警，20.0 不告警；另带两条坏数据
    batch = {
        "batch_id": "batch-1",
        "device_id": DEVICE,
        "records": [
            {"device_id": DEVICE, "metric": METRIC, "value": 31.5, "ts": 100},
            {"device_id": DEVICE, "metric": METRIC, "value": 20.0, "ts": 200},
            {"metric": METRIC, "value": 1.0, "ts": 200},                 # 缺 device_id
            {"device_id": DEVICE, "metric": METRIC, "value": 1.0, "ts": "abc"},  # 非法 ts
        ],
    }
    resp1 = client.post("/telemetry/batch", json=batch).json()
    _show("首次上报 batch-1", batch, resp1)

    # 2) 重试：完全相同的 batch_id
    resp1_retry = client.post("/telemetry/batch", json=batch).json()
    _show("重试同一批次 batch-1（应幂等）", batch, resp1_retry)
    assert resp1_retry == resp1, "重试必须返回首次结论"

    # 3) 设备断连，离线数据进缓存（now=500，回放于 950，TTL=600）
    client.post("/devices/connect", json={"device_id": DEVICE, "connected": False})
    offline_batch = {
        "batch_id": "offline-1",
        "device_id": DEVICE,
        "records": [
            {"device_id": DEVICE, "metric": METRIC, "value": 33.0, "ts": 490},
            {"device_id": DEVICE, "metric": METRIC, "value": 29.0, "ts": 450},
            {"device_id": DEVICE, "metric": METRIC, "value": 33.0, "ts": 490},  # 缓存内重复
            {"device_id": DEVICE, "metric": METRIC, "value": 99.0, "ts": 10},   # 过期
            {"device_id": DEVICE, "metric": "hum", "value": 80.0, "ts": 470},
            {"device_id": DEVICE, "metric": "co2", "value": 5.0, "ts": 480},    # 容量超限
        ],
    }
    resp_buf = client.post("/telemetry/buffer", json=offline_batch).json()
    _show("断连期间缓存 offline-1（含重复/过期/超限）", offline_batch, resp_buf)

    # 4) 断连期间规则升级：v2 阈值 40，自 950 起生效
    t["now"] = 950
    r2 = client.post(
        "/rules",
        json={
            "rule_id": RULE_ID, "metric": METRIC, "operator": "gt",
            "threshold": 40.0,  # 不传 effective_from -> 当前时间 950 生效
        },
    ).json()
    _show("规则升级 v2（950 起 temp>40）", None, r2)

    # 5) 设备恢复，补传回放（按 ts 升序；旧数据仍按 v1 判定）
    replay = client.post(f"/devices/{DEVICE}/replay").json()
    _show("恢复后补传回放（450/470/490 按时间升序，temp 按 v1）", None, replay)

    # 6) 滞后到达：读数 150 产生于 v1 时期，现在才在线上报 -> 仍按 v1 告警
    t["now"] = 2_000
    late_batch = {
        "batch_id": "late-1",
        "device_id": DEVICE,
        "records": [
            {"device_id": DEVICE, "metric": METRIC, "value": 35.0, "ts": 150},
        ],
    }
    resp_late = client.post("/telemetry/batch", json=late_batch).json()
    _show("滞后到达数据 ts=150（v1 时期产生，按 v1 判定）", late_batch, resp_late)
    v0 = resp_late["verdicts"][0]
    assert v0["rule_version"] == 1 and v0["alert_id"] is not None

    alerts = client.get("/alerts").json()
    _show("最终告警列表（每条带规则版本）", None, alerts)

    print("\n模拟完成：重试幂等、补传升序、旧数据按旧规则版本判定均符合预期。")


if __name__ == "__main__":
    main()
