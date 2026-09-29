#!/usr/bin/env python3
"""本地模拟：驱动一批设备上报，演示幂等、离线补传与规则变更语义。

用法: uv run python scripts/simulate.py
不启动 HTTP，直接编排 TelemetryGateway，输出每一步的输入与判定依据。
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.gateway import TelemetryGateway

HOUR = 3600 * 1000
T0 = 1_788_268_800_000  # 2026-09-01T00:00:00Z


def show(title: str, result) -> None:
    print(f"\n--- {title} ---")
    for r in result.accepted:
        print(f"  接受 seq={r.seq} ts={r.timestamp_ms} value={r.value}")
    for rej in result.rejected:
        seq = rej.record.seq if rej.record is not None else "-"
        print(f"  拒绝 seq={seq} reason={rej.reason.value} :: {rej.detail}")
    for a in result.alerts:
        print(f"  告警 seq={a.record.seq} value={a.value} 命中规则 v{a.rule_version} "
              f"({a.metric} {a.op} {a.threshold})")


def main() -> int:
    gw = TelemetryGateway(backlog_limit=3, retention_ms=60 * 24 * HOUR,
                          backlog_retention_ms=7 * 24 * HOUR)
    gw.register_device("sensor-1")
    gw.update_rule("temp", ">", 10, effective_from_ms=0, now_ms_=T0)

    batch = [
        {"device_id": "sensor-1", "metric": "temp", "value": 12.0,
         "timestamp_ms": T0 + 1000, "seq": 1},
        {"device_id": "sensor-1", "metric": "temp", "value": 7.0,
         "timestamp_ms": T0 + 2000, "seq": 2},
        {"device_id": "sensor-1", "metric": "temp", "value": "N/A",
         "timestamp_ms": T0 + 3000, "seq": 3},
    ]
    show("首次上报（2 接受 / 1 坏值 / 1 告警 v1）",
         gw.ingest_batch("sensor-1", batch, now_ms_=T0 + 4000))
    show("网络重试，整批重发（好记录幂等；坏记录也保留首次原因不再改判）",
         gw.ingest_batch("sensor-1", batch, now_ms_=T0 + 4000))

    # 设备断连，期间分批、乱序上报
    gw.mark_offline("sensor-1")
    gw.ingest_batch("sensor-1", [
        {"device_id": "sensor-1", "metric": "temp", "value": 20.0,
         "timestamp_ms": T0 + 3 * HOUR, "seq": 6},
    ], now_ms_=T0 + 3 * HOUR)
    gw.ingest_batch("sensor-1", [
        {"device_id": "sensor-1", "metric": "temp", "value": 9.0,
         "timestamp_ms": T0 + 1 * HOUR, "seq": 4},
        {"device_id": "sensor-1", "metric": "temp", "value": 50.0,
         "timestamp_ms": T0 + 2 * HOUR, "seq": 5},
    ], now_ms_=T0 + 3 * HOUR)

    # 断连期间规则收紧到 v2（阈值 5），但旧事件时间的数据必须仍按 v1 判定
    gw.update_rule("temp", ">", 5, effective_from_ms=T0 + 90 * 60 * 1000,
                   now_ms_=T0 + 3 * HOUR)

    show("恢复连接，按事件时间补传回放（seq4 按 v1 不告警；seq5/6 按 v2 告警）",
         gw.mark_online_and_replay("sensor-1", now_ms_=T0 + 4 * HOUR))

    print("\n规则版本历史:")
    for v in gw.rules.history("temp"):
        print(f"  v{v.version}: {v.metric} {v.op} {v.threshold} "
              f"生效于 {v.effective_from_ms}")
    print("最终已接受记录数:", gw.store.seen_count("sensor-1"))
    print("最终告警(seq, 版本):",
          [(a.record.seq, a.rule_version) for a in gw.store.alerts_for("sensor-1")])
    return 0


if __name__ == "__main__":
    sys.exit(main())
