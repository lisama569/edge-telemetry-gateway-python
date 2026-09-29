"""测试夹具与日志辅助。

约定：所有测试使用显式 now_ms（虚拟时钟），保证判定可复现；
每条用例打印输入数据与判定依据（拒绝原因/规则版本），满足“可解释”。
"""

from __future__ import annotations

import logging

import pytest

from app.gateway import TelemetryGateway

# 固定虚拟基准时间：2026-09-01T00:00:00Z
T0 = 1_788_268_800_000
HOUR = 3600 * 1000

log = logging.getLogger("telemetry-tests")


@pytest.fixture
def gw() -> TelemetryGateway:
    # 入站保留窗口 60 天；离线积压只保留 7 天，以便积压阶段独立区分“过期”
    return TelemetryGateway(
        backlog_limit=3,
        retention_ms=60 * 24 * HOUR,
        backlog_retention_ms=7 * 24 * HOUR,
    )


def log_case(title: str, payload, result) -> None:
    """打印输入数据与判定依据，便于人工核对可解释性。"""
    log.warning("=== %s ===", title)
    log.warning("输入: %s", payload)
    for r in result.accepted:
        log.warning("接受: device=%s metric=%s value=%s ts=%s seq=%s key=%s",
                    r.device_id, r.metric, r.value, r.timestamp_ms, r.seq,
                    r.dedup_key())
    for rej in result.rejected:
        log.warning("拒绝: reason=%s detail=%s raw=%s",
                    rej.reason.value, rej.detail, rej.raw)
    for a in result.alerts:
        log.warning("告警: device=%s metric=%s value=%s %s 阈值=%s "
                    "事件ts=%s => 按规则版本 v%s 判定命中",
                    a.record.device_id, a.metric, a.value, a.op, a.threshold,
                    a.record.timestamp_ms, a.rule_version)
