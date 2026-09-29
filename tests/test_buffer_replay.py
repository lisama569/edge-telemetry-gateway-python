"""离线缓存：补传按时间升序、拒绝原因可区分、并发写入不丢不串。"""
from __future__ import annotations

import asyncio

from app.buffer import OfflineBuffer
from app.models import RecordOutcome, RejectReason, TelemetryRecord
from tests.conftest import log_case


def _rec(ts: int, metric: str = "temp", value: float = 1.0) -> TelemetryRecord:
    return TelemetryRecord("d1", metric, value, ts)


async def test_buffer_reject_reasons_distinguishable():
    buf = OfflineBuffer(capacity=2, ttl_seconds=100)
    outcomes = []
    # 先填满
    outcomes.append(await buf.enqueue("d1", _rec(990), 1000))
    outcomes.append(await buf.enqueue("d1", _rec(980, "hum"), 1000))
    # 重复
    outcomes.append(await buf.enqueue("d1", _rec(990), 1000))
    # 过期
    outcomes.append(await buf.enqueue("d1", _rec(1), 1000))
    # 超限
    outcomes.append(await buf.enqueue("d1", _rec(985, "co2"), 1000))

    log_case("buffer-rejects", {"capacity": 2, "ttl": 100},
             [v.reject_reason.value if v.reject_reason else "accepted" for v in outcomes])

    assert outcomes[0].outcome is RecordOutcome.ACCEPTED
    assert outcomes[1].outcome is RecordOutcome.ACCEPTED
    assert outcomes[2].reject_reason is RejectReason.DUPLICATE
    assert outcomes[3].reject_reason is RejectReason.STALE
    assert outcomes[4].reject_reason is RejectReason.OVERFLOW


async def test_replay_orders_by_timestamp(svc, engine):
    engine.add_rule("r", "temp", "gt", 30.0, effective_from=0)
    buf = OfflineBuffer(capacity=10, ttl_seconds=1000)
    for ts in (300, 100, 200, 50):  # 故意乱序写入
        await buf.enqueue("d1", _rec(ts, value=31.0), 600)

    processed: list[int] = []

    async def proc(record: TelemetryRecord):
        processed.append(record.ts)
        return await svc.process_record(record)

    verdicts = await buf.replay("d1", proc, 600)
    log_case("replay-order", {"write_order": [300, 100, 200, 50]},
             {"processed_ts": processed})
    assert processed == [50, 100, 200, 300], "补传必须按 ts 升序处理"
    assert buf.size("d1") == 0
    assert all(v.outcome is RecordOutcome.ACCEPTED for v in verdicts)
    assert len(svc.alerts()) == 4


async def test_concurrent_writers_no_loss_or_reorder_risk():
    """多协程并发写同一设备：不同读数全部保留，重复读数只保留一份。"""
    buf = OfflineBuffer(capacity=1000, ttl_seconds=10_000)

    async def writer(base: int):
        for i in range(25):
            await buf.enqueue("d1", _rec(base + i), 1000)

    # 两个 writer 的 ts 区间：500..524 与 520..544，重叠 520..524
    await asyncio.gather(writer(500), writer(520))
    assert buf.size("d1") == 45, f"期望并集 45 条去重后记录, 实际 {buf.size('d1')}"

    processed: list[int] = []
    await buf.replay("d1", lambda r: _capture(processed, r), 1000)
    log_case("concurrent-writers", {"writers": 2, "overlap": [520, 524]},
             {"buffered_unique": 45, "first5": processed[:5], "sorted": processed == sorted(processed)})
    assert processed == sorted(processed)
    assert len(processed) == 45


async def _capture(processed, record):
    processed.append(record.ts)
    from app.models import RecordVerdict
    return RecordVerdict(outcome=RecordOutcome.ACCEPTED, record=record)


async def test_stale_at_replay_time_still_rejected():
    buf = OfflineBuffer(capacity=10, ttl_seconds=100)
    await buf.enqueue("d1", _rec(900), 950)   # 入缓存时未过期
    async def proc(record):
        raise AssertionError("过期记录不应进入判定")
    verdicts = await buf.replay("d1", proc, 1050)  # 回放时已过期
    log_case("stale-at-replay", {"enqueue_now": 950, "replay_now": 1050, "ts": 900},
             {"reason": verdicts[0].reject_reason.value})
    assert verdicts[0].reject_reason is RejectReason.STALE
