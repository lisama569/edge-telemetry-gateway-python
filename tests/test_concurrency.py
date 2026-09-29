"""同一设备并发写入：串行化保证不丢失、不串序、不重复计数。"""

from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor

from tests.conftest import T0, log_case


def test_concurrent_batches_to_same_device_lose_nothing(gw):
    gw.register_device("d1")
    gw.update_rule("temp", ">", 100, effective_from_ms=0, now_ms_=T0)

    n_threads = 8
    per_thread = 25
    barrier = threading.Barrier(n_threads)

    def make_batch(t: int):
        barrier.wait()  # 尽量同时发起，放大竞争
        start = t * per_thread
        records = [
            {"device_id": "d1", "metric": "temp", "value": 1,
             "timestamp_ms": T0 + start + i, "seq": start + i + 1}
            for i in range(per_thread)
        ]
        return gw.ingest_batch("d1", records, now_ms_=T0 + 100_000)

    with ThreadPoolExecutor(max_workers=n_threads) as pool:
        results = list(pool.map(make_batch, range(n_threads)))

    total_accepted = sum(len(r.accepted) for r in results)
    log_case(f"{n_threads} 线程并发共 {n_threads * per_thread} 条",
             "(见各线程批次)", _Aggregate(results))
    # 一条不丢、一条不重
    assert total_accepted == n_threads * per_thread
    assert gw.store.seen_count("d1") == n_threads * per_thread
    assert gw.store.alerts_for("d1") == []


def test_concurrent_retry_of_same_batch_counts_once(gw):
    gw.register_device("d1")
    gw.update_rule("temp", ">", 10, effective_from_ms=0, now_ms_=T0)
    batch = [
        {"device_id": "d1", "metric": "temp", "value": 11,
         "timestamp_ms": T0 + i, "seq": i + 1}
        for i in range(20)
    ]
    n = 6
    barrier = threading.Barrier(n)

    def submit(_):
        barrier.wait()
        return gw.ingest_batch("d1", batch, now_ms_=T0 + 100_000)

    with ThreadPoolExecutor(max_workers=n) as pool:
        results = list(pool.map(submit, range(n)))

    accepted_per_call = [len(r.accepted) for r in results]
    # 恰好一次调用“赢”，其余全部判为重复
    assert sorted(accepted_per_call) == [0] * (n - 1) + [20]
    assert gw.store.seen_count("d1") == 20
    assert len(gw.store.alerts_for("d1")) == 20


class _Aggregate:
    """仅用于 log_case 统一打印聚合结果。"""

    def __init__(self, results) -> None:
        self.accepted = [r for res in results for r in res.accepted]
        self.rejected = [r for res in results for r in res.rejected]
        self.alerts = [a for res in results for a in res.alerts]
