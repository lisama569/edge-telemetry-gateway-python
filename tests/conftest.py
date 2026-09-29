"""测试公共夹具与日志工具。

每个用例通过 log_case 打印“输入数据”与“判定依据”，
方便定位是哪类场景（重复上报 / 乱序补传 / 规则变更）失败。
"""
from __future__ import annotations

import json
import sys

import pytest

from app.ingest import TelemetryIngestService
from app.rules import RuleEngine


@pytest.fixture
def clock():
    return {"now": 1000}


@pytest.fixture
def engine():
    return RuleEngine()


@pytest.fixture
def svc(engine, clock):
    service = TelemetryIngestService(engine, clock=lambda: clock["now"])
    service.register_device("d1")
    service.register_device("d2")
    return service


def log_case(name: str, inp: object, verdict: object) -> None:
    """统一打印输入与判定依据，失败原因在 detail/reject_reason 中可区分。"""
    print(
        f"\n[CASE] {name}\n"
        f"  输入数据: {json.dumps(inp, ensure_ascii=False, default=str, sort_keys=True)}\n"
        f"  判定依据: {json.dumps(verdict, ensure_ascii=False, default=str, sort_keys=True)}",
        file=sys.stderr,
    )


def pytest_configure(config):
    config.addinivalue_line("markers", "asyncio: run test coroutine in an event loop")


@pytest.hookimpl(tryfirst=True)
def pytest_pyfunc_call(pyfuncitem):
    testfunction = pyfuncitem.obj
    if asyncio_iscoro(testfunction):
        import asyncio

        funcargs = {
            arg: pyfuncitem.funcargs[arg]
            for arg in pyfuncitem._fixtureinfo.argnames
        }
        asyncio.run(testfunction(**funcargs))
        return True
    return None


def asyncio_iscoro(func):
    import inspect

    return inspect.iscoroutinefunction(func)
