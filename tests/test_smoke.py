"""骨架冒烟测试：验证包可导入、FastAPI app 可构造。"""

from fastapi.testclient import TestClient

from app.api import app


def test_smoke():
    client = TestClient(app)
    resp = client.get("/")
    assert resp.status_code == 200
    assert resp.json()["status"] == "ok"
