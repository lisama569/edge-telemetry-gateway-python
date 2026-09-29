# edge-telemetry-gateway

边缘网关设备遥测接入与阈值告警服务（FastAPI）。用本地模拟设备上报驱动，保证：

- **接入幂等**：同一批数据因重试重复提交时不重复计数、不重复告警；坏记录不静默丢弃，
  返回可区分的拒绝原因。
- **规则可演进、结论不可变**：阈值规则按版本追加；判定按**数据事件时间**选版，
  规则变更前后（含迟到、补传）的数据结论确定可复现，每条告警固化 `rule_version`。
- **离线补传有序**：设备断连期间数据进单设备积压队列，恢复后按事件时间回放；
  同一设备并发写入经专用锁串行化，不丢失、不串序；超限/过期/重复分别拒绝。

## 快速开始

```bash
uv sync
uv run pytest                      # 全量单测（打印输入数据与判定依据）
uv run python scripts/simulate.py  # 本地模拟设备上报，观察完整流程
uv run uvicorn main:app --reload   # 启动 HTTP 服务
```

## 上报示例

```bash
curl -X POST localhost:8000/devices/sensor-1/register
curl -X PUT localhost:8000/rules -H 'content-type: application/json' \
  -d '{"metric":"temp","op":">","threshold":10,"effective_from_ms":0}'
curl -X POST localhost:8000/devices/sensor-1/telemetry \
  -H 'content-type: application/json' \
  -d '{"records":[{"device_id":"sensor-1","metric":"temp","value":42,
       "timestamp_ms":1788268801000,"seq":101}]}'
```

响应逐条给出 `accepted` / `rejected`（含 `reason`、`detail`）/ `alerts`
（含 `rule_version`）。

## 文档

详细接入约定、幂等/乱序规则、告警判定语义与验证方法见
[docs/design.md](docs/design.md)。
