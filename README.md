# 边缘设备遥测网关（Python / FastAPI）

本地模拟设备上报驱动的遥测接入与阈值告警服务，重点保证三类场景下结论
**一致、可复现、可解释**：重复上报、离线补传（乱序/并发）、规则运行中变更。

## 1. 快速开始

```bash
uv sync                       # 安装依赖（fastapi、pytest 随环境可用）
python -m app.simulator       # 运行本地模拟故事线，逐步打印输入与判定依据
python main.py                # 启动 HTTP 服务（http://127.0.0.1:8000，含 /docs）
python -m pytest -s           # 运行测试，-s 可看到每个用例打印的输入与判定
```

## 2. 接入约定

所有时间戳均为**自 Unix 纪元起的整数秒**；`value` 为数值。入口不直接返回
422，而是把坏数据放进处理结果，逐条给出可区分的拒绝原因（见 §4）。

| 方法 & 路径 | 说明 |
| --- | --- |
| `POST /devices/register` | 注册设备：`{"device_id": "d1"}` |
| `POST /devices/connect` | 标记在线/离线：`{"device_id":"d1","connected":false}` |
| `POST /telemetry/batch` | 在线批量上报 |
| `POST /telemetry/buffer` | 断连期间写入离线缓存 |
| `POST /devices/{id}/replay` | 设备恢复后按时间顺序补传回放 |
| `POST /rules` | 新增/更新阈值规则（产生新版本） |
| `GET  /alerts` | 已触发告警列表（每条带规则版本） |

上报报文：

```json
{
  "batch_id": "batch-001",
  "device_id": "sensor-A",
  "records": [
    {"device_id": "sensor-A", "metric": "temp", "value": 31.5, "ts": 100}
  ]
}
```

响应中每条记录都带 `outcome`（`accepted` / `duplicate` / `rejected`）、
`reject_reason`、人类可读的 `detail`、判定所用 `rule_id`/`rule_version`，
若触发告警还带 `alert_id`。

## 3. 幂等与乱序处理规则

幂等分两层，重试**不会重复计数、不会重复触发告警**：

1. **批次层 —— `batch_id`**：同一 `batch_id` 的重试直接返回首次的
   `BatchResult`（同一对象），不重新执行任何处理。
2. **记录层 —— 去重键 `(device_id, metric, ts)`**：同一读数跨批次重报或在
   补传中再次出现，一律判 `duplicate`，原样返回首次结论（含相同 `alert_id`）。

离线缓存（`OfflineBuffer`）：

- **每台设备一把异步锁**，多个并发写入同一设备时串行化，不丢失、不串序；
- 回放前按 `(ts, metric)` 升序排序，**补传严格按读数产生时间顺序处理**；
- 容量上限 `capacity`、补传有效期 `ttl_seconds`（构造 app 时可配，默认
  100 条 / 3600 秒）；
- 拒绝原因判定顺序为 `duplicate -> stale -> overflow`，三者互斥、可区分：
  - `duplicate`：缓存中已存在相同去重键；
  - `stale`：`now - ts > ttl`，读数太老（入缓存时与回放时都会检查）；
  - `overflow`：设备积压已达容量上限。

坏数据的其它拒绝原因：`missing_field`（缺字段/空 metric/value 非数值）、
`invalid_timestamp`（ts 缺失/非整数秒/非正数/未来时间）、
`device_mismatch`（记录 device_id 与批次设备不一致）、
`unregistered_device`（设备未注册，整批拒绝）。

## 4. 告警判定语义（规则变更一致性）

规则**只追加、不改写**：对同一 `rule_id` 的每次更新都生成不可变的新版本
（version 单调递增），并带 `effective_from`（生效时刻，含）。判定时：

- 按**读数产生时间 `ts`** 选择当时生效的规则版本，而不是按数据到达时间；
- 设备专用规则（`device_id` 具体值）优先于全局规则（`device_id="*"`）；
- 同一时刻多条规则生效时取 `effective_from` 最近开始生效者。

由此保证：

- **规则变更前产生的数据**（即使之后滞后到达）永远按**旧版**规则判定；
  旧告警结论不会被新规则改写；
- **规则变更后产生的数据**按新版判定；
- 给定规则历史，任意到达顺序下重新处理同一条读数，选出的版本与结论
  **确定且可复现**；
- 每条告警固化 `rule_id` 与 `rule_version`，`alert_id` 形如
  `alert:{device}:{metric}:{ts}:{rule_id}:v{version}`，直接表明依据的是哪一版规则。

`POST /rules` 不传 `effective_from` 时取当前时间，即“运行中变更、即时生效”。

## 5. 验证方法

| 测试文件 | 覆盖场景 |
| --- | --- |
| `tests/test_ingest_idempotency.py` | 同批次重试、跨批次重报的幂等与告警不重复 |
| `tests/test_validation_reasons.py` | 缺字段/非法 ts/设备不符/未注册的拒绝原因区分 |
| `tests/test_rule_versioning.py` | 规则变更前后按 ts 选版本、旧结论不被改写、可复现 |
| `tests/test_buffer_replay.py` | 缓存三类拒绝、补传升序、并发写入不丢不串、回放时过期 |
| `tests/test_api_e2e.py` | HTTP 全链路：上报→重试→断连缓存→规则升级→补传/滞后到达 |

```bash
python -m pytest -s          # 每个用例经 log_case 打印“输入数据/判定依据”
python -m app.simulator      # 完整故事线演示（断言内置）
```

## 6. 代码结构

```
app/models.py       领域模型与枚举（拒绝原因、记录、结论、告警）
app/rules.py        版本化阈值规则引擎（按 ts 选版本、不可变追加）
app/buffer.py       离线缓存：设备锁、去重/过期/超限、按 ts 回放
app/ingest.py       接入流水线：校验、批次/记录双层幂等、规则判定
app/api_schemas.py  Pydantic 请求/响应模型（入口放宽，原因在结果中区分）
app/main.py         FastAPI 路由与依赖装配
app/simulator.py    本地模拟驱动
```
