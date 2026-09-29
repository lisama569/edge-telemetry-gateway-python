# 边缘遥测网关：接入约定、幂等/乱序与告警判定语义

本文档说明设备遥测接入约定、幂等与乱序补传规则、阈值告警判定语义，以及验证方法。

## 1. 总体结构

| 模块 | 职责 |
| --- | --- |
| `app/models.py` | 领域类型：遥测记录、不可变规则版本、告警、带原因的拒绝结果 |
| `app/validation.py` | 单条记录校验与归一化，输出**可区分**的拒绝原因 |
| `app/rules.py` | 阈值规则版本库；按**数据事件时间**选版判定 |
| `app/store.py` | 设备注册表、幂等去重表、不可变告警存档（线程安全） |
| `app/backlog.py` | 离线积压、按设备串行化、按事件时间顺序回放 |
| `app/gateway.py` | 编排：校验 → 归属 → 幂等 → 在线直处理 / 离线入积压 |
| `app/api.py` | FastAPI HTTP 接入层 |

## 2. 接入约定

### 2.1 记录格式

```json
{
  "device_id": "sensor-1",   // 必填，且必须与 URL 路径中的设备标识一致
  "metric": "temp",          // 必填，非空字符串
  "value": 42.0,             // 必填，必须是有限数值（数字或可转数字的字符串）
  "timestamp_ms": 1788268801000,
  "seq": 101,                // 二选一：设备内单调递增的序列号（推荐）
  "nonce": "uuid-..."        // 二选一：无 seq 时配合 ts 做幂等键
}
```

- `timestamp_ms` 接受 epoch 毫秒（数字/数字字符串）或 ISO-8601 字符串
  （`Z` 按 UTC 处理）。也可用别名 `ts`。
- 上报端点：`POST /devices/{device_id}/telemetry`，body 为 `{"records": [...]}`。
- **单条坏记录不会让整批失败**：结果逐条返回 `accepted` 与 `rejected`。

### 2.2 拒绝原因（枚举值稳定，可被客户端程序化处理）

| reason | 触发条件 |
| --- | --- |
| `empty_batch` | 批次不是数组或为空 |
| `missing_device_id` | 记录缺少非空 `device_id` |
| `device_id_mismatch` | 记录内 `device_id` 与 URL 路径设备不一致 |
| `unknown_device` | 设备未先调用注册接口 |
| `missing_metric` | 缺少 `metric` 或 `value` |
| `value_not_number` | `value` 不可解析为有限数值 |
| `timestamp_missing` | 同时缺少 `timestamp_ms` 与 `ts` |
| `timestamp_invalid` | 时间戳格式不可解析 |
| `timestamp_in_future` | 时间戳超过当前时间 + 60s 抗时钟漂移窗口 |
| `timestamp_expired` | 事件时间早于入站保留窗口（默认 7 天） |
| `duplicate_record` | 幂等命中（重试/补传重复） |
| `backlog_over_limit` | 设备离线积压达到上限（默认每设备 1000 条） |

校验顺序固定：标识 → 指标/取值 → 时间戳。因此同一条坏记录无论重放多少次，
得到的原因都相同。每条拒绝都带人类可读的 `detail` 与原始记录 `raw`，**不静默丢弃**。

## 3. 幂等语义

幂等键（`TelemetryRecord.dedup_key`）：

1. 有 `seq`：`("seq", device_id, seq)`；
2. 无 `seq`：`("evt", device_id, metric, timestamp_ms, nonce)`。

含义：

- 同一幂等键只接受一次。整批重试、批次顺序打乱、并发同时提交，结论一致：
  重复记录返回 `duplicate_record`，**计数不增加、告警不重复产生**。
- 去重与告警存档在 `DeviceStore.accept()` 中以同一把锁原子完成，
  因此并发批次不会出现“两个线程都判定为新记录”的竞态。
- 同一时间戳但 `seq` 不同是两条独立记录；同一 `seq` 即便 value 不同也视为重复
  （上报方必须保证 seq 单调且不重用）。
- **结构性拒绝也可幂等重试**：只要坏记录具备稳定键（有 `seq`，或标识/时间戳/nonce
  齐全），网关会记忆其首次拒绝原因；同样的记录重试时返回 `duplicate_record`，
  并在 `detail` 中保留“首次已被拒绝（原原因）”，不会在不同重试间摇摆。
  缺少稳定键的记录（例如既无 seq 又无时间戳）无法记忆，每次都按其固有原因拒绝，
  但结论仍然固定（校验顺序不变）。

## 4. 离线缓存与补传回放

### 4.1 连接状态

- `POST /devices/{id}/offline` 标记设备断连；此后到达的数据进入**该设备独占的积压队列**。
- `POST /devices/{id}/reconnect` 标记恢复并触发回放。
- `GET /devices/{id}/backlog` 查看在线状态与积压长度。

### 4.2 并发与顺序保证

- `BacklogManager` 为**每个设备维护一把专用锁**：同一设备的上报、状态切换、
  回放全部串行；不同设备互不阻塞。
- 因此同一设备的并发写入既不丢失也不串序。
- 回放排序键：`(timestamp_ms, seq, 提交顺序兜底)`，严格按事件时间升序处理。

### 4.3 积压阶段的拒绝（可区分、确定）

积压提交采用**两遍扫描**，使每条记录的拒绝原因只取决于自身与队列当前容量，
不取决于同批其他记录的排列：

1. 第一遍拒绝 `duplicate_record`（已在队列或已接受）与 `timestamp_expired`
   （早于积压保留窗口，默认 7 天），这两类记录不占容量；
2. 第二遍对幸存记录按顺序做容量检查，超出上限的拒绝为 `backlog_over_limit`。

回放时再次做过期与幂等检查：若记录在回放前已被其它路径接受
（例如并发的直处理），该条以 `duplicate_record` 跳过，其余记录照常回放。

## 5. 告警判定语义

### 5.1 规则版本不可变

- 阈值规则按版本保存（`AlertRule(version, metric, op, threshold, effective_from_ms)`）。
- 修改阈值 = 追加新版本；历史版本永不修改。重复追加同一版本号会被拒绝。
- 算子支持 `> < >= <=`。

### 5.2 按事件时间选版，而不是按处理时间

对一条记录生效的规则是：

```
effective_from_ms <= record.timestamp_ms 的所有版本中，版本号最大的一版
```

生效时间约定：

- **首版**规则若不指定 `effective_from_ms`，默认为 `0`，即对全部（含历史）事件
  时间的数据有效，避免毫秒级时钟差导致“刚发布的规则不判定刚到的数据”；
- **后续版本**若不指定，默认从**发布时刻**生效，绝不追溯改写历史结论；
- 任何版本都可显式指定 `effective_from_ms` 做计划性切换。

这条规则同时覆盖三种到达时机，结论完全一致、可复现：

- 数据实时到达：按当时版本；
- 规则变更后**滞后到达**的旧数据：仍按其事件时刻的旧版本判定，**不会被新规则翻案**；
- 离线补传：回放虽然发生在现在，但每条数据仍按自己的事件时间选版。

### 5.3 结论可追溯、不可变

- 每条告警都固化 `rule_version`、算子与阈值快照（`Alert`）；
- 告警只追加、从不变更、从不回填。任何时刻都能回答
  “这条告警是按哪一版规则得出的”。
- 例：v1 为 `temp>10`、v2 自 T 起降为 `temp>5`；事件时间早于 T 的值 `8`
  在 v1 下不告警，即使 v2 发布后才补到达，结论依旧是不告警。

## 6. HTTP 接口一览

| 方法/路径 | 说明 |
| --- | --- |
| `POST /devices/{id}/register` | 注册设备并置为在线 |
| `POST /devices/{id}/telemetry` | 上报一批记录（逐条接受/拒绝结果） |
| `POST /devices/{id}/offline` | 标记断连，后续数据进积压 |
| `POST /devices/{id}/reconnect` | 恢复并按事件时间回放积压 |
| `GET  /devices/{id}/backlog` | 在线状态与积压长度 |
| `PUT  /rules` | 发布一版新规则（body: metric/op/threshold/effective_from_ms?） |
| `GET  /rules/{metric}` | 查看某指标的全部历史版本 |
| `GET  /alerts?device_id=` | 查询告警（携带 rule_version） |

## 7. 验证方法

```bash
# 全量单测（会实时打印每条用例的输入数据与判定依据）
uv run pytest

# 只看某类场景
uv run pytest tests/test_idempotency.py    # 重复上报 / 幂等
uv run pytest tests/test_backlog.py        # 乱序补传 / 超限 / 过期 / 重复
uv run pytest tests/test_rule_changes.py   # 规则变更前后一致性
uv run pytest tests/test_concurrency.py    # 同设备并发写入
uv run pytest tests/test_validation.py     # 各类拒绝原因
uv run pytest tests/test_api.py            # HTTP 端到端

# 本地模拟设备上报（不依赖网络，直接打印每步输入与结论）
uv run python scripts/simulate.py

# 启动服务后手工验证
uv run uvicorn main:app --reload
```

测试覆盖矩阵：

| 场景 | 用例 |
| --- | --- |
| 整批重试/乱序重试幂等 | `test_duplicate_batch_retry_is_idempotent`、`test_retry_with_records_in_different_order` |
| 无 seq 的 ts+nonce 去重 | `test_dedup_falls_back_to_ts_nonce_without_seq` |
| 九类拒绝原因可区分 | `test_each_bad_record_gets_distinct_reason` |
| 坏记录不连累兄弟记录 | `test_one_bad_record_does_not_drop_others` |
| 旧结论不被新规则改写 | `test_old_alerts_keep_old_rule_version` |
| 同批跨版本边界选版 | `test_rule_version_pinned_on_each_alert` |
| 乱序离线数据按序回放 | `test_offline_batches_buffered_and_replayed_in_time_order` |
| 超限/过期/重复三分 | `test_backlog_over_limit_expired_duplicate_are_distinct` |
| 补传期间改规则 | `test_replay_after_rule_change_uses_event_time_versions` |
| 并发不丢/不重 | `test_concurrent_batches_to_same_device_lose_nothing`、`test_concurrent_retry_of_same_batch_counts_once` |
