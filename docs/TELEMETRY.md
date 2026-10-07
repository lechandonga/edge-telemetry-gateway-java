# 边缘网关遥测采集与断连补传链路 — 设计说明

全部组件在本地模拟（设备、云端、存储均为本地文件/内存），不依赖任何外部服务。
代码位于 `com.github.lechandonga.edgetelemetrygatewayjava.telemetry` 包下。

## 1. 遥测模型与归一化

两种设备上报形式在入口统一为 `TelemetryRecord`（`model/TelemetryRecord.java`）：

- JSON 报文：`Normalizer.ingestJson(rawJson)`。
- 寄存器采样（Modbus 风格帧 `RegisterFrame`：设备号 + 寄存器地址 `R01` + 字符串值 + 时间戳）：
  `Normalizer.ingestRegister(frame)`，寄存器地址经 `DeviceSpec.registerAliases` 映射为指标名。

统一字段：`sequence`（设备内递增）、`deviceId`、`metric`、`value`、`timestamp(epoch ms)`、
`source(JSON/REGISTER)`、`quality`、`degradeReasons`。

校验链（顺序固定，原因可区分，见 `model/RejectReason`）：

| 结果 | 分类 | 处理 |
| --- | --- | --- |
| 非法报文 | `MALFORMED` | 拒收 → 死信日志 |
| 未登记设备 | `UNKNOWN_DEVICE` | 拒收 → 死信日志 |
| 未声明指标/寄存器 | `UNKNOWN_METRIC` | 拒收 → 死信日志 |
| 必填字段缺失 | `MISSING_FIELD` | 拒收 → 死信日志 |
| 值非数值 / NaN / Infinity | `INVALID_VALUE` | 拒收 → 死信日志 |
| 超出硬量程 `[hardMin,hardMax]` | `OUT_OF_RANGE` | 拒收 → 死信日志 |
| 时间戳早于“设备+指标”水位线 | `OUT_OF_ORDER` | 拒收 → 死信日志 |
| 同设备同指标同时间戳重复 | `DUPLICATE` | 拒收 → 死信日志 |
| 超出可信区间但在硬量程内 | （软异常） | **降级** `GOOD→DEGRADED`，记录原因但数据保留并继续上联 |

任何异常输入都不会静默丢弃：拒收写入 `dead-letter.jsonl`（`DeadLetterLog`），可按原因查询。

顺序保证：

- 每设备一把顺序锁，校验+水位线推进原子完成；水位线粒度为“设备 + 指标”，
  因此同设备不同指标互不影响，同一设备指标的数据顺序与时间戳严格单调一致。
- 幂等键 `deviceId|metric|timestamp`，用于去重与补传确认。

## 2. 有界缓冲、断连补传与幂等确认

链路（`gateway/TelemetryGateway`）：归一化 → 追加归档 → 告警判定 → 入缓冲并尽力上联。
数据采用“先持久化后发送”（write-ahead）：入缓冲即追加写 `pending.jsonl`（WAL），
只有收到云端幂等回执后才出队。

缓冲语义（`buffer/PendingBuffer.java`）：

- **有界**：容量为构造参数 `capacity`，占用永不无限增长。
- **写满淘汰（确定且可查询）**：策略为 `OLDEST_FIRST`（全局 FIFO 队头最旧数据），
  被淘汰记录带策略名写入 `evictions.jsonl`（含原始记录与淘汰时间），可随时查询。
- **断连**：`SimulatedCloud.setAvailable(false)` 期间数据全部留在缓冲，恢复后调用
  `flushPending()`（`UplinkManager.drain`）按全局 FIFO 补传；设备内顺序天然保持。
- **不丢**：未确认数据始终在 WAL；进程重启时从 `pending.jsonl` 回放恢复（去重后重建队列；
  已进入隔离区的记录恢复时自动从缓冲剔除，绝不二次补传）。
- **不重（两层幂等）**：
  - 云端 `SimulatedCloud` 按幂等键去重，重复上报只计一次，可通过 `acceptedCount()` 核验；
  - 网关出队校验“确认 key 必须等于当前队头 key”，过期/重复 ACK 无法误删后续数据；
  - ACK 丢失（`simulateAckLossOnce()`：云端收到但回执丢失）时数据不出队，重发由云端去重。
- 多设备并发：缓冲操作全部在同一把监视器锁内串行化，结合设备顺序锁保证不丢不重。

### 2.1 批量补传

链路恢复后 `UplinkManager.drain()` 按批发送待补传数据（默认走 `CloudEndpoint.receiveBatch`），
不再逐条来回确认：

- **批大小与上限**：`TelemetryGateway` 构造参数 `batchSize` 配置批大小，缺省 64；
  硬上限 `UplinkManager.MAX_BATCH_SIZE = 256`，配置超限自动截断到 256。批大小只是发送窗口，
  缓冲内存上界仍是 `capacity`、落盘仍是一份 `pending.jsonl`（出队时压缩重写），
  批量发送不会额外占用容量、不突破任何原有上界。
- **顺序与批边界**：每批取队头连续一段，批内顺序就是缓冲里的先后顺序；
  批边界可以切在任意两条记录之间，但绝不会把同一设备的数据重新排序或插队。
- **回执三种结论**（`uplink/ItemOutcome.java`）：`ACCEPTED`（确认接收）、
  `PERMANENTLY_REJECTED`（永久拒收，见 2.2）、`UNRESOLVED`（暂时发不出去）。
- **部分确认 / 整批回执丢失（前缀语义）**：云端可以整批回一次结果，也可以只回前面一部分
  （`SimulatedCloud.setBatchConfirmLimit(k)`），或整批回执丢失
  （`simulateBatchReceiptLossOnce()`）。网关严格按批内顺序解释：
  从批首逐条出队，遇到第一条 `UNRESOLVED`（或结果缺失）立即停止，该条及其后所有数据
  全部留在队列，下一批从队头重发。因此只有真正确认接收的数据才出队；没确认的一条不丢、
  不会被后面的数据插队；重发由云端幂等去重，ACK 丢失、重复确认在批量语义下同样安全。
- **效率**：积压 1000 条、批大小 64 时，云端接收调用为 16 次（与批次数同数量级），
  而不是上千次（见 `BatchUplinkAndQuarantineTest.batchCallsAreOnOrderOfBatchCount`）。
  高并发在线提交时，`UplinkManager` 用单一 drain 标志合并并发补传请求，避免每条各起一批。

### 2.2 永久拒收与隔离区

云端对个别记录给出**不可恢复拒绝**（`CloudRejectReason`：`EXPIRED` 历史超接收时限、
`METRIC_DISCONTINUED` 指标下线、`PERMANENTLY_REJECTED` 其他）时，与“暂时发不出去”严格区分：

- 永久拒收记录**不重试、不堵队头、不静默消失**：先持久化写入隔离区
  （`quarantine.jsonl`，`storage/QuarantineStore.java`），再从补传队列移除；
  同一条数据云端重复拒收只保留一份隔离记录（按幂等键去重）。
- 被隔离记录从序列中间移走后，其前后数据的相对顺序不变；后面排队的正常数据继续按序补传。
- 隔离区可查询：`quarantineStore().all()`、`byReason(reason)`、`countsByReason()`，
  含完整记录、分类原因、云端说明与隔离时间；进程重启后回放恢复，仍可查询。
- 崩溃安全：出队前隔离必须已落盘；若恰好在“隔离落盘后、缓冲 WAL 重写前”崩溃，
  重启恢复缓冲时会把已隔离的队头安全剔除（它已可在隔离区查到），既不重发也不丢失。

### 2.3 对账

以“被网关接收入链的数据”（`telemetry-archive.jsonl` 总数）为基准，
`TelemetryGateway.reconcile()` 返回 `ReconciliationReport`，四类数字精确配平：

```
acceptedIntoChain（入链总数）
  = cloudAccepted（云端已接收）
  + quarantined  （已隔离，永久拒收）
  + inFlight     （仍在途，补传队列未决）
  + evicted      （已淘汰，OLDEST_FIRST）
```

其中 `cloudAccepted = 入链 − 隔离 − 在途 − 淘汰`，并可与云端
`SimulatedCloud.acceptedCount()` 互相印证。`balanced()` 为 true 表示总账对得上。

## 3. 阈值告警：持续窗口、跨重启、规则版本

规则（`alert/ThresholdRule`）：`ruleId + deviceId + metric + operator(GT/LT/GE/LE)
+ threshold + durationMillis`。规则集 `RuleSet(version, createdAt, rules)`。

状态机（`RuleState`，每“规则+设备+指标”一份）：

```
NORMAL --越限采样--> PENDING(记录窗口起点)
PENDING --持续时间 >= duration--> FIRED(产生一次 FIRE 事件)
PENDING --窗口内恢复--> NORMAL（窗口作废）
FIRED  --继续越限--> FIRED（不重复告警）
FIRED  --恢复采样--> RECOVER 事件 -> NORMAL
```

因此短时反复越限/恢复不会产生重复告警；只有“恢复后再次持续越限”才产生新的 FIRE。

跨重启：

- 每次状态迁移后整体快照原子写入 `alert-state.json`（临时文件 + 原子 move）；
  重启时载入，PENDING 窗口起点与 FIRED 状态都延续，不会重复触发。
- 告警事件仅追加到 `alert-events.jsonl`，事件带 `ruleVersion`、窗口起点与持续时长，结论可解释。

规则版本机制（`RuleStore` + `AlertEngine.publishRules`）：

- 每个版本单独存 `rule-versions/<version>.json`，`rules-current.json` 指向当前版本。
- 更新时按 `ruleId|deviceId|metric` 迁移所有进行中的 PENDING/FIRED 窗口（不重置计时、
  不重新告警），新版删除的规则状态下线——更新过程中已进入窗口的判定既不丢失也不重复触发；
  迁移后产生的事件归属新版本号。
- 确定性重放：`AlertEngine.replay(history, ruleSet)` 用全新状态、按
  `(timestamp, deviceId, metric)` 稳定排序后对任意历史版本规则重放，
  同一段历史 + 同一版本必然得到相同结论；不同版本结论可对比。

## 4. 本地数据文件（默认在构造时传入的 dataDir）

| 文件 | 内容 |
| --- | --- |
| `telemetry-archive.jsonl` | 已接受遥测（含降级），规则重放/水位线恢复的数据源 |
| `pending.jsonl` | 未确认补传 WAL |
| `evictions.jsonl` | 缓冲淘汰记录（OLDEST_FIRST） |
| `quarantine.jsonl` | 云端永久拒收隔离记录（含分类原因，重启后可查询、参与对账） |
| `dead-letter.jsonl` | 拒收记录与原因分类 |
| `alert-events.jsonl` | FIRE/RECOVER 告警事件 |
| `alert-state.json` | 告警窗口状态快照 |
| `rules-current.json`、`rule-versions/*.json` | 当前与历史规则版本 |

## 4.1 兼容范围

- `pending.jsonl` 文件格式未变：升级前已落盘未确认的老数据，重启后原样回放并继续补传。
- 原单条补传行为完全保留：`CloudEndpoint` 只实现单条 `receive` 时，`receiveBatch`
  默认退化为逐条调用（首个未确认即停）；批大小取 1 与历史语义逐字等价。
- 缓冲容量、`OLDEST_FIRST` 淘汰、`evictions.jsonl` / `dead-letter.jsonl` 语义不变；
  新增的 `quarantine.jsonl` 为独立新文件，不影响既有数据与既有用例。
- 既有 `TelemetryGateway` / `PendingBuffer` 构造均保留兼容签名，默认批大小 64。

## 5. 本地验证方法

前置：JDK 21+、Maven（无需联网到任何业务服务；首次构建需拉取 Maven 依赖）。

```bash
# 运行全部测试（23 个场景用例 + Spring 上下文冒烟）
mvn test

# 端到端演示（输出关键状态变化，数据落 ./data/demo）
mvn compile exec:java -Dexec.mainClass=com.github.lechandonga.edgetelemetrygatewayjava.telemetry.demo.LocalDemo
```

测试覆盖（日志均打印“输入 / 判定依据 / 关键状态变化”，logger 名含 `SCENARIO`）：

- `IngestNormalizationTest`：两种形式归一化、软界降级、8 类拒收原因可区分、乱序与重复、跨设备水位线。
- `BufferAndUplinkTest`：断连积压与 FIFO 有序补传、ACK 抖动幂等、重复/过期确认安全、
  写满 OLDEST_FIRST 淘汰可查询、重启恢复、2 设备 4 指标 200 条并发接力（不丢不重、设备内有序）。
- `BatchUplinkAndQuarantineTest`：
  积压 1000 条/批 64 仅 16 次批量调用；部分确认（只回执前 k 条）其余保序重发；
  整批回执丢失幂等重发不丢不重；永久拒收（`EXPIRED`/`METRIC_DISCONTINUED`）隔离后队列继续前进、
  前后数据顺序不变、隔离可查询且对账配平；批量补传中途重启（已确认不重、未确认不丢）；
  隔离状态跨重启保持并清除崩溃窗口孤儿队头；
  **真并发**：8 设备 × 每台两条入口（JSON + 寄存器）各 2000 条共 32000 条，
  18 线程同时灌数据 + 2 线程并发批量补传，断言无异常、不丢、不重、设备内顺序稳定、
  四类对账数字精确配平。
- `AlertEngineTest`：持续窗口、抖动去重、规则热更新窗口迁移、同版本确定性重放/跨版本对比、跨重启状态保持。
- `EndToEndScenarioTest`：正常/异常/降级 → 断连 → 重启 → ACK 抖动 → 补传 → 告警/恢复 → 规则升级 全剧本。
