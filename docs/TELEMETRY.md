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

## 2. 有界缓冲、批量补传、永久拒收隔离与幂等确认

链路（`gateway/TelemetryGateway`）：归一化 → 追加归档 → 告警判定 → 入缓冲并尽力上联。
数据采用“先持久化后发送”（write-ahead）：入缓冲即追加写 `pending.jsonl`（WAL），
只有收到云端幂等回执后才出队。

补传编排（`uplink/UplinkManager`）支持**批量补传**，构造参数 `batchSize` 控制批大小：

- 默认 `DEFAULT_BATCH_SIZE=64`，硬上限 `MAX_BATCH_SIZE=256`；配置超过上限一律截断，
  非正值回退默认。批大小只改变发送/确认粒度，**不改变**缓冲容量与 WAL 占用上界：
  一批数据不额外复制到新的存储结构，只从队头取快照；一批 N 条落定后只压缩一次 WAL。
- 传 `batchSize=1`（或旧的 `new UplinkManager(buffer, cloud)` /
  `new TelemetryGateway(...,cloud,alerts)` 旧构造）即原有**逐条补传**行为，完全兼容。
- 效率：积压 N 条、批大小 B 时，云端批量接收接口的调用次数为 `ceil(N/B)` 量级。
  例：积压 1000 条、批大小 64，共 16 次云端调用（而不是 1000 次）；
  调用次数可通过 `cloudBatchCallCount()` / `CloudEndpoint.batchCallCount()` 观测。

批量回执语义（`uplink/BatchReceipt`、`RecordReceipt`、`UplinkStatus`）：

- 云端可以整批回一次结果，也可以**只回顺序前缀**（部分确认，比如只确认了前几条），
  还可以整批回执都丢掉（空回执，等价于一条都没确认）。
- 网关严格按顺序处理：回执的下标与幂等键必须与发出的批次逐条对上；
  - `ACCEPTED`：出队，并把幂等键写入 `accepted-keys.jsonl`（云端已接收本地账本）；
  - `PERMANENTLY_REJECTED`：从队列移除并转入隔离区（见下）；
  - `RETRYABLE` / 缺回执 / 键不匹配：在该条处立刻停下，它和它之后的所有记录原样留队，
    下一轮按原顺序整批重发——**未确认的一条都不丢，也不允许被后面数据插队**。
- 批内顺序就是缓冲全局 FIFO 顺序，**批边界不会把同一设备的数据顺序打乱**；
  跨批次时同设备记录也严格保持先后。
- ACK 丢失与重复确认在批量语义下继续成立：整批回执丢失则整批重发；
  云端按幂等键去重（重复上报只计一次）；网关账本/隔离区/缓冲均按键幂等，
  重发后的重复确认、重复拒收结论不会重复记账。重试后不重、不漏。

**永久拒收隔离**（`storage/QuarantineLog`，文件 `quarantine.jsonl`）：

- 云端对个别记录给出不可恢复拒绝时返回 `PERMANENTLY_REJECTED`，
  分类原因见 `model/CloudRejectCode`：`EXPIRED`（历史数据超过接收时限）、
  `METRIC_DISCONTINUED`（云端已不接收该指标）；`RETRYABLE` 表示“暂时发不出去”。
- “暂时发不出去”与“永远收不了”因此被彻底分开：前者留在队头重试，
  后者移出缓冲进隔离区——永久拒收记录**不再占着队头反复重试、不堵后面正常数据**，
  也**不会静默消失**：原文、幂等键、分类原因、细节、隔离时间全部落盘，
  可通过 `gateway.quarantineLog().all()` 查询，可按原因分类统计。
- **顺序规则（隔离挪走记录时）**：被隔离记录从 FIFO 序列中间移除，
  只影响它自己一个位置；它**前面记录与后面记录的相对顺序保持不变**，
  后续正常数据继续按序补传（实现上是对同一个 Deque 的顺序移除，不发生重排）。
- 隔离区按键幂等：同一条记录重发后再次收到永久拒收结论，不会产生第二条隔离记录。

**对账**（`gateway/TelemetryGateway.reconcile()`，返回 `reconcile/ReconciliationReport`）：

- 基准为“已被网关接收入链的数据”，即 `telemetry-archive.jsonl` 按幂等键去重后的总数。
- 四类去向互斥且穷尽，恒等式：
  `acceptedIngress = cloudAccepted + quarantined + inFlight + evicted`
  - `cloudAccepted`：云端已确认接收。以**本地持久化账本** `accepted-keys.jsonl` 为准
    （不依赖云端内存），进程重启后数字仍在；
  - `quarantined`：已进隔离区的永久拒收记录；
  - `inFlight`：仍在补传缓冲中（含暂时发不出去、等待重试的）；
  - `evicted`：缓冲写满按 OLDEST_FIRST 已淘汰（`evictions.jsonl`）。
- `report.balanced()` 为该恒等式是否精确成立。建议在静默点（无并发上报/补传）取快照。

缓冲语义（`buffer/PendingBuffer.java`）：

- **有界**：容量为构造参数 `capacity`，占用永不无限增长。
- **写满淘汰（确定且可查询）**：策略为 `OLDEST_FIRST`（全局 FIFO 队头最旧数据），
  被淘汰记录带策略名写入 `evictions.jsonl`（含原始记录与淘汰时间），可随时查询。
  淘汰在当次入队内立即压缩 WAL，被淘汰记录绝不残留在日志中。
- **断连**：`SimulatedCloud.setAvailable(false)` 期间数据全部留在缓冲，恢复后调用
  `flushPending()`（`UplinkManager.drain`）按全局 FIFO 批量补传；设备内顺序天然保持。
- **不丢**：未确认数据始终在 WAL；为控制写放大，队头离队（确认/拒收）后 WAL
  按阈值（256 条）节流压缩，期间已离队记录只表现为日志里的“冗余前缀”；
  进程重启从 `pending.jsonl` 回放时，会用云端已接收账本、隔离区、淘汰日志三重过滤
  剔除冗余前缀并按键去重，已确认/已隔离/已淘汰的数据不会复活，未确认数据一条不丢。
- **不重（两层幂等）**：
  - 云端 `SimulatedCloud` 按幂等键去重，重复上报只计一次，可通过 `acceptedCount()` 核验；
  - 网关出队校验“确认 key 必须等于当前队头 key”，过期/重复 ACK 无法误删后续数据；
  - ACK 丢失（`simulateAckLossOnce()`：云端收到但回执丢失）时数据不出队，重发由云端去重。
- **批量补传中途崩溃**：一批中已确认的前缀已同步记入账本并从队列/WAL 移除，
  未确认后缀完整留在 WAL；重启后只补传后缀，已确认的不重报、未确认的不丢。
- 多设备并发：缓冲操作全部在同一把监视器锁内串行化，结合设备顺序锁保证不丢不重。
  `UplinkManager.submit` 与 `drain` 在同一把锁上串行，发送在锁内完成，
  因而不存在“发送中数据被淘汰/插队”的窗口。

### 2.1 兼容范围（升级前数据与原有行为）

- **落盘文件语义不变**：`pending.jsonl`、`telemetry-archive.jsonl`、
  `evictions.jsonl`、`dead-letter.jsonl` 的行格式与含义保持原样，
  升级前已落盘但未确认的老 `pending.jsonl`，重启后直接回放继续补传（可用批量模式）。
- **新增文件纯追加**：`accepted-keys.jsonl`、`quarantine.jsonl` 是本轮新增文件，
  旧数据目录缺失它们时按空集合初始化，不影响既有数据。
- **接口向后兼容**：`CloudEndpoint` 的批量方法是 `default` 方法（默认退化为逐条
  `receive`），既有云端实现无需改动；`UplinkManager` / `TelemetryGateway` 的旧构造
  保持逐条补传语义（批大小 1），既有用例行为不变。

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
| `accepted-keys.jsonl` | 云端已确认接收幂等键账本（去重、跨重启，对账依据） |
| `quarantine.jsonl` | 云端永久拒收隔离记录（原因分类，可查，跨重启保留） |
| `evictions.jsonl` | 缓冲淘汰记录（OLDEST_FIRST） |
| `dead-letter.jsonl` | 拒收记录与原因分类 |
| `alert-events.jsonl` | FIRE/RECOVER 告警事件 |
| `alert-state.json` | 告警窗口状态快照 |
| `rules-current.json`、`rule-versions/*.json` | 当前与历史规则版本 |

## 5. 本地验证方法

前置：JDK 21+、Maven（无需联网到任何业务服务；首次构建需拉取 Maven 依赖）。

```bash
# 运行全部测试（24 个场景用例 + Spring 上下文冒烟）
mvn test

# 端到端演示（输出关键状态变化，数据落 ./data/demo）
mvn compile exec:java -Dexec.mainClass=com.github.lechandonga.edgetelemetrygatewayjava.telemetry.demo.LocalDemo
```

测试覆盖（日志均打印“输入 / 判定依据 / 关键状态变化”，logger 名含 `SCENARIO`）：

- `IngestNormalizationTest`：两种形式归一化、软界降级、8 类拒收原因可区分、乱序与重复、跨设备水位线。
- `BufferAndUplinkTest`：断连积压与 FIFO 有序补传、ACK 抖动幂等、重复/过期确认安全、
  写满 OLDEST_FIRST 淘汰可查询、重启恢复、2 设备 4 指标 200 条并发接力（不丢不重、设备内有序）。
- `BatchUplinkTest`（本轮新增 8 个用例）：
  - 1000 条积压/批大小 64 时云端批量调用 16 次（调用次数与批次数同量级，非上千次）；
  - 配置批大小超硬上限被截断到 256；
  - 部分确认（只确认前 3 条，其余留队，恢复后精确补齐，不重不漏）；
  - 整批回执丢失（一条不出队、重发由云端去重）；
  - 永久拒收（EXPIRED / METRIC_DISCONTINUED）隔离后队头继续前进、
    其余记录相对顺序不变、隔离记录可查且隔离状态跨重启保留、对账平衡；
  - 批量补传中途重启（已确认前缀不重报、未确认后缀从 WAL 恢复，本地账本对账仍平衡）；
  - 升级兼容（旧逐条模式落盘的 `pending.jsonl` 在批量模式下继续补传）；
  - **真并发**：8 台设备、每台 2000 条（双指标，共 32000 条），多线程同时从
    JSON 与寄存器两条入口灌数据，另有补传线程与上报并行（非单线程顺序模拟）；
    断言无异常、不丢不重、设备内顺序严格稳定、四类对账数字精确平衡。
- `AlertEngineTest`：持续窗口、抖动去重、规则热更新窗口迁移、同版本确定性重放/跨版本对比、跨重启状态保持。
- `EndToEndScenarioTest`：正常/异常/降级 → 断连 → 重启 → ACK 抖动 → 补传 → 告警/恢复 → 规则升级 全剧本。
