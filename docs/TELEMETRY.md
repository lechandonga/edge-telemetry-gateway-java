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
- **不丢**：未确认数据始终在 WAL；进程重启时从 `pending.jsonl` 回放恢复（去重后重建队列）。
- **不重（两层幂等）**：
  - 云端 `SimulatedCloud` 按幂等键去重，重复上报只计一次，可通过 `acceptedCount()` 核验；
  - 网关出队校验“确认 key 必须等于当前队头 key”，过期/重复 ACK 无法误删后续数据；
  - ACK 丢失（`simulateAckLossOnce()`：云端收到但回执丢失）时数据不出队，重发由云端去重。
- 多设备并发：缓冲操作全部在同一把监视器锁内串行化，结合设备顺序锁保证不丢不重。

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
| `dead-letter.jsonl` | 拒收记录与原因分类 |
| `alert-events.jsonl` | FIRE/RECOVER 告警事件 |
| `alert-state.json` | 告警窗口状态快照 |
| `rules-current.json`、`rule-versions/*.json` | 当前与历史规则版本 |

## 5. 本地验证方法

前置：JDK 21+、Maven（无需联网到任何业务服务；首次构建需拉取 Maven 依赖）。

```bash
# 运行全部测试（16 个场景用例 + Spring 上下文冒烟）
mvn test

# 端到端演示（输出关键状态变化，数据落 ./data/demo）
mvn compile exec:java -Dexec.mainClass=com.github.lechandonga.edgetelemetrygatewayjava.telemetry.demo.LocalDemo
```

测试覆盖（日志均打印“输入 / 判定依据 / 关键状态变化”，logger 名含 `SCENARIO`）：

- `IngestNormalizationTest`：两种形式归一化、软界降级、8 类拒收原因可区分、乱序与重复、跨设备水位线。
- `BufferAndUplinkTest`：断连积压与 FIFO 有序补传、ACK 抖动幂等、重复/过期确认安全、
  写满 OLDEST_FIRST 淘汰可查询、重启恢复、2 设备 4 指标 200 条并发接力（不丢不重、设备内有序）。
- `AlertEngineTest`：持续窗口、抖动去重、规则热更新窗口迁移、同版本确定性重放/跨版本对比、跨重启状态保持。
- `EndToEndScenarioTest`：正常/异常/降级 → 断连 → 重启 → ACK 抖动 → 补传 → 告警/恢复 → 规则升级 全剧本。
