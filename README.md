# 边缘网关遥测采集与断连补传（本地模拟）

设备端与云端全部在单进程内本地模拟，无任何外部服务依赖。实现了：多形式上报归一、
可区分的拒收/降级、本地有界持久缓冲、断连按序补传与幂等确认、跨重启不丢不重、
带版本的持续时长阈值告警与确定性历史重放。

## 1. 遥测模型

两种上报形式最终归一为 `model/TelemetryRecord`：

| 字段 | 含义 |
| --- | --- |
| `recordId` | 缓冲层分配的全局单调 id，全链路去重唯一标识 |
| `deviceId` / `metric` | 设备与指标 |
| `value` / `timestampMs` | 指标值与有效时间戳 |
| `deviceSeq` | 归一化层按设备单调分配的序号，保证同设备判定顺序稳定 |
| `sourceType` | `REGISTER_SAMPLE`（寄存器采样）或 `JSON_REPORT`（JSON 报文） |
| `quality` | `GOOD` / `TIMESTAMP_CLAMPED`（乱序被钳制的降级标记） |

- 寄存器采样：`RegisterSample(deviceId, registerAddress, value, ts)`，寄存器地址通过
  `DeviceSpec.registerMap` 映射为指标。
- JSON 报文：`{"deviceId","metric","value","ts"}`。

归一化结果三态（`model/IngestResult.Status`），**绝不静默丢弃**：

| 结果 | 含义 |
| --- | --- |
| `ACCEPTED` | 正常进入链路 |
| `DEGRADED` | 保留但降级：默认乱序策略为钳制（时间戳=设备最新值，`quality=TIMESTAMP_CLAMPED`） |
| `REJECTED` | 拒收，`RejectReason` 区分：`MISSING_FIELD` / `VALUE_OUT_OF_RANGE` / `UNKNOWN_DEVICE` / `MALFORMED_PAYLOAD` / `OUT_OF_ORDER` |

乱序策略可在 `Normalizer` 构造时选择：`CLAMP`（默认，降级保留）或 `REJECT`（拒收）。
同一 `deviceId` 的处理在归一化层串行化，因此多线程并发下 `deviceSeq` 与时间戳判定仍稳定。

## 2. 缓冲与补传语义

实现位于 `buffer/` 与 `uplink/`。

- **有界**：容量固定（演示为 50，测试可配），占用永不超过容量，`BufferStats` 可随时查询
  （`pendingCount/totalAccepted/totalAcked/totalEvicted/evictionPolicy`）。
- **淘汰规则确定**：写满时按 FIFO 淘汰最早进入（最小 `recordId`）的记录；被淘汰记录追加写入
  `evictions.log`（`TelemetryBuffer.evictionJournal()` 可查询淘汰流水）。
- **持久化**：每次变更写原子快照（`*.tmp` + `ATOMIC_MOVE`）。进程重启后 pending、`recordId`
  水位与全部计数完整恢复。
- **补传顺序**：恢复后 `Gateway.onLinkRestored()` / `ReplayService.replayPending()` 按
  `recordId` 升序逐条“云端落库 → 本地确认”；中途再次断链则剩余记录有序保留。
- **幂等**：
  - 云端模拟 `CloudIngestService` 自身按 `recordId` 去重，重复上报得到相同确认且只存一份；
  - `TelemetryBuffer.confirm` 幂等，重复确认返回 `DUPLICATE_ACK`（未知 id 返回 `UNKNOWN_ID`）；
  - 崩溃发生在“云端落库”与“本地确认”之间时，重启重发同一条被云端去重——仍恰好有效一次。
- **并发**：缓冲内部全局串行化，多设备并发 append 不丢不重（见
  `BufferReplayTest.concurrentMultiDeviceNoLossNoDuplicate`，8 线程 × 50 条）。

状态文件（默认在运行目录下，演示/测试各自独立目录）：

```
<stateDir>/buffer/buffer-state.json   # 待补传记录 + recordId 水位 + 计数
<stateDir>/buffer/evictions.log       # FIFO 淘汰流水
<stateDir>/alert/alert-rules.json     # 全部历史版本规则
<stateDir>/alert/alert-state.json     # 各 (device,metric) 窗口状态
<stateDir>/alert/alert-events.json    # 已触发告警审计（eventKey 去重）
```

## 3. 告警规则与配置版本

实现位于 `alert/`。

- 规则 `AlertRule(metric, gtThreshold, durationMs, version, createdAtMs)` **不可变**；
  更新规则 = `RuleStore.publish(...)` 追加一条更高 `version`（版本按指标自增）。
- 判定：同一 `(device, metric)` 值连续满足 `value > gtThreshold` 达到 `durationMs` 触发一次；
  任意一条未越限样本立即复位窗口。短时间反复越限/恢复产生不同 `windowStart` 的独立窗口，
  各自最多触发一次，已触发窗口在复位前不重复告警。
- `eventKey = deviceId|metric|v<version>|<windowStart>`，持久化审计按 key 去重。
- **跨重启**：窗口状态与已触发事件均持久化；重启后进行中的窗口继续计时，已触发的不重复。
- **规则更新与在途窗口**：窗口开窗时绑定规则版本并沿用到底（触发或复位），新窗口才取最新版本。
  因此更新过程中已进入窗口的判定既不会被丢弃，也不会被新版本重复触发。
- **结论可复现**：`AlertReplay.replay(history, rule)` 是无状态纯函数，与在线引擎同一判定算法；
  同一段历史 + 同一版本规则，任意次重放得到完全相同的事件序列；换版本则得到该版本下的确定结论。

## 4. 包结构

```
model/      遥测记录、规则、告警事件、拒收原因等领域模型
ingest/     设备登记、两种上报解析、归一化与顺序/降级策略
buffer/     有界持久缓冲、FIFO 淘汰、幂等确认、统计与流水
uplink/     链路通断模拟、云端去重模拟、按序补传服务
alert/      版本化规则库、有状态告警引擎、事件审计、无状态历史重放
pipeline/   Gateway 编排：归一化 -> 缓冲 -> 即时/恢复补传 -> 告警
demo/       LocalDemo 端到端本地模拟（8 个阶段）
```

## 5. 本地验证方法

全部使用 JUnit 5 与 JDK 临时目录，离线可跑，不依赖外部服务：

```bash
mvn test
```

覆盖场景（共 15 个用例，含输入、判定依据与关键状态变化日志）：

- `IngestNormalizationTest`：两种形式归一、五类拒收可区分、乱序钳制降级/拒收、同设备顺序稳定。
- `BufferReplayTest`：断连积压与按序补传、重复确认/重复补传幂等、重启恢复、链路抖动、
  缓冲写满 FIFO 淘汰与流水查询、多设备并发不丢不重。
- `AlertRuleVersionTest`：持续窗口触发一次、抖动新窗口、重启不重复触发、
  规则更新在途窗口版本固定、历史重放确定性。

端到端演示（断连/并发/淘汰/规则更新/重启/重放 8 个阶段，打印完整决策日志）：

```bash
# 方式一：直接运行（IDE 中运行 main 最简单）
mvn -q compile
java -cp "target/classes:$(find ~/.m2/repository -name '*.jar' | tr '\n' ':')" \
  com.github.lechandonga.edgetelemetrygatewayjava.demo.LocalDemo

# 演示状态写入 target/demo-state/，重新运行会清空重建
```
