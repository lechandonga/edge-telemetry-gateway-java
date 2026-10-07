package com.github.lechandonga.edgetelemetrygatewayjava.telemetry.uplink;

import com.github.lechandonga.edgetelemetrygatewayjava.telemetry.model.TelemetryRecord;
import com.github.lechandonga.edgetelemetrygatewayjava.telemetry.model.CloudRejectReason;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;

/**
 * 本地模拟云端：维护已接收幂等键集合，天然对重复上报去重；
 * 可用开关模拟断连；丢包开关模拟“云端收到但 ACK 丢失”的抖动。
 */
public class SimulatedCloud implements CloudEndpoint {

    private static final Logger log = LoggerFactory.getLogger(SimulatedCloud.class);

    private final AtomicBoolean available = new AtomicBoolean(true);
    private final AtomicBoolean dropNextAck = new AtomicBoolean(false);
    private final AtomicBoolean dropNextBatchReceipt = new AtomicBoolean(false);
    private final Set<String> receivedKeys = new HashSet<>();
    private final List<String> arrivalOrder = new CopyOnWriteArrayList<>();
    private final Object rulesLock = new Object();
    private final List<PermanentRule> permanentRules = new ArrayList<>();
    private volatile int batchConfirmLimit = Integer.MAX_VALUE;
    private final java.util.concurrent.atomic.AtomicInteger batchCallCount =
            new java.util.concurrent.atomic.AtomicInteger();

    private record PermanentRule(Predicate<TelemetryRecord> predicate, CloudRejectReason reason,
                                 String detail) {
    }

    public void setAvailable(boolean up) {
        boolean old = available.getAndSet(up);
        log.info("[CLOUD] 链路状态切换: {} -> {}", old ? "UP" : "DOWN", up ? "UP" : "DOWN");
    }

    /** 下一次 receive 模拟 ACK 丢失：数据会被云端收下，但对网关表现为失败。 */
    public void simulateAckLossOnce() {
        dropNextAck.set(true);
    }

    /** 下一次 receiveBatch 模拟整批回执丢失：云端按幂等收下数据，但对网关不返回任何回执。 */
    public void simulateBatchReceiptLossOnce() {
        dropNextBatchReceipt.set(true);
    }

    /**
     * 之后每个批次最多只对前 n 条给出结果（模拟云端只确认一部分）。
     * 恢复整批回执传 {@link Integer#MAX_VALUE}。
     */
    public void setBatchConfirmLimit(int n) {
        this.batchConfirmLimit = n;
    }

    /** 注册永久拒收规则：命中的数据云端明确永远不接收（如历史超时限/指标下线）。 */
    public void rejectPermanentlyIf(Predicate<TelemetryRecord> predicate, CloudRejectReason reason,
                                    String detail) {
        synchronized (rulesLock) {
            permanentRules.add(new PermanentRule(predicate, reason, detail));
        }
    }

    private PermanentRule matchPermanent(TelemetryRecord record) {
        synchronized (rulesLock) {
            for (PermanentRule rule : permanentRules) {
                if (rule.predicate().test(record)) {
                    return rule;
                }
            }
        }
        return null;
    }

    @Override
    public synchronized Optional<String> receive(TelemetryRecord record) {
        if (!available.get()) {
            return Optional.empty();
        }
        PermanentRule reject = matchPermanent(record);
        if (reject != null) {
            log.warn("[CLOUD] 永久拒收 device={} metric={} ts={} reason={} detail={}",
                    record.deviceId(), record.metric(), record.timestamp(),
                    reject.reason(), reject.detail());
            return Optional.empty();
        }
        boolean firstTime = receivedKeys.add(record.idempotencyKey());
        if (firstTime) {
            arrivalOrder.add(record.idempotencyKey());
        }
        log.info("[CLOUD] 收到 device={} metric={} ts={} 重复={}",
                record.deviceId(), record.metric(), record.timestamp(), !firstTime);
        if (dropNextAck.compareAndSet(true, false)) {
            log.warn("[CLOUD] 模拟 ACK 丢失 key={}", record.idempotencyKey());
            return Optional.empty();
        }
        return Optional.of("ACK-" + record.idempotencyKey());
    }

    @Override
    public synchronized Optional<BatchReceipt> receiveBatch(List<TelemetryRecord> batch) {
        batchCallCount.incrementAndGet();
        if (!available.get()) {
            return Optional.empty();
        }
        if (dropNextBatchReceipt.compareAndSet(true, false)) {
            // 整批回执丢失：云端仍按幂等收下非永久拒收数据，但不给网关任何回执。
            for (TelemetryRecord record : batch) {
                if (matchPermanent(record) == null && receivedKeys.add(record.idempotencyKey())) {
                    arrivalOrder.add(record.idempotencyKey());
                }
            }
            log.warn("[CLOUD] 模拟整批回执丢失 批大小={}，数据已收下，网关需整批重发", batch.size());
            return Optional.empty();
        }
        int limit = batchConfirmLimit;
        int answerCount = Math.min(batch.size(), Math.max(0, limit));
        List<ItemOutcome> outcomes = new ArrayList<>(answerCount);
        for (int i = 0; i < answerCount; i++) {
            TelemetryRecord record = batch.get(i);
            PermanentRule reject = matchPermanent(record);
            if (reject != null) {
                log.warn("[CLOUD] 批量永久拒收 device={} metric={} ts={} reason={}",
                        record.deviceId(), record.metric(), record.timestamp(), reject.reason());
                outcomes.add(ItemOutcome.permanentlyRejected(reject.reason(), reject.detail()));
                continue;
            }
            boolean firstTime = receivedKeys.add(record.idempotencyKey());
            if (firstTime) {
                arrivalOrder.add(record.idempotencyKey());
            }
            log.info("[CLOUD] 批量收到 device={} metric={} ts={} 重复={} 批内位置={}",
                    record.deviceId(), record.metric(), record.timestamp(), !firstTime, i);
            if (dropNextAck.compareAndSet(true, false)) {
                log.warn("[CLOUD] 模拟批内 ACK 丢失 key={}（其后结果不再给出）",
                        record.idempotencyKey());
                outcomes.add(ItemOutcome.unresolved());
                break;
            }
            outcomes.add(ItemOutcome.accepted("ACK-" + record.idempotencyKey()));
        }
        if (answerCount < batch.size()) {
            log.warn("[CLOUD] 仅回执批内前 {}/{} 条，其余留待网关重发",
                    answerCount, batch.size());
        }
        return Optional.of(new BatchReceipt(outcomes));
    }

    @Override
    public boolean isAvailable() {
        return available.get();
    }

    @Override
    public synchronized int acceptedCount() {
        return receivedKeys.size();
    }

    /** 云端实际收到（含回执丢失）的不同幂等键数。 */
    public synchronized int observedCount() {
        return receivedKeys.size();
    }

    /** 批量 receiveBatch 被调用的次数（验证补传调用次数降到批次数量级）。 */
    public int batchCallCount() {
        return batchCallCount.get();
    }

    public synchronized boolean hasReceived(String key) {
        return receivedKeys.contains(key);
    }

    public List<String> arrivalOrder() {
        return List.copyOf(arrivalOrder);
    }
}
