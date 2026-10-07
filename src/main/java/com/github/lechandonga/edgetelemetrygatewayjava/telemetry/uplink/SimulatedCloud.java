package com.github.lechandonga.edgetelemetrygatewayjava.telemetry.uplink;

import com.github.lechandonga.edgetelemetrygatewayjava.telemetry.model.TelemetryRecord;
import com.github.lechandonga.edgetelemetrygatewayjava.telemetry.model.CloudRejectCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 本地模拟云端：维护已接收幂等键集合，天然对重复上报去重；
 * 可用开关模拟断连；丢包开关模拟“云端收到但 ACK 丢失”的抖动。
 */
public class SimulatedCloud implements CloudEndpoint {

    private static final Logger log = LoggerFactory.getLogger(SimulatedCloud.class);

    private final AtomicBoolean available = new AtomicBoolean(true);
    private final AtomicBoolean dropNextAck = new AtomicBoolean(false);
    private final AtomicBoolean dropNextBatchReceipt = new AtomicBoolean(false);
    private final AtomicInteger partialConfirmNext = new AtomicInteger(-1);
    private final AtomicBoolean downAfterNextBatch = new AtomicBoolean(false);
    private final AtomicInteger batchCalls = new AtomicInteger(0);
    private final Set<String> receivedKeys = new HashSet<>();
    private final List<String> arrivalOrder = new CopyOnWriteArrayList<>();

    public void setAvailable(boolean up) {
        boolean old = available.getAndSet(up);
        log.info("[CLOUD] 链路状态切换: {} -> {}", old ? "UP" : "DOWN", up ? "UP" : "DOWN");
    }

    /** 下一次 receive 模拟 ACK 丢失：数据会被云端收下，但对网关表现为失败。 */
    public void simulateAckLossOnce() {
        dropNextAck.set(true);
    }

    /** 下一次批量调用整批回执丢失：云端可能收下部分/全部数据，但网关一条确认都拿不到。 */
    public void simulateBatchReceiptLossOnce() {
        dropNextBatchReceipt.set(true);
    }

    /**
     * 下一次批量调用只对前 confirmPrefix 条给出回执（部分确认）；
     * 其余记录云端视为“本次未给出结论”，网关必须整批前缀化处理后重试余下数据。
     */
    public void simulatePartialConfirmOnce(int confirmPrefix) {
        partialConfirmNext.set(confirmPrefix);
    }

    /** 下一次批量调用结束后立刻断连：用于精确制造“确认了前缀、剩余要等下次补传”的边界。 */
    public void simulateDownAfterNextBatch() {
        downAfterNextBatch.set(true);
    }

    /** 令指定幂等键的数据此后永远被云端永久拒收（例如已超接收时限/指标停收）。 */
    public void addPermanentReject(String key, CloudRejectCode code, String detail) {
        rejectReasons.put(key, new Reject(code, detail));
    }

    private final java.util.Map<String, Reject> rejectReasons = new ConcurrentHashMap<>();

    private record Reject(CloudRejectCode code, String detail) {
    }

    @Override
    public synchronized Optional<String> receive(TelemetryRecord record) {
        if (!available.get()) {
            return Optional.empty();
        }
        Reject reject = rejectReasons.get(record.idempotencyKey());
        if (reject != null) {
            log.warn("[CLOUD] 永久拒收 key={} code={} detail={}",
                    record.idempotencyKey(), reject.code(), reject.detail());
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
    public synchronized BatchReceipt receiveBatch(List<TelemetryRecord> batch) {
        batchCalls.incrementAndGet();
        log.info("[CLOUD] 收到补传批次 size={} 第{}次批量调用", batch.size(), batchCalls.get());
        if (!available.get()) {
            return BatchReceipt.lost();
        }
        if (dropNextAck.compareAndSet(true, false)) {
            // 兼容旧的单条 ACK 抖动注入：云端收下队头但对网关表现为整批无确认。
            for (TelemetryRecord record : batch) {
                acceptIfAllowed(record);
            }
            log.warn("[CLOUD] 模拟单条 ACK 丢失（批量语义下视为队头无确认），整批留重试");
            return BatchReceipt.lost();
        }
        if (dropNextBatchReceipt.compareAndSet(true, false)) {
            // 云端仍可能已去重收下数据，但回执整体丢失：先照收，再返回空回执。
            for (TelemetryRecord record : batch) {
                acceptIfAllowed(record);
            }
            log.warn("[CLOUD] 模拟整批回执丢失 size={}", batch.size());
            return BatchReceipt.lost();
        }
        int prefixLimit = partialConfirmNext.getAndSet(-1);
        List<RecordReceipt> receipts = new ArrayList<>();
        for (int i = 0; i < batch.size(); i++) {
            if (prefixLimit >= 0 && i >= prefixLimit) {
                break;
            }
            TelemetryRecord record = batch.get(i);
            Reject reject = rejectReasons.get(record.idempotencyKey());
            if (reject != null) {
                log.warn("[CLOUD] 批量永久拒收 key={} code={}", record.idempotencyKey(), reject.code());
                receipts.add(RecordReceipt.rejected(i, record.idempotencyKey(),
                        reject.code(), reject.detail()));
                continue;
            }
            if (acceptIfAllowed(record)) {
                receipts.add(RecordReceipt.accepted(i, record.idempotencyKey()));
            } else {
                // 本模拟中非永久拒收的不成功即可恢复失败：顺序前缀到此为止。
                receipts.add(RecordReceipt.retryable(i, record.idempotencyKey(), "云端暂未接收"));
                break;
            }
        }
        if (downAfterNextBatch.compareAndSet(true, false)) {
            available.set(false);
            log.warn("[CLOUD] 本次批量调用后链路断开，剩余记录须等恢复后补传");
        }
        return new BatchReceipt(receipts);
    }

    /** @return true 表示首次/重复去重后已接收；false 表示未接收。 */
    private boolean acceptIfAllowed(TelemetryRecord record) {
        if (rejectReasons.containsKey(record.idempotencyKey())) {
            return false;
        }
        boolean firstTime = receivedKeys.add(record.idempotencyKey());
        if (firstTime) {
            arrivalOrder.add(record.idempotencyKey());
        }
        return true;
    }

    @Override
    public boolean isAvailable() {
        return available.get();
    }

    @Override
    public synchronized int acceptedCount() {
        return receivedKeys.size();
    }

    @Override
    public int batchCallCount() {
        return batchCalls.get();
    }

    @Override
    public synchronized boolean hasReceived(String key) {
        return receivedKeys.contains(key);
    }

    public List<String> arrivalOrder() {
        return List.copyOf(arrivalOrder);
    }
}
