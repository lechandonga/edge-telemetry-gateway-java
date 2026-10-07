package com.github.lechandonga.edgetelemetrygatewayjava.telemetry.uplink;

import com.github.lechandonga.edgetelemetrygatewayjava.telemetry.buffer.PendingBuffer;
import com.github.lechandonga.edgetelemetrygatewayjava.telemetry.model.TelemetryRecord;
import com.github.lechandonga.edgetelemetrygatewayjava.telemetry.storage.QuarantineStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 补传编排：先持久化入缓冲，再尝试上联（write-ahead）。
 *
 * 不丢：数据入队成功即落盘，只有收到云端幂等回执后才出队；
 * 不重：云端按 idempotencyKey 去重，网关出队校验队头 key，重复 ACK 无效；
 * 有序：严格按全局 FIFO 队头按批发送，批内顺序即缓冲先后顺序，
 *       任何未确认数据都不会被后面的数据插队；
 * 重启：缓冲从 WAL 恢复，进程重启后 drain() 继续补传。
 *
 * 批量补传：批大小可配置且有硬上限（{@link #MAX_BATCH_SIZE}），批量只是发送窗口，
 * 不改变缓冲容量与落盘上界。云端可整批回执、只回执一个前缀或整批回执丢失；
 * 网关严格按顺序解释回执（{@link BatchReceipt}）：只有真正确认接收或已持久化
 * 永久拒收隔离的数据才出队，其余一条不留地按原序重试，云端幂等去重保证不重。
 */
public class UplinkManager {

    private static final Logger log = LoggerFactory.getLogger(UplinkManager.class);
    /** 批大小硬上限：配置值不得超过该上界。 */
    public static final int MAX_BATCH_SIZE = 256;
    /** 缺省批大小（原单条行为等价于 batchSize=1）。 */
    public static final int DEFAULT_BATCH_SIZE = 64;

    private final PendingBuffer buffer;
    private final CloudEndpoint cloud;
    private final QuarantineStore quarantineStore;
    private final int batchSize;
    /** 并发上报/补传串行化：同一时刻只允许一个 drain 在途，避免重复批次。 */
    private final Object drainLock = new Object();
    /** 高并发提交时合并补传：已有 drain 在跑则直接复用，不重复起批次（保证批量效率）。 */
    private final AtomicBoolean draining = new AtomicBoolean(false);

    public UplinkManager(PendingBuffer buffer, CloudEndpoint cloud,
                         QuarantineStore quarantineStore, int configuredBatchSize) {
        this.buffer = buffer;
        this.cloud = cloud;
        this.quarantineStore = quarantineStore;
        int effective = configuredBatchSize <= 0 ? DEFAULT_BATCH_SIZE : configuredBatchSize;
        if (effective > MAX_BATCH_SIZE) {
            log.warn("[UPLINK] 配置批大小 {} 超过硬上限 {}，按 {} 执行",
                    configuredBatchSize, MAX_BATCH_SIZE, MAX_BATCH_SIZE);
            effective = MAX_BATCH_SIZE;
        }
        this.batchSize = effective;
    }

    public UplinkManager(PendingBuffer buffer, CloudEndpoint cloud, QuarantineStore quarantineStore) {
        this(buffer, cloud, quarantineStore, DEFAULT_BATCH_SIZE);
    }

    /**
     * 接收一条已归一化数据：落盘入队，并尽力立即发送。
     * @return 是否为缓冲中新记录（false 表示重复幂等键被忽略）
     */
    public boolean submit(TelemetryRecord record) {
        boolean enqueued = buffer.offer(record);
        if (!enqueued) {
            log.info("[UPLINK] 缓冲中已存在 key={}，幂等忽略", record.idempotencyKey());
            return false;
        }
        log.info("[UPLINK] 入缓冲 key={} 缓冲占用={}/{}",
                record.idempotencyKey(), buffer.size(), buffer.capacity());
        drain();
        return true;
    }

    /**
     * 按 FIFO 分批补传缓冲中所有能发送的数据；遇到断连/未确认前缀即停止，恢复后重发。
     *
     * @return 本次拿到最终结论并出队的条数（云端确认接收 + 永久拒收隔离移除）
     */
    public int drain() {
        synchronized (drainLock) {
            if (!draining.compareAndSet(false, true)) {
                return 0;
            }
            DrainOutcome outcome = new DrainOutcome(0, false);
            try {
                outcome = drainLoop();
                return outcome.resolved();
            } finally {
                draining.set(false);
                // 收尾窗口补漏：最后一批取空与重置标志之间（仍持 drainLock）
                // 可能有新数据入队，持锁重检并补跑，避免数据滞留到下一次显式 flush。
                if (outcome != null && outcome.drainedEmpty()
                        && buffer.size() > 0 && cloud.isAvailable()) {
                    drainLoop();
                }
            }
        }
    }

    private record DrainOutcome(int resolved, boolean drainedEmpty) {
    }

    private DrainOutcome drainLoop() {
        int resolvedTotal = 0;
        while (true) {
            List<TelemetryRecord> batch;
            synchronized (buffer) {
                batch = buffer.peekBatch(batchSize);
            }
            if (batch.isEmpty()) {
                break;
            }
            if (!cloud.isAvailable()) {
                log.info("[UPLINK] 上联不可用，暂停批量补传，滞留 {} 条", buffer.size());
                return new DrainOutcome(resolvedTotal, false);
            }
            Optional<BatchReceipt> response = cloud.receiveBatch(batch);
            if (response.isEmpty()) {
                log.warn("[UPLINK] 批次无任何回执（断连/整批回执丢失），批大小={} 全部留队重发",
                        batch.size());
                return new DrainOutcome(resolvedTotal, false);
            }
            BatchReceipt receipt = response.get();
            List<PendingBuffer.PrefixResult> resolved = new ArrayList<>();
            int accepted = 0;
            int quarantined = 0;
            String stopKey = null;
            for (int i = 0; i < batch.size(); i++) {
                TelemetryRecord record = batch.get(i);
                String key = record.idempotencyKey();
                ItemOutcome outcome = i < receipt.outcomes().size()
                        ? receipt.outcomes().get(i) : ItemOutcome.unresolved();
                if (outcome.accepted()) {
                    resolved.add(new PendingBuffer.PrefixResult(key, false));
                    accepted++;
                } else if (outcome.permanentlyRejected()) {
                    boolean added = quarantineStore.quarantine(record, outcome.reason(),
                            outcome.detail());
                    if (added) {
                        quarantined++;
                    }
                    resolved.add(new PendingBuffer.PrefixResult(key, true));
                } else {
                    stopKey = key;
                    break;
                }
            }
            int removed;
            synchronized (buffer) {
                removed = buffer.resolvePrefix(resolved);
            }
            resolvedTotal += removed;
            log.info("[UPLINK] 批次结论 批大小={} 确认接收={} 隔离={} 实际出队={} 剩余={} 首个未决={}",
                    batch.size(), accepted, quarantined, removed, buffer.size(),
                    stopKey == null ? "无" : stopKey);
            if (removed < batch.size()) {
                return new DrainOutcome(resolvedTotal, false);
            }
        }
        return new DrainOutcome(resolvedTotal, true);
    }

    public int pendingCount() {
        return buffer.size();
    }
}
