package com.github.lechandonga.edgetelemetrygatewayjava.telemetry.uplink;

import com.github.lechandonga.edgetelemetrygatewayjava.telemetry.buffer.PendingBuffer;
import com.github.lechandonga.edgetelemetrygatewayjava.telemetry.buffer.PendingBuffer.BatchOutcome;
import com.github.lechandonga.edgetelemetrygatewayjava.telemetry.buffer.PendingBuffer.BatchReceiptView;
import com.github.lechandonga.edgetelemetrygatewayjava.telemetry.buffer.PendingBuffer.BatchResolution;
import com.github.lechandonga.edgetelemetrygatewayjava.telemetry.model.CloudRejectCode;
import com.github.lechandonga.edgetelemetrygatewayjava.telemetry.model.TelemetryRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * 补传编排：先持久化入缓冲，再尝试上联（write-ahead）。
 *
 * 不丢：数据入队成功即落盘，只有收到云端幂等回执后才出队；
 * 不重：云端按 idempotencyKey 去重，网关出队校验队头 key，重复 ACK 无效；
 * 有序：严格按全局 FIFO 队头发送，前一条未确认则不发后一条；
 * 重启：缓冲从 WAL 恢复，进程重启后 drain() 继续补传。
 *
 * 批量补传：每次取队头连续一批（默认 {@link #DEFAULT_BATCH_SIZE}，
 * 配置再大也不超过硬上限 {@link #MAX_BATCH_SIZE}）调用一次云端批量接口；
 * 云端可整批回执、只回执前缀（部分确认）或整批回执丢失（空回执）。
 * 网关只按“顺序前缀”落定：确认的出队、永久拒收的入隔离区后继续推进，
 * 第一个未确认处整体停下重试——未确认的不丢、不被插队，重试不重不漏。
 * 批大小只影响发送/确认粒度，不改变缓冲容量与 WAL 占用上界。
 */
public class UplinkManager {

    private static final Logger log = LoggerFactory.getLogger(UplinkManager.class);

    /** 默认批大小。 */
    public static final int DEFAULT_BATCH_SIZE = 64;
    /** 批大小硬上限：配置超过该值一律截断，防止单批内存/报文失控。 */
    public static final int MAX_BATCH_SIZE = 256;

    private final PendingBuffer buffer;
    private final CloudEndpoint cloud;
    private final int batchSize;

    /** 兼容旧装配：等价于批大小 1 的逐条补传（旧行为、旧调用模式完全不变）。 */
    public UplinkManager(PendingBuffer buffer, CloudEndpoint cloud) {
        this(buffer, cloud, 1);
    }

    public UplinkManager(PendingBuffer buffer, CloudEndpoint cloud, int configuredBatchSize) {
        this.buffer = buffer;
        this.cloud = cloud;
        int normalized = configuredBatchSize <= 0 ? DEFAULT_BATCH_SIZE : configuredBatchSize;
        if (normalized > MAX_BATCH_SIZE) {
            log.warn("[UPLINK] 配置批大小 {} 超过硬上限 {}，截断为 {}",
                    configuredBatchSize, MAX_BATCH_SIZE, MAX_BATCH_SIZE);
            normalized = MAX_BATCH_SIZE;
        }
        this.batchSize = normalized;
    }

    public int batchSize() {
        return batchSize;
    }

    /**
     * 接收一条已归一化数据：落盘入队，并尽力立即发送。
     * @return 是否为缓冲中新记录（false 表示重复幂等键被忽略）
     */
    public synchronized boolean submit(TelemetryRecord record) {
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
     * 按 FIFO 补传缓冲中所有能发送的数据；遇到断连即停止，已恢复后重发。
     *
     * @return 本次成功确认出队的条数
     */
    public synchronized int drain() {
        int confirmedTotal = 0;
        int quarantinedTotal = 0;
        int batches = 0;
        while (true) {
            List<TelemetryRecord> batch = buffer.peekBatch(batchSize);
            if (batch.isEmpty()) {
                break;
            }
            if (!cloud.isAvailable()) {
                log.info("[UPLINK] 上联不可用，暂停补传，滞留 {} 条", buffer.size());
                break;
            }
            batches++;
            BatchReceipt receipt = cloud.receiveBatch(batch);
            BatchResolution resolution = buffer.resolveBatch(adapt(receipt));
            confirmedTotal += resolution.accepted();
            quarantinedTotal += resolution.quarantined();
            log.info("[UPLINK] 批次落定 发送={} 回执={} 确认={} 隔离={} 滞留={}{}",
                    batch.size(), receipt.size(), resolution.accepted(),
                    resolution.quarantined(), buffer.size(),
                    resolution.stalled() ? " 未确认前缀->停留重试" : "");
            // 空队列、整批未获任何确认（断连/整批回执丢失/可恢复失败）都在此停下。
            if (resolution.stalled() || receipt.isEmpty()
                    || resolution.accepted() + resolution.quarantined() == 0) {
                if (receipt.isEmpty()) {
                    log.warn("[UPLINK] 整批无回执（可能整批回执丢失/断连），{} 条原样留待重试",
                            batch.size());
                }
                break;
            }
        }
        log.info("[UPLINK] 本轮补传结束 批次调用={} 累计确认={} 累计隔离={} 滞留={}",
                batches, confirmedTotal, quarantinedTotal, buffer.size());
        return confirmedTotal;
    }

    public synchronized int pendingCount() {
        return buffer.size();
    }

    /** 云端批量接口累计调用次数（效率观测：积压 N、批大小 B 时应为 ceil(N/B) 量级）。 */
    public synchronized int cloudBatchCalls() {
        return cloud.batchCallCount();
    }

    private static List<BatchReceiptView> adapt(BatchReceipt receipt) {
        List<BatchReceiptView> views = new java.util.ArrayList<>(receipt.records().size());
        for (var r : receipt.records()) {
            views.add(new BatchReceiptView() {
            @Override
            public int index() {
                return r.index();
            }

            @Override
            public String key() {
                return r.key();
            }

            @Override
            public BatchOutcome status() {
                return switch (r.status()) {
                    case ACCEPTED -> BatchOutcome.ACCEPTED;
                    case RETRYABLE -> BatchOutcome.RETRYABLE;
                    case PERMANENTLY_REJECTED -> BatchOutcome.PERMANENTLY_REJECTED;
                };
            }

            @Override
            public CloudRejectCode reasonCode() {
                return r.code();
            }

            @Override
            public String detail() {
                return r.detail();
            }
            });
        }
        return views;
    }
}
