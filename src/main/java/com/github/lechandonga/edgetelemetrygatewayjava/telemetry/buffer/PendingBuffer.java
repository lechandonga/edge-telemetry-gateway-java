package com.github.lechandonga.edgetelemetrygatewayjava.telemetry.buffer;

import com.github.lechandonga.edgetelemetrygatewayjava.telemetry.model.TelemetryRecord;
import com.github.lechandonga.edgetelemetrygatewayjava.telemetry.storage.EvictionEntry;
import com.github.lechandonga.edgetelemetrygatewayjava.telemetry.storage.EvictionLog;
import com.github.lechandonga.edgetelemetrygatewayjava.telemetry.storage.AcceptedLedger;
import com.github.lechandonga.edgetelemetrygatewayjava.telemetry.storage.Journal;
import com.github.lechandonga.edgetelemetrygatewayjava.telemetry.storage.QuarantineLog;

import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * 断连期间的本地有界缓冲。
 *
 * 语义：
 * - 全局 FIFO：入队按设备交错顺序，补传时保持该顺序；同设备数据天然保持设备内顺序。
 * - 容量硬上界 capacity：写满后按 OLDEST_FIRST（最旧优先）淘汰，被淘汰数据写入淘汰日志，
 *   内存/磁盘占用永不无限增长。
 * - 每条记录入队即落 WAL（pending.jsonl，逐条 fsync 式 append）；
 *   确认出队时重写压缩。进程重启后从 WAL 恢复，不丢未确认数据。
 * - 幂等：同一 idempotencyKey 的记录在缓冲中只保留一份（设备/指标/时间戳去重）。
 *
 * 批量补传下新增两类“有序离队”：
 * - {@link #peekBatch(int)} 取出队头连续一批（批内就是全局 FIFO 顺序，
 *   因而批边界不会重排任何设备的数据）；
 * - {@link #resolveBatch} 按云端回执的顺序前缀原子落定：
 *   ACCEPTED 直接出队，PERMANENTLY_REJECTED 从队列中移除并写入隔离区——
 *   被隔离记录从序列中间挪走时，其前后记录的相对顺序保持不变；
 *   遇到第一个 RETRYABLE（或缺回执/键不匹配）立即停止，未确认的一条都不出队、
 *   也不允许被后面的数据插队。整批复用同一次 WAL 压缩重写，
 *   批量发送不会引入超出容量上界的内存/磁盘占用。
 */
public class PendingBuffer {

    private final int capacity;
    private final Journal<TelemetryRecord> wal;
    private final EvictionLog evictionLog;
    private final QuarantineLog quarantineLog;
    private final AcceptedLedger acceptedLedger;

    private final Deque<TelemetryRecord> queue = new ArrayDeque<>();
    private final Set<String> pendingKeys = new HashSet<>();
    /**
     * WAL 压缩节流：队头离队（确认/拒收/淘汰）不逐条重写文件，而是累计待压缩离队条数；
     * 达到 {@link #COMPACTION_THRESHOLD} 或缓冲清空时才重写一次。
     *
     * 崩溃语义仍然安全：WAL 是仅追加日志，离队记录还留在文件里只是“冗余前缀”，
     * 重启回放时用当前去重集合重建队列（见 {@link #loadFromWal}），
     * 已确认/已隔离/已淘汰的键不会重新进入队列——冗余只占顺序文件空间，不影响正确性。
     * 空间上界：每次压缩把文件截到“当前队列 + 阈值内冗余”，磁盘占用有硬上界。
     */
    private int pendingCompaction;

    /** 离队条数达到该阈值即压缩一次 WAL（批量 256 时单批最多产生 256 条冗余）。 */
    private static final int COMPACTION_THRESHOLD = 256;

    public PendingBuffer(Path dataDir, int capacity, EvictionLog evictionLog) {
        this(dataDir, capacity, evictionLog, null, null);
    }

    public PendingBuffer(Path dataDir, int capacity, EvictionLog evictionLog,
                         QuarantineLog quarantineLog) {
        this(dataDir, capacity, evictionLog, quarantineLog, null);
    }

    public PendingBuffer(Path dataDir, int capacity, EvictionLog evictionLog,
                         QuarantineLog quarantineLog, AcceptedLedger acceptedLedger) {
        this.capacity = capacity;
        this.evictionLog = evictionLog;
        this.quarantineLog = quarantineLog;
        this.acceptedLedger = acceptedLedger;
        this.wal = new Journal<>(dataDir.resolve("pending.jsonl"), TelemetryRecord.class);
        loadFromWal();
    }

    private void loadFromWal() {
        Set<String> evictedKeys = new HashSet<>();
        for (var entry : evictionLog.all()) {
            evictedKeys.add(entry.record().idempotencyKey());
        }
        Set<String> quarantinedKeys = new HashSet<>();
        if (quarantineLog != null) {
            for (var entry : quarantineLog.all()) {
                quarantinedKeys.add(entry.idempotencyKey());
            }
        }
        for (TelemetryRecord record : wal.readAll()) {
            String key = record.idempotencyKey();
            if (evictedKeys.contains(key) || quarantinedKeys.contains(key)
                    || (acceptedLedger != null && acceptedLedger.containsKey(key))) {
                continue;
            }
            if (pendingKeys.add(key)) {
                queue.addLast(record);
            }
        }
        pendingCompaction = 0;
    }

    /**
     * @return 入队结果；若因重复已在缓冲中则返回 false（幂等忽略）。
     */
    public synchronized boolean offer(TelemetryRecord record) {
        String key = record.idempotencyKey();
        if (pendingKeys.contains(key)) {
            return false;
        }
        boolean evictedNow = false;
        while (queue.size() >= capacity) {
            TelemetryRecord victim = queue.pollFirst();
            if (victim == null) {
                break;
            }
            pendingKeys.remove(victim.idempotencyKey());
            evictionLog.record(new EvictionEntry(System.currentTimeMillis(),
                    EvictionEntry.OLDEST_FIRST, victim));
            pendingCompaction++;
            evictedNow = true;
        }
        queue.addLast(record);
        pendingKeys.add(key);
        // 淘汰发生时必须立刻压缩 WAL：被淘汰记录不能残留在仅追加日志中，
        // 否则会与并发补传的确认/账本交错，造成崩溃后复活或误对账。
        // 淘汰仅在缓冲写满（罕见）时发生，且同一次 offer 内的多条淘汰合并为一次重写。
        if (evictedNow) {
            rewriteWal();
            pendingCompaction = 0;
            wal.append(record);
            return true;
        }
        if (pendingCompaction >= COMPACTION_THRESHOLD) {
            rewriteWal();
            pendingCompaction = 0;
        }
        wal.append(record);
        return true;
    }

    /** 查看队头但不出队（两阶段确认的第一阶段）。 */
    public synchronized TelemetryRecord peek() {
        return queue.peekFirst();
    }

    /**
     * 确认队头数据已被云端接收，正式出队。对同一批数据重复调用 ack：
     * 队头已变更时 key 不匹配则返回 false，绝不误删新数据（幂等确认）。
     */
    public synchronized boolean ack(String idempotencyKey) {
        TelemetryRecord head = queue.peekFirst();
        if (head == null || !head.idempotencyKey().equals(idempotencyKey)) {
            return false;
        }
        queue.pollFirst();
        pendingKeys.remove(idempotencyKey);
        if (acceptedLedger != null) {
            acceptedLedger.record(idempotencyKey);
        }
        pendingCompaction++;
        maybeCompact();
        return true;
    }

    public synchronized int size() {
        return queue.size();
    }

    public int capacity() {
        return capacity;
    }

    public synchronized boolean containsKey(String key) {
        return pendingKeys.contains(key);
    }

    /** 注意：返回快照副本。 */
    public synchronized List<TelemetryRecord> snapshot() {
        return new ArrayList<>(queue);
    }

    /**
     * 取出队头连续至多 maxBatch 条记录的快照（不出队）。
     * 顺序就是缓冲全局 FIFO 顺序——同一设备的记录在批内、跨批都保持先后。
     */
    public synchronized List<TelemetryRecord> peekBatch(int maxBatch) {
        int n = Math.min(maxBatch, queue.size());
        List<TelemetryRecord> batch = new ArrayList<>(n);
        int i = 0;
        for (TelemetryRecord record : queue) {
            if (i++ >= n) {
                break;
            }
            batch.add(record);
        }
        return batch;
    }

    /**
     * 按顺序前缀落定一批回执。
     *
     * 处理规则：
     * - 依次比对回执与当前队头，下标/幂等键必须与发送批次完全一致；
     * - ACCEPTED：出队；
     * - PERMANENTLY_REJECTED：出队并写入隔离区（带云端分类原因）；
     * - RETRYABLE、回执缺失、键不匹配：立即停止，该条及其后全部留在队列，顺序不变。
     *
     * @param receipts 下标、key、状态三要素必须与 {@link #peekBatch} 发出的批次对应；
     *                 允许只覆盖批次前缀（部分确认）。
     * @return 本次落定结果（确认数、隔离数、是否因未确认而中断）。
     */
    public synchronized BatchResolution resolveBatch(
            List<? extends BatchReceiptView> receipts) {
        int accepted = 0;
        int quarantined = 0;
        int processed = 0;
        boolean stalled = false;
        for (BatchReceiptView receipt : receipts) {
            TelemetryRecord head = queue.peekFirst();
            if (head == null
                    || receipt.index() != processed
                    || !head.idempotencyKey().equals(receipt.key())) {
                stalled = true;
                break;
            }
            if (receipt.status() == BatchOutcome.RETRYABLE) {
                stalled = true;
                break;
            }
            TelemetryRecord removed = queue.pollFirst();
            pendingKeys.remove(removed.idempotencyKey());
            processed++;
            if (receipt.status() == BatchOutcome.ACCEPTED) {
                if (acceptedLedger != null) {
                    acceptedLedger.record(removed.idempotencyKey());
                }
                accepted++;
            } else {
                if (quarantineLog == null) {
                    // 未装配隔离区的旧装配方式下，永久拒收退化为留队重试，绝不静默丢弃。
                    queue.addFirst(removed);
                    pendingKeys.add(removed.idempotencyKey());
                    stalled = true;
                    processed--;
                    break;
                }
                quarantineLog.record(removed, receipt.reasonCode(), receipt.detail());
                quarantined++;
            }
        }
        if (processed > 0) {
            pendingCompaction += processed;
        }
        maybeCompact();
        return new BatchResolution(accepted, quarantined, stalled);
    }

    /** 达到阈值或队列已清空时压缩 WAL，把冗余离队前缀截断，磁盘占用维持有界。 */
    private void maybeCompact() {
        if (pendingCompaction >= COMPACTION_THRESHOLD || queue.isEmpty()) {
            rewriteWal();
            pendingCompaction = 0;
        }
    }

    /** 批内单条回执视图（由补传编排层把云端回执适配进来）。 */
    public interface BatchReceiptView {
        int index();

        String key();

        BatchOutcome status();

        com.github.lechandonga.edgetelemetrygatewayjava.telemetry.model.CloudRejectCode reasonCode();

        String detail();
    }

    /** 缓冲视角的三种落定结论。 */
    public enum BatchOutcome {
        ACCEPTED,
        RETRYABLE,
        PERMANENTLY_REJECTED
    }

    /** 一批回执落定后的计数结果。 */
    public record BatchResolution(int accepted, int quarantined, boolean stalled) {
    }

    private void rewriteWal() {
        wal.rewrite(new ArrayList<>(queue));
    }
}
