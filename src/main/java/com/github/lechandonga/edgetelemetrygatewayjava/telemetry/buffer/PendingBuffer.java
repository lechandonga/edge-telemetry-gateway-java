package com.github.lechandonga.edgetelemetrygatewayjava.telemetry.buffer;

import com.github.lechandonga.edgetelemetrygatewayjava.telemetry.model.TelemetryRecord;
import com.github.lechandonga.edgetelemetrygatewayjava.telemetry.storage.EvictionEntry;
import com.github.lechandonga.edgetelemetrygatewayjava.telemetry.storage.EvictionLog;
import com.github.lechandonga.edgetelemetrygatewayjava.telemetry.storage.Journal;
import com.github.lechandonga.edgetelemetrygatewayjava.telemetry.storage.QuarantineStore;

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
 * 批量补传下新增“前缀确认”：一次可以按队头顺序确认/隔离移除一个前缀，
 * 只有结果序列与当前队头逐条匹配的部分才会出队，任何不匹配即停止——
 * 没确认的一条都不会丢，也不会被后面的数据插队。
 */
public class PendingBuffer {

    private final int capacity;
    private final Journal<TelemetryRecord> wal;
    private final EvictionLog evictionLog;
    private final QuarantineStore quarantineStore;

    private final Deque<TelemetryRecord> queue = new ArrayDeque<>();
    private final Set<String> pendingKeys = new HashSet<>();

    public PendingBuffer(Path dataDir, int capacity, EvictionLog evictionLog,
                         QuarantineStore quarantineStore) {
        this.capacity = capacity;
        this.evictionLog = evictionLog;
        this.quarantineStore = quarantineStore;
        this.wal = new Journal<>(dataDir.resolve("pending.jsonl"), TelemetryRecord.class);
        loadFromWal();
    }

    private void loadFromWal() {
        boolean cleanedOrphans = false;
        for (TelemetryRecord record : wal.readAll()) {
            if (quarantineStore != null && quarantineStore.containsKey(record.idempotencyKey())) {
                // 崩溃窗口恢复：隔离已落盘但缓冲 WAL 尚未重写，该记录不得再次补传。
                cleanedOrphans = true;
                continue;
            }
            if (pendingKeys.add(record.idempotencyKey())) {
                queue.addLast(record);
            }
        }
        if (cleanedOrphans) {
            rewriteWal();
        }
    }

    /**
     * @return 入队结果；若因重复已在缓冲中则返回 false（幂等忽略）。
     */
    public synchronized boolean offer(TelemetryRecord record) {
        String key = record.idempotencyKey();
        if (pendingKeys.contains(key)) {
            return false;
        }
        while (queue.size() >= capacity) {
            TelemetryRecord victim = queue.pollFirst();
            if (victim == null) {
                break;
            }
            pendingKeys.remove(victim.idempotencyKey());
            evictionLog.record(new EvictionEntry(System.currentTimeMillis(),
                    EvictionEntry.OLDEST_FIRST, victim));
            rewriteWal();
        }
        queue.addLast(record);
        pendingKeys.add(key);
        wal.append(record);
        return true;
    }

    /** 查看队头但不出队（两阶段确认的第一阶段）。 */
    public synchronized TelemetryRecord peek() {
        return queue.peekFirst();
    }

    /** 前缀内单条结果：quarantined=true 表示该条被云端永久拒收（须已进入隔离区）。 */
    public record PrefixResult(String idempotencyKey, boolean quarantined) {
    }

    /**
     * 查看队头连续至多 {@code maxBatchSize} 条（快照副本，顺序即缓冲先后顺序）。
     * 批边界只是发送窗口，不改变队列，自然不会打乱任何设备的数据顺序。
     */
    public synchronized List<TelemetryRecord> peekBatch(int maxBatchSize) {
        int n = Math.min(maxBatchSize, queue.size());
        List<TelemetryRecord> batch = new ArrayList<>(n);
        java.util.Iterator<TelemetryRecord> it = queue.iterator();
        for (int i = 0; i < n; i++) {
            batch.add(it.next());
        }
        return batch;
    }

    /**
     * 顺序确认/隔离移除一个队头前缀。results 必须与当前队头逐条对应：
     * key 匹配且（若为隔离）隔离区已持久化，才移除该队头；第一条不匹配立即停止。
     * 因此重复回执、过期回执、ACK 丢失导致的结果缺口都不会误删任何数据。
     *
     * @return 实际出队条数（含确认接收与已隔离移除）
     */
    public synchronized int resolvePrefix(List<PrefixResult> results) {
        int resolved = 0;
        for (PrefixResult result : results) {
            TelemetryRecord head = queue.peekFirst();
            if (head == null || !head.idempotencyKey().equals(result.idempotencyKey())) {
                break;
            }
            if (result.quarantined()
                    && (quarantineStore == null
                    || !quarantineStore.containsKey(result.idempotencyKey()))) {
                // 隔离记录尚未持久化，绝不能先把数据移出缓冲（先隔离后出队的顺序约束）。
                break;
            }
            queue.pollFirst();
            pendingKeys.remove(result.idempotencyKey());
            resolved++;
        }
        if (resolved > 0) {
            rewriteWal();
        }
        return resolved;
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
        rewriteWal();
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

    private void rewriteWal() {
        wal.rewrite(new ArrayList<>(queue));
    }
}
