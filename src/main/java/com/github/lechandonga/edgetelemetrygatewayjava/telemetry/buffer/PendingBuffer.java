package com.github.lechandonga.edgetelemetrygatewayjava.telemetry.buffer;

import com.github.lechandonga.edgetelemetrygatewayjava.telemetry.model.TelemetryRecord;
import com.github.lechandonga.edgetelemetrygatewayjava.telemetry.storage.EvictionEntry;
import com.github.lechandonga.edgetelemetrygatewayjava.telemetry.storage.EvictionLog;
import com.github.lechandonga.edgetelemetrygatewayjava.telemetry.storage.Journal;

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
 */
public class PendingBuffer {

    private final int capacity;
    private final Journal<TelemetryRecord> wal;
    private final EvictionLog evictionLog;

    private final Deque<TelemetryRecord> queue = new ArrayDeque<>();
    private final Set<String> pendingKeys = new HashSet<>();

    public PendingBuffer(Path dataDir, int capacity, EvictionLog evictionLog) {
        this.capacity = capacity;
        this.evictionLog = evictionLog;
        this.wal = new Journal<>(dataDir.resolve("pending.jsonl"), TelemetryRecord.class);
        loadFromWal();
    }

    private void loadFromWal() {
        for (TelemetryRecord record : wal.readAll()) {
            if (pendingKeys.add(record.idempotencyKey())) {
                queue.addLast(record);
            }
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
