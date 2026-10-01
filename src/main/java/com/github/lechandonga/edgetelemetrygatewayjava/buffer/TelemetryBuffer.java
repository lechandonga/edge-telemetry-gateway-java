package com.github.lechandonga.edgetelemetrygatewayjava.buffer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.lechandonga.edgetelemetrygatewayjava.model.TelemetryRecord;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 本地有界持久缓冲。
 *
 * 语义：
 * - recordId 全局单调，append 时分配；补传/确认均以 recordId 去重。
 * - 容量写满时按 FIFO（最小 recordId，即最早进入）确定淘汰，淘汰记录写入 evictions.log，可查询。
 * - 每次变更后写原子快照（tmp + move），进程重启后完整恢复 pending 与计数，占用永不超过 capacity。
 * - confirm 幂等：重复确认返回 {@link AckOutcome#DUPLICATE_ACK}，不报错、不重复。
 */
public class TelemetryBuffer {

    public static final String EVICTION_POLICY = "EVICT_OLDEST_FIFO";

    private record Snapshot(
            long nextRecordId,
            List<TelemetryRecord> pending,
            long totalAccepted,
            long totalAcked,
            long totalEvicted,
            long lastEvictedRecordId
    ) {}

    private final Path stateFile;
    private final Path evictionLog;
    private final int capacity;
    private final ObjectMapper mapper = new ObjectMapper();
    private final Object lock = new Object();

    private final LinkedHashMap<Long, TelemetryRecord> pending = new LinkedHashMap<>();
    private long nextRecordId;
    private long totalAccepted;
    private long totalAcked;
    private long totalEvicted;
    private long lastEvictedRecordId = -1L;

    public TelemetryBuffer(Path stateDir, int capacity) {
        this.capacity = capacity;
        try {
            Files.createDirectories(stateDir);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        this.stateFile = stateDir.resolve("buffer-state.json");
        this.evictionLog = stateDir.resolve("evictions.log");
        load();
    }

    /** 追加一条记录（recordId 入参为 -1，由缓冲分配），写满时先做 FIFO 淘汰。 */
    public TelemetryRecord append(TelemetryRecord incoming) {
        synchronized (lock) {
            TelemetryRecord stored = incoming.withRecordId(nextRecordId++);
            List<EvictionEntry> evictedNow = new ArrayList<>();
            while (pending.size() >= capacity) {
                Map.Entry<Long, TelemetryRecord> oldest =
                        pending.entrySet().iterator().next();
                pending.remove(oldest.getKey());
                totalEvicted++;
                lastEvictedRecordId = oldest.getKey();
                evictedNow.add(new EvictionEntry(System.currentTimeMillis(),
                        EVICTION_POLICY + ":buffer_full", oldest.getValue()));
            }
            pending.put(stored.recordId(), stored);
            totalAccepted++;
            persist(evictedNow);
            return stored;
        }
    }

    /** 按 recordId 升序返回待补传快照。 */
    public List<TelemetryRecord> pendingRecords() {
        synchronized (lock) {
            return new ArrayList<>(pending.values());
        }
    }

    /** 幂等确认。 */
    public AckOutcome confirm(long recordId) {
        synchronized (lock) {
            TelemetryRecord removed = pending.remove(recordId);
            if (removed != null) {
                totalAcked++;
                persist(List.of());
                return AckOutcome.ACKED;
            }
            if (recordId < nextRecordId) {
                return AckOutcome.DUPLICATE_ACK;
            }
            return AckOutcome.UNKNOWN_ID;
        }
    }

    public BufferStats stats() {
        synchronized (lock) {
            return new BufferStats(capacity, pending.size(), totalAccepted, totalAcked,
                    totalEvicted, lastEvictedRecordId, EVICTION_POLICY);
        }
    }

    /** 读取淘汰流水（含历史与本次运行）。 */
    public List<String> evictionJournal() {
        try {
            if (!Files.exists(evictionLog)) {
                return List.of();
            }
            return Files.readAllLines(evictionLog, StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void load() {
        if (!Files.exists(stateFile)) {
            nextRecordId = 1;
            return;
        }
        try {
            Snapshot snapshot = mapper.readValue(Files.readString(stateFile, StandardCharsets.UTF_8),
                    Snapshot.class);
            nextRecordId = snapshot.nextRecordId();
            totalAccepted = snapshot.totalAccepted();
            totalAcked = snapshot.totalAcked();
            totalEvicted = snapshot.totalEvicted();
            lastEvictedRecordId = snapshot.lastEvictedRecordId();
            for (TelemetryRecord r : snapshot.pending()) {
                pending.put(r.recordId(), r);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("failed to load buffer state: " + stateFile, e);
        }
    }

    private void persist(List<EvictionEntry> evictedNow) {
        Snapshot snapshot = new Snapshot(nextRecordId, new ArrayList<>(pending.values()),
                totalAccepted, totalAcked, totalEvicted, lastEvictedRecordId);
        try {
            Path tmp = stateFile.resolveSibling("buffer-state.json.tmp");
            Files.writeString(tmp, mapper.writerWithDefaultPrettyPrinter()
                    .writeValueAsString(snapshot), StandardCharsets.UTF_8);
            Files.move(tmp, stateFile, StandardCopyOption.REPLACE_EXISTING,
                    StandardCopyOption.ATOMIC_MOVE);
            if (!evictedNow.isEmpty()) {
                List<String> lines = evictedNow.stream().map(EvictionEntry::toLogLine).toList();
                Files.write(evictionLog, lines, StandardCharsets.UTF_8,
                        StandardOpenOption.CREATE, StandardOpenOption.APPEND);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("failed to persist buffer state", e);
        }
    }
}
