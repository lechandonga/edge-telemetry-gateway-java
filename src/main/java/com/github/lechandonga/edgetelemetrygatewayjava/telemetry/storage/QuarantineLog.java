package com.github.lechandonga.edgetelemetrygatewayjava.telemetry.storage;

import com.github.lechandonga.edgetelemetrygatewayjava.telemetry.model.CloudRejectCode;
import com.github.lechandonga.edgetelemetrygatewayjava.telemetry.model.TelemetryRecord;

import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * 永久拒收隔离区（quarantine.jsonl，仅追加 JSONL，与 WAL/归档同目录）。
 *
 * - 可查询：{@link #all()} 返回全部隔离记录，可按 {@link CloudRejectCode} 分类统计；
 * - 可对账：以幂等键去重，重发后的重复拒收不重复计数；
 * - 跨重启：纯追加文件，重启后内容原样可查。
 */
public class QuarantineLog {

    private final Journal<QuarantineEntry> journal;
    private final Set<String> keys = new HashSet<>();

    public QuarantineLog(Path dataDir) {
        this.journal = new Journal<>(dataDir.resolve("quarantine.jsonl"), QuarantineEntry.class);
        for (QuarantineEntry entry : journal.readAll()) {
            keys.add(entry.idempotencyKey());
        }
    }

    /**
     * 隔离一条永久拒收记录。
     *
     * @return true 表示新记账；false 表示该键已在隔离区（重发后的重复结论，幂等忽略）。
     */
    public synchronized boolean record(TelemetryRecord record, CloudRejectCode reason, String detail) {
        String key = record.idempotencyKey();
        if (!keys.add(key)) {
            return false;
        }
        journal.append(new QuarantineEntry(key, System.currentTimeMillis(), reason, detail, record));
        return true;
    }

    /** 隔离记录（磁盘顺序的快照副本）。 */
    public synchronized List<QuarantineEntry> all() {
        return journal.readAll();
    }

    public synchronized int size() {
        return keys.size();
    }

    public synchronized boolean containsKey(String key) {
        return keys.contains(key);
    }
}
