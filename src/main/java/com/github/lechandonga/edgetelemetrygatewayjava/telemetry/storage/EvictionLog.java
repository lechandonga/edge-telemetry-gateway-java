package com.github.lechandonga.edgetelemetrygatewayjava.telemetry.storage;

import java.nio.file.Path;
import java.util.List;

/**
 * 缓冲淘汰日志：每条因写满而被丢弃的数据都有据可查。
 */
public class EvictionLog {

    private final Journal<EvictionEntry> journal;

    public EvictionLog(Path dataDir) {
        this.journal = new Journal<>(dataDir.resolve("evictions.jsonl"), EvictionEntry.class);
    }

    public void record(EvictionEntry entry) {
        journal.append(entry);
    }

    public List<EvictionEntry> all() {
        return journal.readAll();
    }
}
