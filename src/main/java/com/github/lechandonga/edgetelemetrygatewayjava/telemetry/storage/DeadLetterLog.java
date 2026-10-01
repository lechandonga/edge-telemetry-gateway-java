package com.github.lechandonga.edgetelemetrygatewayjava.telemetry.storage;

import java.nio.file.Path;
import java.util.List;

import com.github.lechandonga.edgetelemetrygatewayjava.telemetry.model.RejectReason;

/**
 * 拒收死信日志（dead-letter），查询接口供运维核查数据去向。
 */
public class DeadLetterLog {

    private final Journal<RejectedEntry> journal;

    public DeadLetterLog(Path dataDir) {
        this.journal = new Journal<>(dataDir.resolve("dead-letter.jsonl"), RejectedEntry.class);
    }

    public void record(String source, String rawPayload, RejectReason reason, String detail) {
        journal.append(new RejectedEntry(System.currentTimeMillis(), source, rawPayload, reason, detail));
    }

    public List<RejectedEntry> all() {
        return journal.readAll();
    }
}
