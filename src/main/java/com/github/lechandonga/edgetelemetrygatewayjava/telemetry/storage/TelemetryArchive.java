package com.github.lechandonga.edgetelemetrygatewayjava.telemetry.storage;

import com.github.lechandonga.edgetelemetrygatewayjava.telemetry.model.TelemetryRecord;

import java.nio.file.Path;
import java.util.List;

/**
 * 已接受遥测数据的仅追加归档。用途：
 * 1) 规则更新后用指定版本规则对同一段历史数据确定性重放；
 * 2) 重启后重建告警判定状态。
 */
public class TelemetryArchive {

    private final Journal<TelemetryRecord> journal;

    public TelemetryArchive(Path dataDir) {
        this.journal = new Journal<>(dataDir.resolve("telemetry-archive.jsonl"), TelemetryRecord.class);
    }

    public void append(TelemetryRecord record) {
        journal.append(record);
    }

    public List<TelemetryRecord> all() {
        return journal.readAll();
    }
}
