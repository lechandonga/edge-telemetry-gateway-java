package com.github.lechandonga.edgetelemetrygatewayjava.buffer;

import com.github.lechandonga.edgetelemetrygatewayjava.model.TelemetryRecord;

/** 淘汰流水：缓冲写满时被确定地（FIFO）移出的记录。 */
public record EvictionEntry(
        long evictedAtMs,
        String reason,
        TelemetryRecord record
) {
    public String toLogLine() {
        TelemetryRecord r = record;
        return evictedAtMs + "\t" + reason + "\trecordId=" + r.recordId()
                + "\tdevice=" + r.deviceId() + "\tmetric=" + r.metric()
                + "\tvalue=" + r.value() + "\tts=" + r.timestampMs();
    }
}
