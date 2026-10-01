package com.github.lechandonga.edgetelemetrygatewayjava.telemetry.model;

import java.util.List;

/**
 * 归一化结果。accepted 与 rejected/reason 互斥；降级记录 accepted=true 且 quality=DEGRADED。
 */
public record IngestResult(
        boolean accepted,
        TelemetryRecord record,
        RejectReason reason,
        String detail,
        List<String> degradeReasons
) {
    public static IngestResult ok(TelemetryRecord record) {
        return new IngestResult(true, record, null, null,
                record.quality() == Quality.DEGRADED ? record.degradeReasons() : List.of());
    }

    public static IngestResult reject(RejectReason reason, String detail) {
        return new IngestResult(false, null, reason, detail, List.of());
    }
}
