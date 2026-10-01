package com.github.lechandonga.edgetelemetrygatewayjava.model;

import java.util.Optional;

/**
 * 归一化结果：要么接受（可能降级），要么拒收。绝不静默丢弃。
 */
public record IngestResult(
        Status status,
        TelemetryRecord record,
        RejectReason reason,
        String detail
) {
    public enum Status { ACCEPTED, DEGRADED, REJECTED }

    public static IngestResult accepted(TelemetryRecord r) {
        return new IngestResult(Status.ACCEPTED, r, null, "accepted");
    }

    public static IngestResult degraded(TelemetryRecord r, String detail) {
        return new IngestResult(Status.DEGRADED, r, null, detail);
    }

    public static IngestResult rejected(RejectReason reason, String detail) {
        return new IngestResult(Status.REJECTED, null, reason, detail);
    }

    public Optional<TelemetryRecord> acceptedRecord() {
        return status == Status.REJECTED ? Optional.empty() : Optional.ofNullable(record);
    }
}
