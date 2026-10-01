package com.github.lechandonga.edgetelemetrygatewayjava.model;

/**
 * 归一化后的遥测记录。寄存器采样与 JSON 报文最终都转换为该模型。
 * recordId 由缓冲层分配，是全链路去重的唯一标识；deviceSeq 由归一化层按设备单调分配，
 * 用于保证同一设备的判定顺序稳定。
 */
public record TelemetryRecord(
        long recordId,
        String deviceId,
        String metric,
        double value,
        long timestampMs,
        long deviceSeq,
        SourceType sourceType,
        Quality quality
) {
    public TelemetryRecord withRecordId(long id) {
        return new TelemetryRecord(id, deviceId, metric, value, timestampMs, deviceSeq, sourceType, quality);
    }
}
