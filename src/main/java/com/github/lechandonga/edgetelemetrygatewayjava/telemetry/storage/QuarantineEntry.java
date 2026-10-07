package com.github.lechandonga.edgetelemetrygatewayjava.telemetry.storage;

import com.github.lechandonga.edgetelemetrygatewayjava.telemetry.model.CloudRejectReason;
import com.github.lechandonga.edgetelemetrygatewayjava.telemetry.model.TelemetryRecord;

/**
 * 隔离区记录：被云端“永久拒收”的、已被网关正常接收入链的数据。
 *
 * 这类记录不会在补传队头无限重试（避免堵死后续正常数据），
 * 也不会静默消失——完整记录、分类原因与隔离时间持久化到隔离区可查询。
 *
 * @param quarantinedAt 隔离时间 epoch millis
 * @param reason        云端永久拒收原因分类
 * @param detail        云端给出的说明（可空）
 * @param record        完整遥测记录
 */
public record QuarantineEntry(
        long quarantinedAt,
        CloudRejectReason reason,
        String detail,
        TelemetryRecord record
) {
    public String idempotencyKey() {
        return record.idempotencyKey();
    }
}
