package com.github.lechandonga.edgetelemetrygatewayjava.telemetry.storage;

import com.github.lechandonga.edgetelemetrygatewayjava.telemetry.model.CloudRejectCode;
import com.github.lechandonga.edgetelemetrygatewayjava.telemetry.model.TelemetryRecord;

/**
 * 被云端永久拒收、移入隔离区的记录。
 *
 * 隔离不是丢弃：记录原文、云端分类原因、隔离时间都落盘可查；
 * 同一幂等键只保留一份（重发导致的重复拒收结论不重复记账）。
 */
public record QuarantineEntry(
        String idempotencyKey,
        long quarantinedAt,
        CloudRejectCode reason,
        String detail,
        TelemetryRecord record
) {
}
