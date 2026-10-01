package com.github.lechandonga.edgetelemetrygatewayjava.telemetry.storage;

import com.github.lechandonga.edgetelemetrygatewayjava.telemetry.model.TelemetryRecord;

/**
 * 缓冲淘汰记录。淘汰规则为：缓冲写满时丢弃最旧记录（OLDEST_FIRST，
 * 全局 FIFO），并把被淘汰数据写入本日志——淘汰可查询、占用有上界。
 */
public record EvictionEntry(
        long evictedAt,
        String policy,
        TelemetryRecord record
) {
    public static final String OLDEST_FIRST = "OLDEST_FIRST";
}
