package com.github.lechandonga.edgetelemetrygatewayjava.buffer;

/** 缓冲占用与淘汰统计，可随时查询。 */
public record BufferStats(
        int capacity,
        int pendingCount,
        long totalAccepted,
        long totalAcked,
        long totalEvicted,
        long lastEvictedRecordId,
        String evictionPolicy
) {
}
