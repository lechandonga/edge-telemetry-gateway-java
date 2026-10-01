package com.github.lechandonga.edgetelemetrygatewayjava.telemetry.storage;

import com.github.lechandonga.edgetelemetrygatewayjava.telemetry.model.RejectReason;

/**
 * 死信记录：被拒收的原始输入与分类原因，保证任何异常输入都可追溯。
 */
public record RejectedEntry(
        long rejectedAt,
        String source,
        String rawPayload,
        RejectReason reason,
        String detail
) {
}
