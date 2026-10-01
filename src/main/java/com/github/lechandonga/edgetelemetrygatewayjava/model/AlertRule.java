package com.github.lechandonga.edgetelemetrygatewayjava.model;

/**
 * 阈值告警规则。value 持续满足 gtThreshold（> 阈值）达到 durationMs 即触发。
 * 规则不可变；更新通过新增带新版本号的规则完成。
 */
public record AlertRule(
        String metric,
        double gtThreshold,
        long durationMs,
        int version,
        long createdAtMs
) {
    public boolean breached(double value) {
        return value > gtThreshold;
    }
}
