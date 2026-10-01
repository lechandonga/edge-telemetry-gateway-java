package com.github.lechandonga.edgetelemetrygatewayjava.telemetry.model;

/**
 * 数据质量等级。GOOD 为正常采样；DEGRADED 表示数据可用但可信度降低，
 * 降级原因记录在 {@link TelemetryRecord#degradeReasons()} 中。
 */
public enum Quality {
    GOOD,
    DEGRADED
}
