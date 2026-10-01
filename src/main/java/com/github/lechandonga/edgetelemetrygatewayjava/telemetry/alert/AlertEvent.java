package com.github.lechandonga.edgetelemetrygatewayjava.telemetry.alert;

/**
 * 告警事件。FIRE=越限持续达标；RECOVER=FIRED 后出现恢复采样。
 * 事件携带产生它的规则版本，结论可解释、可复现。
 */
public record AlertEvent(
        String type,
        String ruleId,
        String ruleVersion,
        String deviceId,
        String metric,
        double value,
        long eventTimestamp,
        long violationStart,
        long durationMillis,
        String message
) {
    public static final String FIRE = "FIRE";
    public static final String RECOVER = "RECOVER";
}
