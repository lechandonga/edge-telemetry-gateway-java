package com.github.lechandonga.edgetelemetrygatewayjava.telemetry.alert;

/**
 * 阈值规则。判定：operator 作用于 (value, threshold) 成立即“越限”；
 * 越限必须持续至少 durationMillis 才产生 FIRE 告警。
 */
public record ThresholdRule(
        String ruleId,
        String deviceId,
        String metric,
        Operator operator,
        double threshold,
        long durationMillis
) {
    public enum Operator {
        GT, LT, GE, LE;

        public boolean violated(double value, double threshold) {
            return switch (this) {
                case GT -> value > threshold;
                case LT -> value < threshold;
                case GE -> value >= threshold;
                case LE -> value <= threshold;
            };
        }
    }

    public String stateKey() {
        return ruleId + "|" + deviceId + "|" + metric;
    }
}
