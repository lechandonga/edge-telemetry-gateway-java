package com.github.lechandonga.edgetelemetrygatewayjava.alert;

/**
 * 单个 (device, metric) 的窗口状态，持久化以跨重启保持。
 * 窗口与其规则版本绑定：规则更新时已开窗的判定继续用旧版本直至触发或复位。
 */
public record AlertState(
        String deviceId,
        String metric,
        int ruleVersion,
        boolean active,
        long windowStartMs,
        double maxValue,
        long firstRecordId,
        boolean fired
) {
    public static AlertState empty(String deviceId, String metric) {
        return new AlertState(deviceId, metric, -1, false, 0L, 0d, -1L, false);
    }

    public AlertState with(int ruleVersion, boolean active, long windowStartMs,
                          double maxValue, long firstRecordId, boolean fired) {
        return new AlertState(deviceId, metric, ruleVersion, active, windowStartMs,
                maxValue, firstRecordId, fired);
    }
}
