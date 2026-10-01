package com.github.lechandonga.edgetelemetrygatewayjava.model;

/** 告警事件。eventKey 用于跨重启/重放去重，保证不重复告警。 */
public record AlertEvent(
        String eventKey,
        String deviceId,
        String metric,
        int ruleVersion,
        long windowStartMs,
        long firedAtMs,
        double maxValueInWindow,
        long firstRecordId,
        long fireRecordId
) {
    public static String key(String deviceId, String metric, int ruleVersion, long windowStartMs) {
        return deviceId + "|" + metric + "|v" + ruleVersion + "|" + windowStartMs;
    }
}
