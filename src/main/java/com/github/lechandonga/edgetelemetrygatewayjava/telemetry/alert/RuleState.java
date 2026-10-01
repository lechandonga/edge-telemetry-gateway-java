package com.github.lechandonga.edgetelemetrygatewayjava.telemetry.alert;

/**
 * 单条规则的窗口状态（可持久化快照）。
 *
 * phase: NORMAL 正常；PENDING 已出现越限、持续时长尚未达标；FIRED 已告警未恢复。
 */
public class RuleState {
    public String phase = "NORMAL";
    public String ruleVersion;
    public long windowStart;
    public long lastSeenTimestamp;
    public double lastValue;

    public RuleState() {
    }

    public RuleState(String phase, String ruleVersion, long windowStart,
                     long lastSeenTimestamp, double lastValue) {
        this.phase = phase;
        this.ruleVersion = ruleVersion;
        this.windowStart = windowStart;
        this.lastSeenTimestamp = lastSeenTimestamp;
        this.lastValue = lastValue;
    }

    public static final String NORMAL = "NORMAL";
    public static final String PENDING = "PENDING";
    public static final String FIRED = "FIRED";
}
